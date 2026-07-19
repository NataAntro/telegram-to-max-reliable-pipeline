import pytest

from reliable_pipeline.domain import (
    CheckpointCorruptedError,
    ImportJob,
    ImportRequest,
    InvalidTransitionError,
    JobState,
    PreparedItem,
    PublicationReceipt,
    SourceItem,
)


def test_state_machine_rejects_skipped_stage() -> None:
    job = ImportJob(ImportRequest("source", "destination"), "key")

    with pytest.raises(InvalidTransitionError):
        job.transition_to(JobState.PUBLISHING)


def test_recovery_state_uses_latest_checkpoint() -> None:
    job = ImportJob(ImportRequest("source", "destination"), "key")
    job.transition_to(JobState.FETCHING)
    job.mark_failed(ConnectionError("temporary failure"))

    assert job.state is JobState.NEEDS_ATTENTION
    assert job.recovery_state() is JobState.FETCHING


def test_recovery_plan_explains_partial_publication_checkpoint() -> None:
    job = ImportJob(ImportRequest("source", "destination"), "key")
    job.raw_items = [SourceItem("one", "1"), SourceItem("two", "2")]
    job.prepared_items = [PreparedItem("one", "1"), PreparedItem("two", "2")]
    job.receipts = {"one": PublicationReceipt("one", "published-one")}

    plan = job.recovery_plan()

    assert plan.resume_from is JobState.PUBLISHING
    assert plan.published_ids == ("one",)
    assert plan.pending_publication_ids == ("two",)


def test_recovery_plan_rejects_inconsistent_checkpoint() -> None:
    job = ImportJob(ImportRequest("source", "destination"), "key")
    job.raw_items = [SourceItem("duplicate", "1"), SourceItem("duplicate", "2")]

    with pytest.raises(CheckpointCorruptedError, match="duplicate external_id"):
        job.recovery_plan()


@pytest.mark.parametrize(
    ("prepared", "receipts", "message"),
    [
        ([PreparedItem("other", "1")], {}, "correspond exactly"),
        ([], {"ghost": PublicationReceipt("ghost", "published")}, "unknown prepared"),
    ],
)
def test_recovery_plan_rejects_cross_checkpoint_inconsistency(
    prepared: list[PreparedItem],
    receipts: dict[str, PublicationReceipt],
    message: str,
) -> None:
    job = ImportJob(ImportRequest("source", "destination"), "key")
    job.raw_items = [SourceItem("one", "1")]
    job.prepared_items = prepared
    job.receipts = receipts

    with pytest.raises(CheckpointCorruptedError, match=message):
        job.recovery_plan()
