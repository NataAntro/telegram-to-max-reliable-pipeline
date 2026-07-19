from __future__ import annotations

import asyncio
from pathlib import Path

import pytest

from reliable_pipeline.adapters import (
    DestinationAdapter,
    InMemoryDestinationAdapter,
    InMemorySourceAdapter,
    SourceAdapter,
)
from reliable_pipeline.domain import (
    ImportJob,
    ImportRequest,
    JobState,
    PreparedItem,
    PublicationReceipt,
    SourceItem,
)
from reliable_pipeline.pool import ResourcePool
from reliable_pipeline.service import ImportService
from reliable_pipeline.store import SqlAlchemyJobStore


class CancelAfterSaveStore:
    def __init__(self, delegate: SqlAlchemyJobStore, save_number: int) -> None:
        self.delegate = delegate
        self.save_number = save_number
        self.save_calls = 0
        self.job_id: str | None = None

    async def get_or_create(
        self, request: ImportRequest, idempotency_key: str
    ) -> tuple[ImportJob, bool]:
        job, created = await self.delegate.get_or_create(request, idempotency_key)
        self.job_id = job.id
        return job, created

    async def get(self, job_id: str) -> ImportJob | None:
        return await self.delegate.get(job_id)

    async def save(self, job: ImportJob) -> None:
        await self.delegate.save(job)
        self.save_calls += 1
        if self.save_calls == self.save_number:
            raise asyncio.CancelledError

    async def snapshot(self, job_id: str) -> ImportJob | None:
        return await self.delegate.snapshot(job_id)


class FailAfterFirstDestination:
    def __init__(self) -> None:
        self.publish_calls = 0

    async def publish(
        self,
        destination_ref: str,
        item: PreparedItem,
        *,
        idempotency_token: str,
    ) -> PublicationReceipt:
        del destination_ref, idempotency_token
        self.publish_calls += 1
        if self.publish_calls > 1:
            raise ConnectionError("destination is temporarily unavailable")
        return PublicationReceipt(item.external_id, "publication-1")


def make_service(
    source: InMemorySourceAdapter,
    destination: InMemoryDestinationAdapter,
    *,
    max_attempts: int = 2,
) -> ImportService:
    sources: dict[str, SourceAdapter] = {"source": source}
    destinations: dict[str, DestinationAdapter] = {"destination": destination}
    return ImportService(
        source_pool=ResourcePool(sources, max_concurrency=2, cooldown_seconds=0),
        destination_pool=ResourcePool(destinations, max_concurrency=2, cooldown_seconds=0),
        max_attempts=max_attempts,
    )


@pytest.mark.asyncio
async def test_concurrent_requests_share_idempotent_job() -> None:
    source = InMemorySourceAdapter([SourceItem("1", " import   text ")], delay_seconds=0.02)
    destination = InMemoryDestinationAdapter()
    service = make_service(source, destination)
    request = ImportRequest("source", "destination")

    jobs = await asyncio.gather(*(service.submit(request, "same-key") for _ in range(6)))

    assert {job.id for job in jobs} == {jobs[0].id}
    assert jobs[0].state is JobState.COMPLETED
    assert jobs[0].prepared_items[0].text == "import text"
    assert source.fetch_calls == 1
    assert destination.publish_calls == 1


@pytest.mark.asyncio
async def test_job_recovers_from_partial_destination_failure_without_refetch() -> None:
    source = InMemorySourceAdapter([SourceItem("1", "one"), SourceItem("2", "two")])
    destination = InMemoryDestinationAdapter(failures_before_success=1)
    service = make_service(source, destination, max_attempts=1)

    first = await service.submit(ImportRequest("source", "destination"), "recoverable")
    assert first.state is JobState.NEEDS_ATTENTION
    assert first.prepared_items
    assert first.receipts == {}

    recovered = await service.run(first.id)
    assert recovered.state is JobState.COMPLETED
    assert set(recovered.receipts) == {"1", "2"}
    assert source.fetch_calls == 1
    assert JobState.PUBLISHING in [item.to_state for item in recovered.transitions]


@pytest.mark.asyncio
async def test_source_failover_uses_second_adapter() -> None:
    failed = InMemorySourceAdapter([], failures_before_success=2)
    healthy = InMemorySourceAdapter([SourceItem("1", "safe fixture")])
    sources: dict[str, SourceAdapter] = {"a-failed": failed, "b-healthy": healthy}
    destination = InMemoryDestinationAdapter()
    destinations: dict[str, DestinationAdapter] = {"destination": destination}
    service = ImportService(
        source_pool=ResourcePool(sources, max_concurrency=1, cooldown_seconds=1),
        destination_pool=ResourcePool(destinations, max_concurrency=1),
    )

    job = await service.submit(ImportRequest("source", "destination"), "failover")

    assert job.state is JobState.COMPLETED
    assert failed.fetch_calls == 1
    assert healthy.fetch_calls == 1


@pytest.mark.asyncio
async def test_sql_checkpoint_recovers_after_process_restart(tmp_path: Path) -> None:
    database_path = str(tmp_path / "jobs.sqlite")
    database_url = f"sqlite+aiosqlite:///{database_path}"
    first_store = SqlAlchemyJobStore.from_url(database_url)
    await first_store.initialize()

    source = InMemorySourceAdapter([SourceItem("1", "one"), SourceItem("2", "two")])
    first_destination = FailAfterFirstDestination()
    first_service = ImportService(
        source_pool=ResourcePool({"source": source}, max_concurrency=1),
        destination_pool=ResourcePool(
            {"destination": first_destination}, max_concurrency=1, cooldown_seconds=0
        ),
        store=first_store,
        max_attempts=1,
    )

    interrupted = await first_service.submit(
        ImportRequest("source", "destination"), "durable-recovery"
    )
    assert interrupted.state is JobState.NEEDS_ATTENTION
    assert set(interrupted.receipts) == {"1"}
    await first_store.close()

    second_store = SqlAlchemyJobStore.from_url(database_url)
    second_source = InMemorySourceAdapter([])
    second_destination = InMemoryDestinationAdapter()
    second_service = ImportService(
        source_pool=ResourcePool({"source": second_source}, max_concurrency=1),
        destination_pool=ResourcePool({"destination": second_destination}, max_concurrency=1),
        store=second_store,
        max_attempts=1,
    )

    recovered = await second_service.run(interrupted.id)

    assert recovered.state is JobState.COMPLETED
    assert set(recovered.receipts) == {"1", "2"}
    assert second_source.fetch_calls == 0
    assert second_destination.publish_calls == 1
    await second_store.close()


@pytest.mark.parametrize("save_number", range(1, 9))
@pytest.mark.asyncio
async def test_every_durable_checkpoint_resumes_without_duplicate_effects(
    tmp_path: Path,
    save_number: int,
) -> None:
    database_url = f"sqlite+aiosqlite:///{tmp_path / f'checkpoint-{save_number}.sqlite'}"
    first_store = SqlAlchemyJobStore.from_url(database_url)
    await first_store.initialize()
    cancelling_store = CancelAfterSaveStore(first_store, save_number)
    items = [SourceItem("1", "one"), SourceItem("2", "two")]
    first_source = InMemorySourceAdapter(items)
    first_destination = InMemoryDestinationAdapter()
    first_service = ImportService(
        source_pool=ResourcePool({"source": first_source}, max_concurrency=1),
        destination_pool=ResourcePool({"destination": first_destination}, max_concurrency=1),
        store=cancelling_store,
        max_attempts=1,
    )

    with pytest.raises(asyncio.CancelledError):
        await first_service.submit(
            ImportRequest("source", "destination"),
            f"cancel-after-save-{save_number}",
        )
    assert cancelling_store.job_id is not None
    await first_store.close()

    second_store = SqlAlchemyJobStore.from_url(database_url)
    second_source = InMemorySourceAdapter(items)
    second_destination = InMemoryDestinationAdapter()
    second_service = ImportService(
        source_pool=ResourcePool({"source": second_source}, max_concurrency=1),
        destination_pool=ResourcePool({"destination": second_destination}, max_concurrency=1),
        store=second_store,
        max_attempts=1,
    )

    recovered = await second_service.run(cancelling_store.job_id)

    assert recovered.state is JobState.COMPLETED
    assert set(recovered.receipts) == {"1", "2"}
    assert first_source.fetch_calls + second_source.fetch_calls == 1
    assert first_destination.publish_calls + second_destination.publish_calls == 2
    await second_store.close()
