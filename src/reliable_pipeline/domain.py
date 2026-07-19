from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import StrEnum
from hashlib import sha256
from uuid import uuid4


def utc_now() -> datetime:
    return datetime.now(UTC)


class JobState(StrEnum):
    RECEIVED = "received"
    FETCHING = "fetching"
    TRANSFORMING = "transforming"
    PUBLISHING = "publishing"
    COMPLETED = "completed"
    NEEDS_ATTENTION = "needs_attention"


ALLOWED_TRANSITIONS: dict[JobState, set[JobState]] = {
    JobState.RECEIVED: {JobState.FETCHING},
    JobState.FETCHING: {JobState.TRANSFORMING, JobState.NEEDS_ATTENTION},
    JobState.TRANSFORMING: {JobState.PUBLISHING, JobState.NEEDS_ATTENTION},
    JobState.PUBLISHING: {JobState.COMPLETED, JobState.NEEDS_ATTENTION},
    JobState.NEEDS_ATTENTION: {
        JobState.FETCHING,
        JobState.TRANSFORMING,
        JobState.PUBLISHING,
    },
    JobState.COMPLETED: set(),
}


class InvalidTransitionError(ValueError):
    pass


class CheckpointCorruptedError(ValueError):
    pass


class IdempotencyConflictError(ValueError):
    pass


class JobLeaseLostError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class ImportRequest:
    source_ref: str
    destination_ref: str
    limit: int = 5

    @property
    def fingerprint(self) -> str:
        canonical = f"{self.source_ref}\0{self.destination_ref}\0{self.limit}"
        return sha256(canonical.encode()).hexdigest()


@dataclass(frozen=True, slots=True)
class SourceItem:
    external_id: str
    text: str


@dataclass(frozen=True, slots=True)
class PreparedItem:
    external_id: str
    text: str


@dataclass(frozen=True, slots=True)
class PublicationReceipt:
    external_id: str
    destination_id: str


@dataclass(frozen=True, slots=True)
class StateTransition:
    from_state: JobState
    to_state: JobState
    at: datetime = field(default_factory=utc_now)


@dataclass(frozen=True, slots=True)
class RecoveryPlan:
    resume_from: JobState
    fetched_count: int
    prepared_count: int
    published_ids: tuple[str, ...]
    pending_publication_ids: tuple[str, ...]


@dataclass(slots=True)
class ImportJob:
    request: ImportRequest
    idempotency_key: str
    id: str = field(default_factory=lambda: uuid4().hex)
    state: JobState = JobState.RECEIVED
    raw_items: list[SourceItem] = field(default_factory=list)
    prepared_items: list[PreparedItem] = field(default_factory=list)
    receipts: dict[str, PublicationReceipt] = field(default_factory=dict)
    transitions: list[StateTransition] = field(default_factory=list)
    last_error: str | None = None

    def transition_to(self, next_state: JobState) -> None:
        if next_state not in ALLOWED_TRANSITIONS[self.state]:
            raise InvalidTransitionError(f"{self.state} -> {next_state} is not allowed")
        self.transitions.append(StateTransition(self.state, next_state))
        self.state = next_state
        if next_state is not JobState.NEEDS_ATTENTION:
            self.last_error = None

    def mark_failed(self, error: BaseException) -> None:
        if self.state in {JobState.COMPLETED, JobState.NEEDS_ATTENTION}:
            return
        self.last_error = f"{type(error).__name__}: {error}"
        self.transition_to(JobState.NEEDS_ATTENTION)
        self.last_error = f"{type(error).__name__}: {error}"

    def recovery_state(self) -> JobState:
        return self.recovery_plan().resume_from

    def recovery_plan(self) -> RecoveryPlan:
        raw_ids = [item.external_id for item in self.raw_items]
        prepared_ids = [item.external_id for item in self.prepared_items]
        self._require_unique(raw_ids, "raw_items")
        self._require_unique(prepared_ids, "prepared_items")

        if prepared_ids and set(prepared_ids) != set(raw_ids):
            raise CheckpointCorruptedError("prepared_items must correspond exactly to raw_items")
        unknown_receipts = set(self.receipts) - set(prepared_ids)
        if unknown_receipts:
            raise CheckpointCorruptedError(
                f"receipts reference unknown prepared items: {sorted(unknown_receipts)}"
            )
        mismatched_receipts = [
            key for key, receipt in self.receipts.items() if receipt.external_id != key
        ]
        if mismatched_receipts:
            raise CheckpointCorruptedError(
                f"receipt external_id does not match its key: {sorted(mismatched_receipts)}"
            )
        if self.state is JobState.COMPLETED and set(self.receipts) != set(prepared_ids):
            raise CheckpointCorruptedError("completed job must contain every publication receipt")

        if self.prepared_items:
            resume_from = JobState.PUBLISHING
        elif self.raw_items:
            resume_from = JobState.TRANSFORMING
        else:
            resume_from = JobState.FETCHING

        return RecoveryPlan(
            resume_from=resume_from,
            fetched_count=len(raw_ids),
            prepared_count=len(prepared_ids),
            published_ids=tuple(item for item in prepared_ids if item in self.receipts),
            pending_publication_ids=tuple(
                item for item in prepared_ids if item not in self.receipts
            ),
        )

    @staticmethod
    def _require_unique(values: list[str], checkpoint: str) -> None:
        if len(values) != len(set(values)):
            raise CheckpointCorruptedError(f"{checkpoint} contains duplicate external_id")

    @classmethod
    def validate_source_items(cls, items: list[SourceItem]) -> None:
        cls._require_unique([item.external_id for item in items], "source response")
