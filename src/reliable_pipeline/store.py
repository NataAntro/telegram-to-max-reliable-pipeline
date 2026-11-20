from __future__ import annotations

import asyncio
from dataclasses import replace
from datetime import datetime, timedelta
from typing import Any, Protocol, cast

from sqlalchemy import or_, select, text, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from reliable_pipeline.domain import (
    IdempotencyConflictError,
    ImportJob,
    ImportRequest,
    JobState,
    PreparedItem,
    PublicationReceipt,
    SourceItem,
    StateTransition,
    utc_now,
)
from reliable_pipeline.models import Base, ImportJobRecord, PublicationRecord


class JobStore(Protocol):
    """Persistence port used by the import application service."""

    async def get_or_create(
        self,
        request: ImportRequest,
        idempotency_key: str,
    ) -> tuple[ImportJob, bool]: ...

    async def get(self, job_id: str) -> ImportJob | None: ...

    async def save(self, job: ImportJob) -> None: ...

    async def snapshot(self, job_id: str) -> ImportJob | None: ...

    async def acquire_lease(self, job_id: str, owner: str, ttl_seconds: float) -> bool: ...

    async def renew_lease(self, job_id: str, owner: str, ttl_seconds: float) -> bool: ...

    async def release_lease(self, job_id: str, owner: str) -> None: ...


class InMemoryJobStore:
    """In-memory repository with atomic idempotency-key allocation."""

    def __init__(self) -> None:
        self._jobs: dict[str, ImportJob] = {}
        self._job_ids_by_key: dict[str, str] = {}
        self._lock = asyncio.Lock()
        self._leases: dict[str, tuple[str, datetime]] = {}

    async def get_or_create(
        self,
        request: ImportRequest,
        idempotency_key: str,
    ) -> tuple[ImportJob, bool]:
        async with self._lock:
            existing_id = self._job_ids_by_key.get(idempotency_key)
            if existing_id is not None:
                existing = self._jobs[existing_id]
                self._require_same_request(existing.request, request)
                return existing, False
            job = ImportJob(request=request, idempotency_key=idempotency_key)
            self._jobs[job.id] = job
            self._job_ids_by_key[idempotency_key] = job.id
            return job, True

    async def get(self, job_id: str) -> ImportJob | None:
        async with self._lock:
            return self._jobs.get(job_id)

    async def save(self, job: ImportJob) -> None:
        async with self._lock:
            self._jobs[job.id] = job
            self._job_ids_by_key[job.idempotency_key] = job.id

    async def snapshot(self, job_id: str) -> ImportJob | None:
        async with self._lock:
            job = self._jobs.get(job_id)
            if job is None:
                return None
            return replace(
                job,
                raw_items=list(job.raw_items),
                prepared_items=list(job.prepared_items),
                receipts=dict(job.receipts),
                transitions=list(job.transitions),
            )

    async def acquire_lease(self, job_id: str, owner: str, ttl_seconds: float) -> bool:
        async with self._lock:
            now = utc_now()
            lease = self._leases.get(job_id)
            if lease is not None and lease[0] != owner and lease[1] > now:
                return False
            self._leases[job_id] = (owner, now + timedelta(seconds=ttl_seconds))
            return True

    async def renew_lease(self, job_id: str, owner: str, ttl_seconds: float) -> bool:
        async with self._lock:
            lease = self._leases.get(job_id)
            if lease is None or lease[0] != owner:
                return False
            self._leases[job_id] = (owner, utc_now() + timedelta(seconds=ttl_seconds))
            return True

    async def release_lease(self, job_id: str, owner: str) -> None:
        async with self._lock:
            lease = self._leases.get(job_id)
            if lease is not None and lease[0] == owner:
                self._leases.pop(job_id, None)

    @staticmethod
    def _require_same_request(existing: ImportRequest, requested: ImportRequest) -> None:
        if existing.fingerprint != requested.fingerprint:
            raise IdempotencyConflictError(
                "idempotency_key is already bound to a different import request"
            )


class SqlAlchemyJobStore:
    """Durable JobStore adapter with atomic idempotency-key allocation."""

    def __init__(self, engine: AsyncEngine) -> None:
        self._engine = engine
        self._sessions = async_sessionmaker(engine, expire_on_commit=False)

    @property
    def backend_name(self) -> str:
        return self._engine.url.get_backend_name()

    @classmethod
    def from_url(cls, url: str, *, echo: bool = False) -> SqlAlchemyJobStore:
        return cls(create_async_engine(url, echo=echo))

    async def initialize(self) -> None:
        async with self._engine.begin() as connection:
            await connection.run_sync(Base.metadata.create_all)

    async def close(self) -> None:
        await self._engine.dispose()

    async def ping(self) -> None:
        async with self._engine.connect() as connection:
            await connection.execute(text("SELECT 1"))

    async def get_or_create(
        self,
        request: ImportRequest,
        idempotency_key: str,
    ) -> tuple[ImportJob, bool]:
        async with self._sessions() as session:
            existing = await self._find_by_key(session, idempotency_key)
            if existing is not None:
                self._require_same_request(existing, request)
                return await self._hydrate(session, existing), False

            job = ImportJob(request=request, idempotency_key=idempotency_key)
            session.add(self._record_from_job(job))
            try:
                await session.commit()
            except IntegrityError:
                await session.rollback()
                existing = await self._find_by_key(session, idempotency_key)
                if existing is None:
                    raise
                self._require_same_request(existing, request)
                return await self._hydrate(session, existing), False
            return job, True

    async def get(self, job_id: str) -> ImportJob | None:
        async with self._sessions() as session:
            record = await session.get(ImportJobRecord, job_id)
            if record is None:
                return None
            return await self._hydrate(session, record)

    async def snapshot(self, job_id: str) -> ImportJob | None:
        return await self.get(job_id)

    async def save(self, job: ImportJob) -> None:
        async with self._sessions() as session, session.begin():
            record = await session.get(ImportJobRecord, job.id)
            if record is None:
                session.add(self._record_from_job(job))
            else:
                record.state = job.state.value
                record.checkpoint = self._checkpoint(job)
                record.last_error = job.last_error
                record.updated_at = utc_now()

            existing_receipts = {
                item.external_id
                for item in (
                    await session.scalars(
                        select(PublicationRecord).where(PublicationRecord.job_id == job.id)
                    )
                ).all()
            }
            for external_id, receipt in job.receipts.items():
                if external_id not in existing_receipts:
                    session.add(
                        PublicationRecord(
                            job_id=job.id,
                            external_id=external_id,
                            destination_id=receipt.destination_id,
                        )
                    )

    async def acquire_lease(self, job_id: str, owner: str, ttl_seconds: float) -> bool:
        now = utc_now()
        async with self._sessions() as session, session.begin():
            claimed_id = await session.scalar(
                update(ImportJobRecord)
                .where(
                    ImportJobRecord.id == job_id,
                    or_(
                        ImportJobRecord.lease_owner.is_(None),
                        ImportJobRecord.lease_expires_at < now,
                        ImportJobRecord.lease_owner == owner,
                    ),
                )
                .values(
                    lease_owner=owner,
                    lease_expires_at=now + timedelta(seconds=ttl_seconds),
                )
                .returning(ImportJobRecord.id)
            )
            return claimed_id is not None

    async def renew_lease(self, job_id: str, owner: str, ttl_seconds: float) -> bool:
        async with self._sessions() as session, session.begin():
            renewed_id = await session.scalar(
                update(ImportJobRecord)
                .where(
                    ImportJobRecord.id == job_id,
                    ImportJobRecord.lease_owner == owner,
                )
                .values(lease_expires_at=utc_now() + timedelta(seconds=ttl_seconds))
                .returning(ImportJobRecord.id)
            )
            return renewed_id is not None

    async def release_lease(self, job_id: str, owner: str) -> None:
        async with self._sessions() as session, session.begin():
            await session.execute(
                update(ImportJobRecord)
                .where(
                    ImportJobRecord.id == job_id,
                    ImportJobRecord.lease_owner == owner,
                )
                .values(lease_owner=None, lease_expires_at=None)
            )

    async def _find_by_key(
        self,
        session: AsyncSession,
        idempotency_key: str,
    ) -> ImportJobRecord | None:
        return cast(
            ImportJobRecord | None,
            await session.scalar(
                select(ImportJobRecord).where(ImportJobRecord.idempotency_key == idempotency_key)
            ),
        )

    @staticmethod
    def _require_same_request(record: ImportJobRecord, request: ImportRequest) -> None:
        if record.request_fingerprint != request.fingerprint:
            raise IdempotencyConflictError(
                "idempotency_key is already bound to a different import request"
            )

    async def _hydrate(self, session: AsyncSession, record: ImportJobRecord) -> ImportJob:
        checkpoint = cast(dict[str, Any], record.checkpoint or {})
        raw_items = cast(list[dict[str, str]], checkpoint.get("raw_items", []))
        prepared_items = cast(list[dict[str, str]], checkpoint.get("prepared_items", []))
        transitions = cast(list[dict[str, str]], checkpoint.get("transitions", []))
        job = ImportJob(
            id=record.id,
            idempotency_key=record.idempotency_key,
            request=ImportRequest(
                source_ref=record.source_ref,
                destination_ref=record.destination_ref,
                limit=int(checkpoint.get("limit", 5)),
            ),
            state=JobState(record.state),
            raw_items=[SourceItem(**item) for item in raw_items],
            prepared_items=[PreparedItem(**item) for item in prepared_items],
            transitions=[
                StateTransition(
                    from_state=JobState(item["from_state"]),
                    to_state=JobState(item["to_state"]),
                    at=datetime.fromisoformat(item["at"]),
                )
                for item in transitions
            ],
            last_error=record.last_error,
        )
        publications = (
            await session.scalars(
                select(PublicationRecord).where(PublicationRecord.job_id == record.id)
            )
        ).all()
        job.receipts = {
            item.external_id: PublicationReceipt(
                external_id=item.external_id,
                destination_id=item.destination_id,
            )
            for item in publications
        }
        return job

    @staticmethod
    def _checkpoint(job: ImportJob) -> dict[str, object]:
        return {
            "limit": job.request.limit,
            "raw_items": [
                {"external_id": item.external_id, "text": item.text} for item in job.raw_items
            ],
            "prepared_items": [
                {"external_id": item.external_id, "text": item.text} for item in job.prepared_items
            ],
            "transitions": [
                {
                    "from_state": item.from_state.value,
                    "to_state": item.to_state.value,
                    "at": item.at.isoformat(),
                }
                for item in job.transitions
            ],
        }

    @classmethod
    def _record_from_job(cls, job: ImportJob) -> ImportJobRecord:
        return ImportJobRecord(
            id=job.id,
            idempotency_key=job.idempotency_key,
            source_ref=job.request.source_ref,
            destination_ref=job.request.destination_ref,
            request_fingerprint=job.request.fingerprint,
            state=job.state.value,
            checkpoint=cls._checkpoint(job),
            last_error=job.last_error,
        )
