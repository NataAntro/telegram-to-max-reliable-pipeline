from __future__ import annotations

import asyncio
from dataclasses import replace
from datetime import datetime
from typing import Any, Protocol, cast

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from reliable_pipeline.domain import (
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


class InMemoryJobStore:
    """In-memory repository with atomic idempotency-key allocation."""

    def __init__(self) -> None:
        self._jobs: dict[str, ImportJob] = {}
        self._job_ids_by_key: dict[str, str] = {}
        self._lock = asyncio.Lock()

    async def get_or_create(
        self,
        request: ImportRequest,
        idempotency_key: str,
    ) -> tuple[ImportJob, bool]:
        async with self._lock:
            existing_id = self._job_ids_by_key.get(idempotency_key)
            if existing_id is not None:
                return self._jobs[existing_id], False
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


class SqlAlchemyJobStore:
    """Durable JobStore adapter with atomic idempotency-key allocation."""

    def __init__(self, engine: AsyncEngine) -> None:
        self._engine = engine
        self._sessions = async_sessionmaker(engine, expire_on_commit=False)

    @classmethod
    def from_url(cls, url: str, *, echo: bool = False) -> SqlAlchemyJobStore:
        return cls(create_async_engine(url, echo=echo))

    async def initialize(self) -> None:
        async with self._engine.begin() as connection:
            await connection.run_sync(Base.metadata.create_all)

    async def close(self) -> None:
        await self._engine.dispose()

    async def get_or_create(
        self,
        request: ImportRequest,
        idempotency_key: str,
    ) -> tuple[ImportJob, bool]:
        async with self._sessions() as session:
            existing = await self._find_by_key(session, idempotency_key)
            if existing is not None:
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
            state=job.state.value,
            checkpoint=cls._checkpoint(job),
            last_error=job.last_error,
        )
