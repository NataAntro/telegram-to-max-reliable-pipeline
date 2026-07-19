from __future__ import annotations

import asyncio
import re
from collections import defaultdict

from reliable_pipeline.adapters import DestinationAdapter, SourceAdapter
from reliable_pipeline.domain import (
    ImportJob,
    ImportRequest,
    JobState,
    PreparedItem,
    PublicationReceipt,
)
from reliable_pipeline.pool import ResourcePool
from reliable_pipeline.store import InMemoryJobStore, JobStore


class ImportService:
    def __init__(
        self,
        *,
        source_pool: ResourcePool[SourceAdapter],
        destination_pool: ResourcePool[DestinationAdapter],
        store: JobStore | None = None,
        timeout_seconds: float = 0.25,
        max_attempts: int = 2,
    ) -> None:
        self.source_pool = source_pool
        self.destination_pool = destination_pool
        self.store = store or InMemoryJobStore()
        self.timeout_seconds = timeout_seconds
        self.max_attempts = max_attempts
        self._job_locks: defaultdict[str, asyncio.Lock] = defaultdict(asyncio.Lock)

    async def submit(self, request: ImportRequest, idempotency_key: str) -> ImportJob:
        normalized_key = idempotency_key.strip()
        if not normalized_key:
            raise ValueError("idempotency_key must not be empty")
        job, _ = await self.store.get_or_create(request, normalized_key)
        return await self.run(job.id)

    async def run(self, job_id: str) -> ImportJob:
        async with self._job_locks[job_id]:
            job = await self.store.get(job_id)
            if job is None:
                raise KeyError(job_id)
            recovery_plan = job.recovery_plan()
            if job.state is JobState.COMPLETED:
                return job
            if job.state is JobState.NEEDS_ATTENTION:
                job.transition_to(recovery_plan.resume_from)
                await self.store.save(job)

            try:
                await self._advance(job)
            except Exception as error:
                job.mark_failed(error)
                await self.store.save(job)
            return job

    async def _advance(self, job: ImportJob) -> None:
        if job.state is JobState.RECEIVED:
            job.transition_to(JobState.FETCHING)
            await self.store.save(job)

        if job.state is JobState.FETCHING:
            if not job.raw_items:
                job.raw_items = await self.source_pool.execute(
                    lambda adapter: adapter.fetch(job.request.source_ref, job.request.limit),
                    timeout_seconds=self.timeout_seconds,
                    max_attempts=self.max_attempts,
                    serialization_key=job.request.source_ref,
                )
                await self.store.save(job)
            job.transition_to(JobState.TRANSFORMING)
            await self.store.save(job)

        if job.state is JobState.TRANSFORMING:
            if not job.prepared_items:
                job.prepared_items = [
                    self._prepare(item.external_id, item.text) for item in job.raw_items
                ]
                await self.store.save(job)
            job.transition_to(JobState.PUBLISHING)
            await self.store.save(job)

        if job.state is JobState.PUBLISHING:
            for item in job.prepared_items:
                if item.external_id in job.receipts:
                    continue

                async def publish(
                    adapter: DestinationAdapter,
                    current: PreparedItem = item,
                ) -> PublicationReceipt:
                    return await adapter.publish(
                        job.request.destination_ref,
                        current,
                        idempotency_token=f"{job.id}:{current.external_id}",
                    )

                receipt = await self.destination_pool.execute(
                    publish,
                    timeout_seconds=self.timeout_seconds,
                    max_attempts=self.max_attempts,
                    serialization_key=job.request.destination_ref,
                )
                job.receipts[item.external_id] = receipt
                await self.store.save(job)
            job.transition_to(JobState.COMPLETED)
            await self.store.save(job)

    @staticmethod
    def _prepare(external_id: str, text: str) -> PreparedItem:
        normalized_text = re.sub(r"\s+", " ", text).strip()
        return PreparedItem(external_id=external_id, text=normalized_text)
