from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI, Header, HTTPException, Request, status

from reliable_pipeline.config import RuntimeSettings
from reliable_pipeline.domain import IdempotencyConflictError, ImportJob, ImportRequest
from reliable_pipeline.factory import create_reference_service
from reliable_pipeline.schemas import (
    HealthBody,
    ImportJobBody,
    ImportRequestBody,
    ResourceHealthBody,
    ServiceStatusBody,
    TransitionBody,
)
from reliable_pipeline.service import ImportService
from reliable_pipeline.store import SqlAlchemyJobStore


def _serialize_job(job: ImportJob) -> ImportJobBody:
    recovery_plan = job.recovery_plan()
    return ImportJobBody(
        id=job.id,
        state=job.state.value,
        idempotency_key=job.idempotency_key,
        source_count=len(job.raw_items),
        published_count=len(job.receipts),
        pending_publication_count=len(recovery_plan.pending_publication_ids),
        resume_from=(
            recovery_plan.resume_from.value if job.state.value == "needs_attention" else None
        ),
        last_error=job.last_error,
        transitions=[
            TransitionBody(from_state=item.from_state.value, to_state=item.to_state.value)
            for item in job.transitions
        ],
    )


def _service(request: Request) -> ImportService:
    service = request.app.state.import_service
    if not isinstance(service, ImportService):
        raise RuntimeError("Import service is not configured")
    return service


def create_app(import_service: ImportService | None = None) -> FastAPI:
    owns_service = import_service is None
    service = import_service or create_reference_service(RuntimeSettings.from_environment())

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        store = service.store
        if isinstance(store, SqlAlchemyJobStore):
            await store.initialize()
        app.state.import_service = service
        try:
            yield
        finally:
            if owns_service and isinstance(store, SqlAlchemyJobStore):
                await store.close()

    app = FastAPI(
        title="Telegram to MAX Reliability Core",
        version="0.1.0",
        description="Resource pool и восстанавливаемая state machine для import jobs.",
        lifespan=lifespan,
    )
    app.state.import_service = service

    @app.get("/health/live", response_model=ServiceStatusBody)
    async def liveness() -> ServiceStatusBody:
        return ServiceStatusBody(status="ok", persistence="process")

    @app.get("/health/ready", response_model=ServiceStatusBody)
    async def readiness(request: Request) -> ServiceStatusBody:
        store = _service(request).store
        if isinstance(store, SqlAlchemyJobStore):
            try:
                await store.ping()
            except Exception as error:
                raise HTTPException(
                    status.HTTP_503_SERVICE_UNAVAILABLE,
                    "Persistent store is not ready",
                ) from error
            return ServiceStatusBody(status="ok", persistence=store.backend_name)
        return ServiceStatusBody(status="ok", persistence="memory")

    @app.post("/imports", response_model=ImportJobBody)
    async def submit_import(
        body: ImportRequestBody,
        request: Request,
        idempotency_key: str = Header(alias="Idempotency-Key", min_length=1, max_length=128),
    ) -> ImportJobBody:
        try:
            job = await _service(request).submit(
                ImportRequest(body.source_ref, body.destination_ref, body.limit),
                idempotency_key,
            )
        except IdempotencyConflictError as error:
            raise HTTPException(status.HTTP_409_CONFLICT, str(error)) from error
        return _serialize_job(job)

    @app.post("/imports/{job_id}/retry", response_model=ImportJobBody)
    async def retry_import(job_id: str, request: Request) -> ImportJobBody:
        try:
            job = await _service(request).run(job_id)
        except KeyError as error:
            raise HTTPException(status.HTTP_404_NOT_FOUND, "Import job not found") from error
        return _serialize_job(job)

    @app.get("/imports/{job_id}", response_model=ImportJobBody)
    async def get_import(job_id: str, request: Request) -> ImportJobBody:
        job = await _service(request).store.snapshot(job_id)
        if job is None:
            raise HTTPException(status.HTTP_404_NOT_FOUND, "Import job not found")
        return _serialize_job(job)

    @app.get("/health/resources", response_model=HealthBody)
    async def resource_health(request: Request) -> HealthBody:
        service = _service(request)
        return HealthBody(
            source=[
                ResourceHealthBody.model_validate(item, from_attributes=True)
                for item in service.source_pool.health_snapshot()
            ],
            destination=[
                ResourceHealthBody.model_validate(item, from_attributes=True)
                for item in service.destination_pool.health_snapshot()
            ],
        )

    return app


app = create_app()
