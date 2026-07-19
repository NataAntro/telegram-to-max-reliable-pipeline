from __future__ import annotations

from pydantic import BaseModel, Field


class ImportRequestBody(BaseModel):
    source_ref: str = Field(default="source-a", min_length=1, max_length=128)
    destination_ref: str = Field(default="destination-a", min_length=1, max_length=128)
    limit: int = Field(default=3, ge=1, le=25)


class TransitionBody(BaseModel):
    from_state: str
    to_state: str


class ImportJobBody(BaseModel):
    id: str
    state: str
    idempotency_key: str
    source_count: int
    published_count: int
    pending_publication_count: int
    resume_from: str | None
    last_error: str | None
    transitions: list[TransitionBody]


class ResourceHealthBody(BaseModel):
    name: str
    status: str
    current_load: int
    max_parallel_tasks: int
    consecutive_failures: int
    total_successes: int
    total_failures: int
    cooldown_remaining_seconds: float
    last_error_type: str | None


class HealthBody(BaseModel):
    source: list[ResourceHealthBody]
    destination: list[ResourceHealthBody]


class ServiceStatusBody(BaseModel):
    status: str
    persistence: str
