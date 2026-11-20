from __future__ import annotations

from datetime import datetime

from sqlalchemy import JSON, DateTime, ForeignKey, String, Text, UniqueConstraint
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from reliable_pipeline.domain import JobState, utc_now


class Base(DeclarativeBase):
    pass


class ImportJobRecord(Base):
    """Durable persistence record for import job state and checkpoints."""

    __tablename__ = "import_jobs"
    __table_args__ = (UniqueConstraint("idempotency_key"),)

    id: Mapped[str] = mapped_column(String(32), primary_key=True)
    idempotency_key: Mapped[str] = mapped_column(String(128), nullable=False)
    source_ref: Mapped[str] = mapped_column(String(128), nullable=False)
    destination_ref: Mapped[str] = mapped_column(String(128), nullable=False)
    request_fingerprint: Mapped[str] = mapped_column(String(64), nullable=False)
    state: Mapped[str] = mapped_column(String(32), default=JobState.RECEIVED.value)
    checkpoint: Mapped[dict[str, object]] = mapped_column(JSON, default=dict)
    last_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=utc_now,
        onupdate=utc_now,
    )
    lease_owner: Mapped[str | None] = mapped_column(String(32), nullable=True)
    lease_expires_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )


class PublicationRecord(Base):
    __tablename__ = "publications"
    __table_args__ = (UniqueConstraint("job_id", "external_id"),)

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    job_id: Mapped[str] = mapped_column(
        ForeignKey("import_jobs.id", ondelete="CASCADE"), index=True
    )
    external_id: Mapped[str] = mapped_column(String(128))
    destination_id: Mapped[str] = mapped_column(String(128))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)
