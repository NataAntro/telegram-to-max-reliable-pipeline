from __future__ import annotations

from reliable_pipeline.adapters import (
    DestinationAdapter,
    InMemoryDestinationAdapter,
    InMemorySourceAdapter,
    SourceAdapter,
)
from reliable_pipeline.config import RuntimeSettings
from reliable_pipeline.domain import SourceItem
from reliable_pipeline.pool import ResourcePool
from reliable_pipeline.service import ImportService
from reliable_pipeline.store import SqlAlchemyJobStore


def create_reference_service(settings: RuntimeSettings | None = None) -> ImportService:
    settings = settings or RuntimeSettings()
    items = [
        SourceItem("item-1", "Короткая заметка о безопасной обработке очередей."),
        SourceItem("item-2", "  Материал   о retry, timeout и failover.  "),
        SourceItem("item-3", "Материал о восстановлении import job после частичной ошибки."),
    ]
    sources: dict[str, SourceAdapter] = {
        "source-a": InMemorySourceAdapter(items),
        "source-b": InMemorySourceAdapter(items),
    }
    destinations: dict[str, DestinationAdapter] = {
        "destination-a": InMemoryDestinationAdapter(),
        "destination-b": InMemoryDestinationAdapter(),
    }
    return ImportService(
        source_pool=ResourcePool(
            sources,
            max_concurrency=settings.max_concurrency,
            cooldown_seconds=settings.cooldown_seconds,
        ),
        destination_pool=ResourcePool(
            destinations,
            max_concurrency=settings.max_concurrency,
            cooldown_seconds=settings.cooldown_seconds,
        ),
        store=(
            SqlAlchemyJobStore.from_url(settings.database_url)
            if settings.database_url is not None
            else None
        ),
        timeout_seconds=settings.operation_timeout_seconds,
    )
