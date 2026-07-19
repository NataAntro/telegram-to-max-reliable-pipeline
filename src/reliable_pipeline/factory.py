from __future__ import annotations

from reliable_pipeline.adapters import (
    DestinationAdapter,
    InMemoryDestinationAdapter,
    InMemorySourceAdapter,
    SourceAdapter,
)
from reliable_pipeline.domain import SourceItem
from reliable_pipeline.pool import ResourcePool
from reliable_pipeline.service import ImportService


def create_reference_service() -> ImportService:
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
        source_pool=ResourcePool(sources, max_concurrency=2, cooldown_seconds=0.1),
        destination_pool=ResourcePool(destinations, max_concurrency=2, cooldown_seconds=0.1),
    )
