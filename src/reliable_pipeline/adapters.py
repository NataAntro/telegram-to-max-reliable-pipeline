from __future__ import annotations

import asyncio
from collections.abc import MutableMapping, Sequence
from typing import Protocol

from reliable_pipeline.domain import PreparedItem, PublicationReceipt, SourceItem


class SourceAdapter(Protocol):
    async def fetch(self, source_ref: str, limit: int) -> list[SourceItem]: ...


class DestinationAdapter(Protocol):
    async def publish(
        self,
        destination_ref: str,
        item: PreparedItem,
        *,
        idempotency_token: str,
    ) -> PublicationReceipt: ...


class InMemorySourceAdapter:
    def __init__(
        self,
        items: Sequence[SourceItem],
        *,
        delay_seconds: float = 0.0,
        failures_before_success: int = 0,
    ) -> None:
        self._items = list(items)
        self.delay_seconds = delay_seconds
        self.failures_remaining = failures_before_success
        self.fetch_calls = 0

    async def fetch(self, source_ref: str, limit: int) -> list[SourceItem]:
        del source_ref
        self.fetch_calls += 1
        if self.delay_seconds:
            await asyncio.sleep(self.delay_seconds)
        if self.failures_remaining:
            self.failures_remaining -= 1
            raise ConnectionError("source is temporarily unavailable")
        return self._items[:limit]


class InMemoryDestinationAdapter:
    def __init__(
        self,
        *,
        delay_seconds: float = 0.0,
        failures_before_success: int = 0,
        idempotency_ledger: MutableMapping[str, PublicationReceipt] | None = None,
    ) -> None:
        self.delay_seconds = delay_seconds
        self.failures_remaining = failures_before_success
        self.publish_calls = 0
        self._receipts = idempotency_ledger if idempotency_ledger is not None else {}

    async def publish(
        self,
        destination_ref: str,
        item: PreparedItem,
        *,
        idempotency_token: str,
    ) -> PublicationReceipt:
        del destination_ref
        self.publish_calls += 1
        if self.delay_seconds:
            await asyncio.sleep(self.delay_seconds)
        if self.failures_remaining:
            self.failures_remaining -= 1
            raise ConnectionError("destination is temporarily unavailable")
        if idempotency_token not in self._receipts:
            self._receipts[idempotency_token] = PublicationReceipt(
                external_id=item.external_id,
                destination_id=f"publication-{len(self._receipts) + 1}",
            )
        return self._receipts[idempotency_token]
