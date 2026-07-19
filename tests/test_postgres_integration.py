from __future__ import annotations

import asyncio
import os

import pytest
from sqlalchemy.exc import OperationalError

from reliable_pipeline.domain import ImportRequest
from reliable_pipeline.store import SqlAlchemyJobStore


@pytest.mark.skipif(
    not os.getenv("TEST_POSTGRES_URL"),
    reason="TEST_POSTGRES_URL is not configured",
)
@pytest.mark.asyncio
async def test_postgres_store_initializes_and_preserves_idempotency() -> None:
    store = SqlAlchemyJobStore.from_url(os.environ["TEST_POSTGRES_URL"])
    for attempt in range(20):
        try:
            await store.initialize()
            break
        except (OperationalError, OSError):
            if attempt == 19:
                raise
            await asyncio.sleep(0.25)

    request = ImportRequest("source-a", "destination-a", 3)
    first, first_created = await store.get_or_create(request, "postgres-ci-idempotency")
    second, second_created = await store.get_or_create(request, "postgres-ci-idempotency")
    await store.ping()
    await store.close()

    assert first_created is True
    assert second_created is False
    assert first.id == second.id
