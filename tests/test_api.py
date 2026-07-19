from pathlib import Path

import httpx
import pytest

from reliable_pipeline.api import create_app
from reliable_pipeline.config import RuntimeSettings
from reliable_pipeline.factory import create_reference_service
from reliable_pipeline.store import SqlAlchemyJobStore


@pytest.mark.asyncio
async def test_api_reuses_idempotency_key_and_exposes_health() -> None:
    transport = httpx.ASGITransport(app=create_app())
    headers = {"Idempotency-Key": "api-import-key"}

    async with httpx.AsyncClient(transport=transport, base_url="http://pipeline.local") as client:
        first = await client.post("/imports", headers=headers, json={})
        second = await client.post("/imports", headers=headers, json={})
        health = await client.get("/health/resources")
        live = await client.get("/health/live")
        ready = await client.get("/health/ready")

    assert first.status_code == 200
    assert second.status_code == 200
    assert first.json()["id"] == second.json()["id"]
    assert first.json()["state"] == "completed"
    assert first.json()["pending_publication_count"] == 0
    assert first.json()["resume_from"] is None
    assert health.status_code == 200
    assert len(health.json()["source"]) == 2
    assert live.json() == {"status": "ok", "persistence": "process"}
    assert ready.json() == {"status": "ok", "persistence": "memory"}


@pytest.mark.asyncio
async def test_api_rejects_idempotency_key_reuse_with_different_payload() -> None:
    transport = httpx.ASGITransport(app=create_app())
    headers = {"Idempotency-Key": "payload-bound-key"}

    async with httpx.AsyncClient(transport=transport, base_url="http://pipeline.local") as client:
        first = await client.post(
            "/imports",
            headers=headers,
            json={"source_ref": "source-a", "destination_ref": "destination-a", "limit": 1},
        )
        conflict = await client.post(
            "/imports",
            headers=headers,
            json={"source_ref": "source-b", "destination_ref": "destination-b", "limit": 2},
        )

    assert first.status_code == 200
    assert conflict.status_code == 409
    assert "different import request" in conflict.json()["detail"]


@pytest.mark.asyncio
async def test_api_lifespan_initializes_durable_store_and_readiness(tmp_path: Path) -> None:
    service = create_reference_service(
        RuntimeSettings(database_url=f"sqlite+aiosqlite:///{tmp_path / 'api.sqlite'}")
    )
    app = create_app(service)

    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(
            transport=transport,
            base_url="http://pipeline.local",
        ) as client:
            ready = await client.get("/health/ready")
            imported = await client.post(
                "/imports",
                headers={"Idempotency-Key": "durable-api"},
                json={},
            )

    assert ready.json() == {"status": "ok", "persistence": "sqlite"}
    assert imported.json()["state"] == "completed"
    assert isinstance(service.store, SqlAlchemyJobStore)
    await service.store.close()
