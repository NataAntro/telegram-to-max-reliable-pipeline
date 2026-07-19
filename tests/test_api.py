import httpx
import pytest

from reliable_pipeline.api import create_app


@pytest.mark.asyncio
async def test_api_reuses_idempotency_key_and_exposes_health() -> None:
    transport = httpx.ASGITransport(app=create_app())
    headers = {"Idempotency-Key": "api-import-key"}

    async with httpx.AsyncClient(transport=transport, base_url="http://pipeline.local") as client:
        first = await client.post("/imports", headers=headers, json={})
        second = await client.post("/imports", headers=headers, json={})
        health = await client.get("/health/resources")

    assert first.status_code == 200
    assert second.status_code == 200
    assert first.json()["id"] == second.json()["id"]
    assert first.json()["state"] == "completed"
    assert first.json()["pending_publication_count"] == 0
    assert first.json()["resume_from"] is None
    assert health.status_code == 200
    assert len(health.json()["source"]) == 2
