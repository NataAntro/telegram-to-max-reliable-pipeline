import pytest

from reliable_pipeline.config import RuntimeSettings
from reliable_pipeline.factory import create_reference_service
from reliable_pipeline.store import SqlAlchemyJobStore


@pytest.mark.asyncio
async def test_runtime_settings_build_postgres_backed_service() -> None:
    settings = RuntimeSettings.from_environment(
        {
            "DATABASE_URL": "postgresql+asyncpg://pipeline:secret@db/pipeline",
            "MAX_CONCURRENCY": "4",
            "OPERATION_TIMEOUT_SECONDS": "1.5",
            "COOLDOWN_SECONDS": "3",
        }
    )
    service = create_reference_service(settings)

    assert isinstance(service.store, SqlAlchemyJobStore)
    assert service.timeout_seconds == 1.5
    assert len(service.source_pool.health_snapshot()) == 2


def test_runtime_settings_reject_invalid_limits() -> None:
    with pytest.raises(ValueError, match="MAX_CONCURRENCY"):
        RuntimeSettings.from_environment({"MAX_CONCURRENCY": "0"})
