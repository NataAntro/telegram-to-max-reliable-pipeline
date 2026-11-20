from __future__ import annotations

import asyncio

import pytest

from reliable_pipeline.pool import ResourcePool, ResourcePoolExhaustedError


@pytest.mark.asyncio
async def test_pool_respects_global_concurrency_limit() -> None:
    active = 0
    maximum_active = 0
    guard = asyncio.Lock()

    async def operation(resource: str) -> str:
        nonlocal active, maximum_active
        async with guard:
            active += 1
            maximum_active = max(maximum_active, active)
        await asyncio.sleep(0.02)
        async with guard:
            active -= 1
        return resource

    pool = ResourcePool(
        {"a": "a", "b": "b", "c": "c"},
        max_concurrency=2,
        max_parallel_tasks_per_resource=1,
    )
    results = await asyncio.gather(
        *(pool.execute(operation, timeout_seconds=0.2, max_attempts=1) for _ in range(6))
    )

    assert len(results) == 6
    assert maximum_active == 2


@pytest.mark.asyncio
async def test_pool_fails_over_after_timeout_and_reports_cooldown() -> None:
    async def operation(resource: str) -> str:
        if resource == "slow":
            await asyncio.sleep(0.05)
        return resource

    pool = ResourcePool(
        {"a-slow": "slow", "b-fast": "fast"},
        max_concurrency=1,
        cooldown_seconds=1,
    )
    result = await pool.execute(operation, timeout_seconds=0.01, max_attempts=2)

    assert result == "fast"
    snapshot = {item.name: item for item in pool.health_snapshot()}
    assert snapshot["a-slow"].status == "cooldown"
    assert snapshot["a-slow"].last_error_type == "TimeoutError"
    assert snapshot["b-fast"].total_successes == 1


@pytest.mark.asyncio
async def test_pool_exposes_all_failures() -> None:
    async def operation(resource: str) -> str:
        raise ConnectionError(f"temporary failure: {resource}")

    pool = ResourcePool({"a": "one", "b": "two"}, max_concurrency=1)

    with pytest.raises(ResourcePoolExhaustedError) as captured:
        await pool.execute(operation, timeout_seconds=0.1, max_attempts=2)

    assert [failure.resource_name for failure in captured.value.failures] == ["a", "b"]


@pytest.mark.asyncio
async def test_pool_does_not_retry_permanent_errors() -> None:
    calls = 0

    async def operation(resource: str) -> str:
        nonlocal calls
        calls += 1
        raise ValueError(f"invalid payload: {resource}")

    pool = ResourcePool({"a": "one", "b": "two"}, max_concurrency=1)

    with pytest.raises(ValueError, match="invalid payload"):
        await pool.execute(operation, timeout_seconds=0.1, max_attempts=2)

    assert calls == 1
    assert all(item.total_failures == 0 for item in pool.health_snapshot())


@pytest.mark.asyncio
async def test_pool_serializes_same_key_without_blocking_other_keys() -> None:
    active_by_key: dict[str, int] = {"same": 0, "other": 0}
    maximum_total = 0
    maximum_same = 0
    guard = asyncio.Lock()

    async def operation(resource_and_key: tuple[str, str]) -> str:
        nonlocal maximum_same, maximum_total
        resource, key = resource_and_key
        async with guard:
            active_by_key[key] += 1
            maximum_same = max(maximum_same, active_by_key["same"])
            maximum_total = max(maximum_total, sum(active_by_key.values()))
        await asyncio.sleep(0.02)
        async with guard:
            active_by_key[key] -= 1
        return resource

    pool = ResourcePool(
        {"a": "a", "b": "b", "c": "c"},
        max_concurrency=3,
        max_parallel_tasks_per_resource=1,
    )

    await asyncio.gather(
        pool.execute(
            lambda resource: operation((resource, "same")),
            timeout_seconds=0.2,
            max_attempts=1,
            serialization_key="@Same_Channel",
        ),
        pool.execute(
            lambda resource: operation((resource, "same")),
            timeout_seconds=0.2,
            max_attempts=1,
            serialization_key="@same_channel",
        ),
        pool.execute(
            lambda resource: operation((resource, "other")),
            timeout_seconds=0.2,
            max_attempts=1,
            serialization_key="@other_channel",
        ),
    )

    assert maximum_same == 1
    assert maximum_total == 2
    assert await pool.active_serialization_key_count() == 0


@pytest.mark.asyncio
async def test_cancelled_waiter_does_not_leak_serialization_key() -> None:
    entered = asyncio.Event()
    release = asyncio.Event()

    async def blocking_operation(resource: str) -> str:
        entered.set()
        await release.wait()
        return resource

    pool = ResourcePool({"a": "a"}, max_concurrency=1)
    holder = asyncio.create_task(
        pool.execute(
            blocking_operation,
            timeout_seconds=1,
            max_attempts=1,
            serialization_key="shared",
        )
    )
    await entered.wait()
    waiter = asyncio.create_task(
        pool.execute(
            blocking_operation,
            timeout_seconds=1,
            max_attempts=1,
            serialization_key="shared",
        )
    )
    await asyncio.sleep(0)
    waiter.cancel()
    with pytest.raises(asyncio.CancelledError):
        await waiter
    release.set()
    await holder

    assert await pool.active_serialization_key_count() == 0
