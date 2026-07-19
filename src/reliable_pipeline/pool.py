from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Generic, TypeVar

ResourceT = TypeVar("ResourceT")
ResultT = TypeVar("ResultT")


@dataclass(frozen=True, slots=True)
class AttemptFailure:
    resource_name: str
    error_type: str
    message: str


class ResourcePoolExhaustedError(RuntimeError):
    def __init__(self, failures: list[AttemptFailure]) -> None:
        self.failures = failures
        summary = "; ".join(f"{item.resource_name}: {item.error_type}" for item in failures)
        super().__init__(f"All resource attempts failed ({summary})")


@dataclass(frozen=True, slots=True)
class ResourceHealth:
    name: str
    status: str
    current_load: int
    max_parallel_tasks: int
    consecutive_failures: int
    total_successes: int
    total_failures: int
    cooldown_remaining_seconds: float
    last_error_type: str | None


@dataclass(slots=True)
class _ResourceSlot(Generic[ResourceT]):
    name: str
    resource: ResourceT
    max_parallel_tasks: int
    current_load: int = 0
    consecutive_failures: int = 0
    total_successes: int = 0
    total_failures: int = 0
    cooldown_until: float = 0.0
    last_error_type: str | None = None


@dataclass(slots=True)
class _SerializationLock:
    lock: asyncio.Lock
    users: int = 0


class ResourcePool(Generic[ResourceT]):
    """A generic bounded pool with timeout, retry, failover and cooldown."""

    def __init__(
        self,
        resources: dict[str, ResourceT],
        *,
        max_concurrency: int,
        max_parallel_tasks_per_resource: int = 1,
        failure_threshold: int = 1,
        cooldown_seconds: float = 1.0,
        is_retryable: Callable[[BaseException], bool] | None = None,
    ) -> None:
        if not resources:
            raise ValueError("ResourcePool requires at least one resource")
        if max_concurrency < 1 or max_parallel_tasks_per_resource < 1:
            raise ValueError("Concurrency limits must be positive")
        self._slots = {
            name: _ResourceSlot(name, resource, max_parallel_tasks_per_resource)
            for name, resource in sorted(resources.items())
        }
        self._semaphore = asyncio.Semaphore(max_concurrency)
        self._condition = asyncio.Condition()
        self._serialization_locks_guard = asyncio.Lock()
        self._serialization_locks: dict[str, _SerializationLock] = {}
        self._failure_threshold = max(1, failure_threshold)
        self._cooldown_seconds = max(0.0, cooldown_seconds)
        self._is_retryable = is_retryable or self._default_retryable

    async def execute(
        self,
        operation: Callable[[ResourceT], Awaitable[ResultT]],
        *,
        timeout_seconds: float,
        max_attempts: int,
        serialization_key: str | None = None,
    ) -> ResultT:
        if timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")
        if max_attempts < 1:
            raise ValueError("max_attempts must be positive")

        async with self._serialize(serialization_key):
            return await self._execute(operation, timeout_seconds, max_attempts)

    async def _execute(
        self,
        operation: Callable[[ResourceT], Awaitable[ResultT]],
        timeout_seconds: float,
        max_attempts: int,
    ) -> ResultT:
        failures: list[AttemptFailure] = []
        excluded: set[str] = set()
        async with self._semaphore:
            for _ in range(max_attempts):
                slot = await self._acquire(excluded)
                try:
                    result = await asyncio.wait_for(
                        operation(slot.resource),
                        timeout=timeout_seconds,
                    )
                except Exception as error:
                    if not self._is_retryable(error):
                        raise
                    failures.append(AttemptFailure(slot.name, type(error).__name__, str(error)))
                    await self._record_failure(slot, error)
                    excluded.add(slot.name)
                else:
                    await self._record_success(slot)
                    return result
                finally:
                    await self._release(slot)

                if len(excluded) == len(self._slots):
                    excluded.clear()

        raise ResourcePoolExhaustedError(failures)

    @asynccontextmanager
    async def _serialize(self, key: str | None) -> AsyncIterator[None]:
        normalized_key = key.strip().casefold() if key and key.strip() else None
        if normalized_key is None:
            yield
            return

        async with self._serialization_locks_guard:
            entry = self._serialization_locks.setdefault(
                normalized_key,
                _SerializationLock(asyncio.Lock()),
            )
            entry.users += 1

        await entry.lock.acquire()
        try:
            yield
        finally:
            entry.lock.release()
            async with self._serialization_locks_guard:
                entry.users -= 1
                if entry.users == 0:
                    self._serialization_locks.pop(normalized_key, None)

    @staticmethod
    def _default_retryable(error: BaseException) -> bool:
        return isinstance(error, (TimeoutError, ConnectionError))

    def health_snapshot(self) -> list[ResourceHealth]:
        now = asyncio.get_running_loop().time()
        result: list[ResourceHealth] = []
        for slot in self._slots.values():
            cooldown_remaining = max(0.0, slot.cooldown_until - now)
            if cooldown_remaining > 0:
                status = "cooldown"
            elif slot.current_load >= slot.max_parallel_tasks:
                status = "busy"
            else:
                status = "ready"
            result.append(
                ResourceHealth(
                    name=slot.name,
                    status=status,
                    current_load=slot.current_load,
                    max_parallel_tasks=slot.max_parallel_tasks,
                    consecutive_failures=slot.consecutive_failures,
                    total_successes=slot.total_successes,
                    total_failures=slot.total_failures,
                    cooldown_remaining_seconds=round(cooldown_remaining, 3),
                    last_error_type=slot.last_error_type,
                )
            )
        return result

    async def _acquire(self, excluded: set[str]) -> _ResourceSlot[ResourceT]:
        async with self._condition:
            while True:
                now = asyncio.get_running_loop().time()
                candidates = [
                    slot
                    for slot in self._slots.values()
                    if slot.name not in excluded
                    and slot.current_load < slot.max_parallel_tasks
                    and slot.cooldown_until <= now
                ]
                if candidates:
                    slot = min(
                        candidates,
                        key=lambda item: (
                            item.current_load,
                            item.consecutive_failures,
                            item.total_failures,
                            item.name,
                        ),
                    )
                    slot.current_load += 1
                    return slot

                reusable = [slot for slot in self._slots.values() if slot.name not in excluded]
                if not reusable:
                    excluded.clear()
                    continue

                cooldown_delays = [
                    slot.cooldown_until - now
                    for slot in reusable
                    if slot.current_load < slot.max_parallel_tasks and slot.cooldown_until > now
                ]
                if cooldown_delays:
                    try:
                        await asyncio.wait_for(
                            self._condition.wait(),
                            timeout=max(0.001, min(cooldown_delays)),
                        )
                    except TimeoutError:
                        continue
                else:
                    await self._condition.wait()

    async def _release(self, slot: _ResourceSlot[ResourceT]) -> None:
        async with self._condition:
            slot.current_load = max(0, slot.current_load - 1)
            self._condition.notify_all()

    async def _record_success(self, slot: _ResourceSlot[ResourceT]) -> None:
        async with self._condition:
            slot.total_successes += 1
            slot.consecutive_failures = 0
            slot.cooldown_until = 0.0
            slot.last_error_type = None
            self._condition.notify_all()

    async def _record_failure(
        self,
        slot: _ResourceSlot[ResourceT],
        error: BaseException,
    ) -> None:
        async with self._condition:
            slot.total_failures += 1
            slot.consecutive_failures += 1
            slot.last_error_type = type(error).__name__
            if slot.consecutive_failures >= self._failure_threshold:
                slot.cooldown_until = asyncio.get_running_loop().time() + self._cooldown_seconds
            self._condition.notify_all()
