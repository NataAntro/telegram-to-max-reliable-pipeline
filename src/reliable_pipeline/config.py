from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class RuntimeSettings:
    database_url: str | None = None
    max_concurrency: int = 2
    operation_timeout_seconds: float = 0.25
    cooldown_seconds: float = 0.10

    @classmethod
    def from_environment(cls, environment: Mapping[str, str] | None = None) -> RuntimeSettings:
        values = environment or os.environ
        database_url = values.get("DATABASE_URL")
        settings = cls(
            database_url=database_url.strip() if database_url else None,
            max_concurrency=int(values.get("MAX_CONCURRENCY", "2")),
            operation_timeout_seconds=float(values.get("OPERATION_TIMEOUT_SECONDS", "0.25")),
            cooldown_seconds=float(values.get("COOLDOWN_SECONDS", "0.10")),
        )
        if settings.max_concurrency < 1:
            raise ValueError("MAX_CONCURRENCY must be positive")
        if settings.operation_timeout_seconds <= 0:
            raise ValueError("OPERATION_TIMEOUT_SECONDS must be positive")
        if settings.cooldown_seconds < 0:
            raise ValueError("COOLDOWN_SECONDS must not be negative")
        return settings
