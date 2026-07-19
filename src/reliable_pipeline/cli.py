from __future__ import annotations

import asyncio
import json

from reliable_pipeline.domain import ImportRequest
from reliable_pipeline.factory import create_reference_service


async def run_pipeline() -> dict[str, object]:
    service = create_reference_service()
    job = await service.submit(
        ImportRequest("source-a", "destination-a", limit=3),
        "reference-run-001",
    )
    return {
        "job_id": job.id,
        "state": job.state.value,
        "published": len(job.receipts),
        "transitions": [item.to_state.value for item in job.transitions],
        "source_health": [item.status for item in service.source_pool.health_snapshot()],
        "destination_health": [item.status for item in service.destination_pool.health_snapshot()],
    }


def main() -> None:
    print(json.dumps(asyncio.run(run_pipeline()), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
