from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime

from incident_pipeline.acquisition.ntsb.config import AppConfig
from incident_pipeline.acquisition.ntsb.manifests import new_run_id
from incident_pipeline.acquisition.ntsb.paths import ProjectPaths


def utc_now() -> str:
    timestamp = datetime.now(UTC).replace(microsecond=0)
    return timestamp.isoformat().replace("+00:00", "Z")


@dataclass(frozen=True)
class RunContext:
    run_id: str
    started_at: str
    config: AppConfig
    paths: ProjectPaths


def create_run_context(config: AppConfig, *, run_id: str | None = None) -> RunContext:
    return RunContext(
        run_id=run_id or new_run_id(),
        started_at=utc_now(),
        config=config,
        paths=config.paths,
    )
