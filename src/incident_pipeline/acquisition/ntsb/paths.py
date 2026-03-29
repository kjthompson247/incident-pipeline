from __future__ import annotations

from pathlib import Path

from incident_pipeline.common.paths import (
    NTSBSourcePaths,
    REPO_ROOT,
    build_ntsb_source_paths,
    default_ntsb_source_root,
    resolve_repo_path,
)

ProjectPaths = NTSBSourcePaths


def compute_project_paths(
    *,
    data_root: str | Path | None = None,
    sqlite_path: str | Path | None = None,
    downstream_raw_root: str | Path | None = None,
    repo_root: Path = REPO_ROOT,
) -> ProjectPaths:
    return build_ntsb_source_paths(
        data_root=default_ntsb_source_root() if data_root is None else data_root,
        sqlite_path=sqlite_path,
        downstream_raw_root=downstream_raw_root,
        repo_root=repo_root,
    )


__all__ = ["ProjectPaths", "REPO_ROOT", "compute_project_paths", "resolve_repo_path"]
