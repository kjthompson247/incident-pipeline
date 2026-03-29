from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
CONFIGS_ROOT = REPO_ROOT / "configs"
DEFAULT_SETTINGS_PATH = CONFIGS_ROOT / "settings.yaml"
DATA_ROOT_ENV_VAR = "INCIDENT_PIPELINE_DATA_ROOT"
NTSB_SOURCE_ROOT_ENV_VAR = "INCIDENT_PIPELINE_NTSB_SOURCE_ROOT"


def resolve_repo_path(value: str | Path, *, repo_root: Path = REPO_ROOT) -> Path:
    path = Path(value)
    if path.is_absolute():
        return path
    return (repo_root / path).resolve()


def default_canonical_data_root(
    *,
    repo_root: Path = REPO_ROOT,
    env: Mapping[str, str] | None = None,
) -> Path:
    resolved_env = os.environ if env is None else env
    configured = resolved_env.get(DATA_ROOT_ENV_VAR)
    if configured:
        return resolve_repo_path(configured, repo_root=repo_root)
    return (repo_root / "data").resolve()


def default_ntsb_source_root(
    *,
    repo_root: Path = REPO_ROOT,
    env: Mapping[str, str] | None = None,
) -> Path:
    resolved_env = os.environ if env is None else env
    configured = resolved_env.get(NTSB_SOURCE_ROOT_ENV_VAR)
    if configured:
        return resolve_repo_path(configured, repo_root=repo_root)
    return default_canonical_data_root(repo_root=repo_root, env=resolved_env) / "ntsb"


CANONICAL_DATA_ROOT = default_canonical_data_root()
DEFAULT_NTSB_SOURCE_ROOT = default_ntsb_source_root()


@dataclass(frozen=True)
class NTSBSourcePaths:
    repo_root: Path
    data_root: Path
    source_root: Path
    sqlite_path: Path
    acquisition_root: Path
    acquisition_state_root: Path
    acquisition_raw_root: Path
    acquisition_normalized_root: Path
    acquisition_blobs_root: Path
    acquisition_manifests_root: Path
    acquisition_logs_root: Path
    acquisition_exports_root: Path
    corpus_root: Path
    downstream_raw_root: Path
    raw_root: Path
    ingestion_root: Path
    triage_root: Path
    narrative_root: Path
    extract_root: Path
    index_root: Path
    manifests_root: Path
    logs_root: Path
    case_views_root: Path


def build_ntsb_source_paths(
    *,
    data_root: str | Path = DEFAULT_NTSB_SOURCE_ROOT,
    sqlite_path: str | Path | None = None,
    downstream_raw_root: str | Path | None = None,
    repo_root: Path = REPO_ROOT,
) -> NTSBSourcePaths:
    resolved_data_root = resolve_repo_path(data_root, repo_root=repo_root)
    resolved_sqlite_path = resolve_repo_path(
        sqlite_path or resolved_data_root / "acquisition" / "state" / "acquisition.db",
        repo_root=repo_root,
    )
    resolved_downstream_raw_root = resolve_repo_path(
        downstream_raw_root or resolved_data_root / "raw",
        repo_root=repo_root,
    )

    acquisition_root = resolved_data_root / "acquisition"
    manifests_root = resolved_data_root / "manifests"
    logs_root = resolved_data_root / "logs"

    return NTSBSourcePaths(
        repo_root=repo_root.resolve(),
        data_root=resolved_data_root,
        source_root=resolved_data_root,
        sqlite_path=resolved_sqlite_path,
        acquisition_root=acquisition_root,
        acquisition_state_root=acquisition_root / "state",
        acquisition_raw_root=acquisition_root / "raw",
        acquisition_normalized_root=acquisition_root / "normalized",
        acquisition_blobs_root=acquisition_root / "blobs",
        acquisition_manifests_root=acquisition_root / "manifests",
        acquisition_logs_root=acquisition_root / "logs",
        acquisition_exports_root=acquisition_root / "exports",
        corpus_root=resolved_data_root / "corpus",
        downstream_raw_root=resolved_downstream_raw_root,
        raw_root=resolved_data_root / "raw",
        ingestion_root=resolved_data_root / "ingestion",
        triage_root=resolved_data_root / "triage",
        narrative_root=resolved_data_root / "narrative",
        extract_root=resolved_data_root / "extract",
        index_root=resolved_data_root / "index",
        manifests_root=manifests_root,
        logs_root=logs_root,
        case_views_root=resolved_data_root / "case_views",
    )
