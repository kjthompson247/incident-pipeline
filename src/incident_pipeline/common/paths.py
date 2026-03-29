from __future__ import annotations

import os
import re
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
CONFIGS_ROOT = REPO_ROOT / "configs"
DEFAULT_SETTINGS_PATH = CONFIGS_ROOT / "settings.yaml"
DATA_ROOT_ENV_VAR = "INCIDENT_PIPELINE_DATA_ROOT"
DEFAULT_STORAGE_NAMESPACE = "ntsb"
UNRESOLVED_ENV_PATTERN = re.compile(r"\$\{[A-Z0-9_]+(?::-([^}]*))?\}")


def ensure_no_unresolved_placeholders(value: str, *, label: str) -> None:
    match = UNRESOLVED_ENV_PATTERN.search(value)
    if match is not None:
        raise ValueError(f"Unresolved environment placeholder in {label}: {value}")


def require_absolute_path(value: str | Path, *, label: str) -> Path:
    if isinstance(value, str):
        ensure_no_unresolved_placeholders(value, label=label)
    path = Path(value)
    if not path.is_absolute():
        raise ValueError(f"{label} must be an absolute path, got: {value}")
    return path.resolve()


def resolve_repo_path(value: str | Path, *, repo_root: Path = REPO_ROOT) -> Path:
    if isinstance(value, str):
        ensure_no_unresolved_placeholders(value, label="repo path")
    path = Path(value)
    if path.is_absolute():
        return path
    return (repo_root / path).resolve()


def require_storage_root(
    *,
    env: Mapping[str, str] | None = None,
) -> Path:
    resolved_env = os.environ if env is None else env
    configured = resolved_env.get(DATA_ROOT_ENV_VAR)
    if configured in (None, ""):
        raise ValueError(
            f"Missing required environment variable {DATA_ROOT_ENV_VAR}. "
            "Set it to an absolute storage root before running pipeline stages."
        )
    return require_absolute_path(configured, label=DATA_ROOT_ENV_VAR)


def require_storage_namespace(
    value: str | Path,
    *,
    label: str = "storage namespace",
) -> Path:
    if isinstance(value, str):
        ensure_no_unresolved_placeholders(value, label=label)
    namespace = Path(value)
    if namespace.is_absolute():
        raise ValueError(f"{label} must be relative, got absolute path: {value}")
    parts = namespace.parts
    if len(parts) != 1 or parts[0] in {"", ".", ".."}:
        raise ValueError(
            f"{label} must be a single relative namespace segment, got: {value}"
        )
    return namespace


def resolve_storage_namespace_root(
    *,
    storage_root: Path,
    storage_namespace: str | Path,
) -> Path:
    namespace = require_storage_namespace(storage_namespace, label="storage namespace")
    return (storage_root / namespace).resolve()


def resolve_storage_path(
    value: str | Path,
    *,
    storage_root: Path,
    storage_namespace: str | Path,
) -> Path:
    if isinstance(value, str):
        ensure_no_unresolved_placeholders(value, label="storage path")
    path = Path(value)
    if path.is_absolute():
        raise ValueError(
            "Storage-owned paths must be relative to "
            "storage_root / storage_namespace, "
            f"got absolute path: {value}"
        )
    return (resolve_storage_namespace_root(
        storage_root=storage_root,
        storage_namespace=storage_namespace,
    ) / path).resolve()


def default_canonical_data_root(
    *,
    env: Mapping[str, str] | None = None,
) -> Path:
    return require_storage_root(env=env)


def default_ntsb_source_root(
    *,
    env: Mapping[str, str] | None = None,
) -> Path:
    return resolve_storage_namespace_root(
        storage_root=require_storage_root(env=env),
        storage_namespace=DEFAULT_STORAGE_NAMESPACE,
    )


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
    data_root: str | Path | None = None,
    sqlite_path: str | Path | None = None,
    downstream_raw_root: str | Path | None = None,
    repo_root: Path = REPO_ROOT,
) -> NTSBSourcePaths:
    resolved_data_root = require_absolute_path(
        default_ntsb_source_root() if data_root is None else data_root,
        label="NTSB data_root",
    )
    resolved_sqlite_path = require_absolute_path(
        sqlite_path or resolved_data_root / "acquisition" / "state" / "acquisition.db",
        label="NTSB sqlite_path",
    )
    resolved_downstream_raw_root = require_absolute_path(
        downstream_raw_root or resolved_data_root / "raw",
        label="NTSB downstream_raw_root",
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
