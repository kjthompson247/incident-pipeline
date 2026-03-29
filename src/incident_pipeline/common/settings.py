from __future__ import annotations
from dotenv import load_dotenv

import os
import re
from pathlib import Path
from typing import Any

import yaml
load_dotenv()  # auto-load .env if present
from incident_pipeline.common.paths import (
    DEFAULT_SETTINGS_PATH,
    REPO_ROOT,
    ensure_no_unresolved_placeholders,
    require_storage_namespace,
    require_storage_root,
    resolve_repo_path,
    resolve_storage_path,
)

SETTINGS_PATH_ENV_VAR = "INCIDENT_PIPELINE_SETTINGS_PATH"
_ENV_PATTERN = re.compile(r"\$\{([A-Z0-9_]+)(?::-([^}]*))?\}")
_STORAGE_PATH_FIELDS = (
    ("paths", "raw"),
    ("paths", "staged"),
    ("paths", "processed"),
    ("paths", "manifests"),
    ("paths", "logs"),
    ("database", "manifest_path"),
    ("docket_ingest", "manifest_path"),
    ("docket_ingest", "output_root"),
    ("docket_triage", "metadata_root"),
    ("docket_triage", "text_root"),
    ("docket_triage", "output_root"),
    ("primary_docket_narrative", "input_root"),
    ("primary_docket_narrative", "output_root"),
    ("sentence_span_generation", "output_root"),
    ("atomic_extraction", "output_root"),
    ("atomic_extraction", "sentence_span_root"),
)


def resolve_settings_path(
    config_path: str | Path | None = None,
    *,
    repo_root: Path = REPO_ROOT,
) -> Path:
    if config_path is not None:
        return resolve_repo_path(config_path, repo_root=repo_root)

    configured = os.environ.get(SETTINGS_PATH_ENV_VAR)
    if configured:
        return resolve_repo_path(configured, repo_root=repo_root)

    return DEFAULT_SETTINGS_PATH


def _expand_string(value: str) -> str:
    def replace(match: re.Match[str]) -> str:
        name = match.group(1)
        fallback = match.group(2)
        configured = os.environ.get(name)
        if configured not in (None, ""):
            return configured
        if fallback is not None:
            return fallback
        return match.group(0)

    return _ENV_PATTERN.sub(replace, value)


def _expand_value(value: Any) -> Any:
    if isinstance(value, str):
        return _expand_string(value)
    if isinstance(value, list):
        return [_expand_value(item) for item in value]
    if isinstance(value, dict):
        return {key: _expand_value(item) for key, item in value.items()}
    return value


def _assert_no_unresolved_placeholders(value: Any, *, label: str = "settings") -> None:
    if isinstance(value, str):
        ensure_no_unresolved_placeholders(value, label=label)
        return
    if isinstance(value, list):
        for index, item in enumerate(value):
            _assert_no_unresolved_placeholders(item, label=f"{label}[{index}]")
        return
    if isinstance(value, dict):
        for key, item in value.items():
            _assert_no_unresolved_placeholders(item, label=f"{label}.{key}")


def _get_nested(mapping: dict[str, Any], keys: tuple[str, ...]) -> Any:
    current: Any = mapping
    for key in keys:
        if not isinstance(current, dict) or key not in current:
            return None
        current = current[key]
    return current


def _validate_storage_relative_path(settings: dict[str, Any], keys: tuple[str, ...]) -> None:
    value = _get_nested(settings, keys)
    if value is None:
        return
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{'.'.join(keys)} must be a non-empty relative storage path")
    ensure_no_unresolved_placeholders(value, label=".".join(keys))
    if Path(value).is_absolute():
        raise ValueError(
            f"{'.'.join(keys)} must be relative to paths.storage_root / "
            f"paths.storage_namespace, got absolute path: {value}"
        )


def _validate_path_model(settings: dict[str, Any]) -> None:
    paths = settings.get("paths")
    if not isinstance(paths, dict):
        raise ValueError("Settings file must define a paths mapping")

    storage_root = paths.get("storage_root")
    if not isinstance(storage_root, str) or not storage_root.strip():
        raise ValueError("paths.storage_root must be configured from INCIDENT_PIPELINE_DATA_ROOT")
    require_storage_root(env={"INCIDENT_PIPELINE_DATA_ROOT": storage_root})

    storage_namespace = paths.get("storage_namespace")
    if not isinstance(storage_namespace, str) or not storage_namespace.strip():
        raise ValueError("paths.storage_namespace must be configured as a relative namespace")
    require_storage_namespace(storage_namespace, label="paths.storage_namespace")

    for keys in _STORAGE_PATH_FIELDS:
        _validate_storage_relative_path(settings, keys)


def load_settings(config_path: str | Path | None = None) -> dict[str, Any]:
    resolved_config_path = resolve_settings_path(config_path)
    with resolved_config_path.open("r", encoding="utf-8") as handle:
        payload = yaml.safe_load(handle) or {}

    if not isinstance(payload, dict):
        raise ValueError(f"Settings file must contain a YAML mapping: {resolved_config_path}")

    expanded = _expand_value(payload)
    if not isinstance(expanded, dict):
        raise ValueError(f"Expanded settings file must contain a YAML mapping: {resolved_config_path}")

    _assert_no_unresolved_placeholders(expanded)
    _validate_path_model(expanded)
    return expanded


def storage_root_from_settings(settings: dict[str, Any]) -> Path:
    paths = settings.get("paths")
    if not isinstance(paths, dict):
        raise ValueError("Settings file must define a paths mapping")
    storage_root = paths.get("storage_root")
    if not isinstance(storage_root, str):
        raise ValueError("paths.storage_root must be configured from INCIDENT_PIPELINE_DATA_ROOT")
    return require_storage_root(env={"INCIDENT_PIPELINE_DATA_ROOT": storage_root})


def storage_namespace_from_settings(settings: dict[str, Any]) -> Path:
    paths = settings.get("paths")
    if not isinstance(paths, dict):
        raise ValueError("Settings file must define a paths mapping")
    storage_namespace = paths.get("storage_namespace")
    if not isinstance(storage_namespace, str):
        raise ValueError("paths.storage_namespace must be configured as a relative namespace")
    return require_storage_namespace(storage_namespace, label="paths.storage_namespace")


def resolve_storage_setting(settings: dict[str, Any], value: str | Path) -> Path:
    return resolve_storage_path(
        value,
        storage_root=storage_root_from_settings(settings),
        storage_namespace=storage_namespace_from_settings(settings),
    )
