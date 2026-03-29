from __future__ import annotations
from dotenv import load_dotenv

import os
import re
from pathlib import Path
from typing import Any

import yaml
load_dotenv()  # auto-load .env if present
from incident_pipeline.common.paths import DEFAULT_SETTINGS_PATH, REPO_ROOT, resolve_repo_path

SETTINGS_PATH_ENV_VAR = "INCIDENT_PIPELINE_SETTINGS_PATH"
_ENV_PATTERN = re.compile(r"\$\{([A-Z0-9_]+)(?::-([^}]*))?\}")


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


def load_settings(config_path: str | Path | None = None) -> dict[str, Any]:
    resolved_config_path = resolve_settings_path(config_path)
    with resolved_config_path.open("r", encoding="utf-8") as handle:
        payload = yaml.safe_load(handle) or {}

    if not isinstance(payload, dict):
        raise ValueError(f"Settings file must contain a YAML mapping: {resolved_config_path}")

    expanded = _expand_value(payload)
    if not isinstance(expanded, dict):
        raise ValueError(f"Expanded settings file must contain a YAML mapping: {resolved_config_path}")

    return expanded
