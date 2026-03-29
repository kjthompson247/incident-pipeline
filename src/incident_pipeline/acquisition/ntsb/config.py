from __future__ import annotations

import os
from collections.abc import Mapping
from pathlib import Path

from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator, model_validator

from incident_pipeline.acquisition.ntsb.paths import REPO_ROOT, ProjectPaths, compute_project_paths
from incident_pipeline.common.paths import default_ntsb_source_root

ENV_PREFIX = "NTSB_ACQUIRE_"
DEFAULT_ENV_FILE = ".env"
UNSUPPORTED_PATH_OVERRIDE_KEYS = frozenset({"data_root", "sqlite_path", "downstream_raw_root"})


class AppConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    data_root: Path = Field(default_factory=default_ntsb_source_root)
    sqlite_path: Path | None = None
    downstream_raw_root: Path | None = None
    log_level: str = "INFO"
    http_user_agent: str = "ntsb-acquire/0.1.0"
    http_rate_limit_per_second: float = 1.0
    http_max_retries: int = 3
    http_backoff_seconds: float = 1.0
    carol_base_url: str | None = None
    docket_base_url: str | None = None

    @field_validator("log_level")
    @classmethod
    def validate_log_level(cls, value: str) -> str:
        normalized = value.upper()
        allowed = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}
        if normalized not in allowed:
            raise ValueError(f"Unsupported log level: {value}")
        return normalized

    @field_validator("carol_base_url", "docket_base_url", mode="before")
    @classmethod
    def empty_string_to_none(cls, value: object) -> object:
        if value == "":
            return None
        return value

    @model_validator(mode="after")
    def apply_derived_path_defaults(self) -> AppConfig:
        if self.sqlite_path is None:
            self.sqlite_path = self.data_root / "acquisition" / "state" / "acquisition.db"
        if self.downstream_raw_root is None:
            self.downstream_raw_root = self.data_root / "raw"
        return self

    @property
    def paths(self) -> ProjectPaths:
        return compute_project_paths(
            data_root=self.data_root,
            sqlite_path=self.sqlite_path,
            downstream_raw_root=self.downstream_raw_root,
            repo_root=REPO_ROOT,
        )


def parse_dotenv_file(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    if not path.exists():
        return values

    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        key, separator, raw_value = line.partition("=")
        if separator != "=":
            continue
        value = raw_value.strip()
        if value[:1] == value[-1:] and value.startswith(("'", '"')):
            value = value[1:-1]
        values[key.strip()] = value
    return values


def _extract_prefixed_values(values: Mapping[str, str]) -> dict[str, str]:
    extracted: dict[str, str] = {}
    for key, value in values.items():
        if not key.startswith(ENV_PREFIX):
            continue
        extracted[key[len(ENV_PREFIX) :].lower()] = value
    return extracted


def _clean_cli_overrides(overrides: Mapping[str, object | None]) -> dict[str, object]:
    cleaned: dict[str, object] = {}
    for key, value in overrides.items():
        if value is None:
            continue
        cleaned[key] = value
    return cleaned


def _reject_path_overrides(values: Mapping[str, object], *, source: str) -> None:
    configured = sorted(key for key in values if key in UNSUPPORTED_PATH_OVERRIDE_KEYS)
    if not configured:
        return
    joined = ", ".join(configured)
    raise ValueError(
        f"Unsupported acquisition path override(s) from {source}: {joined}. "
        "Set INCIDENT_PIPELINE_DATA_ROOT to an absolute storage root and let "
        "acquisition derive its internal paths."
    )


def default_env_file_path(*, repo_root: Path = REPO_ROOT) -> Path:
    return repo_root / DEFAULT_ENV_FILE


def load_config(
    *,
    cli_overrides: Mapping[str, object | None] | None = None,
    env: Mapping[str, str] | None = None,
    env_file: Path | None = None,
) -> AppConfig:
    resolved_env = dict(os.environ if env is None else env)
    dotenv_path = default_env_file_path() if env_file is None else env_file
    dotenv_values = _extract_prefixed_values(parse_dotenv_file(dotenv_path))
    env_values = _extract_prefixed_values(resolved_env)
    cli_values = _clean_cli_overrides(cli_overrides or {})

    _reject_path_overrides(dotenv_values, source=f"{dotenv_path}")
    _reject_path_overrides(env_values, source="environment")
    _reject_path_overrides(cli_values, source="cli_overrides")

    merged: dict[str, object] = {}
    merged.update(dotenv_values)
    merged.update(env_values)
    merged.update(cli_values)
    if "data_root" not in merged:
        merged["data_root"] = str(default_ntsb_source_root(env=resolved_env))

    return AppConfig.model_validate(merged)


def validate_config(
    *,
    cli_overrides: Mapping[str, object | None] | None = None,
    env: Mapping[str, str] | None = None,
    env_file: Path | None = None,
) -> tuple[AppConfig | None, ValidationError | None]:
    try:
        return load_config(cli_overrides=cli_overrides, env=env, env_file=env_file), None
    except ValidationError as error:
        return None, error
