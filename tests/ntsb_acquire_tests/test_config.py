from __future__ import annotations

from pathlib import Path

import pytest
from pydantic import ValidationError

from incident_pipeline.common.paths import DEFAULT_NTSB_SOURCE_ROOT
from incident_pipeline.acquisition.ntsb.config import load_config
from incident_pipeline.acquisition.ntsb.paths import REPO_ROOT


def test_load_config_uses_repo_defaults_when_no_overrides() -> None:
    config = load_config(env={}, env_file=Path("/tmp/does-not-exist.env"))

    assert config.paths.data_root == (REPO_ROOT / "data" / "ntsb").resolve()
    assert config.paths.data_root == DEFAULT_NTSB_SOURCE_ROOT
    assert (
        config.paths.sqlite_path
        == DEFAULT_NTSB_SOURCE_ROOT / "acquisition" / "state" / "acquisition.db"
    )
    assert config.paths.downstream_raw_root == DEFAULT_NTSB_SOURCE_ROOT / "raw"


def test_load_config_honors_cli_env_and_dotenv_precedence(tmp_path: Path) -> None:
    env_file = tmp_path / ".env"
    env_file.write_text(
        "\n".join(
            [
                "NTSB_ACQUIRE_DATA_ROOT=./dotenv-data",
                "NTSB_ACQUIRE_LOG_LEVEL=warning",
                "NTSB_ACQUIRE_HTTP_MAX_RETRIES=4",
            ]
        ),
        encoding="utf-8",
    )
    env = {
        "NTSB_ACQUIRE_DATA_ROOT": "./env-data",
        "NTSB_ACQUIRE_HTTP_MAX_RETRIES": "5",
    }

    config = load_config(
        cli_overrides={"data_root": "./cli-data", "http_max_retries": 6},
        env=env,
        env_file=env_file,
    )

    assert config.paths.data_root == (REPO_ROOT / "cli-data").resolve()
    assert config.log_level == "WARNING"
    assert config.http_max_retries == 6


def test_load_config_rejects_invalid_override_values(tmp_path: Path) -> None:
    env_file = tmp_path / ".env"
    env_file.write_text("NTSB_ACQUIRE_HTTP_MAX_RETRIES=not-an-int\n", encoding="utf-8")

    with pytest.raises(ValidationError):
        load_config(env={}, env_file=env_file)
