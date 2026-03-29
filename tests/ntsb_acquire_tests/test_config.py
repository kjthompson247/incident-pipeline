from __future__ import annotations

from pathlib import Path

import pytest
from pydantic import ValidationError

from incident_pipeline.acquisition.ntsb.config import load_config


def test_load_config_uses_storage_root_defaults_when_no_overrides(tmp_path: Path) -> None:
    storage_root = tmp_path / "storage"
    config = load_config(
        env={"INCIDENT_PIPELINE_DATA_ROOT": str(storage_root)},
        env_file=Path("/tmp/does-not-exist.env"),
    )

    expected_root = (storage_root / "ntsb").resolve()
    assert config.paths.data_root == expected_root
    assert (
        config.paths.sqlite_path
        == expected_root / "acquisition" / "state" / "acquisition.db"
    )
    assert config.paths.downstream_raw_root == expected_root / "raw"


def test_load_config_honors_cli_env_and_dotenv_precedence(tmp_path: Path) -> None:
    env_file = tmp_path / ".env"
    env_file.write_text(
        "\n".join(
            [
                "NTSB_ACQUIRE_LOG_LEVEL=warning",
                "NTSB_ACQUIRE_HTTP_MAX_RETRIES=4",
            ]
        ),
        encoding="utf-8",
    )
    env = {
        "INCIDENT_PIPELINE_DATA_ROOT": str(tmp_path / "storage"),
        "NTSB_ACQUIRE_HTTP_MAX_RETRIES": "5",
    }

    config = load_config(
        cli_overrides={"http_max_retries": 6},
        env=env,
        env_file=env_file,
    )

    assert config.paths.data_root == ((tmp_path / "storage") / "ntsb").resolve()
    assert config.log_level == "WARNING"
    assert config.http_max_retries == 6


def test_load_config_rejects_invalid_override_values(tmp_path: Path) -> None:
    env_file = tmp_path / ".env"
    env_file.write_text("NTSB_ACQUIRE_HTTP_MAX_RETRIES=not-an-int\n", encoding="utf-8")

    with pytest.raises(ValidationError):
        load_config(
            env={"INCIDENT_PIPELINE_DATA_ROOT": str(tmp_path / "storage")},
            env_file=env_file,
        )


def test_load_config_rejects_path_overrides(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="INCIDENT_PIPELINE_DATA_ROOT"):
        load_config(
            cli_overrides={"data_root": str(tmp_path / "other-root")},
            env={"INCIDENT_PIPELINE_DATA_ROOT": str(tmp_path / "storage")},
            env_file=Path("/tmp/does-not-exist.env"),
        )
