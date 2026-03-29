from __future__ import annotations

from pathlib import Path

import yaml

from incident_pipeline.common.settings import load_settings, resolve_settings_path
from incident_pipeline.ingestion import docket_ingest


def test_load_settings_expands_env_placeholders(tmp_path: Path, monkeypatch) -> None:
    config_path = tmp_path / "settings.yaml"
    config_path.write_text(
        yaml.safe_dump(
            {
                "paths": {
                    "canonical_data_root": "${INCIDENT_PIPELINE_DATA_ROOT:-data}",
                    "source_root": "${INCIDENT_PIPELINE_NTSB_SOURCE_ROOT:-data/ntsb}",
                }
            }
        ),
        encoding="utf-8",
    )

    monkeypatch.setenv("INCIDENT_PIPELINE_DATA_ROOT", "./custom-data")

    settings = load_settings(config_path)

    assert settings["paths"]["canonical_data_root"] == "./custom-data"
    assert settings["paths"]["source_root"] == "data/ntsb"


def test_resolve_settings_path_prefers_environment_override(tmp_path: Path, monkeypatch) -> None:
    config_path = tmp_path / "settings.yaml"
    config_path.write_text("project:\n  name: incident-pipeline\n", encoding="utf-8")

    monkeypatch.setenv("INCIDENT_PIPELINE_SETTINGS_PATH", str(config_path))

    assert resolve_settings_path() == config_path.resolve()


def test_docket_ingest_load_config_uses_environment_settings_path(
    tmp_path: Path, monkeypatch
) -> None:
    config_path = tmp_path / "settings.yaml"
    config_path.write_text(
        yaml.safe_dump(
            {
                "docket_ingest": {
                    "manifest_path": "./manifest.jsonl",
                    "output_root": "./output",
                    "overwrite_existing": False,
                }
            }
        ),
        encoding="utf-8",
    )

    monkeypatch.setenv("INCIDENT_PIPELINE_SETTINGS_PATH", str(config_path))

    config = docket_ingest.load_config()

    assert config["docket_ingest"]["manifest_path"] == "./manifest.jsonl"
