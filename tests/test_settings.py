from __future__ import annotations

import json
from pathlib import Path

import pytest
import yaml

from incident_pipeline.common.paths import REPO_ROOT, resolve_repo_path, resolve_storage_path
from incident_pipeline.common.settings import (
    load_settings,
    resolve_settings_path,
    resolve_storage_setting,
    storage_namespace_from_settings,
    storage_root_from_settings,
)
from incident_pipeline.ingestion import docket_ingest
from incident_pipeline.ingestion.pdf_extract import PDFExtractionResult


def write_settings(config_path: Path, payload: dict[str, object]) -> None:
    config_path.write_text(yaml.safe_dump(payload), encoding="utf-8")


def namespace_root(storage_root: Path) -> Path:
    return storage_root / "ntsb"


def test_load_settings_expands_storage_root_and_relative_paths(tmp_path: Path, monkeypatch) -> None:
    config_path = tmp_path / "settings.yaml"
    write_settings(
        config_path,
        {
            "paths": {
                "storage_root": "${INCIDENT_PIPELINE_DATA_ROOT}",
                "storage_namespace": "ntsb",
                "raw": "raw",
                "processed": "extract",
                "manifests": "manifests",
                "logs": "logs",
            },
            "database": {"manifest_path": "manifests/manifest.db"},
            "docket_ingest": {
                "manifest_path": "raw/acquisition/exports/ingestion_manifest_latest.jsonl",
                "output_root": "ingestion",
                "overwrite_existing": False,
            },
        },
    )
    monkeypatch.setenv("INCIDENT_PIPELINE_DATA_ROOT", str(tmp_path / "storage"))

    settings = load_settings(config_path)

    assert settings["paths"]["storage_root"] == str((tmp_path / "storage").resolve())
    assert storage_root_from_settings(settings) == (tmp_path / "storage").resolve()
    assert storage_namespace_from_settings(settings) == Path("ntsb")
    assert resolve_storage_setting(settings, settings["database"]["manifest_path"]) == (
        tmp_path / "storage" / "ntsb" / "manifests" / "manifest.db"
    ).resolve()


def test_load_settings_fails_when_storage_root_is_missing(tmp_path: Path, monkeypatch) -> None:
    config_path = tmp_path / "settings.yaml"
    write_settings(
        config_path,
        {
            "paths": {
                "storage_root": "${INCIDENT_PIPELINE_DATA_ROOT}",
                "storage_namespace": "ntsb",
                "raw": "raw",
            },
            "database": {"manifest_path": "manifests/manifest.db"},
        },
    )
    monkeypatch.delenv("INCIDENT_PIPELINE_DATA_ROOT", raising=False)

    with pytest.raises(ValueError, match="INCIDENT_PIPELINE_DATA_ROOT"):
        load_settings(config_path)


def test_load_settings_fails_when_placeholder_remains_unresolved(tmp_path: Path, monkeypatch) -> None:
    config_path = tmp_path / "settings.yaml"
    write_settings(
        config_path,
        {
            "paths": {
                "storage_root": "${INCIDENT_PIPELINE_DATA_ROOT}",
                "storage_namespace": "ntsb",
                "raw": "${UNSET_RAW_PATH}",
            },
            "database": {"manifest_path": "manifests/manifest.db"},
        },
    )
    monkeypatch.setenv("INCIDENT_PIPELINE_DATA_ROOT", str(tmp_path / "storage"))

    with pytest.raises(ValueError, match="Unresolved environment placeholder"):
        load_settings(config_path)


def test_storage_paths_must_be_relative_to_storage_root(tmp_path: Path, monkeypatch) -> None:
    config_path = tmp_path / "settings.yaml"
    write_settings(
        config_path,
        {
            "paths": {
                "storage_root": "${INCIDENT_PIPELINE_DATA_ROOT}",
                "storage_namespace": "ntsb",
                "raw": str((tmp_path / "absolute-raw").resolve()),
            },
            "database": {"manifest_path": "manifests/manifest.db"},
        },
    )
    monkeypatch.setenv("INCIDENT_PIPELINE_DATA_ROOT", str(tmp_path / "storage"))

    with pytest.raises(ValueError, match="must be relative to paths.storage_root"):
        load_settings(config_path)


def test_resolve_repo_path_uses_implicit_repo_root() -> None:
    assert resolve_repo_path("configs/settings.yaml") == (REPO_ROOT / "configs" / "settings.yaml").resolve()


def test_resolve_storage_path_rejects_absolute_leaf_paths(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="must be relative to storage_root"):
        resolve_storage_path(
            str((tmp_path / "absolute").resolve()),
            storage_root=tmp_path,
            storage_namespace="ntsb",
        )


def test_load_settings_rejects_absolute_storage_namespace(tmp_path: Path, monkeypatch) -> None:
    config_path = tmp_path / "settings.yaml"
    write_settings(
        config_path,
        {
            "paths": {
                "storage_root": "${INCIDENT_PIPELINE_DATA_ROOT}",
                "storage_namespace": str((tmp_path / "ntsb").resolve()),
                "raw": "raw",
            },
            "database": {"manifest_path": "manifests/manifest.db"},
        },
    )
    monkeypatch.setenv("INCIDENT_PIPELINE_DATA_ROOT", str(tmp_path / "storage"))

    with pytest.raises(ValueError, match="paths.storage_namespace"):
        load_settings(config_path)


def test_resolve_settings_path_prefers_environment_override(tmp_path: Path, monkeypatch) -> None:
    config_path = tmp_path / "settings.yaml"
    config_path.write_text("project:\n  name: incident-pipeline\n", encoding="utf-8")

    monkeypatch.setenv("INCIDENT_PIPELINE_SETTINGS_PATH", str(config_path))

    assert resolve_settings_path() == config_path.resolve()


def test_docket_ingest_load_config_uses_environment_settings_path(
    tmp_path: Path, monkeypatch
) -> None:
    config_path = tmp_path / "settings.yaml"
    write_settings(
        config_path,
        {
            "paths": {
                "storage_root": "${INCIDENT_PIPELINE_DATA_ROOT}",
                "storage_namespace": "ntsb",
                "raw": "raw",
                "processed": "extract",
                "manifests": "manifests",
                "logs": "logs",
            },
            "database": {"manifest_path": "manifests/manifest.db"},
            "docket_ingest": {
                "manifest_path": "raw/acquisition/exports/ingestion_manifest_latest.jsonl",
                "output_root": "ingestion",
                "overwrite_existing": False,
            },
        },
    )

    monkeypatch.setenv("INCIDENT_PIPELINE_SETTINGS_PATH", str(config_path))
    monkeypatch.setenv("INCIDENT_PIPELINE_DATA_ROOT", str(tmp_path / "storage"))

    config = docket_ingest.load_config()

    assert config["docket_ingest"]["manifest_path"] == "raw/acquisition/exports/ingestion_manifest_latest.jsonl"


def test_stage_outputs_resolve_under_storage_root(tmp_path: Path, monkeypatch) -> None:
    config_path = tmp_path / "settings.yaml"
    storage_root = tmp_path / "storage"
    namespaced_root = namespace_root(storage_root)
    manifest_path = namespaced_root / "raw" / "acquisition" / "exports" / "ingestion_manifest_latest.jsonl"
    output_root = namespaced_root / "ingestion"
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(
        json.dumps(
            {
                "project_id": "1",
                "ntsb_number": "DCA24FM900",
                "docket_item_id": "ntsb:docket_item:DCA24FM900:1:report",
                "ordinal": 1,
                "title": "Report",
                "view_url": "https://example.test/view",
                "source_url": "https://example.test/source",
                "blob_sha256": "sha-blob",
                "blob_path": str(namespaced_root / "raw" / "blob.pdf"),
                "media_type": "application/pdf",
            }
        )
        + "\n",
        encoding="utf-8",
    )
    write_settings(
        config_path,
        {
            "paths": {
                "storage_root": "${INCIDENT_PIPELINE_DATA_ROOT}",
                "storage_namespace": "ntsb",
                "raw": "raw",
                "processed": "extract",
                "manifests": "manifests",
                "logs": "logs",
            },
            "database": {"manifest_path": "manifests/manifest.db"},
            "docket_ingest": {
                "manifest_path": "raw/acquisition/exports/ingestion_manifest_latest.jsonl",
                "output_root": "ingestion",
                "overwrite_existing": False,
            },
        },
    )
    monkeypatch.setenv("INCIDENT_PIPELINE_DATA_ROOT", str(storage_root))
    monkeypatch.setattr(docket_ingest, "CONFIG_PATH", config_path)
    monkeypatch.setattr(
        docket_ingest,
        "extract_pdf_text_with_warnings",
        lambda path: PDFExtractionResult(text="deterministic text"),
    )
    (namespaced_root / "raw" / "blob.pdf").write_bytes(b"%PDF-1.4\n")

    summary = docket_ingest.run_docket_ingest_batch()

    assert summary["completed"] == 1
    assert (output_root / "metadata").exists()
    assert resolve_storage_setting(
        load_settings(config_path),
        "manifests/manifest.db",
    ) == (namespaced_root / "manifests" / "manifest.db").resolve()
    assert not (REPO_ROOT / "${INCIDENT_PIPELINE_DATA_ROOT}").exists()
