from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import yaml

from incident_pipeline.extract.pdf_extract import run_extraction_batch
from incident_pipeline.extract.structure_extract import run_structure_batch
from incident_pipeline.ingestion import register_reports


REPO_ROOT = Path(__file__).resolve().parents[1]
SQL_FILE = REPO_ROOT / "sql" / "init_manifest.sql"
SAMPLE_TEXT = """NTSB Aviation Accident Report

ABSTRACT
Brief overview of the event.

ANALYSIS
Investigators reviewed the available evidence.

PROBABLE CAUSE
The pilot's decision to continue visual flight into instrument meteorological conditions.
"""


def init_manifest_db(db_path: Path) -> None:
    conn = sqlite3.connect(db_path)
    conn.executescript(SQL_FILE.read_text(encoding="utf-8"))
    conn.commit()
    conn.close()


def write_register_config(
    config_path: Path,
    *,
    db_path: Path,
    raw_root: Path,
    manifest_path: Path | None = None,
) -> None:
    config = {
        "database": {"manifest_path": str(db_path)},
        "paths": {"raw": str(raw_root)},
        "ingestion": {"allowed_extensions": [".pdf"]},
    }
    if manifest_path is not None:
        config["docket_ingest"] = {"manifest_path": str(manifest_path)}
    config_path.write_text(yaml.safe_dump(config), encoding="utf-8")


def test_register_reports_uses_deterministic_sha_based_doc_id(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    raw_root = tmp_path / "raw"
    raw_root.mkdir(parents=True, exist_ok=True)
    pdf_path = raw_root / "ntsb" / "example.pdf"
    pdf_path.parent.mkdir(parents=True, exist_ok=True)
    pdf_path.write_bytes(b"%PDF-1.4\nexample\n")

    first_db_path = tmp_path / "first.db"
    second_db_path = tmp_path / "second.db"
    init_manifest_db(first_db_path)
    init_manifest_db(second_db_path)

    first_config_path = tmp_path / "first-settings.yaml"
    second_config_path = tmp_path / "second-settings.yaml"
    write_register_config(first_config_path, db_path=first_db_path, raw_root=raw_root)
    write_register_config(second_config_path, db_path=second_db_path, raw_root=raw_root)

    monkeypatch.setattr(register_reports, "CONFIG_PATH", first_config_path)
    register_reports.main()
    capsys.readouterr()

    monkeypatch.setattr(register_reports, "CONFIG_PATH", second_config_path)
    register_reports.main()
    capsys.readouterr()

    with sqlite3.connect(first_db_path) as first_conn:
        first_row = first_conn.execute("SELECT doc_id, sha256 FROM documents").fetchone()
    with sqlite3.connect(second_db_path) as second_conn:
        second_row = second_conn.execute("SELECT doc_id, sha256 FROM documents").fetchone()

    assert first_row is not None
    assert second_row is not None
    assert first_row[1] == second_row[1]
    assert first_row[0] == second_row[0] == f"doc:sha256:{first_row[1]}"


def test_register_reports_preserves_acquisition_lineage_from_manifest_snapshot(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    raw_root = tmp_path / "raw"
    raw_root.mkdir(parents=True, exist_ok=True)
    raw_path = raw_root / "ntsb" / "promoted.pdf"
    raw_path.parent.mkdir(parents=True, exist_ok=True)
    raw_path.write_bytes(b"%PDF-1.4\nlineage\n")
    raw_sha = register_reports.sha256_file(raw_path)

    db_path = tmp_path / "manifest.db"
    init_manifest_db(db_path)

    manifest_path = tmp_path / "acquisition" / "exports" / "ingestion_manifest_run-123.jsonl"
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(
        json.dumps(
            {
                "project_id": "101",
                "ntsb_number": "DCA24FM001",
                "docket_item_id": "ntsb:docket_item:DCA24FM001:1:operations_report",
                "acquisition_run_id": "run-123",
                "ordinal": 1,
                "title": "Operations Report",
                "view_url": "https://example.test/view/operations",
                "source_url": "https://example.test/operations.pdf",
                "blob_sha256": raw_sha,
                "blob_path": str(tmp_path / "acquisition" / "blobs" / "sha256" / raw_sha),
                "media_type": "application/pdf",
            }
        )
        + "\n",
        encoding="utf-8",
    )

    config_path = tmp_path / "settings.yaml"
    write_register_config(
        config_path,
        db_path=db_path,
        raw_root=raw_root,
        manifest_path=manifest_path,
    )

    monkeypatch.setattr(register_reports, "CONFIG_PATH", config_path)
    register_reports.main()
    capsys.readouterr()

    with sqlite3.connect(db_path) as conn:
        row = conn.execute(
            """
            SELECT
                doc_id,
                source_url,
                acquisition_run_id,
                acquisition_manifest_path,
                project_id,
                ntsb_number,
                docket_item_id,
                view_url,
                blob_sha256,
                sha256
            FROM documents
            """
        ).fetchone()

    assert row is not None
    assert row[0] == f"doc:sha256:{raw_sha}"
    assert row[1] == "https://example.test/operations.pdf"
    assert row[2] == "run-123"
    assert row[3] == str(manifest_path.resolve())
    assert row[4] == "101"
    assert row[5] == "DCA24FM001"
    assert row[6] == "ntsb:docket_item:DCA24FM001:1:operations_report"
    assert row[7] == "https://example.test/view/operations"
    assert row[8] == raw_sha
    assert row[9] == raw_sha


def test_register_reports_backfills_lineage_for_existing_documents(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    raw_root = tmp_path / "raw"
    raw_root.mkdir(parents=True, exist_ok=True)
    raw_path = raw_root / "ntsb" / "promoted.pdf"
    raw_path.parent.mkdir(parents=True, exist_ok=True)
    raw_path.write_bytes(b"%PDF-1.4\nlineage-backfill\n")
    raw_sha = register_reports.sha256_file(raw_path)

    db_path = tmp_path / "manifest.db"
    init_manifest_db(db_path)

    first_config_path = tmp_path / "first-settings.yaml"
    write_register_config(first_config_path, db_path=db_path, raw_root=raw_root)
    monkeypatch.setattr(register_reports, "CONFIG_PATH", first_config_path)
    register_reports.main()
    capsys.readouterr()

    manifest_path = tmp_path / "acquisition" / "exports" / "ingestion_manifest_run-456.jsonl"
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(
        json.dumps(
            {
                "project_id": "202",
                "ntsb_number": "DCA24FM002",
                "docket_item_id": "ntsb:docket_item:DCA24FM002:2:systems_report",
                "acquisition_run_id": "run-456",
                "ordinal": 2,
                "title": "Systems Report",
                "view_url": "https://example.test/view/systems",
                "source_url": "https://example.test/systems.pdf",
                "blob_sha256": raw_sha,
                "blob_path": str(tmp_path / "acquisition" / "blobs" / "sha256" / raw_sha),
                "media_type": "application/pdf",
            }
        )
        + "\n",
        encoding="utf-8",
    )

    second_config_path = tmp_path / "second-settings.yaml"
    write_register_config(
        second_config_path,
        db_path=db_path,
        raw_root=raw_root,
        manifest_path=manifest_path,
    )

    monkeypatch.setattr(register_reports, "CONFIG_PATH", second_config_path)
    register_reports.main()
    second_output = capsys.readouterr().out

    with sqlite3.connect(db_path) as conn:
        row = conn.execute(
            """
            SELECT
                source_url,
                acquisition_run_id,
                acquisition_manifest_path,
                project_id,
                ntsb_number,
                docket_item_id,
                view_url,
                blob_sha256
            FROM documents
            """
        ).fetchone()

    assert row is not None
    assert row[0] == "https://example.test/systems.pdf"
    assert row[1] == "run-456"
    assert row[2] == str(manifest_path.resolve())
    assert row[3] == "202"
    assert row[4] == "DCA24FM002"
    assert row[5] == "ntsb:docket_item:DCA24FM002:2:systems_report"
    assert row[6] == "https://example.test/view/systems"
    assert row[7] == raw_sha
    assert "Lineage backfilled:     1" in second_output


def test_extract_and_structure_record_output_paths_in_manifest_db(
    tmp_path: Path,
    monkeypatch,
) -> None:
    db_path = tmp_path / "manifest.db"
    processed_root = tmp_path / "processed"
    raw_path = tmp_path / "raw.pdf"
    raw_path.write_bytes(b"%PDF-1.4\n")
    init_manifest_db(db_path)

    conn = sqlite3.connect(db_path)
    conn.execute(
        """
        INSERT INTO documents (
            doc_id,
            doc_type,
            raw_path,
            sha256,
            file_size,
            status,
            stage,
            ingested_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            "doc-1",
            "pdf",
            str(raw_path),
            "sha-doc-1",
            raw_path.stat().st_size,
            "pending",
            "registration",
            "2026-03-22T12:00:00+00:00",
        ),
    )
    conn.commit()
    conn.close()

    config_path = tmp_path / "settings.yaml"
    config_path.write_text(
        yaml.safe_dump(
            {
                "database": {"manifest_path": str(db_path)},
                "paths": {"processed": str(processed_root)},
                "processing": {"overwrite_existing": False},
                "extraction": {"min_text_threshold": 0},
                "ocr": {"enabled": False},
            }
        ),
        encoding="utf-8",
    )

    monkeypatch.setattr(
        "incident_pipeline.extract.pdf_extract.extract_pdf_text",
        lambda path: SAMPLE_TEXT,
    )
    monkeypatch.setattr("incident_pipeline.extract.pdf_extract.CONFIG_PATH", config_path)
    monkeypatch.setattr("incident_pipeline.extract.structure_extract.CONFIG_PATH", config_path)

    extraction_summary = run_extraction_batch()
    structure_summary = run_structure_batch(config_path)

    assert extraction_summary == {
        "selected": 1,
        "completed": 1,
        "queued_for_ocr": 0,
        "reused_existing": 0,
        "failed": 0,
    }
    assert structure_summary == {
        "selected": 1,
        "completed": 1,
        "reused_existing": 0,
        "failed": 0,
    }

    extracted_text_path = processed_root / "extracted" / "doc-1.txt"
    structured_json_path = processed_root / "structured" / "doc-1.json"
    structured_debug_path = processed_root / "structured_debug" / "doc-1.json"

    with sqlite3.connect(db_path) as conn:
        row = conn.execute(
            """
            SELECT
                status,
                stage,
                extracted_text_path,
                structured_json_path,
                structured_debug_path
            FROM documents
            WHERE doc_id = ?
            """,
            ("doc-1",),
        ).fetchone()

    assert row is not None
    assert row[0] == "completed"
    assert row[1] == "structure"
    assert row[2] == str(extracted_text_path)
    assert row[3] == str(structured_json_path)
    assert row[4] == str(structured_debug_path)
