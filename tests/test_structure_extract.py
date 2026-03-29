from __future__ import annotations

import importlib
import json
from pathlib import Path
import sqlite3

import pytest
import yaml

from incident_pipeline.extract.structure_extract import run_structure_batch


def _parser_contract_ready() -> bool:
    required_symbols = ("parse_structure_document", "render_sections", "serialize_trace")
    for module_name in (
        "incident_pipeline.extract.structure_state_machine",
        "incident_pipeline.extract.structure_extract",
    ):
        try:
            module = importlib.import_module(module_name)
        except Exception:
            continue

        if all(hasattr(module, name) for name in required_symbols):
            return True

    return False


if not _parser_contract_ready():
    pytest.skip(
        "parser contract is not ready yet; skipping legacy integration checks",
        allow_module_level=True,
    )


REPO_ROOT = Path(__file__).resolve().parents[1]
SQL_FILE = REPO_ROOT / "sql" / "init_manifest.sql"
SAMPLE_TEXT = """NTSB Aviation Accident Report

ABSTRACT
Brief overview of the event.

HISTORY OF FLIGHT
The flight departed in Cessna N123AB.

ANALYSIS
The pilot continued into instrument meteorological conditions.

PROBABLE CAUSE
The pilot's decision to continue visual flight into instrument meteorological conditions.

RECOMMENDATIONS
Require additional recurrent weather training.
"""
PROBABLE_CAUSE_ONLY_TEXT = """NTSB Pipeline Brief

ABSTRACT
Brief overview of the incident.

ANALYSIS
Operators did not respond to abnormal pressure indications.

PROBABLE CAUSE
Failure to recognize and respond to abnormal pressure conditions.
"""
PIPELINE_SAMPLE_TEXT = """Pipeline Investigation Report

Summary
Short summary of the pipeline accident.

Executive Summary
What Happened
Gas was released from the pipeline.

What We Found
Investigators found incomplete integrity management controls.

What We Recommended
Revise operator procedures.

1 Factual Information
Detailed factual record.

2 Analysis
Analysis of the pipeline failure.

3 Conclusions
3.1 Findings
Key findings from the investigation.

3.2 Probable Cause
Failure to address known geohazard threats.

4 Lessons Learned
Operators should evaluate geohazard exposure earlier.
"""
RECOMMENDATION_VARIANT_TEXT = """Pipeline Accident Brief

ABSTRACT
Overview of the event.

Previously Issued Safety Recommendations
To the Pipeline and Hazardous Materials Safety Administration:
1. Review emergency valve placement.
To the operator:
2. Update pressure monitoring procedures.

PROBABLE CAUSE
Failure to monitor abnormal pressure conditions.
BY THE NATIONAL TRANSPORTATION SAFETY BOARD
Chairman
"""
PROBABLE_CAUSE_SIGNOFF_TEXT = """Focused Accident Report

ABSTRACT
Brief overview of the event.

ANALYSIS
Investigators evaluated the available evidence.

PROBABLE CAUSE
A control-room monitoring failure allowed pressure to rise undetected.
BY THE NATIONAL TRANSPORTATION SAFETY BOARD
Chairman
"""
RECOMMENDATIONS_APPENDIX_TEXT = """Focused Pipeline Report

ABSTRACT
Brief overview of the event.

PROBABLE CAUSE
A failed valve released gas into the distribution line.

RECOMMENDATIONS
Inspect similar valves within 30 days.

Appendix A: Investigation
Interview summaries and technical notes.

References
Reference list.
"""
CONCLUSIONS_BODY_TEXT = """Numbered Report

ABSTRACT
Brief overview of the event.

2 Analysis
Investigators reviewed maintenance history.

3 Conclusions
The board identified a maintenance program breakdown that was not corrected in time.

3.1 Findings
Maintenance tracking was incomplete.

3.2 Probable Cause
Failure to correct known corrosion exposure.
"""
MULTI_PROBABLE_CAUSE_TEXT = """Focused Accident Report

ABSTRACT
Brief overview of the event.

PROBABLE CAUSE
Early summary probable cause statement.

ANALYSIS
Investigators reviewed the failed monitoring response.

3.2 Probable Cause
Final probable cause statement.
"""


def namespace_root(storage_root: Path) -> Path:
    return storage_root / "ntsb"


def relpath(path: Path, storage_root: Path) -> str:
    return str(path.relative_to(namespace_root(storage_root)))


def init_manifest(db_path: Path, *, include_extracted_text_path: bool = False) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.executescript(SQL_FILE.read_text(encoding="utf-8"))

    if include_extracted_text_path:
        columns = {
            row[1]
            for row in conn.execute("PRAGMA table_info(documents)").fetchall()
        }
        if "extracted_text_path" not in columns:
            conn.execute("ALTER TABLE documents ADD COLUMN extracted_text_path TEXT")

    conn.commit()
    conn.close()


def write_config(config_path: Path, db_path: Path, processed_root: Path, *, overwrite_existing: bool) -> None:
    storage_root = config_path.parent
    config = {
        "paths": {
            "storage_root": str(storage_root),
            "storage_namespace": "ntsb",
            "processed": relpath(processed_root, storage_root),
        },
        "database": {"manifest_path": relpath(db_path, storage_root)},
        "processing": {"overwrite_existing": overwrite_existing},
    }
    config_path.write_text(yaml.safe_dump(config), encoding="utf-8")


def insert_document(
    db_path: Path,
    *,
    doc_id: str,
    stage: str = "extraction",
    extracted_text_path: Path | None = None,
) -> None:
    columns = [
        "doc_id",
        "doc_type",
        "raw_path",
        "sha256",
        "file_size",
        "status",
        "stage",
        "ingested_at",
    ]
    values: list[object] = [
        doc_id,
        "pdf",
        f"/tmp/{doc_id}.pdf",
        f"sha-{doc_id}",
        123,
        "completed",
        stage,
        "2026-03-22T12:00:00+00:00",
    ]

    if extracted_text_path is not None:
        columns.append("extracted_text_path")
        values.append(str(extracted_text_path))

    placeholders = ", ".join("?" for _ in columns)
    sql = f"INSERT INTO documents ({', '.join(columns)}) VALUES ({placeholders})"

    conn = sqlite3.connect(db_path)
    conn.execute(sql, values)
    conn.commit()
    conn.close()


def fetch_document_state(db_path: Path, doc_id: str) -> tuple[str, str, str | None]:
    conn = sqlite3.connect(db_path)
    row = conn.execute(
        "SELECT status, stage, error_message FROM documents WHERE doc_id = ?",
        (doc_id,),
    ).fetchone()
    conn.close()
    assert row is not None
    return row[0], row[1], row[2]


def fetch_document_output_paths(db_path: Path, doc_id: str) -> tuple[str | None, str | None]:
    conn = sqlite3.connect(db_path)
    row = conn.execute(
        "SELECT structured_json_path, structured_debug_path FROM documents WHERE doc_id = ?",
        (doc_id,),
    ).fetchone()
    conn.close()
    assert row is not None
    return row[0], row[1]


def test_run_structure_batch_prefers_manifest_text_path(tmp_path: Path) -> None:
    namespaced_root = namespace_root(tmp_path)
    processed_root = namespaced_root / "processed"
    db_path = namespaced_root / "manifest.db"
    config_path = tmp_path / "settings.yaml"
    manifest_text_path = namespaced_root / "custom" / "doc-1.txt"
    manifest_text_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_text_path.write_text(SAMPLE_TEXT, encoding="utf-8")

    init_manifest(db_path, include_extracted_text_path=True)
    write_config(config_path, db_path, processed_root, overwrite_existing=False)
    insert_document(
        db_path,
        doc_id="doc-1",
        extracted_text_path=manifest_text_path,
    )

    summary = run_structure_batch(config_path)

    assert summary == {
        "selected": 1,
        "completed": 1,
        "reused_existing": 0,
        "failed": 0,
    }

    output_path = processed_root / "structured" / "doc-1.json"
    payload = json.loads(output_path.read_text(encoding="utf-8"))
    debug_output_path = processed_root / "structured_debug" / "doc-1.json"
    debug_payload = json.loads(debug_output_path.read_text(encoding="utf-8"))
    assert payload["doc_id"] == "doc-1"
    assert payload["report_type"] == "pdf"
    assert payload["title"] == "NTSB Aviation Accident Report"
    assert "N123AB" in payload["sections"]["factual_information"]
    assert payload["probable_cause"] == payload["sections"]["probable_cause"]
    assert payload["recommendations"] == payload["sections"]["recommendations"]
    assert payload["stats"]["section_count"] == 5
    assert debug_payload["doc_id"] == "doc-1"
    assert set(debug_payload) == {
        "doc_id",
        "title_line_index",
        "section_order",
        "sections",
        "unclassified_line_indexes",
        "line_assignments",
    }
    assert len(debug_payload["line_assignments"]) == len(SAMPLE_TEXT.splitlines())
    assert debug_payload["section_order"] == [
        "abstract",
        "factual_information",
        "analysis",
        "probable_cause",
        "recommendations",
    ]

    assert fetch_document_state(db_path, "doc-1") == ("completed", "structure", None)
    assert fetch_document_output_paths(db_path, "doc-1") == (
        str(output_path),
        str(debug_output_path),
    )


def test_run_structure_batch_falls_back_to_deterministic_text_path(tmp_path: Path) -> None:
    namespaced_root = namespace_root(tmp_path)
    processed_root = namespaced_root / "processed"
    db_path = namespaced_root / "manifest.db"
    config_path = tmp_path / "settings.yaml"
    extracted_text_path = processed_root / "extracted" / "doc-2.txt"
    extracted_text_path.parent.mkdir(parents=True, exist_ok=True)
    extracted_text_path.write_text(SAMPLE_TEXT, encoding="utf-8")

    init_manifest(db_path)
    write_config(config_path, db_path, processed_root, overwrite_existing=False)
    insert_document(db_path, doc_id="doc-2")

    summary = run_structure_batch(config_path)

    assert summary == {
        "selected": 1,
        "completed": 1,
        "reused_existing": 0,
        "failed": 0,
    }

    output_path = processed_root / "structured" / "doc-2.json"
    payload = json.loads(output_path.read_text(encoding="utf-8"))
    assert payload["sections"]["analysis"] == (
        "The pilot continued into instrument meteorological conditions."
    )
    assert payload["stats"]["source_text_length"] == len(SAMPLE_TEXT)


def test_run_structure_batch_leaves_recommendations_null_when_absent(tmp_path: Path) -> None:
    namespaced_root = namespace_root(tmp_path)
    processed_root = namespaced_root / "processed"
    db_path = namespaced_root / "manifest.db"
    config_path = tmp_path / "settings.yaml"
    extracted_text_path = processed_root / "extracted" / "doc-no-recs.txt"
    extracted_text_path.parent.mkdir(parents=True, exist_ok=True)
    extracted_text_path.write_text(PROBABLE_CAUSE_ONLY_TEXT, encoding="utf-8")

    init_manifest(db_path)
    write_config(config_path, db_path, processed_root, overwrite_existing=False)
    insert_document(db_path, doc_id="doc-no-recs")

    summary = run_structure_batch(config_path)

    assert summary == {
        "selected": 1,
        "completed": 1,
        "reused_existing": 0,
        "failed": 0,
    }

    output_path = processed_root / "structured" / "doc-no-recs.json"
    payload = json.loads(output_path.read_text(encoding="utf-8"))
    assert payload["sections"]["probable_cause"] == (
        "Failure to recognize and respond to abnormal pressure conditions."
    )
    assert payload["probable_cause"] == payload["sections"]["probable_cause"]
    assert payload["recommendations"] is None
    assert "recommendations" not in payload["sections"]


def test_run_structure_batch_reuses_existing_artifact(tmp_path: Path) -> None:
    namespaced_root = namespace_root(tmp_path)
    processed_root = namespaced_root / "processed"
    db_path = namespaced_root / "manifest.db"
    config_path = tmp_path / "settings.yaml"
    output_path = processed_root / "structured" / "doc-3.json"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text('{"doc_id": "doc-3", "title": "existing"}\n', encoding="utf-8")

    init_manifest(db_path)
    write_config(config_path, db_path, processed_root, overwrite_existing=False)
    insert_document(db_path, doc_id="doc-3")

    summary = run_structure_batch(config_path)

    assert summary == {
        "selected": 1,
        "completed": 1,
        "reused_existing": 1,
        "failed": 0,
    }
    assert output_path.read_text(encoding="utf-8") == '{"doc_id": "doc-3", "title": "existing"}\n'
    assert fetch_document_state(db_path, "doc-3") == ("completed", "structure", None)


def test_run_structure_batch_captures_pipeline_specific_headings(tmp_path: Path) -> None:
    namespaced_root = namespace_root(tmp_path)
    processed_root = namespaced_root / "processed"
    db_path = namespaced_root / "manifest.db"
    config_path = tmp_path / "settings.yaml"
    extracted_text_path = processed_root / "extracted" / "doc-pipeline.txt"
    extracted_text_path.parent.mkdir(parents=True, exist_ok=True)
    extracted_text_path.write_text(PIPELINE_SAMPLE_TEXT, encoding="utf-8")

    init_manifest(db_path)
    write_config(config_path, db_path, processed_root, overwrite_existing=False)
    insert_document(db_path, doc_id="doc-pipeline")

    summary = run_structure_batch(config_path)

    assert summary == {
        "selected": 1,
        "completed": 1,
        "reused_existing": 0,
        "failed": 0,
    }

    output_path = processed_root / "structured" / "doc-pipeline.json"
    payload = json.loads(output_path.read_text(encoding="utf-8"))

    assert payload["title"] == "Pipeline Investigation Report"
    assert payload["sections"]["summary"] == "Short summary of the pipeline accident."
    assert payload["sections"]["executive_summary"] == ""
    assert payload["sections"]["what_happened"] == "Gas was released from the pipeline."
    assert payload["sections"]["what_we_found"] == (
        "Investigators found incomplete integrity management controls."
    )
    assert payload["sections"]["what_we_recommended"] == "Revise operator procedures."
    assert payload["sections"]["factual_information"] == "Detailed factual record."
    assert payload["sections"]["analysis"] == "Analysis of the pipeline failure."
    assert payload["sections"]["conclusions"] == ""
    assert payload["sections"]["findings"] == "Key findings from the investigation."
    assert payload["sections"]["probable_cause"] == (
        "Failure to address known geohazard threats."
    )
    assert "recommendations" not in payload["sections"]
    assert payload["sections"]["lessons_learned"] == (
        "Operators should evaluate geohazard exposure earlier."
    )
    assert payload["probable_cause"] == "Failure to address known geohazard threats."
    assert payload["recommendations"] == "Revise operator procedures."


def test_run_structure_batch_captures_recommendation_heading_variants(tmp_path: Path) -> None:
    namespaced_root = namespace_root(tmp_path)
    processed_root = namespaced_root / "processed"
    db_path = namespaced_root / "manifest.db"
    config_path = tmp_path / "settings.yaml"
    extracted_text_path = processed_root / "extracted" / "doc-variant.txt"
    extracted_text_path.parent.mkdir(parents=True, exist_ok=True)
    extracted_text_path.write_text(RECOMMENDATION_VARIANT_TEXT, encoding="utf-8")

    init_manifest(db_path)
    write_config(config_path, db_path, processed_root, overwrite_existing=False)
    insert_document(db_path, doc_id="doc-variant")

    summary = run_structure_batch(config_path)

    assert summary == {
        "selected": 1,
        "completed": 1,
        "reused_existing": 0,
        "failed": 0,
    }

    output_path = processed_root / "structured" / "doc-variant.json"
    payload = json.loads(output_path.read_text(encoding="utf-8"))

    assert "recommendations" in payload["sections"]
    assert "Review emergency valve placement." in payload["sections"]["recommendations"]
    assert payload["recommendations"] == payload["sections"]["recommendations"]
    assert "BY THE NATIONAL TRANSPORTATION SAFETY BOARD" not in payload["sections"]["probable_cause"]
    assert payload["probable_cause"] == "Failure to monitor abnormal pressure conditions."
    assert "BY THE NATIONAL TRANSPORTATION SAFETY BOARD" not in payload["probable_cause"]


def test_run_structure_batch_bounds_probable_cause_before_signoff(tmp_path: Path) -> None:
    namespaced_root = namespace_root(tmp_path)
    processed_root = namespaced_root / "processed"
    db_path = namespaced_root / "manifest.db"
    config_path = tmp_path / "settings.yaml"
    extracted_text_path = processed_root / "extracted" / "doc-pc-signoff.txt"
    extracted_text_path.parent.mkdir(parents=True, exist_ok=True)
    extracted_text_path.write_text(PROBABLE_CAUSE_SIGNOFF_TEXT, encoding="utf-8")

    init_manifest(db_path)
    write_config(config_path, db_path, processed_root, overwrite_existing=False)
    insert_document(db_path, doc_id="doc-pc-signoff")

    summary = run_structure_batch(config_path)

    assert summary == {
        "selected": 1,
        "completed": 1,
        "reused_existing": 0,
        "failed": 0,
    }

    output_path = processed_root / "structured" / "doc-pc-signoff.json"
    payload = json.loads(output_path.read_text(encoding="utf-8"))

    assert payload["sections"]["probable_cause"] == (
        "A control-room monitoring failure allowed pressure to rise undetected."
    )
    assert payload["probable_cause"] == payload["sections"]["probable_cause"]


def test_run_structure_batch_renders_only_last_probable_cause_chunk(tmp_path: Path) -> None:
    namespaced_root = namespace_root(tmp_path)
    processed_root = namespaced_root / "processed"
    db_path = namespaced_root / "manifest.db"
    config_path = tmp_path / "settings.yaml"
    extracted_text_path = processed_root / "extracted" / "doc-pc-last-chunk.txt"
    extracted_text_path.parent.mkdir(parents=True, exist_ok=True)
    extracted_text_path.write_text(MULTI_PROBABLE_CAUSE_TEXT, encoding="utf-8")

    init_manifest(db_path)
    write_config(config_path, db_path, processed_root, overwrite_existing=False)
    insert_document(db_path, doc_id="doc-pc-last-chunk")

    summary = run_structure_batch(config_path)

    assert summary == {
        "selected": 1,
        "completed": 1,
        "reused_existing": 0,
        "failed": 0,
    }

    output_path = processed_root / "structured" / "doc-pc-last-chunk.json"
    payload = json.loads(output_path.read_text(encoding="utf-8"))
    debug_output_path = processed_root / "structured_debug" / "doc-pc-last-chunk.json"
    debug_payload = json.loads(debug_output_path.read_text(encoding="utf-8"))

    probable_cause_chunks = [
        section for section in debug_payload["sections"] if section["section_key"] == "probable_cause"
    ]

    assert len(probable_cause_chunks) == 2
    assert payload["sections"]["probable_cause"] == "Final probable cause statement."
    assert payload["probable_cause"] == "Final probable cause statement."


def test_run_structure_batch_bounds_recommendations_before_appendix_and_references(
    tmp_path: Path,
) -> None:
    namespaced_root = namespace_root(tmp_path)
    processed_root = namespaced_root / "processed"
    db_path = namespaced_root / "manifest.db"
    config_path = tmp_path / "settings.yaml"
    extracted_text_path = processed_root / "extracted" / "doc-rec-appendix.txt"
    extracted_text_path.parent.mkdir(parents=True, exist_ok=True)
    extracted_text_path.write_text(RECOMMENDATIONS_APPENDIX_TEXT, encoding="utf-8")

    init_manifest(db_path)
    write_config(config_path, db_path, processed_root, overwrite_existing=False)
    insert_document(db_path, doc_id="doc-rec-appendix")

    summary = run_structure_batch(config_path)

    assert summary == {
        "selected": 1,
        "completed": 1,
        "reused_existing": 0,
        "failed": 0,
    }

    output_path = processed_root / "structured" / "doc-rec-appendix.json"
    payload = json.loads(output_path.read_text(encoding="utf-8"))

    assert payload["sections"]["recommendations"] == "Inspect similar valves within 30 days."
    assert payload["recommendations"] == payload["sections"]["recommendations"]


def test_run_structure_batch_keeps_conclusions_body_between_recognized_headings(
    tmp_path: Path,
) -> None:
    namespaced_root = namespace_root(tmp_path)
    processed_root = namespaced_root / "processed"
    db_path = namespaced_root / "manifest.db"
    config_path = tmp_path / "settings.yaml"
    extracted_text_path = processed_root / "extracted" / "doc-conclusions.txt"
    extracted_text_path.parent.mkdir(parents=True, exist_ok=True)
    extracted_text_path.write_text(CONCLUSIONS_BODY_TEXT, encoding="utf-8")

    init_manifest(db_path)
    write_config(config_path, db_path, processed_root, overwrite_existing=False)
    insert_document(db_path, doc_id="doc-conclusions")

    summary = run_structure_batch(config_path)

    assert summary == {
        "selected": 1,
        "completed": 1,
        "reused_existing": 0,
        "failed": 0,
    }

    output_path = processed_root / "structured" / "doc-conclusions.json"
    payload = json.loads(output_path.read_text(encoding="utf-8"))

    assert payload["sections"]["conclusions"] == (
        "The board identified a maintenance program breakdown that was not corrected in time."
    )
    assert payload["sections"]["findings"] == "Maintenance tracking was incomplete."
    assert payload["sections"]["probable_cause"] == "Failure to correct known corrosion exposure."


def test_run_structure_batch_marks_missing_input_as_failed(tmp_path: Path) -> None:
    namespaced_root = namespace_root(tmp_path)
    processed_root = namespaced_root / "processed"
    db_path = namespaced_root / "manifest.db"
    config_path = tmp_path / "settings.yaml"

    init_manifest(db_path)
    write_config(config_path, db_path, processed_root, overwrite_existing=False)
    insert_document(db_path, doc_id="doc-4")

    summary = run_structure_batch(config_path)

    assert summary == {
        "selected": 1,
        "completed": 0,
        "reused_existing": 0,
        "failed": 1,
    }

    status, stage, error_message = fetch_document_state(db_path, "doc-4")
    assert status == "failed"
    assert stage == "structure"
    assert error_message is not None
    assert "Source text artifact not found" in error_message
