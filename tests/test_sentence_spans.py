from __future__ import annotations

import json
from pathlib import Path
import sqlite3

import yaml

from incident_pipeline.extract.sentence_spans import run_sentence_span_batch, split_sentences


REPO_ROOT = Path(__file__).resolve().parents[1]
SQL_FILE = REPO_ROOT / "sql" / "init_manifest.sql"


def init_manifest_db(db_path: Path) -> None:
    conn = sqlite3.connect(db_path)
    conn.executescript(SQL_FILE.read_text(encoding="utf-8"))
    conn.commit()
    conn.close()


def write_config(config_path: Path, db_path: Path, processed_root: Path) -> None:
    config = {
        "database": {"manifest_path": str(db_path)},
        "paths": {"processed": str(processed_root)},
        "extraction": {"method": "pypdf"},
        "ocr": {"engine": "tesseract"},
        "sentence_span_generation": {
            "output_root": str(processed_root / "sentence_spans"),
            "segmentation_version": "sentence-split-v1",
            "include_context": True,
        },
    }
    config_path.write_text(yaml.safe_dump(config), encoding="utf-8")


def insert_document(
    db_path: Path,
    *,
    doc_id: str,
    docket_item_id: str,
    ntsb_number: str,
    extracted_text_path: Path,
    stage: str = "extraction",
    sha256: str = "sha-doc",
) -> None:
    conn = sqlite3.connect(db_path)
    conn.execute(
        """
        INSERT INTO documents (
            doc_id,
            docket_item_id,
            ntsb_number,
            doc_type,
            raw_path,
            sha256,
            file_size,
            extracted_text_path,
            status,
            stage,
            ingested_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            doc_id,
            docket_item_id,
            ntsb_number,
            "pdf",
            f"/tmp/{doc_id}.pdf",
            sha256,
            123,
            str(extracted_text_path),
            "completed",
            stage,
            "2026-03-29T12:00:00+00:00",
        ),
    )
    conn.commit()
    conn.close()


def read_json(path: Path) -> dict[str, object]:
    return json.loads(path.read_text(encoding="utf-8"))


def read_jsonl(path: Path) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if line.strip():
            rows.append(json.loads(line))
    return rows


def test_split_sentences_preserves_abbreviations_and_decimals() -> None:
    sentences = split_sentences(
        "Dr. Smith reviewed the U.S. report. The pressure reached 3.5 psi. A. Yes."
    )

    assert sentences == [
        "Dr. Smith reviewed the U.S. report.",
        "The pressure reached 3.5 psi.",
        "A. Yes.",
    ]


def test_run_sentence_span_batch_writes_certified_artifacts(tmp_path: Path) -> None:
    db_path = tmp_path / "manifest.db"
    processed_root = tmp_path / "extract"
    config_path = tmp_path / "settings.yaml"
    text_path = processed_root / "extracted" / "doc-1.txt"
    text_path.parent.mkdir(parents=True, exist_ok=True)
    text_path.write_text(
        "Dr. Smith reviewed the U.S. report. The pressure reached 3.5 psi.\n\n"
        "- First bullet statement.\n"
        "- Second bullet statement.\n",
        encoding="utf-8",
    )
    init_manifest_db(db_path)
    insert_document(
        db_path,
        doc_id="doc-1",
        docket_item_id="ntsb:docket_item:DCA24FM001:1:report",
        ntsb_number="DCA24FM001",
        extracted_text_path=text_path,
    )
    write_config(config_path, db_path, processed_root)

    summary = run_sentence_span_batch(config_path)

    assert summary == {
        "selected": 1,
        "completed": 1,
        "sentences_generated": 4,
        "failed": 0,
    }
    output_root = processed_root / "sentence_spans"
    run_dir = next(path for path in (output_root / "runs").iterdir() if path.is_dir())
    assert (run_dir / "_CERTIFIED").exists()

    run_summary = read_json(run_dir / "sentence_span_run_summary.json")
    assert run_summary["stage_name"] == "sentence_span_generation"
    assert run_summary["validation_status"] == "passed"
    assert run_summary["certification_status"] == "certified"
    assert run_summary["primary_output_count"] == 4

    metrics = read_json(run_dir / "sentence_span_metrics.json")
    assert metrics["artifacts_processed"] == 1
    assert metrics["sentences_generated"] == 4
    assert metrics["failures_count"] == 0
    assert metrics["avg_sentences_per_artifact"] == 4.0

    spans = read_jsonl(run_dir / "sentence_spans.jsonl")
    assert [span["sentence_index"] for span in spans] == [1, 2, 1, 1]
    assert spans[0]["context"]["following_text"] == "The pressure reached 3.5 psi."
    assert spans[1]["context"]["preceding_text"] == "Dr. Smith reviewed the U.S. report."


def test_run_sentence_span_batch_is_deterministic_for_same_input(tmp_path: Path) -> None:
    db_path = tmp_path / "manifest.db"
    processed_root = tmp_path / "extract"
    config_path = tmp_path / "settings.yaml"
    text_path = processed_root / "extracted" / "doc-1.txt"
    text_path.parent.mkdir(parents=True, exist_ok=True)
    text_path.write_text(
        "Inspectors observed coating damage. The coating had failed previously.",
        encoding="utf-8",
    )
    init_manifest_db(db_path)
    insert_document(
        db_path,
        doc_id="doc-1",
        docket_item_id="ntsb:docket_item:DCA24FM002:1:report",
        ntsb_number="DCA24FM002",
        extracted_text_path=text_path,
    )
    write_config(config_path, db_path, processed_root)

    first_summary = run_sentence_span_batch(config_path)
    second_summary = run_sentence_span_batch(config_path)

    assert first_summary == second_summary == {
        "selected": 1,
        "completed": 1,
        "sentences_generated": 2,
        "failed": 0,
    }
    run_dirs = sorted(path for path in ((processed_root / "sentence_spans") / "runs").iterdir() if path.is_dir())
    assert len(run_dirs) == 2
    first_run = read_json(run_dirs[0] / "sentence_span_run_summary.json")
    second_run = read_json(run_dirs[1] / "sentence_span_run_summary.json")
    assert first_run["primary_output_digest"] == second_run["primary_output_digest"]
    assert first_run["primary_output_count"] == second_run["primary_output_count"] == 2


def test_run_sentence_span_batch_records_missing_text_as_failure(tmp_path: Path) -> None:
    db_path = tmp_path / "manifest.db"
    processed_root = tmp_path / "extract"
    config_path = tmp_path / "settings.yaml"
    missing_text_path = processed_root / "extracted" / "missing.txt"
    init_manifest_db(db_path)
    insert_document(
        db_path,
        doc_id="doc-1",
        docket_item_id="ntsb:docket_item:DCA24FM003:1:report",
        ntsb_number="DCA24FM003",
        extracted_text_path=missing_text_path,
    )
    write_config(config_path, db_path, processed_root)

    summary = run_sentence_span_batch(config_path)

    assert summary == {
        "selected": 1,
        "completed": 0,
        "sentences_generated": 0,
        "failed": 1,
    }
    run_dir = next(path for path in ((processed_root / "sentence_spans") / "runs").iterdir() if path.is_dir())
    assert (run_dir / "_FAILED").exists()
    failures = read_jsonl(run_dir / "sentence_span_failures.jsonl")
    assert len(failures) == 1
    assert failures[0]["failure_class"] == "input_contract"
    assert failures[0]["artifact_id"] == "ntsb:docket_item:DCA24FM003:1:report"
