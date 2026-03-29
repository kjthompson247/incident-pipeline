from __future__ import annotations

import json
from pathlib import Path
import sqlite3

import yaml
from typer.testing import CliRunner

from incident_pipeline.extract.cli import atomic_extract_app, sentence_spans_app


REPO_ROOT = Path(__file__).resolve().parents[1]
SQL_FILE = REPO_ROOT / "sql" / "init_manifest.sql"
RUNNER = CliRunner()


def init_manifest_db(db_path: Path) -> None:
    conn = sqlite3.connect(db_path)
    conn.executescript(SQL_FILE.read_text(encoding="utf-8"))
    conn.commit()
    conn.close()


def insert_document(
    db_path: Path,
    *,
    doc_id: str,
    docket_item_id: str,
    ntsb_number: str,
    extracted_text_path: Path,
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
            f"sha-{doc_id}",
            123,
            str(extracted_text_path),
            "completed",
            "extraction",
            "2026-03-29T12:00:00+00:00",
        ),
    )
    conn.commit()
    conn.close()


def write_sentence_span_config(config_path: Path, db_path: Path, processed_root: Path) -> None:
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


def write_atomic_config(config_path: Path, input_path: Path, output_root: Path) -> None:
    config = {
        "atomic_extraction": {
            "input_path": str(input_path),
            "output_root": str(output_root),
            "batch_size": 2,
            "require_certified_input": False,
            "model_contract_version": "sentence-to-atomic-v1",
            "ontology_version": "minimal-ontology-v0.2",
            "mapping_contract_version": "mapping-contract-v0.1",
        }
    }
    config_path.write_text(yaml.safe_dump(config), encoding="utf-8")


def write_sentence_spans(path: Path) -> None:
    path.write_text(
        json.dumps(
            {
                "sentence_span_id": "span-1",
                "artifact_id": "artifact-1",
                "case_id": "case-1",
                "parent_structural_span_id": "section-1",
                "locator": {"paragraph": 1, "sentence": 1},
                "sentence_text": "The pipe failed because corrosion weakened the wall.",
                "sentence_index": 1,
                "segmentation_version": "sentence-split-v1",
                "provenance": {
                    "artifact_checksum": "sha256:artifact-1",
                    "text_extraction_version": "extract:pypdf",
                    "segmentation_version": "sentence-split-v1",
                },
                "context": {"preceding_text": "", "following_text": ""},
            }
        )
        + "\n",
        encoding="utf-8",
    )


def test_sentence_spans_cli_runs_and_prints_certified_summary(tmp_path: Path) -> None:
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
        docket_item_id="ntsb:docket_item:DCA24FM010:1:report",
        ntsb_number="DCA24FM010",
        extracted_text_path=text_path,
    )
    write_sentence_span_config(config_path, db_path, processed_root)

    result = RUNNER.invoke(sentence_spans_app, ["--config", str(config_path)])

    assert result.exit_code == 0
    assert "stage=sentence_span_generation" in result.output
    assert "certification=certified" in result.output
    assert "run_dir=" in result.output


def test_atomic_extract_cli_requires_explicit_transformer_option(tmp_path: Path) -> None:
    input_path = tmp_path / "sentence_spans.jsonl"
    output_root = tmp_path / "extract" / "atomic"
    config_path = tmp_path / "settings.yaml"
    write_sentence_spans(input_path)
    write_atomic_config(config_path, input_path, output_root)

    result = RUNNER.invoke(atomic_extract_app, ["--config", str(config_path)])

    assert result.exit_code != 0
    assert "--transformer" in result.output


def test_atomic_extract_cli_runs_with_explicit_transformer(tmp_path: Path, monkeypatch) -> None:
    input_path = tmp_path / "sentence_spans.jsonl"
    output_root = tmp_path / "extract" / "atomic"
    config_path = tmp_path / "settings.yaml"
    write_sentence_spans(input_path)
    write_atomic_config(config_path, input_path, output_root)

    def fake_transformer(span):  # type: ignore[no-untyped-def]
        return {
            "sentence_span_id": span.sentence_span_id,
            "status": "ok",
            "atomic_claims": [
                {
                    "atomic_claim_id": "claim-1",
                    "claim_text": span.sentence_text,
                    "assertion_mode": "concluded",
                    "polarity": "affirmed",
                    "claim_type": "conclusion",
                    "needs_review": False,
                }
            ],
            "ontology_candidates": [
                {
                    "candidate_id": "candidate-1",
                    "candidate_type": "causal_factors",
                    "candidate_text": "corrosion weakened the wall",
                    "linked_claim_ids": ["claim-1"],
                    "needs_review": False,
                }
            ],
            "unresolved": [],
            "warnings": [],
        }

    monkeypatch.setattr(
        "incident_pipeline.extract.cli.load_transformer",
        lambda spec: fake_transformer,
    )

    result = RUNNER.invoke(
        atomic_extract_app,
        ["--config", str(config_path), "--transformer", "tests.fake:transformer"],
    )

    assert result.exit_code == 0
    assert "stage=atomic_extract" in result.output
    assert "certification=certified" in result.output
    assert "run_dir=" in result.output
