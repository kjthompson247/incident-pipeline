from __future__ import annotations

import json
from pathlib import Path
import sqlite3

import yaml
from typer.testing import CliRunner

from incident_pipeline.common.stage_runs import sha256_file
from incident_pipeline.extract.cli import atomic_extract_app, sentence_spans_app


REPO_ROOT = Path(__file__).resolve().parents[1]
SQL_FILE = REPO_ROOT / "sql" / "init_manifest.sql"
RUNNER = CliRunner()


def namespace_root(storage_root: Path) -> Path:
    return storage_root / "ntsb"


def relpath(path: Path, storage_root: Path) -> str:
    return str(path.relative_to(namespace_root(storage_root)))


def init_manifest_db(db_path: Path) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)
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
    storage_root = config_path.parent
    config = {
        "paths": {
            "storage_root": str(storage_root),
            "storage_namespace": "ntsb",
            "processed": relpath(processed_root, storage_root),
        },
        "database": {"manifest_path": relpath(db_path, storage_root)},
        "extraction": {"method": "pypdf"},
        "ocr": {"engine": "tesseract"},
        "sentence_span_generation": {
            "output_root": relpath(processed_root / "sentence_spans", storage_root),
            "segmentation_version": "sentence-split-v1",
            "include_context": True,
        },
    }
    config_path.write_text(yaml.safe_dump(config), encoding="utf-8")


def write_atomic_config(config_path: Path, sentence_span_root: Path, output_root: Path) -> None:
    storage_root = config_path.parent
    config = {
        "paths": {
            "storage_root": str(storage_root),
            "storage_namespace": "ntsb",
        },
        "atomic_extraction": {
            "output_root": relpath(output_root, storage_root),
            "sentence_span_root": relpath(sentence_span_root, storage_root),
            "batch_size": 2,
            "require_certified_input": True,
            "model_contract_version": "sentence-to-atomic-v1",
            "ontology_version": "minimal-ontology-v0.2",
            "mapping_contract_version": "mapping-contract-v0.1",
        }
    }
    config_path.write_text(yaml.safe_dump(config), encoding="utf-8")


def write_sentence_spans(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
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
            },
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )


def write_certified_sentence_span_run(
    sentence_span_root: Path,
    *,
    run_id: str = "sentence_span_generation_2026-03-29T12:00:00.000000Z_cli",
) -> Path:
    run_dir = sentence_span_root / "runs" / run_id
    sentence_spans_path = run_dir / "sentence_spans.jsonl"
    write_sentence_spans(sentence_spans_path)
    (run_dir / "sentence_span_run_summary.json").write_text(
        json.dumps(
            {
                "run_id": run_id,
                "stage_name": "sentence_span_generation",
                "certification_status": "certified",
                "certified_at": "2026-03-29T12:00:01Z",
                "primary_output_count": 1,
                "primary_output_digest": sha256_file(sentence_spans_path),
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    (run_dir / "_CERTIFIED").write_text("", encoding="utf-8")
    return run_dir


def test_sentence_spans_cli_runs_and_prints_certified_summary(tmp_path: Path) -> None:
    namespaced_root = namespace_root(tmp_path)
    db_path = namespaced_root / "manifest.db"
    processed_root = namespaced_root / "extract"
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
    namespaced_root = namespace_root(tmp_path)
    sentence_span_root = namespaced_root / "extract" / "sentence_spans"
    output_root = namespaced_root / "extract" / "atomic"
    config_path = tmp_path / "settings.yaml"
    write_certified_sentence_span_run(sentence_span_root)
    write_atomic_config(config_path, sentence_span_root, output_root)

    result = RUNNER.invoke(atomic_extract_app, ["--config", str(config_path)])

    assert result.exit_code != 0
    assert "--transformer" in result.output


def test_atomic_extract_cli_runs_with_explicit_transformer(tmp_path: Path, monkeypatch) -> None:
    namespaced_root = namespace_root(tmp_path)
    sentence_span_root = namespaced_root / "extract" / "sentence_spans"
    output_root = namespaced_root / "extract" / "atomic"
    config_path = tmp_path / "settings.yaml"
    write_certified_sentence_span_run(sentence_span_root)
    write_atomic_config(config_path, sentence_span_root, output_root)

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
    assert "upstream_run_id=sentence_span_generation_2026-03-29T12:00:00.000000Z_cli" in result.output
    assert "run_dir=" in result.output


def test_atomic_extract_cli_accepts_explicit_certified_input_override(
    tmp_path: Path,
    monkeypatch,
) -> None:
    namespaced_root = namespace_root(tmp_path)
    sentence_span_root = namespaced_root / "extract" / "sentence_spans"
    output_root = namespaced_root / "extract" / "atomic"
    config_path = tmp_path / "settings.yaml"
    pinned_run_dir = write_certified_sentence_span_run(
        sentence_span_root,
        run_id="sentence_span_generation_2026-03-29T12:00:00.000000Z_pinned",
    )
    write_certified_sentence_span_run(
        sentence_span_root,
        run_id="sentence_span_generation_2026-03-29T12:05:00.000000Z_latest",
    )
    write_atomic_config(config_path, sentence_span_root, output_root)

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
            "ontology_candidates": [],
            "unresolved": [],
            "warnings": [],
        }

    monkeypatch.setattr(
        "incident_pipeline.extract.cli.load_transformer",
        lambda spec: fake_transformer,
    )

    result = RUNNER.invoke(
        atomic_extract_app,
        [
            "--config",
            str(config_path),
            "--transformer",
            "tests.fake:transformer",
            "--input-path",
            str(pinned_run_dir / "sentence_spans.jsonl"),
        ],
    )

    assert result.exit_code == 0
    assert "upstream_run_id=sentence_span_generation_2026-03-29T12:00:00.000000Z_pinned" in result.output
