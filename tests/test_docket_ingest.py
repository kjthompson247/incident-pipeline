from __future__ import annotations

import json
from pathlib import Path

import yaml

from incident_pipeline.ingestion import docket_ingest
from incident_pipeline.ingestion.pdf_extract import PDFExtractionResult


def namespace_root(storage_root: Path) -> Path:
    return storage_root / "ntsb"


def relpath(path: Path, storage_root: Path) -> str:
    return str(path.relative_to(namespace_root(storage_root)))


def write_config(config_path: Path, manifest_path: Path, output_root: Path, *, overwrite_existing: bool) -> None:
    storage_root = config_path.parent
    config = {
        "paths": {
            "storage_root": str(storage_root),
            "storage_namespace": "ntsb",
        },
        "docket_ingest": {
            "manifest_path": relpath(manifest_path, storage_root),
            "output_root": relpath(output_root, storage_root),
            "overwrite_existing": overwrite_existing,
        }
    }
    config_path.write_text(yaml.safe_dump(config), encoding="utf-8")


def write_manifest(manifest_path: Path, blob_path: Path, *, docket_item_id: str) -> dict[str, object]:
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    record = {
        "project_id": "53416",
        "ntsb_number": "DCA00FP008",
        "docket_item_id": docket_item_id,
        "ordinal": 1,
        "title": "DOT Accident Report",
        "view_url": "https://example.test/view",
        "source_url": "https://example.test/source",
        "blob_sha256": "abc123",
        "blob_path": str(blob_path),
        "media_type": "application/pdf",
    }
    manifest_path.write_text(json.dumps(record) + "\n", encoding="utf-8")
    return record


def write_manifest_records(manifest_path: Path, records: list[dict[str, object]]) -> None:
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    lines = [json.dumps(record) for record in records]
    manifest_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def test_run_docket_ingest_batch_writes_outputs_and_reuses_existing(
    tmp_path: Path, monkeypatch
) -> None:
    namespaced_root = namespace_root(tmp_path)
    manifest_path = namespaced_root / "manifest.jsonl"
    output_root = namespaced_root / "ingestion"
    config_path = tmp_path / "settings.yaml"
    blob_path = namespaced_root / "blob.pdf"
    docket_item_id = "ntsb:docket_item:DCA00FP008:1:dot_accident_report"

    blob_path.parent.mkdir(parents=True, exist_ok=True)
    blob_path.write_bytes(b"%PDF-1.4\n")
    record = write_manifest(manifest_path, blob_path, docket_item_id=docket_item_id)
    write_config(config_path, manifest_path, output_root, overwrite_existing=False)

    monkeypatch.setattr(docket_ingest, "CONFIG_PATH", config_path)
    monkeypatch.setattr(
        docket_ingest,
        "extract_pdf_text_with_warnings",
        lambda path: PDFExtractionResult(text="deterministic text"),
    )

    first_summary = docket_ingest.run_docket_ingest_batch()

    assert first_summary == {
        "selected": 1,
        "completed": 1,
        "reused_existing": 0,
        "failed": 0,
    }

    text_path = output_root / "extracted" / f"{docket_item_id}.txt"
    metadata_path = output_root / "metadata" / f"{docket_item_id}.json"

    assert text_path.read_text(encoding="utf-8") == "deterministic text"
    assert json.loads(metadata_path.read_text(encoding="utf-8")) == record
    run_dirs = sorted(path for path in (output_root / "runs").iterdir() if path.is_dir())
    assert len(run_dirs) == 1
    run_summary = json.loads((run_dirs[0] / "run_summary.json").read_text(encoding="utf-8"))
    assert run_summary["stage_name"] == "ingestion"
    assert run_summary["validation_status"] == "passed"
    assert run_summary["certification_status"] == "certified"
    assert (run_dirs[0] / "_CERTIFIED").exists()

    def fail_if_called(path: Path) -> str:
        raise AssertionError("extract_pdf_text_with_warnings should not be called when outputs already exist")

    monkeypatch.setattr(docket_ingest, "extract_pdf_text_with_warnings", fail_if_called)

    second_summary = docket_ingest.run_docket_ingest_batch()

    assert second_summary == {
        "selected": 1,
        "completed": 1,
        "reused_existing": 1,
        "failed": 0,
    }


def test_run_docket_ingest_batch_emits_record_scoped_warnings(
    tmp_path: Path, monkeypatch, capsys
) -> None:
    namespaced_root = namespace_root(tmp_path)
    manifest_path = namespaced_root / "manifest.jsonl"
    output_root = namespaced_root / "ingestion"
    config_path = tmp_path / "settings.yaml"
    blob_path = namespaced_root / "warning.pdf"
    docket_item_id = "ntsb:docket_item:DCA00FP008:3:warning"

    blob_path.parent.mkdir(parents=True, exist_ok=True)
    blob_path.write_bytes(b"%PDF-1.4\n")
    write_manifest(manifest_path, blob_path, docket_item_id=docket_item_id)
    write_config(config_path, manifest_path, output_root, overwrite_existing=False)

    monkeypatch.setattr(docket_ingest, "CONFIG_PATH", config_path)
    monkeypatch.setattr(
        docket_ingest,
        "extract_pdf_text_with_warnings",
        lambda path: PDFExtractionResult(
            text="recovered text",
            warnings=("Ignoring wrong pointing object 7 0", "page 2: utf-16-be decode failed"),
        ),
    )

    summary = docket_ingest.run_docket_ingest_batch()
    output = capsys.readouterr().out

    assert summary == {
        "selected": 1,
        "completed": 1,
        "reused_existing": 0,
        "failed": 0,
    }
    assert f"[START] {docket_item_id}" in output
    assert f"[WARN] {docket_item_id}" in output
    assert "Ignoring wrong pointing object 7 0" in output
    assert "page 2: utf-16-be decode failed" in output


def test_run_docket_ingest_batch_continues_after_whole_file_failure(
    tmp_path: Path, monkeypatch, capsys
) -> None:
    namespaced_root = namespace_root(tmp_path)
    manifest_path = namespaced_root / "manifest.jsonl"
    output_root = namespaced_root / "ingestion"
    config_path = tmp_path / "settings.yaml"
    broken_blob_path = namespaced_root / "broken.pdf"
    good_blob_path = namespaced_root / "good.pdf"

    broken_blob_path.parent.mkdir(parents=True, exist_ok=True)
    broken_blob_path.write_bytes(b"%PDF-1.4\n")
    good_blob_path.write_bytes(b"%PDF-1.4\n")

    broken_record = {
        "project_id": "53416",
        "ntsb_number": "DCA00FP008",
        "docket_item_id": "ntsb:docket_item:DCA00FP008:1:broken",
        "ordinal": 1,
        "title": "Broken PDF",
        "view_url": "https://example.test/broken",
        "source_url": "https://example.test/broken",
        "blob_sha256": "broken-sha",
        "blob_path": str(broken_blob_path),
        "media_type": "application/pdf",
    }
    good_record = {
        "project_id": "53416",
        "ntsb_number": "DCA00FP008",
        "docket_item_id": "ntsb:docket_item:DCA00FP008:2:good",
        "ordinal": 2,
        "title": "Good PDF",
        "view_url": "https://example.test/good",
        "source_url": "https://example.test/good",
        "blob_sha256": "good-sha",
        "blob_path": str(good_blob_path),
        "media_type": "application/pdf",
    }

    write_manifest_records(manifest_path, [broken_record, good_record])
    write_config(config_path, manifest_path, output_root, overwrite_existing=False)

    monkeypatch.setattr(docket_ingest, "CONFIG_PATH", config_path)

    def fake_extract(pdf_path: Path) -> PDFExtractionResult:
        if pdf_path == broken_blob_path:
            raise RuntimeError("EOF marker not found")
        return PDFExtractionResult(text="recovered text")

    monkeypatch.setattr(docket_ingest, "extract_pdf_text_with_warnings", fake_extract)

    summary = docket_ingest.run_docket_ingest_batch()
    output = capsys.readouterr().out

    assert summary == {
        "selected": 2,
        "completed": 1,
        "reused_existing": 0,
        "failed": 1,
    }

    broken_text_path = output_root / "extracted" / "ntsb:docket_item:DCA00FP008:1:broken.txt"
    good_text_path = output_root / "extracted" / "ntsb:docket_item:DCA00FP008:2:good.txt"
    good_metadata_path = output_root / "metadata" / "ntsb:docket_item:DCA00FP008:2:good.json"

    assert not broken_text_path.exists()
    assert good_text_path.read_text(encoding="utf-8") == "recovered text"
    assert json.loads(good_metadata_path.read_text(encoding="utf-8")) == good_record
    assert "[START] ntsb:docket_item:DCA00FP008:1:broken" in output
    assert "[ERROR] ntsb:docket_item:DCA00FP008:1:broken: EOF marker not found" in output
    assert "[START] ntsb:docket_item:DCA00FP008:2:good" in output
