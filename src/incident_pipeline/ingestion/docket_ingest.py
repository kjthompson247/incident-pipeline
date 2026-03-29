from __future__ import annotations

from pathlib import Path

from incident_pipeline.common.paths import DEFAULT_SETTINGS_PATH, resolve_repo_path
from incident_pipeline.common.settings import load_settings
from incident_pipeline.ingestion.manifest_reader import IngestionRecord, iter_manifest_records
from incident_pipeline.ingestion.pdf_extract import extract_pdf_text_with_warnings
from incident_pipeline.ingestion.writer import build_text_path, build_metadata_path, outputs_exist, write_metadata, write_text

CONFIG_PATH = DEFAULT_SETTINGS_PATH


def load_config() -> dict:
    config_path = None if CONFIG_PATH == DEFAULT_SETTINGS_PATH else CONFIG_PATH
    return load_settings(config_path)


def resolve_path(path_value: str) -> Path:
    return resolve_repo_path(path_value)


def make_summary() -> dict[str, int]:
    return {
        "selected": 0,
        "completed": 0,
        "reused_existing": 0,
        "failed": 0,
    }


def ensure_supported_record(record: IngestionRecord) -> None:
    if record.media_type.lower() != "application/pdf":
        raise ValueError(
            f"Unsupported media_type for {record.docket_item_id}: {record.media_type}"
        )

    if not record.docket_item_id or Path(record.docket_item_id).name != record.docket_item_id:
        raise ValueError(f"Invalid docket_item_id for output key: {record.docket_item_id}")


def emit_warning(docket_item_id: str, warning_lines: tuple[str, ...] | list[str]) -> None:
    if not warning_lines:
        return

    print(f"[WARN] {docket_item_id}")
    for line in warning_lines:
        print(f"  {line}")


def emit_error(docket_item_id: str, message: str) -> None:
    print(f"[ERROR] {docket_item_id}: {message}")


def run_docket_ingest_batch() -> dict[str, int]:
    cfg = load_config()
    ingest_cfg = cfg["docket_ingest"]
    manifest_path = resolve_path(ingest_cfg["manifest_path"])
    output_root = resolve_path(ingest_cfg["output_root"])
    overwrite_existing = bool(ingest_cfg.get("overwrite_existing", False))

    if not manifest_path.exists():
        raise FileNotFoundError(f"Ingestion manifest not found: {manifest_path}")

    summary = make_summary()

    for record in iter_manifest_records(manifest_path):
        summary["selected"] += 1

        try:
            ensure_supported_record(record)

            if not overwrite_existing and outputs_exist(output_root, record.docket_item_id):
                summary["completed"] += 1
                summary["reused_existing"] += 1
                continue

            print(f"[START] {record.docket_item_id}")

            blob_path = Path(record.blob_path)
            if not blob_path.exists():
                raise FileNotFoundError(f"Blob not found for {record.docket_item_id}: {blob_path}")

            extraction = extract_pdf_text_with_warnings(blob_path)
            emit_warning(record.docket_item_id, extraction.warnings)
            text_path = build_text_path(output_root, record.docket_item_id)
            metadata_path = build_metadata_path(output_root, record.docket_item_id)

            write_text(text_path, extraction.text, overwrite_existing=overwrite_existing)
            write_metadata(metadata_path, record, overwrite_existing=overwrite_existing)
            summary["completed"] += 1
        except Exception as exc:
            emit_warning(record.docket_item_id, getattr(exc, "warnings", ()))
            emit_error(record.docket_item_id, str(exc))
            summary["failed"] += 1

    return summary
