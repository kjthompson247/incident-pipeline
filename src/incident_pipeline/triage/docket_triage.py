from __future__ import annotations

import json
from pathlib import Path

from incident_pipeline.common.paths import DEFAULT_SETTINGS_PATH, resolve_repo_path
from incident_pipeline.common.settings import load_settings
from incident_pipeline.ingestion.manifest_reader import IngestionRecord
from incident_pipeline.triage.document_type import DocumentTypeInference, infer_document_type

CONFIG_PATH = DEFAULT_SETTINGS_PATH
DEFAULT_TEXT_READ_CHARS = 4000


def load_config(config_path: Path | None = None) -> dict:
    resolved_config_path = config_path or CONFIG_PATH
    effective_path = None if resolved_config_path == DEFAULT_SETTINGS_PATH else resolved_config_path
    return load_settings(effective_path)


def resolve_path(path_value: str) -> Path:
    return resolve_repo_path(path_value)


def make_summary(selected: int = 0) -> dict[str, int]:
    return {
        "selected": selected,
        "completed": 0,
        "reused_existing": 0,
        "failed": 0,
    }


def iter_metadata_paths(metadata_root: Path) -> list[Path]:
    return sorted(metadata_root.glob("*.json"))


def load_metadata_record(metadata_path: Path) -> IngestionRecord:
    with metadata_path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)

    if not isinstance(payload, dict):
        raise ValueError(f"Metadata file must contain a JSON object: {metadata_path}")

    return IngestionRecord.from_mapping(payload, line_number=1)


def ensure_supported_record(record: IngestionRecord, metadata_path: Path) -> None:
    if not record.docket_item_id or Path(record.docket_item_id).name != record.docket_item_id:
        raise ValueError(f"Invalid docket_item_id for output key: {record.docket_item_id}")

    if metadata_path.stem != record.docket_item_id:
        raise ValueError(
            f"Metadata filename does not match docket_item_id for {metadata_path}: "
            f"{record.docket_item_id}"
        )


def build_text_path(text_root: Path, docket_item_id: str) -> Path:
    return text_root / f"{docket_item_id}.txt"


def build_output_path(output_root: Path, docket_item_id: str) -> Path:
    return output_root / f"{docket_item_id}.json"


def read_text_excerpt(text_path: Path, *, char_limit: int) -> str:
    with text_path.open("r", encoding="utf-8", errors="replace") as handle:
        return handle.read(char_limit)


def build_triage_payload(
    record: IngestionRecord,
    inference: DocumentTypeInference,
    *,
    metadata_path: Path,
    text_path: Path,
) -> dict[str, str]:
    return {
        "docket_item_id": record.docket_item_id,
        "ntsb_number": record.ntsb_number,
        "title": record.title,
        "blob_sha256": record.blob_sha256,
        "inferred_document_type": inference.inferred_document_type,
        "inference_basis": inference.inference_basis,
        "source_metadata_path": str(metadata_path),
        "source_text_path": str(text_path),
    }


def write_json_artifact(path: Path, payload: dict[str, str], *, overwrite_existing: bool) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)

    if path.exists() and not overwrite_existing:
        return

    # Triage JSON outputs are stage records, not convenience sidecars.
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=True) + "\n", encoding="utf-8")


def emit_error(docket_item_id: str, message: str) -> None:
    print(f"[ERROR] {docket_item_id}: {message}")


def run_docket_triage_batch(config_path: Path | None = None) -> dict[str, int]:
    cfg = load_config(config_path)
    triage_cfg = cfg["docket_triage"]
    metadata_root = resolve_path(triage_cfg["metadata_root"])
    text_root = resolve_path(triage_cfg["text_root"])
    output_root = resolve_path(triage_cfg["output_root"])
    overwrite_existing = bool(triage_cfg.get("overwrite_existing", False))
    text_read_chars = int(triage_cfg.get("text_read_chars", DEFAULT_TEXT_READ_CHARS))

    if not metadata_root.exists():
        raise FileNotFoundError(f"Triage metadata directory not found: {metadata_root}")

    if not text_root.exists():
        raise FileNotFoundError(f"Triage text directory not found: {text_root}")

    metadata_paths = iter_metadata_paths(metadata_root)
    summary = make_summary(selected=len(metadata_paths))

    for metadata_path in metadata_paths:
        docket_item_id = metadata_path.stem

        try:
            record = load_metadata_record(metadata_path)
            ensure_supported_record(record, metadata_path)
            docket_item_id = record.docket_item_id
            output_path = build_output_path(output_root, docket_item_id)

            if not overwrite_existing and output_path.exists():
                summary["completed"] += 1
                summary["reused_existing"] += 1
                continue

            print(f"[START] {docket_item_id}")
            text_path = build_text_path(text_root, docket_item_id)
            if not text_path.exists():
                raise FileNotFoundError(f"Source text artifact not found: {text_path}")

            text_excerpt = read_text_excerpt(text_path, char_limit=text_read_chars)
            inference = infer_document_type(record.title, text_excerpt)
            payload = build_triage_payload(
                record,
                inference,
                metadata_path=metadata_path,
                text_path=text_path,
            )
            write_json_artifact(output_path, payload, overwrite_existing=overwrite_existing)
            summary["completed"] += 1
        except Exception as exc:
            emit_error(docket_item_id, str(exc))
            summary["failed"] += 1

    return summary
