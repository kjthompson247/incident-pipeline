from __future__ import annotations

import json
from pathlib import Path

from incident_pipeline.ingestion.manifest_reader import IngestionRecord


def build_text_path(output_root: Path, docket_item_id: str) -> Path:
    return output_root / "extracted" / f"{docket_item_id}.txt"


def build_metadata_path(output_root: Path, docket_item_id: str) -> Path:
    return output_root / "metadata" / f"{docket_item_id}.json"


def outputs_exist(output_root: Path, docket_item_id: str) -> bool:
    return build_text_path(output_root, docket_item_id).exists() and build_metadata_path(
        output_root, docket_item_id
    ).exists()


def write_text(path: Path, text: str, *, overwrite_existing: bool) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)

    if path.exists() and not overwrite_existing:
        return

    path.write_text(text, encoding="utf-8")


def write_metadata(path: Path, record: IngestionRecord, *, overwrite_existing: bool) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)

    if path.exists() and not overwrite_existing:
        return

    # This JSON file is the governed ingestion stage record consumed by triage.
    payload = {
        "project_id": record.project_id,
        "ntsb_number": record.ntsb_number,
        "docket_item_id": record.docket_item_id,
        "ordinal": record.ordinal,
        "title": record.title,
        "view_url": record.view_url,
        "source_url": record.source_url,
        "blob_sha256": record.blob_sha256,
        "blob_path": record.blob_path,
        "media_type": record.media_type,
    }
    if record.acquisition_run_id is not None:
        payload["acquisition_run_id"] = record.acquisition_run_id
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=True) + "\n", encoding="utf-8")
