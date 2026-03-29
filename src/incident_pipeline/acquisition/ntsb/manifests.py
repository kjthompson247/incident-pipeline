from __future__ import annotations

from pathlib import Path
from uuid import uuid4

from pydantic import BaseModel

from incident_pipeline.acquisition.ntsb.hashing import canonical_json


def new_run_id() -> str:
    return f"run_{uuid4().hex}"


def serialize_manifest_record(record: BaseModel | dict[str, object]) -> str:
    if isinstance(record, BaseModel):
        payload = record.model_dump(mode="json")
        if record.__class__.__name__ == "AcquisitionManifestRecord" and "retrieved_at" in payload:
            payload["recorded_at"] = payload.pop("retrieved_at")
    else:
        payload = record
    return canonical_json(payload)


def append_jsonl_record(path: Path, record: BaseModel | dict[str, object]) -> None:
    serialized = serialize_manifest_record(record)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(serialized)
        handle.write("\n")
