from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path
from typing import Any, Iterator


REQUIRED_FIELDS = (
    "project_id",
    "ntsb_number",
    "docket_item_id",
    "ordinal",
    "title",
    "view_url",
    "source_url",
    "blob_sha256",
    "blob_path",
    "media_type",
)


@dataclass(frozen=True)
class IngestionRecord:
    project_id: str
    ntsb_number: str
    docket_item_id: str
    acquisition_run_id: str | None
    ordinal: int
    title: str
    view_url: str
    source_url: str
    blob_sha256: str
    blob_path: str
    media_type: str

    @classmethod
    def from_mapping(cls, payload: dict[str, Any], *, line_number: int) -> "IngestionRecord":
        missing = [field for field in REQUIRED_FIELDS if field not in payload]
        if missing:
            missing_fields = ", ".join(sorted(missing))
            raise ValueError(f"Manifest line {line_number} is missing required fields: {missing_fields}")

        return cls(
            project_id=_require_str(payload, "project_id", line_number=line_number),
            ntsb_number=_require_str(payload, "ntsb_number", line_number=line_number),
            docket_item_id=_require_str(payload, "docket_item_id", line_number=line_number),
            acquisition_run_id=_optional_str(
                payload,
                "acquisition_run_id",
                line_number=line_number,
            ),
            ordinal=_require_int(payload, "ordinal", line_number=line_number),
            title=_require_str(payload, "title", line_number=line_number),
            view_url=_require_str(payload, "view_url", line_number=line_number),
            source_url=_require_str(payload, "source_url", line_number=line_number),
            blob_sha256=_require_str(payload, "blob_sha256", line_number=line_number),
            blob_path=_require_str(payload, "blob_path", line_number=line_number),
            media_type=_require_str(payload, "media_type", line_number=line_number),
        )


def _require_str(payload: dict[str, Any], field: str, *, line_number: int) -> str:
    value = payload[field]
    if not isinstance(value, str) or not value:
        raise ValueError(f"Manifest line {line_number} field '{field}' must be a non-empty string")
    return value


def _require_int(payload: dict[str, Any], field: str, *, line_number: int) -> int:
    value = payload[field]
    if not isinstance(value, int) or isinstance(value, bool):
        raise ValueError(f"Manifest line {line_number} field '{field}' must be an integer")
    return value


def _optional_str(payload: dict[str, Any], field: str, *, line_number: int) -> str | None:
    if field not in payload:
        return None

    value = payload[field]
    if value is None:
        return None
    if not isinstance(value, str) or not value:
        raise ValueError(f"Manifest line {line_number} field '{field}' must be a non-empty string")
    return value


def iter_manifest_records(manifest_path: Path) -> Iterator[IngestionRecord]:
    with manifest_path.open("r", encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, start=1):
            stripped = line.strip()
            if not stripped:
                continue

            payload = json.loads(stripped)
            if not isinstance(payload, dict):
                raise ValueError(f"Manifest line {line_number} must be a JSON object")

            yield IngestionRecord.from_mapping(payload, line_number=line_number)
