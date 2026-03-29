from __future__ import annotations

import sqlite3
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import cast

from incident_pipeline.acquisition.ntsb.db import fetch_one, upsert_investigation_current
from incident_pipeline.acquisition.ntsb.http import HttpClient
from incident_pipeline.acquisition.ntsb.identifiers import (
    build_investigation_id,
    investigation_fingerprint,
    normalize_ntsb_number,
)
from incident_pipeline.acquisition.ntsb.manifests import append_jsonl_record
from incident_pipeline.acquisition.ntsb.models import AcquisitionManifestRecord, Investigation
from incident_pipeline.acquisition.ntsb.runtime import RunContext

CAROL_SOURCE = "carol"
CAROL_PARSER_VERSION = "carol_v1"


def _first_string(raw: Mapping[str, object], *keys: str) -> str | None:
    for key in keys:
        value = raw.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return None


def _location_payload(raw: Mapping[str, object]) -> dict[str, object]:
    location_value = raw.get("location") or raw.get("Location")
    if isinstance(location_value, Mapping):
        return {str(key): value for key, value in location_value.items()}

    location: dict[str, object] = {}
    for source_key, target_key in (
        ("city", "city"),
        ("City", "city"),
        ("state", "state"),
        ("State", "state"),
        ("country", "country"),
        ("Country", "country"),
        ("airport", "airport"),
        ("Airport", "airport"),
    ):
        value = raw.get(source_key)
        if value is not None:
            location[target_key] = value
    return location


def _extract_records(payload: object) -> list[Mapping[str, object]]:
    if isinstance(payload, list):
        return [record for record in payload if isinstance(record, Mapping)]

    if isinstance(payload, Mapping):
        for key in ("results", "investigations", "items", "data"):
            value = payload.get(key)
            if isinstance(value, list):
                return [record for record in value if isinstance(record, Mapping)]

    raise ValueError("Unsupported CAROL payload shape")


def normalize_carol_investigation(raw: Mapping[str, object]) -> Investigation:
    ntsb_number_value = _first_string(raw, "ntsbNumber", "NTSBNumber", "caseNumber", "CaseNumber")
    if ntsb_number_value is None:
        raise ValueError("CAROL investigation is missing ntsb_number")

    ntsb_number = normalize_ntsb_number(ntsb_number_value)
    project_id = _first_string(raw, "projectId", "ProjectId", "projectID")
    mode = _first_string(raw, "mode", "Mode", "modal", "Modal")
    event_date = _first_string(raw, "eventDate", "EventDate")
    title = _first_string(raw, "title", "Title")
    case_type = _first_string(raw, "caseType", "CaseType")
    location = _location_payload(raw)
    investigation_id = build_investigation_id(ntsb_number)

    return Investigation(
        investigation_id=investigation_id,
        ntsb_number=ntsb_number,
        project_id=project_id,
        mode=mode,
        event_date=event_date,
        title=title,
        case_type=case_type,
        source_fingerprint=investigation_fingerprint(
            ntsb_number=ntsb_number,
            investigation_id=investigation_id,
            project_id=project_id,
            mode=mode,
            event_date=event_date,
            title=title,
            case_type=case_type,
            location=location,
        ),
        location=location,
        carol_metadata={str(key): value for key, value in raw.items()},
    )


class CarolClient:
    def __init__(self, *, base_url: str, http_client: HttpClient) -> None:
        self._base_url = base_url
        self._http_client = http_client

    def fetch_investigations(self, *, limit: int | None = None) -> list[Mapping[str, object]]:
        params: dict[str, int] | None = {"limit": limit} if limit is not None else None
        payload = self._http_client.get_json(self._base_url, params=params)
        return _extract_records(payload)


@dataclass(frozen=True)
class CarolSyncResult:
    discovered: int
    changed: int
    unchanged: int


def _investigation_changed(
    connection: sqlite3.Connection,
    investigation: Investigation,
) -> bool:
    existing = fetch_one(
        connection,
        "SELECT source_fingerprint FROM investigations_current WHERE ntsb_number = ?",
        (investigation.ntsb_number,),
    )
    if existing is None:
        return True
    existing_fingerprint = cast(str | None, existing["source_fingerprint"])
    return existing_fingerprint != investigation.source_fingerprint


def sync_investigations(
    connection: sqlite3.Connection,
    client: CarolClient,
    *,
    run_context: RunContext,
    manifest_path: Path,
    limit: int | None = None,
) -> CarolSyncResult:
    raw_records = client.fetch_investigations(limit=limit)
    investigations = sorted(
        (normalize_carol_investigation(record) for record in raw_records),
        key=lambda investigation: investigation.ntsb_number,
    )
    changed_count = 0
    unchanged_count = 0
    recorded_at = run_context.started_at

    for investigation in investigations:
        changed = _investigation_changed(connection, investigation)
        upsert_investigation_current(
            connection,
            investigation,
            observed_at=recorded_at,
            run_id=run_context.run_id,
        )
        append_jsonl_record(
            manifest_path,
            AcquisitionManifestRecord(
                run_id=run_context.run_id,
                source=CAROL_SOURCE,
                ntsb_number=investigation.ntsb_number,
                project_id=investigation.project_id,
                action="sync_investigation",
                input_fingerprint=investigation.source_fingerprint,
                parser_version=CAROL_PARSER_VERSION,
                retrieved_at=recorded_at,
                changed=changed,
            ),
        )
        if changed:
            changed_count += 1
        else:
            unchanged_count += 1

    connection.commit()
    return CarolSyncResult(
        discovered=len(investigations),
        changed=changed_count,
        unchanged=unchanged_count,
    )
