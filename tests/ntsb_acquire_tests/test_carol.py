from __future__ import annotations

import json
from pathlib import Path

import httpx

from incident_pipeline.acquisition.ntsb.carol import (
    CarolClient,
    normalize_carol_investigation,
    sync_investigations,
)
from incident_pipeline.acquisition.ntsb.config import AppConfig
from incident_pipeline.acquisition.ntsb.db import connect_sqlite, fetch_all, init_db
from incident_pipeline.acquisition.ntsb.http import HttpClient
from incident_pipeline.acquisition.ntsb.runtime import create_run_context

FIXTURE_DIR = Path(__file__).resolve().parent / "fixtures" / "carol"


def load_fixture(name: str) -> object:
    return json.loads((FIXTURE_DIR / name).read_text(encoding="utf-8"))


def make_http_client(payload: object) -> HttpClient:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=payload, request=request)

    transport = httpx.MockTransport(handler)
    client = httpx.Client(transport=transport)
    return HttpClient(
        client,
        rate_limit_per_second=1000.0,
        max_retries=0,
        backoff_seconds=0.0,
        sleeper=lambda _: None,
        monotonic=lambda: 0.0,
    )


def test_normalize_carol_investigation_builds_identity_and_fingerprint() -> None:
    payload = load_fixture("investigations.json")
    raw = payload["results"][0]

    investigation = normalize_carol_investigation(raw)

    assert investigation.investigation_id == "ntsb:investigation:DCA24FM001"
    assert investigation.ntsb_number == "DCA24FM001"
    assert investigation.project_id == "12345"
    assert investigation.location == {"city": "Austin", "state": "TX"}
    assert investigation.source_fingerprint is not None


def test_normalize_carol_investigation_is_stable_across_field_casing() -> None:
    raw = {
        "ntsbNumber": "DCA24FM001",
        "projectId": "12345",
        "mode": "aviation",
        "eventDate": "2024-03-01",
        "title": "Runway Excursion",
        "caseType": "accident",
        "location": {"city": "Austin", "state": "TX"},
    }
    variant = {
        "NTSBNumber": raw["ntsbNumber"],
        "ProjectId": raw["projectId"],
        "Mode": raw["mode"],
        "EventDate": raw["eventDate"],
        "Title": raw["title"],
        "CaseType": raw["caseType"],
        "City": "Austin",
        "State": "TX",
    }

    normalized_raw = normalize_carol_investigation(raw)
    normalized_variant = normalize_carol_investigation(variant)

    assert normalized_raw.ntsb_number == normalized_variant.ntsb_number == "DCA24FM001"
    assert normalized_raw.investigation_id == normalized_variant.investigation_id
    assert normalized_raw.source_fingerprint == normalized_variant.source_fingerprint


def test_sync_investigations_writes_manifest_and_current_state(
    tmp_path: Path,
) -> None:
    sqlite_path = tmp_path / "state" / "acquisition.db"
    manifest_path = tmp_path / "manifests" / "investigation_sync.jsonl"
    init_db(sqlite_path)

    payload = load_fixture("investigations.json")
    http_client = make_http_client(payload)
    carol_client = CarolClient(base_url="https://example.test/carol", http_client=http_client)
    config = AppConfig()
    run_context = create_run_context(config, run_id="run_one")

    with connect_sqlite(sqlite_path) as connection:
        result = sync_investigations(
            connection,
            carol_client,
            run_context=run_context,
            manifest_path=manifest_path,
        )
        rows = fetch_all(
            connection,
            (
                "SELECT ntsb_number, title, source_fingerprint "
                "FROM investigations_current ORDER BY ntsb_number"
            ),
        )

    manifest_records = [
        json.loads(line)
        for line in manifest_path.read_text(encoding="utf-8").splitlines()
    ]

    assert result.discovered == 2
    assert result.changed == 2
    assert result.unchanged == 0
    assert [row["ntsb_number"] for row in rows] == ["DCA24FM001", "DCA24FM002"]
    assert manifest_records[0]["run_id"] == "run_one"
    assert manifest_records[0]["source"] == "carol"
    assert manifest_records[0]["action"] == "sync_investigation"
    assert manifest_records[0]["changed"] is True
    assert manifest_records[0]["recorded_at"] == run_context.started_at
    assert manifest_records[1]["ntsb_number"] == "DCA24FM002"
    assert manifest_records[1]["recorded_at"] == run_context.started_at


def test_sync_investigations_is_idempotent_for_unchanged_payloads(tmp_path: Path) -> None:
    sqlite_path = tmp_path / "state" / "acquisition.db"
    manifest_path = tmp_path / "manifests" / "investigation_sync.jsonl"
    init_db(sqlite_path)

    payload = load_fixture("investigations.json")
    config = AppConfig()
    first_run_context = create_run_context(config, run_id="run_one")
    second_run_context = create_run_context(config, run_id="run_two")

    with connect_sqlite(sqlite_path) as connection:
        first_client = CarolClient(
            base_url="https://example.test/carol",
            http_client=make_http_client(payload),
        )
        first_result = sync_investigations(
            connection,
            first_client,
            run_context=first_run_context,
            manifest_path=manifest_path,
        )

        second_client = CarolClient(
            base_url="https://example.test/carol",
            http_client=make_http_client(payload),
        )
        second_result = sync_investigations(
            connection,
            second_client,
            run_context=second_run_context,
            manifest_path=manifest_path,
        )
        rows = fetch_all(
            connection,
            "SELECT ntsb_number, last_run_id FROM investigations_current ORDER BY ntsb_number",
        )

    manifest_records = [
        json.loads(line)
        for line in manifest_path.read_text(encoding="utf-8").splitlines()
    ]

    assert first_result.changed == 2
    assert second_result.changed == 0
    assert second_result.unchanged == 2
    assert len(rows) == 2
    assert [row["last_run_id"] for row in rows] == ["run_two", "run_two"]
    assert [record["changed"] for record in manifest_records] == [True, True, False, False]
    assert [record["recorded_at"] for record in manifest_records] == [
        first_run_context.started_at,
        first_run_context.started_at,
        second_run_context.started_at,
        second_run_context.started_at,
    ]
