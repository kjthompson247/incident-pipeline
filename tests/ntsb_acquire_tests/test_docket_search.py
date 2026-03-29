from __future__ import annotations

import json
from pathlib import Path

import httpx

from incident_pipeline.acquisition.ntsb.config import AppConfig
from incident_pipeline.acquisition.ntsb.db import connect_sqlite, fetch_all, init_db
from incident_pipeline.acquisition.ntsb.docket_search import (
    DocketSearchClient,
    discover_dockets,
    parse_docket_search_results,
)
from incident_pipeline.acquisition.ntsb.http import HttpClient
from incident_pipeline.acquisition.ntsb.runtime import create_run_context

FIXTURE_DIR = Path(__file__).resolve().parent / "fixtures" / "docket_search"


def load_fixture(name: str) -> str:
    return (FIXTURE_DIR / name).read_text(encoding="utf-8")


def make_http_client(*, search_form_html: str, results_html: str) -> HttpClient:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "GET":
            return httpx.Response(200, text=search_form_html, request=request)
        body = request.content.decode("utf-8")
        assert "__RequestVerificationToken=token123" in body
        assert "AccidentDateFrom=2020-01-01" in body
        assert "AccidentDateTo=2026-03-26" in body
        assert "StateRegion=AZ" in body
        assert "Mode=Pipeline" in body
        assert "IncludeClosed=true" not in body
        return httpx.Response(200, text=results_html, request=request)

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


def test_parse_docket_search_results_extracts_project_ids_and_metadata() -> None:
    html = load_fixture("pipeline_results.html")

    records = parse_docket_search_results(
        html=html,
        public_url="https://data.ntsb.gov/Docket/Forms/searchdocket",
    )

    assert [record.ntsb_number for record in records] == ["PLD21FR003", "DCA17FP006"]
    assert records[0].project_id == "103697"
    assert records[0].result_url == "https://data.ntsb.gov/Docket?ProjectID=103697"
    assert records[0].event_date == "08/15/2021"
    assert records[0].city == "Coolidge"
    assert records[0].state_or_region == "AZ"
    assert records[0].docket_details == "61 Docket Items"
    assert records[0].accident_description == "Rupture and fire"
    assert records[0].source_fingerprint is not None


def test_discover_dockets_writes_manifest_current_state_and_raw_html(tmp_path: Path) -> None:
    sqlite_path = tmp_path / "state" / "acquisition.db"
    manifest_path = tmp_path / "manifests" / "docket_search_discovery.jsonl"
    init_db(sqlite_path)

    client = DocketSearchClient(
        search_url="https://example.test/Docket/Forms/searchdocket",
        http_client=make_http_client(
            search_form_html=load_fixture("search_form.html"),
            results_html=load_fixture("pipeline_results.html"),
        ),
    )
    config = AppConfig(data_root=tmp_path / "data", sqlite_path=sqlite_path)
    run_context = create_run_context(config, run_id="run_one")

    with connect_sqlite(sqlite_path) as connection:
        result = discover_dockets(
            connection,
            client,
            run_context=run_context,
            manifest_path=manifest_path,
        )
        rows = fetch_all(
            connection,
            """
            SELECT ntsb_number, project_id, result_url, city, state_or_region
            FROM docket_search_results_current
            ORDER BY ntsb_number
            """,
        )

    manifest_records = [
        json.loads(line)
        for line in manifest_path.read_text(encoding="utf-8").splitlines()
    ]

    assert result.discovered == 2
    assert result.changed == 2
    assert result.unchanged == 0
    assert result.raw_html_path.exists()
    assert [row["ntsb_number"] for row in rows] == ["DCA17FP006", "PLD21FR003"]
    assert rows[0]["project_id"] == "95735"
    assert manifest_records[0]["source"] == "public_docket_search"
    assert manifest_records[0]["action"] == "discover_docket"
    assert manifest_records[0]["recorded_at"] == run_context.started_at
    assert manifest_records[0]["output_path"] == str(result.raw_html_path)
