from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import httpx

from incident_pipeline.acquisition.ntsb.config import AppConfig
from incident_pipeline.acquisition.ntsb.db import (
    connect_sqlite,
    fetch_all,
    init_db,
    upsert_docket_search_result_current,
    upsert_investigation_current,
)
from incident_pipeline.acquisition.ntsb.dockets import (
    DocketClient,
    enumerate_docket,
    parse_docket_html,
    parse_docket_html_result,
)
from incident_pipeline.acquisition.ntsb.http import HttpClient
from incident_pipeline.acquisition.ntsb.identifiers import build_investigation_id
from incident_pipeline.acquisition.ntsb.models import DocketSearchResult, Investigation
from incident_pipeline.acquisition.ntsb.runtime import create_run_context

FIXTURE_DIR = Path(__file__).resolve().parent / "fixtures" / "dockets"


def load_fixture(name: str) -> str:
    return (FIXTURE_DIR / name).read_text(encoding="utf-8")


def make_http_client(html: str) -> HttpClient:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, text=html, request=request)

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


def seed_investigation(
    connection: sqlite3.Connection,
    *,
    ntsb_number: str,
    project_id: str,
) -> None:
    upsert_investigation_current(
        connection,
        Investigation(
            investigation_id=build_investigation_id(ntsb_number),
            ntsb_number=ntsb_number,
            project_id=project_id,
        ),
        observed_at="2026-03-26T00:00:00Z",
        run_id="seed_run",
    )


def seed_docket_search_result(
    connection: sqlite3.Connection,
    *,
    ntsb_number: str,
    project_id: str,
) -> None:
    upsert_docket_search_result_current(
        connection,
        DocketSearchResult(
            ntsb_number=ntsb_number,
            project_id=project_id,
            result_url=f"https://example.test/Docket?ProjectID={project_id}",
        ),
        observed_at="2026-03-26T00:00:00Z",
        run_id="seed_run",
    )


def test_parse_docket_html_happy_path_extracts_metadata_and_sorted_items() -> None:
    html = load_fixture("happy_path.html")

    parsed = parse_docket_html_result(
        ntsb_number="DCA24FM001",
        html=html,
        public_url="https://example.test/dockets/DCA24FM001",
    )
    docket, items = parsed.docket, parsed.items

    assert docket.docket_id == "ntsb:docket:DCA24FM001"
    assert docket.item_count == 2
    assert parsed.ordinal_is_stable is True
    assert docket.creation_date == "2024-03-01"
    assert docket.last_modified == "2024-03-05"
    assert docket.public_release_at == "2024-03-06"
    assert [item.ordinal for item in items] == [1, 2]
    assert items[0].title == "Operations Group Chairman Factual Report"
    assert items[0].download_url == "https://example.test/docs/operations.pdf"


def test_parse_docket_html_uses_normalized_metadata_for_fingerprints() -> None:
    html = load_fixture("happy_path.html")
    whitespace_variant = html.replace(
        "<td>2</td>",
        "<td>\n          2\n        </td>",
    ).replace(
        "Operations Group Chairman Factual Report",
        "Operations   Group   Chairman   Factual   Report",
    )

    first = parse_docket_html_result(
        ntsb_number="DCA24FM001",
        html=html,
        public_url="https://example.test/dockets/DCA24FM001",
    )
    second = parse_docket_html_result(
        ntsb_number="DCA24FM001",
        html=whitespace_variant,
        public_url="https://example.test/dockets/DCA24FM001",
    )

    assert first.ordinal_is_stable is True
    assert first.docket.source_fingerprint == second.docket.source_fingerprint
    assert [item.source_fingerprint for item in first.items] == [
        item.source_fingerprint for item in second.items
    ]


def test_parse_docket_html_tolerates_missing_fields_without_identity_drift() -> None:
    html = load_fixture("missing_fields.html")

    parsed = parse_docket_html_result(
        ntsb_number="DCA24FM001",
        html=html,
        public_url="https://example.test/dockets/DCA24FM001",
    )
    docket, items = parsed.docket, parsed.items

    assert docket.item_count == 2
    assert parsed.ordinal_is_stable is False
    assert [item.ordinal for item in items] == [1, 2]
    assert items[0].display_type is None
    assert items[1].docket_item_id == "ntsb:docket_item:DCA24FM001:2:interview_transcript"


def test_parse_docket_html_filters_noise_rows() -> None:
    html = load_fixture("noise.html")

    _, items = parse_docket_html(
        ntsb_number="DCA24FM001",
        html=html,
        public_url="https://example.test/dockets/DCA24FM001",
    )

    assert len(items) == 1
    assert items[0].title == "Factual Attachment"


def test_parse_docket_html_result_parses_public_docket_page_rows() -> None:
    payload = """
    <html>
      <body>
        <h2>Project Summary: Pipeline Investigation - 61 Docket Items - PLD21FR003</h2>
        <table>
          <tr>
            <th>#</th>
            <th>Title</th>
            <th>Pgs</th>
            <th>Photo</th>
            <th>Type</th>
            <th>File</th>
          </tr>
          <tr>
            <td>2</td>
            <td>PRELIMINARY REPORT</td>
            <td>2</td>
            <td>0</td>
            <td>Text/Image</td>
            <td>
              <a
                href="/Docket/Document/docBLOB?ID=13770445&FileExtension=pdf&FileName=Coolidge+AZ+Preliminary+Report-Rel.pdf"
              >View</a>
            </td>
          </tr>
          <tr>
            <td>1</td>
            <td>SURVIVAL FACTORS FACTUAL REPORT OF INVESTIGATION</td>
            <td>23</td>
            <td>0</td>
            <td>Text/Image</td>
            <td>
              <a
                href="/Docket/Document/docBLOB?ID=13770446&FileExtension=pdf&FileName=Survival+Factors+Factual+Report.pdf"
              >View</a>
            </td>
          </tr>
        </table>
      </body>
    </html>
    """

    parsed = parse_docket_html_result(
        ntsb_number="PLD21FR003",
        html=payload,
        public_url="https://example.test/Docket?ProjectID=103697",
    )

    assert parsed.docket.item_count == 2
    assert parsed.ordinal_is_stable is True
    assert [item.ordinal for item in parsed.items] == [1, 2]
    assert [item.title for item in parsed.items] == [
        "SURVIVAL FACTORS FACTUAL REPORT OF INVESTIGATION",
        "PRELIMINARY REPORT",
    ]
    assert parsed.items[0].download_url == (
        "https://example.test/Docket/Document/docBLOB"
        "?ID=13770446&FileExtension=pdf&FileName=Survival+Factors+Factual+Report.pdf"
    )
    assert parsed.items[0].page_count == 23
    assert parsed.items[0].display_type == "Text/Image"


def test_docket_client_uses_public_project_query_url() -> None:
    client = DocketClient(
        base_url="https://example.test/Docket",
        http_client=make_http_client(""),
    )
    assert client.docket_url("103697") == "https://example.test/Docket?ProjectID=103697"


def test_enumerate_docket_falls_back_to_docket_search_project_id(tmp_path: Path) -> None:
    sqlite_path = tmp_path / "state" / "acquisition.db"
    manifest_path = tmp_path / "manifests" / "docket_enumeration.jsonl"
    init_db(sqlite_path)

    html = load_fixture("happy_path.html")
    requested_urls: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requested_urls.append(str(request.url))
        return httpx.Response(200, text=html, request=request)

    config = AppConfig(
        data_root=tmp_path / "data",
        sqlite_path=sqlite_path,
        downstream_raw_root=tmp_path / "corpus" / "raw",
        docket_base_url="https://example.test/Docket",
    )
    run_context = create_run_context(config, run_id="run_one")
    transport = httpx.MockTransport(handler)
    client = DocketClient(
        base_url="https://example.test/Docket",
        http_client=HttpClient(
            httpx.Client(transport=transport),
            rate_limit_per_second=1000.0,
            max_retries=0,
            backoff_seconds=0.0,
            sleeper=lambda _: None,
            monotonic=lambda: 0.0,
        ),
    )

    with connect_sqlite(sqlite_path) as connection:
        seed_docket_search_result(connection, ntsb_number="DCA24FM001", project_id="103697")
        result = enumerate_docket(
            connection,
            client,
            ntsb_number="DCA24FM001",
            run_context=run_context,
            manifest_path=manifest_path,
        )

    manifest_records = [
        json.loads(line)
        for line in manifest_path.read_text(encoding="utf-8").splitlines()
    ]

    assert result.item_count == 2
    assert requested_urls == ["https://example.test/Docket?ProjectID=103697"]
    assert manifest_records[0]["project_id"] == "103697"


def test_enumerate_docket_suppresses_mutation_for_unstable_ordinals(tmp_path: Path) -> None:
    sqlite_path = tmp_path / "state" / "acquisition.db"
    manifest_path = tmp_path / "manifests" / "docket_enumeration.jsonl"
    init_db(sqlite_path)

    stable_html = load_fixture("happy_path.html")
    unstable_html = load_fixture("unstable_ordinals.html")
    config = AppConfig(
        data_root=tmp_path / "data",
        sqlite_path=sqlite_path,
        downstream_raw_root=tmp_path / "corpus" / "raw",
        docket_base_url="https://example.test/Docket",
    )
    first_run_context = create_run_context(config, run_id="run_one")
    second_run_context = create_run_context(config, run_id="run_two")

    with connect_sqlite(sqlite_path) as connection:
        seed_investigation(connection, ntsb_number="DCA24FM001", project_id="103697")
        stable_result = enumerate_docket(
            connection,
            DocketClient(
                base_url="https://example.test/Docket",
                http_client=make_http_client(stable_html),
            ),
            ntsb_number="DCA24FM001",
            run_context=first_run_context,
            manifest_path=manifest_path,
        )
        rows_before = fetch_all(
            connection,
            (
                "SELECT docket_item_id, ordinal, title, is_active "
                "FROM docket_items_current ORDER BY ordinal"
            ),
        )
        unstable_result = enumerate_docket(
            connection,
            DocketClient(
                base_url="https://example.test/Docket",
                http_client=make_http_client(unstable_html),
            ),
            ntsb_number="DCA24FM001",
            run_context=second_run_context,
            manifest_path=manifest_path,
        )
        rows_after = fetch_all(
            connection,
            (
                "SELECT docket_item_id, ordinal, title, is_active "
                "FROM docket_items_current ORDER BY ordinal"
            ),
        )

    manifest_records = [
        json.loads(line)
        for line in manifest_path.read_text(encoding="utf-8").splitlines()
    ]

    assert stable_result.item_count == 2
    assert stable_result.ordinal_is_stable is True
    assert unstable_result.item_count == 2
    assert unstable_result.ordinal_is_stable is False
    assert unstable_result.mutation_suppressed is True
    assert unstable_result.changed is True
    assert rows_before == rows_after
    assert [row["is_active"] for row in rows_after] == [1, 1]
    assert [record["changed"] for record in manifest_records] == [True, True]
    assert "ordinal_stability=stable" in manifest_records[0]["note"]
    assert "ordinal_stability=unstable" in manifest_records[1]["note"]
    assert "current_state_mutation=suppressed" in manifest_records[1]["note"]
    assert [record["recorded_at"] for record in manifest_records] == [
        first_run_context.started_at,
        second_run_context.started_at,
    ]


def test_enumerate_docket_is_stable_across_reruns(tmp_path: Path) -> None:
    sqlite_path = tmp_path / "state" / "acquisition.db"
    manifest_path = tmp_path / "manifests" / "docket_enumeration.jsonl"
    init_db(sqlite_path)

    html = load_fixture("happy_path.html")
    config = AppConfig(
        data_root=tmp_path / "data",
        sqlite_path=sqlite_path,
        downstream_raw_root=tmp_path / "corpus" / "raw",
        docket_base_url="https://example.test/Docket",
    )
    first_run_context = create_run_context(config, run_id="run_one")
    second_run_context = create_run_context(config, run_id="run_two")

    with connect_sqlite(sqlite_path) as connection:
        seed_investigation(connection, ntsb_number="DCA24FM001", project_id="103697")
        first_result = enumerate_docket(
            connection,
            DocketClient(
                base_url="https://example.test/Docket",
                http_client=make_http_client(html),
            ),
            ntsb_number="DCA24FM001",
            run_context=first_run_context,
            manifest_path=manifest_path,
        )
        second_result = enumerate_docket(
            connection,
            DocketClient(
                base_url="https://example.test/Docket",
                http_client=make_http_client(html),
            ),
            ntsb_number="DCA24FM001",
            run_context=second_run_context,
            manifest_path=manifest_path,
        )
        rows = fetch_all(
            connection,
            (
                "SELECT docket_item_id, ordinal, last_run_id, is_active "
                "FROM docket_items_current ORDER BY ordinal"
            ),
        )
        docket_rows = fetch_all(
            connection,
            "SELECT raw_html_path, last_run_id FROM dockets_current WHERE ntsb_number = ?",
            ("DCA24FM001",),
        )

    manifest_records = [
        json.loads(line)
        for line in manifest_path.read_text(encoding="utf-8").splitlines()
    ]

    assert first_result.item_count == 2
    assert first_result.changed is True
    assert second_result.changed is False
    assert [row["ordinal"] for row in rows] == [1, 2]
    assert [row["is_active"] for row in rows] == [1, 1]
    assert [row["last_run_id"] for row in rows] == ["run_two", "run_two"]
    assert Path(docket_rows[0]["raw_html_path"]).exists()
    assert [record["changed"] for record in manifest_records] == [True, False]
    assert [record["recorded_at"] for record in manifest_records] == [
        first_run_context.started_at,
        second_run_context.started_at,
    ]
