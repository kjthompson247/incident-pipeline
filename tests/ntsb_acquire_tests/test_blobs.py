from __future__ import annotations

import json
from pathlib import Path

import httpx

from incident_pipeline.acquisition.ntsb.blobs import download_selected
from incident_pipeline.acquisition.ntsb.config import AppConfig
from incident_pipeline.acquisition.ntsb.db import (
    connect_sqlite,
    fetch_all,
    init_db,
    upsert_blob_current,
    upsert_docket_item_current,
)
from incident_pipeline.acquisition.ntsb.hashing import sha256_bytes
from incident_pipeline.acquisition.ntsb.http import HttpClient
from incident_pipeline.acquisition.ntsb.identifiers import blob_relative_path
from incident_pipeline.acquisition.ntsb.models import BlobRecord, DocketItem, SelectionDecision
from incident_pipeline.acquisition.ntsb.runtime import create_run_context


def make_selected_item(*, docket_item_id: str, ordinal: int, download_url: str) -> DocketItem:
    return DocketItem(
        docket_item_id=docket_item_id,
        ntsb_number="DCA24FM001",
        ordinal=ordinal,
        normalized_title_slug=docket_item_id.split(":")[4],
        title=f"Selected Report {ordinal}",
        display_type="PDF",
        view_url=download_url,
        download_url=download_url,
        source_fingerprint=f"fp-{ordinal}",
        selection=SelectionDecision(
            selected=True,
            rule_version="selection_v1",
            matched_rules=["pdf_download_url", "title:report"],
            rationale="selected for download",
        ),
    )


def make_http_client(handler: httpx.MockTransport) -> HttpClient:
    client = httpx.Client(transport=handler)
    return HttpClient(
        client,
        rate_limit_per_second=1000.0,
        max_retries=0,
        backoff_seconds=0.0,
        sleeper=lambda _: None,
        monotonic=lambda: 0.0,
    )


def test_download_selected_writes_blob_and_current_state(tmp_path: Path) -> None:
    sqlite_path = tmp_path / "state" / "acquisition.db"
    manifest_path = tmp_path / "manifests" / "downloads.jsonl"
    init_db(sqlite_path)
    pdf_bytes = b"%PDF-1.7 one"

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            content=pdf_bytes,
            headers={"content-type": "application/pdf"},
            request=request,
        )

    config = AppConfig(
        data_root=tmp_path / "data",
        sqlite_path=sqlite_path,
        downstream_raw_root=tmp_path / "corpus" / "raw",
    )
    run_context = create_run_context(config, run_id="run_one")

    with connect_sqlite(sqlite_path) as connection:
        upsert_docket_item_current(
            connection,
            make_selected_item(
                docket_item_id="ntsb:docket_item:DCA24FM001:1:selected_report_1",
                ordinal=1,
                download_url="https://example.test/one.pdf",
            ),
            observed_at="2026-03-25T04:00:00Z",
            run_id="seed_run",
        )
        connection.commit()

        result = download_selected(
            connection,
            make_http_client(httpx.MockTransport(handler)),
            ntsb_number="DCA24FM001",
            run_context=run_context,
            manifest_path=manifest_path,
        )
        blob_rows = fetch_all(
            connection,
            "SELECT blob_sha256, blob_path, last_verified_at FROM blobs_current",
        )
        download_rows = fetch_all(
            connection,
            "SELECT blob_sha256, http_status, downloaded_at FROM downloads_current",
        )

    manifest_records = [
        json.loads(line)
        for line in manifest_path.read_text(encoding="utf-8").splitlines()
    ]
    expected_hash = sha256_bytes(pdf_bytes)

    assert result.evaluated == 1
    assert result.downloaded == 1
    assert result.reused == 0
    assert blob_rows[0]["blob_sha256"] == expected_hash
    assert Path(blob_rows[0]["blob_path"]).read_bytes() == pdf_bytes
    assert blob_rows[0]["last_verified_at"] == run_context.started_at
    assert download_rows[0]["blob_sha256"] == expected_hash
    assert download_rows[0]["http_status"] == 200
    assert download_rows[0]["downloaded_at"] == run_context.started_at
    assert manifest_records[0]["changed"] is True
    assert manifest_records[0]["run_id"] == "run_one"
    assert manifest_records[0]["recorded_at"] == run_context.started_at


def test_download_selected_does_not_duplicate_blobs_for_identical_bytes(tmp_path: Path) -> None:
    sqlite_path = tmp_path / "state" / "acquisition.db"
    manifest_path = tmp_path / "manifests" / "downloads.jsonl"
    init_db(sqlite_path)
    pdf_bytes = b"%PDF-1.7 same"

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            content=pdf_bytes,
            headers={"content-type": "application/pdf"},
            request=request,
        )

    config = AppConfig(
        data_root=tmp_path / "data",
        sqlite_path=sqlite_path,
        downstream_raw_root=tmp_path / "corpus" / "raw",
    )
    run_context = create_run_context(config, run_id="run_one")

    with connect_sqlite(sqlite_path) as connection:
        upsert_docket_item_current(
            connection,
            make_selected_item(
                docket_item_id="ntsb:docket_item:DCA24FM001:1:selected_report_1",
                ordinal=1,
                download_url="https://example.test/one.pdf",
            ),
            observed_at="2026-03-25T04:00:00Z",
            run_id="seed_run",
        )
        upsert_docket_item_current(
            connection,
            make_selected_item(
                docket_item_id="ntsb:docket_item:DCA24FM001:2:selected_report_2",
                ordinal=2,
                download_url="https://example.test/two.pdf",
            ),
            observed_at="2026-03-25T04:00:00Z",
            run_id="seed_run",
        )
        connection.commit()

        result = download_selected(
            connection,
            make_http_client(httpx.MockTransport(handler)),
            ntsb_number="DCA24FM001",
            run_context=run_context,
            manifest_path=manifest_path,
        )
        blob_rows = fetch_all(connection, "SELECT blob_sha256 FROM blobs_current")
        download_rows = fetch_all(
            connection,
            "SELECT docket_item_id, blob_sha256 FROM downloads_current ORDER BY docket_item_id",
        )

    assert result.downloaded == 2
    assert len(blob_rows) == 1
    assert len({row["blob_sha256"] for row in download_rows}) == 1


def test_download_selected_reuses_existing_current_download_on_rerun(tmp_path: Path) -> None:
    sqlite_path = tmp_path / "state" / "acquisition.db"
    manifest_path = tmp_path / "manifests" / "downloads.jsonl"
    init_db(sqlite_path)
    pdf_bytes = b"%PDF-1.7 stable"
    request_count = {"value": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        request_count["value"] += 1
        return httpx.Response(
            200,
            content=pdf_bytes,
            headers={"content-type": "application/pdf"},
            request=request,
        )

    config = AppConfig(
        data_root=tmp_path / "data",
        sqlite_path=sqlite_path,
        downstream_raw_root=tmp_path / "corpus" / "raw",
    )
    first_context = create_run_context(config, run_id="run_one")
    second_context = create_run_context(config, run_id="run_two")

    with connect_sqlite(sqlite_path) as connection:
        upsert_docket_item_current(
            connection,
            make_selected_item(
                docket_item_id="ntsb:docket_item:DCA24FM001:1:selected_report_1",
                ordinal=1,
                download_url="https://example.test/one.pdf",
            ),
            observed_at="2026-03-25T04:00:00Z",
            run_id="seed_run",
        )
        connection.commit()

        first_result = download_selected(
            connection,
            make_http_client(httpx.MockTransport(handler)),
            ntsb_number="DCA24FM001",
            run_context=first_context,
            manifest_path=manifest_path,
        )
        second_result = download_selected(
            connection,
            make_http_client(httpx.MockTransport(handler)),
            ntsb_number="DCA24FM001",
            run_context=second_context,
            manifest_path=manifest_path,
        )

    manifest_records = [
        json.loads(line)
        for line in manifest_path.read_text(encoding="utf-8").splitlines()
    ]

    assert first_result.downloaded == 1
    assert second_result.downloaded == 0
    assert second_result.reused == 1
    assert request_count["value"] == 1
    assert [record["changed"] for record in manifest_records] == [True, False]
    assert [record["run_id"] for record in manifest_records] == ["run_one", "run_two"]
    assert [record["recorded_at"] for record in manifest_records] == [
        first_context.started_at,
        second_context.started_at,
    ]


def test_download_selected_tolerates_existing_blob_record_without_download_row(
    tmp_path: Path,
) -> None:
    sqlite_path = tmp_path / "state" / "acquisition.db"
    manifest_path = tmp_path / "manifests" / "downloads.jsonl"
    init_db(sqlite_path)
    pdf_bytes = b"%PDF-1.7 resume"

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            content=pdf_bytes,
            headers={"content-type": "application/pdf"},
            request=request,
        )

    config = AppConfig(
        data_root=tmp_path / "data",
        sqlite_path=sqlite_path,
        downstream_raw_root=tmp_path / "corpus" / "raw",
    )
    run_context = create_run_context(config, run_id="run_one")
    blob_sha256 = sha256_bytes(pdf_bytes)
    blob_path = run_context.paths.acquisition_blobs_root / blob_relative_path(blob_sha256)
    blob_path.parent.mkdir(parents=True, exist_ok=True)
    blob_path.write_bytes(pdf_bytes)

    with connect_sqlite(sqlite_path) as connection:
        upsert_docket_item_current(
            connection,
            make_selected_item(
                docket_item_id="ntsb:docket_item:DCA24FM001:1:selected_report_1",
                ordinal=1,
                download_url="https://example.test/one.pdf",
            ),
            observed_at="2026-03-25T04:00:00Z",
            run_id="seed_run",
        )
        upsert_blob_current(
            connection,
            BlobRecord(
                blob_sha256=blob_sha256,
                blob_path=str(blob_path),
                media_type="application/pdf",
                size_bytes=len(pdf_bytes),
            ),
            observed_at="2026-03-25T04:05:00Z",
            run_id="seed_run",
        )
        connection.commit()

        result = download_selected(
            connection,
            make_http_client(httpx.MockTransport(handler)),
            ntsb_number="DCA24FM001",
            run_context=run_context,
            manifest_path=manifest_path,
        )
        blob_rows = fetch_all(
            connection,
            "SELECT blob_sha256, blob_path FROM blobs_current ORDER BY blob_sha256",
        )
        download_rows = fetch_all(
            connection,
            "SELECT blob_sha256 FROM downloads_current ORDER BY docket_item_id",
        )

    assert result.downloaded == 1
    assert result.reused == 0
    assert len(blob_rows) == 1
    assert blob_rows[0]["blob_sha256"] == blob_sha256
    assert blob_rows[0]["blob_path"] == str(blob_path)
    assert len(download_rows) == 1
    assert download_rows[0]["blob_sha256"] == blob_sha256


def test_download_selected_manifest_uses_run_context_started_at(tmp_path: Path) -> None:
    sqlite_path = tmp_path / "state" / "acquisition.db"
    manifest_path = tmp_path / "manifests" / "downloads.jsonl"
    init_db(sqlite_path)
    pdf_bytes = b"%PDF-1.7 timestamp"

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            content=pdf_bytes,
            headers={"content-type": "application/pdf"},
            request=request,
        )

    config = AppConfig(
        data_root=tmp_path / "data",
        sqlite_path=sqlite_path,
        downstream_raw_root=tmp_path / "corpus" / "raw",
    )

    with connect_sqlite(sqlite_path) as connection:
        upsert_docket_item_current(
            connection,
            make_selected_item(
                docket_item_id="ntsb:docket_item:DCA24FM001:1:selected_report_1",
                ordinal=1,
                download_url="https://example.test/one.pdf",
            ),
            observed_at="2026-03-25T04:00:00Z",
            run_id="seed_run",
        )
        connection.commit()

        run_context = create_run_context(config, run_id="run_one")
        download_selected(
            connection,
            make_http_client(httpx.MockTransport(handler)),
            ntsb_number="DCA24FM001",
            run_context=run_context,
            manifest_path=manifest_path,
        )

    manifest_records = [
        json.loads(line)
        for line in manifest_path.read_text(encoding="utf-8").splitlines()
    ]

    assert [record["recorded_at"] for record in manifest_records] == [run_context.started_at]
