from __future__ import annotations

from pathlib import Path

from incident_pipeline.acquisition.ntsb.db import (
    connect_db,
    init_db,
    record_download,
    record_promotion,
    register_blob,
    replace_docket_items,
    upsert_docket,
    upsert_investigation,
)
from incident_pipeline.acquisition.ntsb.models import (
    Docket,
    DocketItem,
    DownloadRecord,
    Investigation,
    PromotionRecord,
)


def _init_connection(tmp_path: Path):
    db_path = tmp_path / "acquisition.db"
    init_db(db_path)
    return db_path, connect_db(db_path)


def _fetch_one(connection, query: str, params: tuple[object, ...] = ()) -> dict[str, object]:
    row = connection.execute(query, params).fetchone()
    assert row is not None
    return dict(row)


def test_init_db_creates_authoritative_schema(tmp_path: Path) -> None:
    db_path, connection = _init_connection(tmp_path)
    try:
        schema_version = _fetch_one(
            connection,
            "SELECT value FROM schema_meta WHERE key = ?",
            ("schema_version",),
        )
        assert schema_version["value"] == "1"
        tables = {
            row["name"]
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name"
            )
        }
        assert {
            "investigations_current",
            "dockets_current",
            "docket_items_current",
            "blobs_current",
        }.issubset(tables)
        assert db_path.exists()
    finally:
        connection.close()


def test_upsert_investigation_is_idempotent(tmp_path: Path) -> None:
    _, connection = _init_connection(tmp_path)
    try:
        investigation = Investigation(
            investigation_id="inv-1",
            ntsb_number="AAR123",
            project_id="p1",
            mode="carol",
            event_date="2026-03-24",
            title="First title",
            case_type="pipeline",
            location={"city": "Austin"},
            carol_metadata={"status": "open"},
        )

        assert (
            upsert_investigation(connection, investigation, "2026-03-24T12:00:00Z", "run_1")
            is True
        )
        assert (
            upsert_investigation(connection, investigation, "2026-03-24T13:00:00Z", "run_2")
            is False
        )

        row = _fetch_one(
            connection,
            "SELECT * FROM investigations_current WHERE ntsb_number = ?",
            ("AAR123",),
        )
        assert row["title"] == "First title"
        assert row["last_seen_at"] == "2026-03-24T13:00:00Z"
        assert row["last_run_id"] == "run_2"
    finally:
        connection.close()


def test_upsert_docket_is_idempotent(tmp_path: Path) -> None:
    _, connection = _init_connection(tmp_path)
    try:
        docket = Docket(
            docket_id="dock-1",
            ntsb_number="AAR123",
            public_url="https://example.test/dockets/1",
            item_count=3,
            creation_date="2026-03-24",
            last_modified="2026-03-24",
            public_release_at="2026-03-25",
            raw_html_path="/tmp/docket.html",
            retrieved_at="2026-03-24T12:00:00Z",
            source_fingerprint="abc123",
        )

        assert upsert_docket(connection, docket, "2026-03-24T12:00:00Z", "run_1") is True
        assert (
            upsert_docket(connection, docket, "2026-03-24T13:00:00Z", "run_2") is False
        )

        row = _fetch_one(
            connection,
            "SELECT * FROM dockets_current WHERE ntsb_number = ?",
            ("AAR123",),
        )
        assert row["item_count"] == 3
        assert row["last_seen_at"] == "2026-03-24T13:00:00Z"
    finally:
        connection.close()


def test_replace_docket_items_inserts_deactivates_and_reactivates(tmp_path: Path) -> None:
    _, connection = _init_connection(tmp_path)
    try:
        first_items = [
            DocketItem(
                docket_item_id="ntsb:docket_item:AAR123:1:report_one",
                ntsb_number="AAR123",
                ordinal=1,
                normalized_title_slug="report_one",
                title="Report One",
            ),
            DocketItem(
                docket_item_id="ntsb:docket_item:AAR123:2:report_two",
                ntsb_number="AAR123",
                ordinal=2,
                normalized_title_slug="report_two",
                title="Report Two",
            ),
        ]

        summary = replace_docket_items(
            connection,
            "AAR123",
            first_items,
            "2026-03-24T12:00:00Z",
            "run_1",
        )
        assert summary["inserted"] == 2
        assert summary["deactivated"] == 0

        second_items = [
            DocketItem(
                docket_item_id="ntsb:docket_item:AAR123:1:report_one_updated",
                ntsb_number="AAR123",
                ordinal=1,
                normalized_title_slug="report_one_updated",
                title="Report One Updated",
            )
        ]
        summary = replace_docket_items(
            connection,
            "AAR123",
            second_items,
            "2026-03-24T13:00:00Z",
            "run_2",
        )
        assert summary["replaced"] == 1
        assert summary["deactivated"] == 1

        row_one = _fetch_one(
            connection,
            "SELECT * FROM docket_items_current WHERE ntsb_number = ? AND ordinal = ?",
            ("AAR123", 1),
        )
        row_two = _fetch_one(
            connection,
            "SELECT * FROM docket_items_current WHERE ntsb_number = ? AND ordinal = ?",
            ("AAR123", 2),
        )
        assert row_one["docket_item_id"] == (
            "ntsb:docket_item:AAR123:1:report_one_updated"
        )
        assert row_one["title"] == "Report One Updated"
        assert row_one["is_active"] == 1
        assert row_two["is_active"] == 0
    finally:
        connection.close()


def test_replace_docket_items_cascades_current_downloads_and_promotions(tmp_path: Path) -> None:
    _, connection = _init_connection(tmp_path)
    try:
        item = DocketItem(
            docket_item_id="ntsb:docket_item:AAR123:1:report_one",
            ntsb_number="AAR123",
            ordinal=1,
            normalized_title_slug="report_one",
            title="Report One",
        )
        replace_docket_items(connection, "AAR123", [item], "2026-03-24T12:00:00Z", "run_1")
        register_blob(
            connection,
            blob_sha256="abc",
            blob_path="/tmp/blob/abc",
            size_bytes=12,
            media_type="application/pdf",
            downloaded_at="2026-03-24T12:01:00Z",
            run_id="run_1",
        )
        download = DownloadRecord(
            docket_item_id=item.docket_item_id,
            ntsb_number="AAR123",
            source_url="https://example.test/file.pdf",
            source_fingerprint="src-1",
            blob_sha256="abc",
            blob_path="/tmp/blob/abc",
            media_type="application/pdf",
            size_bytes=12,
            http_status=200,
            downloaded_at="2026-03-24T12:01:00Z",
        )
        promotion = PromotionRecord(
            docket_item_id=item.docket_item_id,
            ntsb_number="AAR123",
            blob_sha256="abc",
            promoted_path="/data/corpus/raw/AAR123/file.pdf",
            rule_version="selection-v1",
            matched_rules=["pdf"],
            rationale="approved",
            promoted_at="2026-03-24T12:02:00Z",
        )
        assert (
            record_download(
                connection,
                download,
                downloaded_at="2026-03-24T12:01:00Z",
                run_id="run_1",
            )
            is True
        )
        assert (
            record_promotion(
                connection,
                promotion,
                promoted_at="2026-03-24T12:02:00Z",
                run_id="run_1",
            )
            is True
        )

        replacement = DocketItem(
            docket_item_id="ntsb:docket_item:AAR123:1:report_one_v2",
            ntsb_number="AAR123",
            ordinal=1,
            normalized_title_slug="report_one_v2",
            title="Report One v2",
        )
        replace_docket_items(connection, "AAR123", [replacement], "2026-03-24T13:00:00Z", "run_2")

        download_row = _fetch_one(
            connection,
            "SELECT * FROM downloads_current WHERE ntsb_number = ?",
            ("AAR123",),
        )
        promotion_row = _fetch_one(
            connection,
            "SELECT * FROM promotions_current WHERE ntsb_number = ?",
            ("AAR123",),
        )
        assert download_row["docket_item_id"] == replacement.docket_item_id
        assert promotion_row["docket_item_id"] == replacement.docket_item_id
    finally:
        connection.close()


def test_blob_and_event_records_are_idempotent(tmp_path: Path) -> None:
    _, connection = _init_connection(tmp_path)
    try:
        item = DocketItem(
            docket_item_id="ntsb:docket_item:AAR123:1:report_one",
            ntsb_number="AAR123",
            ordinal=1,
            normalized_title_slug="report_one",
            title="Report One",
        )
        replace_docket_items(connection, "AAR123", [item], "2026-03-24T12:00:00Z", "run_1")
        assert register_blob(
            connection,
            blob_sha256="abc",
            blob_path="/tmp/blob/abc",
            size_bytes=12,
            media_type="application/pdf",
            downloaded_at="2026-03-24T12:01:00Z",
            run_id="run_1",
        ) is True
        assert register_blob(
            connection,
            blob_sha256="abc",
            blob_path="/tmp/blob/abc",
            size_bytes=12,
            media_type="application/pdf",
            downloaded_at="2026-03-24T13:01:00Z",
            run_id="run_2",
        ) is False

        download = DownloadRecord(
            docket_item_id=item.docket_item_id,
            ntsb_number="AAR123",
            source_url="https://example.test/file.pdf",
            source_fingerprint="src-1",
            blob_sha256="abc",
            blob_path="/tmp/blob/abc",
            media_type="application/pdf",
            size_bytes=12,
            http_status=200,
            downloaded_at="2026-03-24T12:01:00Z",
        )
        promotion = PromotionRecord(
            docket_item_id=item.docket_item_id,
            ntsb_number="AAR123",
            blob_sha256="abc",
            promoted_path="/data/corpus/raw/AAR123/file.pdf",
            rule_version="selection-v1",
            matched_rules=["pdf"],
            rationale="approved",
            promoted_at="2026-03-24T12:02:00Z",
        )
        assert (
            record_download(
                connection,
                download,
                downloaded_at="2026-03-24T12:01:00Z",
                run_id="run_1",
            )
            is True
        )
        assert (
            record_download(
                connection,
                download,
                downloaded_at="2026-03-24T13:01:00Z",
                run_id="run_2",
            )
            is False
        )
        assert (
            record_promotion(
                connection,
                promotion,
                promoted_at="2026-03-24T12:02:00Z",
                run_id="run_1",
            )
            is True
        )
        assert (
            record_promotion(
                connection,
                promotion,
                promoted_at="2026-03-24T13:02:00Z",
                run_id="run_2",
            )
            is False
        )
    finally:
        connection.close()
