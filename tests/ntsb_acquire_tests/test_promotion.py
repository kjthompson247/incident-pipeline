from __future__ import annotations

import json
from pathlib import Path

from incident_pipeline.acquisition.ntsb.config import AppConfig
from incident_pipeline.acquisition.ntsb.db import (
    connect_sqlite,
    fetch_all,
    init_db,
    upsert_blob_current,
    upsert_docket_item_current,
    upsert_download_current,
)
from incident_pipeline.acquisition.ntsb.hashing import sha256_bytes
from incident_pipeline.acquisition.ntsb.models import BlobRecord, DocketItem, DownloadRecord, SelectionDecision
from incident_pipeline.acquisition.ntsb.promotion import promote_selected, promotion_destination_path
from incident_pipeline.acquisition.ntsb.runtime import create_run_context


def make_selected_item(*, docket_item_id: str) -> DocketItem:
    return DocketItem(
        docket_item_id=docket_item_id,
        ntsb_number="DCA24FM001",
        ordinal=int(docket_item_id.split(":")[3]),
        normalized_title_slug=docket_item_id.split(":")[4],
        title="Operations Group Chairman Factual Report",
        display_type="PDF",
        download_url="https://example.test/operations.pdf",
        view_url="https://example.test/operations.pdf",
        source_fingerprint="item-fingerprint",
        selection=SelectionDecision(
            selected=True,
            rule_version="selection_v1",
            matched_rules=["pdf_download_url", "title:report"],
            rationale="selected for promotion",
        ),
    )


def seed_download_state(
    connection,
    *,
    blob_sha256: str,
    blob_path: Path,
    docket_item_id: str,
) -> None:
    upsert_docket_item_current(
        connection,
        make_selected_item(docket_item_id=docket_item_id),
        observed_at="2026-03-25T05:00:00Z",
        run_id="seed_run",
    )
    upsert_blob_current(
        connection,
        BlobRecord(
            blob_sha256=blob_sha256,
            blob_path=str(blob_path),
            media_type="application/pdf",
            size_bytes=len(blob_path.read_bytes()),
        ),
        observed_at="2026-03-25T05:00:00Z",
        run_id="seed_run",
    )
    upsert_download_current(
        connection,
        DownloadRecord(
            docket_item_id=docket_item_id,
            ntsb_number="DCA24FM001",
            source_url="https://example.test/operations.pdf",
            source_fingerprint="item-fingerprint",
            blob_sha256=blob_sha256,
            blob_path=str(blob_path),
            media_type="application/pdf",
            size_bytes=len(blob_path.read_bytes()),
            http_status=200,
            downloaded_at="2026-03-25T05:00:00Z",
        ),
        run_id="seed_run",
    )


def test_promotion_destination_path_is_deterministic(tmp_path: Path) -> None:
    destination = promotion_destination_path(
        downstream_raw_root=tmp_path / "raw",
        ntsb_number="DCA24FM001",
        docket_item_id="ntsb:docket_item:DCA24FM001:1:operations_group_report",
        blob_sha256="abcdef1234567890fedcba0987654321abcdef1234567890fedcba0987654321",
    )

    assert destination == (
        tmp_path
        / "raw"
        / "DCA24FM001"
        / "ntsb:docket_item:DCA24FM001:1:operations_group_report__abcdef1234567890.pdf"
    )


def test_promote_selected_copies_blob_and_records_manifest(tmp_path: Path) -> None:
    sqlite_path = tmp_path / "state" / "acquisition.db"
    manifest_path = tmp_path / "manifests" / "promotions.jsonl"
    blob_path = tmp_path / "blobs" / "source.pdf"
    blob_path.parent.mkdir(parents=True, exist_ok=True)
    blob_path.write_bytes(b"%PDF-1.7 promoted")
    blob_sha256 = sha256_bytes(blob_path.read_bytes())
    init_db(sqlite_path)
    config = AppConfig(
        data_root=tmp_path / "data",
        sqlite_path=sqlite_path,
        downstream_raw_root=tmp_path / "corpus" / "raw",
    )

    with connect_sqlite(sqlite_path) as connection:
        seed_download_state(
            connection,
            blob_sha256=blob_sha256,
            blob_path=blob_path,
            docket_item_id="ntsb:docket_item:DCA24FM001:1:operations_group_report",
        )
        connection.commit()

        result = promote_selected(
            connection,
            ntsb_number="DCA24FM001",
            run_context=create_run_context(config, run_id="run_one"),
            manifest_path=manifest_path,
        )
        promotion_rows = fetch_all(
            connection,
            "SELECT promoted_path, rule_version FROM promotions_current",
        )

    promoted_path = Path(promotion_rows[0]["promoted_path"])
    manifest_records = [
        json.loads(line)
        for line in manifest_path.read_text(encoding="utf-8").splitlines()
    ]

    assert result.evaluated == 1
    assert result.promoted == 1
    assert result.reused == 0
    assert promoted_path.exists()
    assert promoted_path.read_bytes() == blob_path.read_bytes()
    assert promotion_rows[0]["rule_version"] == "selection_v1"
    assert manifest_records[0]["downstream_registration_status"] == "not_attempted"


def test_promote_selected_is_rerun_safe_when_destination_matches(tmp_path: Path) -> None:
    sqlite_path = tmp_path / "state" / "acquisition.db"
    manifest_path = tmp_path / "manifests" / "promotions.jsonl"
    blob_path = tmp_path / "blobs" / "source.pdf"
    blob_path.parent.mkdir(parents=True, exist_ok=True)
    blob_path.write_bytes(b"%PDF-1.7 promoted")
    blob_sha256 = sha256_bytes(blob_path.read_bytes())
    init_db(sqlite_path)
    config = AppConfig(
        data_root=tmp_path / "data",
        sqlite_path=sqlite_path,
        downstream_raw_root=tmp_path / "corpus" / "raw",
    )

    with connect_sqlite(sqlite_path) as connection:
        seed_download_state(
            connection,
            blob_sha256=blob_sha256,
            blob_path=blob_path,
            docket_item_id="ntsb:docket_item:DCA24FM001:1:operations_group_report",
        )
        connection.commit()

        first_result = promote_selected(
            connection,
            ntsb_number="DCA24FM001",
            run_context=create_run_context(config, run_id="run_one"),
            manifest_path=manifest_path,
        )
        second_result = promote_selected(
            connection,
            ntsb_number="DCA24FM001",
            run_context=create_run_context(config, run_id="run_two"),
            manifest_path=manifest_path,
        )

    manifest_records = [
        json.loads(line)
        for line in manifest_path.read_text(encoding="utf-8").splitlines()
    ]

    assert first_result.promoted == 1
    assert second_result.promoted == 0
    assert second_result.reused == 1
    assert len(manifest_records) == 2


def test_promote_selected_manifest_uses_run_context_started_at(tmp_path: Path) -> None:
    sqlite_path = tmp_path / "state" / "acquisition.db"
    manifest_path = tmp_path / "manifests" / "promotions.jsonl"
    blob_path = tmp_path / "blobs" / "source.pdf"
    blob_path.parent.mkdir(parents=True, exist_ok=True)
    blob_path.write_bytes(b"%PDF-1.7 promoted")
    blob_sha256 = sha256_bytes(blob_path.read_bytes())
    init_db(sqlite_path)
    config = AppConfig(
        data_root=tmp_path / "data",
        sqlite_path=sqlite_path,
        downstream_raw_root=tmp_path / "corpus" / "raw",
    )

    with connect_sqlite(sqlite_path) as connection:
        seed_download_state(
            connection,
            blob_sha256=blob_sha256,
            blob_path=blob_path,
            docket_item_id="ntsb:docket_item:DCA24FM001:1:operations_group_report",
        )
        connection.commit()

        run_context = create_run_context(config, run_id="run_one")
        promote_selected(
            connection,
            ntsb_number="DCA24FM001",
            run_context=run_context,
            manifest_path=manifest_path,
        )

    manifest_records = [
        json.loads(line)
        for line in manifest_path.read_text(encoding="utf-8").splitlines()
    ]

    assert [record["promoted_at"] for record in manifest_records] == [
        run_context.started_at,
    ]
    assert [record["run_id"] for record in manifest_records] == ["run_one"]
