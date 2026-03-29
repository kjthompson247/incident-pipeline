from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace
from typing import cast

from pytest import MonkeyPatch
from typer.testing import CliRunner

from incident_pipeline.acquisition.ntsb.cli import _format_collect_heartbeat, _timestamped_collect_line, app
from incident_pipeline.acquisition.ntsb.db import (
    connect_sqlite,
    init_db,
    upsert_blob_current,
    upsert_docket_current,
    upsert_docket_item_current,
    upsert_docket_search_result_current,
    upsert_download_current,
    upsert_investigation_current,
    upsert_promotion_current,
)
from incident_pipeline.acquisition.ntsb.models import (
    BlobRecord,
    Docket,
    DocketItem,
    DocketSearchResult,
    DownloadRecord,
    Investigation,
    PromotionRecord,
)

runner = CliRunner()


def parse_structured_output(stdout: str) -> dict[str, object]:
    return cast(dict[str, object], json.loads(stdout.strip()))


def test_help_lists_acquisition_commands() -> None:
    result = runner.invoke(app, ["--help"])

    assert result.exit_code == 0
    for command in [
        "init-db",
        "sync-investigations",
        "discover-dockets",
        "enumerate-dockets",
        "download-selected",
        "collect",
        "promote",
        "materialize-case",
        "export-ingestion-manifest",
        "summarize",
        "run",
        "doctor",
    ]:
        assert command in result.stdout


def test_init_db_emits_structured_ok_result(tmp_path: Path) -> None:
    sqlite_path = tmp_path / "state" / "acquisition.db"
    data_root = tmp_path / "data"
    downstream_raw_root = tmp_path / "corpus" / "raw"

    result = runner.invoke(
        app,
        [
            "--data-root",
            str(data_root),
            "--sqlite-path",
            str(sqlite_path),
            "--downstream-raw-root",
            str(downstream_raw_root),
            "init-db",
        ],
    )

    assert result.exit_code == 0
    payload = parse_structured_output(result.stdout)
    assert payload["command"] == "init-db"
    assert payload["status"] == "ok"
    assert payload["state_changed"] is True
    assert sqlite_path.exists()


def test_doctor_succeeds_when_runtime_contract_is_valid(monkeypatch: MonkeyPatch) -> None:
    monkeypatch.setattr("incident_pipeline.acquisition.ntsb.cli.platform.python_version", lambda: "3.12.13")
    monkeypatch.setattr("incident_pipeline.acquisition.ntsb.cli.sys.version_info", (3, 12, 13))

    result = runner.invoke(app, ["doctor"])

    assert result.exit_code == 0
    payload = parse_structured_output(result.stdout)
    assert payload["command"] == "doctor"
    assert payload["status"] == "ok"
    assert payload["state_changed"] is False
    checks = payload["checks"]
    assert isinstance(checks, list)
    assert all(isinstance(check, dict) and check.get("ok") for check in checks)


def test_doctor_fails_when_python_version_is_invalid(monkeypatch: MonkeyPatch) -> None:
    monkeypatch.setattr("incident_pipeline.acquisition.ntsb.cli.platform.python_version", lambda: "3.12.12")
    monkeypatch.setattr("incident_pipeline.acquisition.ntsb.cli.sys.version_info", (3, 12, 12))

    result = runner.invoke(app, ["doctor"])

    assert result.exit_code == 1
    payload = parse_structured_output(result.stdout)
    assert payload["command"] == "doctor"
    assert payload["status"] == "error"


def test_collect_helpers_format_heartbeat_and_log_lines() -> None:
    assert _format_collect_heartbeat(
        phase="Enumerating dockets",
        elapsed_seconds=65,
        detail="41 processed, 28 updated, 2,197 items total",
    ) == (
        "Collect | Phase: Enumerating dockets | Elapsed: 00:01:05 | "
        "41 processed, 28 updated, 2,197 items total"
    )
    assert _timestamped_collect_line(
        timestamp="2026-03-26T21:03:00Z",
        line="Collect | Phase: Discovering dockets | Elapsed: 00:01:00 | 74 discovered so far",
    ) == (
        "2026-03-26T21:03:00Z | Collect | Phase: Discovering dockets | "
        "Elapsed: 00:01:00 | 74 discovered so far"
    )


def test_enumerate_dockets_uses_union_of_investigations_and_docket_search(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    sqlite_path = tmp_path / "state" / "acquisition.db"
    data_root = tmp_path / "data"
    downstream_raw_root = tmp_path / "corpus" / "raw"
    init_db(sqlite_path)

    with connect_sqlite(sqlite_path) as connection:
        upsert_investigation_current(
            connection,
            Investigation(
                investigation_id="ntsb:investigation:DCA24FM001",
                ntsb_number="DCA24FM001",
                project_id="101",
            ),
            observed_at="2026-03-26T00:00:00Z",
            run_id="seed_run",
        )
        upsert_investigation_current(
            connection,
            Investigation(
                investigation_id="ntsb:investigation:DCA24FM003",
                ntsb_number="DCA24FM003",
                project_id="103",
            ),
            observed_at="2026-03-26T00:00:00Z",
            run_id="seed_run",
        )
        upsert_docket_search_result_current(
            connection,
            DocketSearchResult(
                ntsb_number="DCA24FM002",
                project_id="102",
                result_url="https://example.test/Docket?ProjectID=102",
            ),
            observed_at="2026-03-26T00:00:00Z",
            run_id="seed_run",
        )
        upsert_docket_search_result_current(
            connection,
            DocketSearchResult(
                ntsb_number="DCA24FM003",
                project_id="103",
                result_url="https://example.test/Docket?ProjectID=103",
            ),
            observed_at="2026-03-26T00:00:00Z",
            run_id="seed_run",
        )

    enumerated_numbers: list[str] = []

    class DummyHttpClient:
        def close(self) -> None:
            return None

    def fake_enumerate_docket(
        connection: object,
        client: object,
        *,
        ntsb_number: str,
        run_context: object,
        manifest_path: Path,
    ) -> SimpleNamespace:
        del connection, client, run_context, manifest_path
        enumerated_numbers.append(ntsb_number)
        return SimpleNamespace(item_count=1, changed=False)

    monkeypatch.setattr("incident_pipeline.acquisition.ntsb.cli.build_http_client", lambda config: DummyHttpClient())
    monkeypatch.setattr("incident_pipeline.acquisition.ntsb.cli.enumerate_docket", fake_enumerate_docket)

    result = runner.invoke(
        app,
        [
            "--data-root",
            str(data_root),
            "--sqlite-path",
            str(sqlite_path),
            "--downstream-raw-root",
            str(downstream_raw_root),
            "--docket-base-url",
            "https://example.test/Docket",
            "enumerate-dockets",
        ],
    )

    assert result.exit_code == 0
    payload = parse_structured_output(result.stdout)
    assert payload["command"] == "enumerate-dockets"
    assert payload["enumerated"] == 3
    assert enumerated_numbers == ["DCA24FM001", "DCA24FM002", "DCA24FM003"]


def test_summarize_reports_current_index_counts(tmp_path: Path) -> None:
    sqlite_path = tmp_path / "state" / "acquisition.db"
    data_root = tmp_path / "data"
    downstream_raw_root = tmp_path / "corpus" / "raw"
    init_db(sqlite_path)

    with connect_sqlite(sqlite_path) as connection:
        upsert_investigation_current(
            connection,
            Investigation(
                investigation_id="ntsb:investigation:DCA24FM001",
                ntsb_number="DCA24FM001",
                project_id="101",
            ),
            observed_at="2026-03-26T00:00:00Z",
            run_id="seed_run",
        )
        upsert_investigation_current(
            connection,
            Investigation(
                investigation_id="ntsb:investigation:DCA24FM002",
                ntsb_number="DCA24FM002",
                project_id="102",
            ),
            observed_at="2026-03-26T00:00:00Z",
            run_id="seed_run",
        )
        upsert_docket_current(
            connection,
            Docket(
                docket_id="ntsb:docket:DCA24FM001",
                ntsb_number="DCA24FM001",
                item_count=2,
            ),
            observed_at="2026-03-26T00:00:00Z",
            run_id="seed_run",
        )
        upsert_docket_current(
            connection,
            Docket(
                docket_id="ntsb:docket:DCA24FM002",
                ntsb_number="DCA24FM002",
                item_count=1,
            ),
            observed_at="2026-03-26T00:00:00Z",
            run_id="seed_run",
        )
        upsert_docket_item_current(
            connection,
            DocketItem(
                docket_item_id="ntsb:docket_item:DCA24FM001:1:operations_report",
                ntsb_number="DCA24FM001",
                ordinal=1,
                normalized_title_slug="operations_report",
                title="Operations Report",
            ),
            observed_at="2026-03-26T00:00:00Z",
            run_id="seed_run",
        )
        upsert_docket_item_current(
            connection,
            DocketItem(
                docket_item_id="ntsb:docket_item:DCA24FM001:2:systems_report",
                ntsb_number="DCA24FM001",
                ordinal=2,
                normalized_title_slug="systems_report",
                title="Systems Report",
            ),
            observed_at="2026-03-26T00:00:00Z",
            run_id="seed_run",
        )
        upsert_docket_item_current(
            connection,
            DocketItem(
                docket_item_id="ntsb:docket_item:DCA24FM002:1:factual_report",
                ntsb_number="DCA24FM002",
                ordinal=1,
                normalized_title_slug="factual_report",
                title="Factual Report",
            ),
            observed_at="2026-03-26T00:00:00Z",
            run_id="seed_run",
        )
        upsert_blob_current(
            connection,
            BlobRecord(
                blob_sha256="a" * 64,
                blob_path=str(tmp_path / "blobs" / "one.pdf"),
                media_type="application/pdf",
                size_bytes=10,
            ),
            observed_at="2026-03-26T00:00:00Z",
            run_id="seed_run",
        )
        upsert_blob_current(
            connection,
            BlobRecord(
                blob_sha256="b" * 64,
                blob_path=str(tmp_path / "blobs" / "two.pdf"),
                media_type="application/pdf",
                size_bytes=20,
            ),
            observed_at="2026-03-26T00:00:00Z",
            run_id="seed_run",
        )
        upsert_download_current(
            connection,
            DownloadRecord(
                docket_item_id="ntsb:docket_item:DCA24FM001:1:operations_report",
                ntsb_number="DCA24FM001",
                source_url="https://example.test/operations.pdf",
                blob_sha256="a" * 64,
                blob_path=str(tmp_path / "blobs" / "one.pdf"),
                media_type="application/pdf",
                size_bytes=10,
                downloaded_at="2026-03-26T00:00:00Z",
            ),
            run_id="seed_run",
        )
        upsert_download_current(
            connection,
            DownloadRecord(
                docket_item_id="ntsb:docket_item:DCA24FM002:1:factual_report",
                ntsb_number="DCA24FM002",
                source_url="https://example.test/factual.pdf",
                blob_sha256="b" * 64,
                blob_path=str(tmp_path / "blobs" / "two.pdf"),
                media_type="application/pdf",
                size_bytes=20,
                downloaded_at="2026-03-26T00:00:00Z",
            ),
            run_id="seed_run",
        )
        upsert_promotion_current(
            connection,
            PromotionRecord(
                docket_item_id="ntsb:docket_item:DCA24FM001:1:operations_report",
                ntsb_number="DCA24FM001",
                blob_sha256="a" * 64,
                promoted_path=str(tmp_path / "corpus" / "raw" / "DCA24FM001" / "one.pdf"),
                rule_version="selection_v1",
                matched_rules=[],
                rationale="seed",
                promoted_at="2026-03-26T00:00:00Z",
            ),
            run_id="seed_run",
        )

    result = runner.invoke(
        app,
        [
            "--data-root",
            str(data_root),
            "--sqlite-path",
            str(sqlite_path),
            "--downstream-raw-root",
            str(downstream_raw_root),
            "summarize",
        ],
    )

    assert result.exit_code == 0
    payload = parse_structured_output(result.stdout)
    assert payload["command"] == "summarize"
    assert payload["status"] == "ok"
    assert payload["state_changed"] is False
    assert payload["investigations_current_count"] == 2
    assert payload["dockets_current_count"] == 2
    assert payload["docket_items_current_count"] == 3
    assert payload["downloads_current_count"] == 2
    assert payload["blobs_current_count"] == 2
    assert payload["promotions_current_count"] == 1
    assert payload["per_case_counts"] == [
        {
            "downloads_count": 1,
            "docket_items_count": 2,
            "ntsb_number": "DCA24FM001",
        },
        {
            "downloads_count": 1,
            "docket_items_count": 1,
            "ntsb_number": "DCA24FM002",
        },
    ]


def test_export_ingestion_manifest_outputs_ndjson_in_deterministic_order(tmp_path: Path) -> None:
    sqlite_path = tmp_path / "state" / "acquisition.db"
    data_root = tmp_path / "data"
    downstream_raw_root = tmp_path / "corpus" / "raw"
    init_db(sqlite_path)

    with connect_sqlite(sqlite_path) as connection:
        upsert_docket_item_current(
            connection,
            DocketItem(
                docket_item_id="ntsb:docket_item:DCA24FM002:2:systems_report",
                ntsb_number="DCA24FM002",
                ordinal=2,
                normalized_title_slug="systems_report",
                title="Systems Report",
                view_url="https://example.test/view/systems",
            ),
            observed_at="2026-03-26T00:00:00Z",
            run_id="seed_run",
        )
        upsert_docket_item_current(
            connection,
            DocketItem(
                docket_item_id="ntsb:docket_item:DCA24FM001:1:operations_report",
                ntsb_number="DCA24FM001",
                ordinal=1,
                normalized_title_slug="operations_report",
                title="Operations Report",
                view_url="https://example.test/view/operations",
            ),
            observed_at="2026-03-26T00:00:00Z",
            run_id="seed_run",
        )
        upsert_docket_search_result_current(
            connection,
            DocketSearchResult(
                ntsb_number="DCA24FM001",
                project_id="101",
                result_url="https://example.test/Docket?ProjectID=101",
            ),
            observed_at="2026-03-26T00:00:00Z",
            run_id="seed_run",
        )
        upsert_docket_search_result_current(
            connection,
            DocketSearchResult(
                ntsb_number="DCA24FM002",
                project_id="102",
                result_url="https://example.test/Docket?ProjectID=102",
            ),
            observed_at="2026-03-26T00:00:00Z",
            run_id="seed_run",
        )
        upsert_blob_current(
            connection,
            BlobRecord(
                blob_sha256="a" * 64,
                blob_path=str(tmp_path / "blobs" / "one.pdf"),
                media_type="application/pdf",
                size_bytes=10,
            ),
            observed_at="2026-03-26T00:00:00Z",
            run_id="seed_run",
        )
        upsert_blob_current(
            connection,
            BlobRecord(
                blob_sha256="b" * 64,
                blob_path=str(tmp_path / "blobs" / "two.pdf"),
                media_type="application/pdf",
                size_bytes=20,
            ),
            observed_at="2026-03-26T00:00:00Z",
            run_id="seed_run",
        )
        upsert_download_current(
            connection,
            DownloadRecord(
                docket_item_id="ntsb:docket_item:DCA24FM002:2:systems_report",
                ntsb_number="DCA24FM002",
                source_url="https://example.test/systems.pdf",
                blob_sha256="b" * 64,
                blob_path=str(tmp_path / "blobs" / "two.pdf"),
                media_type="application/pdf",
                size_bytes=20,
                downloaded_at="2026-03-26T00:00:00Z",
            ),
            run_id="seed_run",
        )
        upsert_download_current(
            connection,
            DownloadRecord(
                docket_item_id="ntsb:docket_item:DCA24FM001:1:operations_report",
                ntsb_number="DCA24FM001",
                source_url="https://example.test/operations.pdf",
                blob_sha256="a" * 64,
                blob_path=str(tmp_path / "blobs" / "one.pdf"),
                media_type="application/pdf",
                size_bytes=10,
                downloaded_at="2026-03-26T00:00:00Z",
            ),
            run_id="seed_run",
        )

    result = runner.invoke(
        app,
        [
            "--data-root",
            str(data_root),
            "--sqlite-path",
            str(sqlite_path),
            "--downstream-raw-root",
            str(downstream_raw_root),
            "export-ingestion-manifest",
        ],
    )

    exports_root = data_root / "acquisition" / "exports"
    snapshot_files = sorted(
        path
        for path in exports_root.glob("ingestion_manifest_*.jsonl")
        if path.name != "ingestion_manifest_latest.jsonl"
    )
    latest_path = exports_root / "ingestion_manifest_latest.jsonl"

    assert result.exit_code == 0
    records = [json.loads(line) for line in result.stdout.splitlines()]
    exported_run_id = snapshot_files[0].stem.removeprefix("ingestion_manifest_")
    assert len(snapshot_files) == 1
    assert snapshot_files[0].read_text(encoding="utf-8") == result.stdout
    assert latest_path.read_text(encoding="utf-8") == result.stdout
    assert records == [
        {
            "project_id": "101",
            "ntsb_number": "DCA24FM001",
            "docket_item_id": "ntsb:docket_item:DCA24FM001:1:operations_report",
            "acquisition_run_id": exported_run_id,
            "ordinal": 1,
            "title": "Operations Report",
            "view_url": "https://example.test/view/operations",
            "blob_sha256": "a" * 64,
            "blob_path": str(tmp_path / "blobs" / "one.pdf"),
            "media_type": "application/pdf",
            "source_url": "https://example.test/operations.pdf",
        },
        {
            "project_id": "102",
            "ntsb_number": "DCA24FM002",
            "docket_item_id": "ntsb:docket_item:DCA24FM002:2:systems_report",
            "acquisition_run_id": exported_run_id,
            "ordinal": 2,
            "title": "Systems Report",
            "view_url": "https://example.test/view/systems",
            "blob_sha256": "b" * 64,
            "blob_path": str(tmp_path / "blobs" / "two.pdf"),
            "media_type": "application/pdf",
            "source_url": "https://example.test/systems.pdf",
        },
    ]


def test_export_ingestion_manifest_writes_deterministic_export_file_by_default(
    tmp_path: Path,
) -> None:
    sqlite_path = tmp_path / "state" / "acquisition.db"
    data_root = tmp_path / "data"
    downstream_raw_root = tmp_path / "corpus" / "raw"
    init_db(sqlite_path)

    with connect_sqlite(sqlite_path) as connection:
        upsert_docket_item_current(
            connection,
            DocketItem(
                docket_item_id="ntsb:docket_item:DCA24FM001:1:operations_report",
                ntsb_number="DCA24FM001",
                ordinal=1,
                normalized_title_slug="operations_report",
                title="Operations Report",
                view_url="https://example.test/view/operations",
            ),
            observed_at="2026-03-26T00:00:00Z",
            run_id="seed_run",
        )
        upsert_docket_search_result_current(
            connection,
            DocketSearchResult(
                ntsb_number="DCA24FM001",
                project_id="101",
                result_url="https://example.test/Docket?ProjectID=101",
            ),
            observed_at="2026-03-26T00:00:00Z",
            run_id="seed_run",
        )
        upsert_blob_current(
            connection,
            BlobRecord(
                blob_sha256="a" * 64,
                blob_path=str(tmp_path / "blobs" / "one.pdf"),
                media_type="application/pdf",
                size_bytes=10,
            ),
            observed_at="2026-03-26T00:00:00Z",
            run_id="seed_run",
        )
        upsert_download_current(
            connection,
            DownloadRecord(
                docket_item_id="ntsb:docket_item:DCA24FM001:1:operations_report",
                ntsb_number="DCA24FM001",
                source_url="https://example.test/operations.pdf",
                blob_sha256="a" * 64,
                blob_path=str(tmp_path / "blobs" / "one.pdf"),
                media_type="application/pdf",
                size_bytes=10,
                downloaded_at="2026-03-26T00:00:00Z",
            ),
            run_id="seed_run",
        )

    result = runner.invoke(
        app,
        [
            "--data-root",
            str(data_root),
            "--sqlite-path",
            str(sqlite_path),
            "--downstream-raw-root",
            str(downstream_raw_root),
            "export-ingestion-manifest",
        ],
    )

    exports_root = data_root / "acquisition" / "exports"
    snapshot_files = sorted(
        path
        for path in exports_root.glob("ingestion_manifest_*.jsonl")
        if path.name != "ingestion_manifest_latest.jsonl"
    )
    latest_path = exports_root / "ingestion_manifest_latest.jsonl"

    assert result.exit_code == 0
    assert len(snapshot_files) == 1
    assert snapshot_files[0].read_text(encoding="utf-8") == result.stdout
    assert latest_path.read_text(encoding="utf-8") == result.stdout


def test_export_ingestion_manifest_quiet_suppresses_stdout_rows(tmp_path: Path) -> None:
    sqlite_path = tmp_path / "state" / "acquisition.db"
    data_root = tmp_path / "data"
    downstream_raw_root = tmp_path / "corpus" / "raw"
    init_db(sqlite_path)

    with connect_sqlite(sqlite_path) as connection:
        upsert_docket_item_current(
            connection,
            DocketItem(
                docket_item_id="ntsb:docket_item:DCA24FM001:1:operations_report",
                ntsb_number="DCA24FM001",
                ordinal=1,
                normalized_title_slug="operations_report",
                title="Operations Report",
                view_url="https://example.test/view/operations",
            ),
            observed_at="2026-03-26T00:00:00Z",
            run_id="seed_run",
        )
        upsert_docket_search_result_current(
            connection,
            DocketSearchResult(
                ntsb_number="DCA24FM001",
                project_id="101",
                result_url="https://example.test/Docket?ProjectID=101",
            ),
            observed_at="2026-03-26T00:00:00Z",
            run_id="seed_run",
        )
        upsert_blob_current(
            connection,
            BlobRecord(
                blob_sha256="a" * 64,
                blob_path=str(tmp_path / "blobs" / "one.pdf"),
                media_type="application/pdf",
                size_bytes=10,
            ),
            observed_at="2026-03-26T00:00:00Z",
            run_id="seed_run",
        )
        upsert_download_current(
            connection,
            DownloadRecord(
                docket_item_id="ntsb:docket_item:DCA24FM001:1:operations_report",
                ntsb_number="DCA24FM001",
                source_url="https://example.test/operations.pdf",
                blob_sha256="a" * 64,
                blob_path=str(tmp_path / "blobs" / "one.pdf"),
                media_type="application/pdf",
                size_bytes=10,
                downloaded_at="2026-03-26T00:00:00Z",
            ),
            run_id="seed_run",
        )

    result = runner.invoke(
        app,
        [
            "--data-root",
            str(data_root),
            "--sqlite-path",
            str(sqlite_path),
            "--downstream-raw-root",
            str(downstream_raw_root),
            "export-ingestion-manifest",
            "--quiet",
        ],
    )

    exports_root = data_root / "acquisition" / "exports"
    snapshot_files = sorted(
        path
        for path in exports_root.glob("ingestion_manifest_*.jsonl")
        if path.name != "ingestion_manifest_latest.jsonl"
    )
    latest_path = exports_root / "ingestion_manifest_latest.jsonl"

    assert result.exit_code == 0
    assert result.stdout == ""
    assert len(snapshot_files) == 1
    assert snapshot_files[0].read_text(encoding="utf-8") == latest_path.read_text(
        encoding="utf-8"
    )
