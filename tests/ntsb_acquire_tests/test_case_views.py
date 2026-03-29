from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from pytest import MonkeyPatch
from typer.testing import CliRunner

from incident_pipeline.acquisition.ntsb.case_views import materialize_case
from incident_pipeline.acquisition.ntsb.cli import app
from incident_pipeline.acquisition.ntsb.db import (
    connect_sqlite,
    init_db,
    upsert_blob_current,
    upsert_docket_item_current,
    upsert_download_current,
)
from incident_pipeline.acquisition.ntsb.feedback import render_html_report
from incident_pipeline.acquisition.ntsb.models import BlobRecord, DocketItem, DownloadRecord

runner = CliRunner()


def make_docket_item(*, ordinal: int, title: str) -> DocketItem:
    normalized_title_slug = title.lower().replace(" ", "_")
    return DocketItem(
        docket_item_id=f"ntsb:docket_item:DCA24FM001:{ordinal}:{normalized_title_slug}",
        ntsb_number="DCA24FM001",
        ordinal=ordinal,
        normalized_title_slug=normalized_title_slug,
        title=title,
        display_type="PDF",
        download_url=(
            "https://example.test/Docket/Document/docBLOB"
            f"?ID={ordinal}&FileExtension=pdf&FileName={normalized_title_slug}.pdf"
        ),
        view_url=(
            "https://example.test/Docket/Document/docBLOB"
            f"?ID={ordinal}&FileExtension=pdf&FileName={normalized_title_slug}.pdf"
        ),
        source_fingerprint=f"item-fingerprint-{ordinal}",
    )


def seed_download(
    connection: sqlite3.Connection,
    *,
    ordinal: int,
    title: str,
    blob_path: Path,
) -> None:
    docket_item = make_docket_item(ordinal=ordinal, title=title)
    content = blob_path.read_bytes()
    upsert_docket_item_current(
        connection,
        docket_item,
        observed_at="2026-03-26T00:00:00Z",
        run_id="seed_run",
    )
    upsert_blob_current(
        connection,
        BlobRecord(
            blob_sha256=f"{ordinal:064d}",
            blob_path=str(blob_path),
            media_type="application/pdf",
            size_bytes=len(content),
        ),
        observed_at="2026-03-26T00:00:00Z",
        run_id="seed_run",
    )
    upsert_download_current(
        connection,
        DownloadRecord(
            docket_item_id=docket_item.docket_item_id,
            ntsb_number=docket_item.ntsb_number,
            source_url=str(docket_item.download_url),
            source_fingerprint=docket_item.source_fingerprint,
            blob_sha256=f"{ordinal:064d}",
            blob_path=str(blob_path),
            media_type="application/pdf",
            size_bytes=len(content),
            http_status=200,
            downloaded_at="2026-03-26T00:00:00Z",
        ),
        run_id="seed_run",
    )


def test_materialize_case_creates_deterministic_case_view_files(tmp_path: Path) -> None:
    sqlite_path = tmp_path / "state" / "acquisition.db"
    blob_root = tmp_path / "blobs"
    blob_root.mkdir(parents=True, exist_ok=True)
    first_blob = blob_root / "first.pdf"
    first_blob.write_bytes(b"%PDF-1.7 first")
    second_blob = blob_root / "second.pdf"
    second_blob.write_bytes(b"%PDF-1.7 second")
    init_db(sqlite_path)

    with connect_sqlite(sqlite_path) as connection:
        seed_download(
            connection,
            ordinal=2,
            title="Systems Group Factual Report",
            blob_path=second_blob,
        )
        seed_download(
            connection,
            ordinal=1,
            title="Operations Group Chairman Factual Report",
            blob_path=first_blob,
        )
        result = materialize_case(
            connection,
            ntsb_number="DCA24FM001",
            output_root=tmp_path / "data" / "case_views",
        )

    first_path = result.output_dir / "001_operations_group_chairman_factual_report.pdf"
    second_path = result.output_dir / "002_systems_group_factual_report.pdf"

    assert result.materialized == 2
    assert result.reused == 0
    assert first_path.exists()
    assert second_path.exists()
    assert sorted(path.name for path in result.output_dir.iterdir()) == [
        "001_operations_group_chairman_factual_report.pdf",
        "002_systems_group_factual_report.pdf",
    ]
    assert first_path.is_symlink() or first_path.read_bytes() == first_blob.read_bytes()
    assert second_path.is_symlink() or second_path.read_bytes() == second_blob.read_bytes()

    with connect_sqlite(sqlite_path) as connection:
        rerun = materialize_case(
            connection,
            ntsb_number="DCA24FM001",
            output_root=tmp_path / "data" / "case_views",
        )

    assert rerun.materialized == 0
    assert rerun.reused == 2
    assert len(list(result.output_dir.iterdir())) == 2


def test_materialize_case_command_reports_materialized_file_count(tmp_path: Path) -> None:
    sqlite_path = tmp_path / "data" / "acquisition" / "state" / "acquisition.db"
    data_root = tmp_path / "data"
    downstream_raw_root = tmp_path / "corpus" / "raw"
    blob_path = tmp_path / "blobs" / "source.pdf"
    blob_path.parent.mkdir(parents=True, exist_ok=True)
    blob_path.write_bytes(b"%PDF-1.7 source")
    init_db(sqlite_path)

    with connect_sqlite(sqlite_path) as connection:
        seed_download(
            connection,
            ordinal=1,
            title="Operations Group Chairman Factual Report",
            blob_path=blob_path,
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
            "materialize-case",
            "--ntsb-number",
            "DCA24FM001",
        ],
    )

    payload = json.loads(result.stdout.strip())
    output_dir = data_root / "case_views" / "DCA24FM001"

    assert result.exit_code == 0
    assert payload["command"] == "materialize-case"
    assert payload["status"] == "ok"
    assert payload["materialized"] == 1
    assert payload["output_dir"] == str(output_dir)
    assert "Acquisition Summary" in result.stderr
    assert "Materialized" in result.stderr
    assert "Output Directory" in result.stderr
    assert (output_dir / "001_operations_group_chairman_factual_report.pdf").exists()


def test_materialize_case_command_help_includes_html_feedback() -> None:
    result = runner.invoke(app, ["materialize-case", "--help"])

    assert result.exit_code == 0
    assert "--html-feedback" in result.stdout


def test_materialize_case_command_generates_html_feedback(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    sqlite_path = tmp_path / "data" / "acquisition" / "state" / "acquisition.db"
    data_root = tmp_path / "data"
    downstream_raw_root = tmp_path / "corpus" / "raw"
    blob_path = tmp_path / "blobs" / "source.pdf"
    report_root = tmp_path / "reports"
    opened_reports: list[Path] = []
    blob_path.parent.mkdir(parents=True, exist_ok=True)
    blob_path.write_bytes(b"%PDF-1.7 source")
    init_db(sqlite_path)

    with connect_sqlite(sqlite_path) as connection:
        seed_download(
            connection,
            ordinal=1,
            title="Operations Group Chairman Factual Report",
            blob_path=blob_path,
        )

    monkeypatch.setattr("incident_pipeline.acquisition.ntsb.feedback.REPORTS_ROOT", report_root)
    monkeypatch.setattr(
        "incident_pipeline.acquisition.ntsb.cli.open_html_report",
        lambda report_path: opened_reports.append(report_path),
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
            "materialize-case",
            "--ntsb-number",
            "DCA24FM001",
            "--html-feedback",
        ],
    )

    payload = json.loads(result.stdout.strip())
    report_files = sorted(report_root.glob("*.html"))

    assert result.exit_code == 0
    assert payload["command"] == "materialize-case"
    assert len(report_files) == 1
    assert opened_reports == report_files
    assert "HTML Report" in result.stderr
    report_html = report_files[0].read_text(encoding="utf-8")
    assert "materialize-case" in report_html
    assert "DCA24FM001" in report_html
    assert "Output Directory" in report_html
    assert "What happened" in report_html
    assert (
        "This run created a new materialized case view for DCA24FM001 "
        "and wrote 1 files into the output directory."
    ) in report_html


def test_render_html_report_explains_materialize_case_full_reuse() -> None:
    report_html = render_html_report(
        command="materialize-case",
        run_id="run_test",
        timestamp="2026-03-26T12:00:00Z",
        ntsb_number="DCA24FM001",
        materialized=0,
        reused=58,
        output_dir="/tmp/case_views/DCA24FM001",
    )

    assert "What happened" in report_html
    assert (
        "This run found the materialized case view for DCA24FM001 already complete. "
        "No new files were created, and 58 existing files were reused."
    ) in report_html
    assert "Why counts look this way" in report_html
    assert "Materialized 0 means no new files needed to be created." in report_html
    assert "Reused 58 means the case view already contained 58 valid files." in report_html
