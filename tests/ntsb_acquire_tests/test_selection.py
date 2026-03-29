from __future__ import annotations

import json
from pathlib import Path

from incident_pipeline.acquisition.ntsb.config import AppConfig
from incident_pipeline.acquisition.ntsb.db import connect_sqlite, fetch_all, init_db, upsert_docket_item_current
from incident_pipeline.acquisition.ntsb.models import DocketItem
from incident_pipeline.acquisition.ntsb.runtime import create_run_context
from incident_pipeline.acquisition.ntsb.selection import apply_selection, evaluate_docket_item


def make_docket_item(
    *,
    docket_item_id: str,
    title: str,
    download_url: str,
    display_type: str | None = "PDF",
    page_count: int | None = 10,
    photo_count: int | None = None,
) -> DocketItem:
    return DocketItem(
        docket_item_id=docket_item_id,
        ntsb_number="DCA24FM001",
        ordinal=int(docket_item_id.split(":")[3]),
        normalized_title_slug=docket_item_id.split(":")[4],
        title=title,
        page_count=page_count,
        photo_count=photo_count,
        display_type=display_type,
        view_url=download_url,
        download_url=download_url,
        source_fingerprint=f"fp-{docket_item_id}",
    )


def test_evaluate_docket_item_selects_high_value_pdf() -> None:
    decision = evaluate_docket_item(
        make_docket_item(
            docket_item_id="ntsb:docket_item:DCA24FM001:1:operations_group_report",
            title="Operations Group Chairman Factual Report",
            download_url="https://example.test/operations.pdf",
        )
    )

    assert decision.selected is True
    assert decision.rule_version == "selection_v1"
    assert "pdf_download_url" in decision.matched_rules
    assert "title:report" in decision.matched_rules


def test_evaluate_docket_item_rejects_false_positive_candidates() -> None:
    photo_decision = evaluate_docket_item(
        make_docket_item(
            docket_item_id="ntsb:docket_item:DCA24FM001:1:photo_gallery",
            title="Photo Gallery",
            download_url="https://example.test/gallery.pdf",
            page_count=None,
            photo_count=8,
        )
    )
    admin_decision = evaluate_docket_item(
        make_docket_item(
            docket_item_id="ntsb:docket_item:DCA24FM001:2:administrative_correspondence",
            title="Administrative Correspondence",
            download_url="https://example.test/correspondence.pdf",
        )
    )

    assert photo_decision.selected is False
    assert "reject_photo_only" in photo_decision.matched_rules
    assert "photo-only" in photo_decision.rationale
    assert admin_decision.selected is False
    assert any(rule.startswith("reject_title:") for rule in admin_decision.matched_rules)
    assert "low-value" in admin_decision.rationale


def test_evaluate_docket_item_rejects_non_pdf_signal() -> None:
    decision = evaluate_docket_item(
        make_docket_item(
            docket_item_id="ntsb:docket_item:DCA24FM001:3:meeting_minutes",
            title="Meeting Minutes",
            download_url="https://example.test/meeting.html",
            display_type="HTML",
        )
    )

    assert decision.selected is False
    assert decision.rule_version == "selection_v1"
    assert decision.matched_rules == []
    assert "clear PDF signal" in decision.rationale


def test_apply_selection_updates_current_state_and_manifest(tmp_path: Path) -> None:
    sqlite_path = tmp_path / "state" / "acquisition.db"
    manifest_path = tmp_path / "manifests" / "selection_decisions.jsonl"
    init_db(sqlite_path)
    run_context = create_run_context(AppConfig(), run_id="run_select")

    with connect_sqlite(sqlite_path) as connection:
        upsert_docket_item_current(
            connection,
            make_docket_item(
                docket_item_id="ntsb:docket_item:DCA24FM001:1:operations_group_report",
                title="Operations Group Chairman Factual Report",
                download_url="https://example.test/operations.pdf",
            ),
            observed_at="2026-03-25T03:00:00Z",
            run_id="seed_run",
        )
        upsert_docket_item_current(
            connection,
            make_docket_item(
                docket_item_id="ntsb:docket_item:DCA24FM001:2:administrative_correspondence",
                title="Administrative Correspondence",
                download_url="https://example.test/correspondence.pdf",
            ),
            observed_at="2026-03-25T03:00:00Z",
            run_id="seed_run",
        )
        connection.commit()

        result = apply_selection(
            connection,
            ntsb_number="DCA24FM001",
            run_context=run_context,
            manifest_path=manifest_path,
        )
        rows = fetch_all(
            connection,
            """
            SELECT docket_item_id, selection_selected, selection_rule_version, selection_rationale
            FROM docket_items_current
            ORDER BY ordinal
            """,
        )

    manifest_records = [
        json.loads(line)
        for line in manifest_path.read_text(encoding="utf-8").splitlines()
    ]

    assert result.evaluated == 2
    assert result.selected == 1
    assert result.rejected == 1
    assert [row["selection_selected"] for row in rows] == [1, 0]
    assert all(row["selection_rule_version"] == "selection_v1" for row in rows)
    assert manifest_records[0]["selected"] is True
    assert manifest_records[1]["selected"] is False
    assert manifest_records[1]["rationale"]
    assert [record["decided_at"] for record in manifest_records] == [
        run_context.started_at,
        run_context.started_at,
    ]
    assert all(
        key in record
        for record in manifest_records
        for key in ("run_id", "selected", "rule_version", "matched_rules", "rationale")
    )


def test_apply_selection_is_deterministic_across_reruns(tmp_path: Path) -> None:
    sqlite_path = tmp_path / "state" / "acquisition.db"
    manifest_path = tmp_path / "manifests" / "selection_decisions.jsonl"
    init_db(sqlite_path)

    with connect_sqlite(sqlite_path) as connection:
        upsert_docket_item_current(
            connection,
            make_docket_item(
                docket_item_id="ntsb:docket_item:DCA24FM001:1:operations_group_report",
                title="Operations Group Chairman Factual Report",
                download_url="https://example.test/operations.pdf",
            ),
            observed_at="2026-03-25T03:00:00Z",
            run_id="seed_run",
        )
        upsert_docket_item_current(
            connection,
            make_docket_item(
                docket_item_id="ntsb:docket_item:DCA24FM001:2:administrative_correspondence",
                title="Administrative Correspondence",
                download_url="https://example.test/correspondence.pdf",
            ),
            observed_at="2026-03-25T03:00:00Z",
            run_id="seed_run",
        )
        connection.commit()

        first_context = create_run_context(AppConfig(), run_id="run_one")
        second_context = create_run_context(AppConfig(), run_id="run_two")

        first_result = apply_selection(
            connection,
            ntsb_number="DCA24FM001",
            run_context=first_context,
            manifest_path=manifest_path,
        )
        first_manifest = [
            json.loads(line)
            for line in manifest_path.read_text(encoding="utf-8").splitlines()
        ]
        second_result = apply_selection(
            connection,
            ntsb_number="DCA24FM001",
            run_context=second_context,
            manifest_path=manifest_path,
        )
        rows = fetch_all(
            connection,
            """
            SELECT docket_item_id, selection_selected, selection_rule_version, selection_rationale
            FROM docket_items_current
            ORDER BY ordinal
            """,
        )

    manifest_records = [
        json.loads(line)
        for line in manifest_path.read_text(encoding="utf-8").splitlines()
    ]
    second_manifest = manifest_records[len(first_manifest) :]

    assert first_result == second_result
    assert [row["selection_selected"] for row in rows] == [1, 0]
    assert [row["selection_rule_version"] for row in rows] == ["selection_v1", "selection_v1"]
    assert [
        (record["selected"], record["rule_version"], record["matched_rules"], record["rationale"])
        for record in first_manifest
    ] == [
        (record["selected"], record["rule_version"], record["matched_rules"], record["rationale"])
        for record in second_manifest
    ]


def test_apply_selection_manifest_decided_at_uses_run_context_started_at(tmp_path: Path) -> None:
    sqlite_path = tmp_path / "state" / "acquisition.db"
    manifest_path = tmp_path / "manifests" / "selection_decisions.jsonl"
    init_db(sqlite_path)

    with connect_sqlite(sqlite_path) as connection:
        upsert_docket_item_current(
            connection,
            make_docket_item(
                docket_item_id="ntsb:docket_item:DCA24FM001:1:operations_group_report",
                title="Operations Group Chairman Factual Report",
                download_url="https://example.test/operations.pdf",
            ),
            observed_at="2026-03-25T03:00:00Z",
            run_id="seed_run",
        )
        connection.commit()

        run_context = create_run_context(AppConfig(), run_id="run_one")
        apply_selection(
            connection,
            ntsb_number="DCA24FM001",
            run_context=run_context,
            manifest_path=manifest_path,
        )

    manifest_records = [
        json.loads(line)
        for line in manifest_path.read_text(encoding="utf-8").splitlines()
    ]

    assert [record["decided_at"] for record in manifest_records] == [run_context.started_at]


def test_apply_selection_is_idempotent_across_reruns(tmp_path: Path) -> None:
    sqlite_path = tmp_path / "state" / "acquisition.db"
    manifest_path = tmp_path / "manifests" / "selection_decisions.jsonl"
    init_db(sqlite_path)

    with connect_sqlite(sqlite_path) as connection:
        upsert_docket_item_current(
            connection,
            make_docket_item(
                docket_item_id="ntsb:docket_item:DCA24FM001:1:operations_group_report",
                title="Operations Group Chairman Factual Report",
                download_url="https://example.test/operations.pdf",
            ),
            observed_at="2026-03-25T03:00:00Z",
            run_id="seed_run",
        )
        connection.commit()

        first_result = apply_selection(
            connection,
            ntsb_number="DCA24FM001",
            run_context=create_run_context(AppConfig(), run_id="run_one"),
            manifest_path=manifest_path,
        )
        second_result = apply_selection(
            connection,
            ntsb_number="DCA24FM001",
            run_context=create_run_context(AppConfig(), run_id="run_two"),
            manifest_path=manifest_path,
        )
        rows = fetch_all(
            connection,
            """
            SELECT docket_item_id, selection_selected, selection_rule_version
            FROM docket_items_current
            ORDER BY ordinal
            """,
        )

    manifest_records = [
        json.loads(line)
        for line in manifest_path.read_text(encoding="utf-8").splitlines()
    ]

    assert first_result.evaluated == 1
    assert first_result.selected == 1
    assert second_result.evaluated == 1
    assert second_result.selected == 1
    assert rows[0]["selection_selected"] == 1
    assert rows[0]["selection_rule_version"] == "selection_v1"
    assert [record["selected"] for record in manifest_records] == [True, True]
