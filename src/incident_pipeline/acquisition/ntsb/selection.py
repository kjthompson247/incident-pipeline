from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from pathlib import Path

from incident_pipeline.acquisition.ntsb.db import fetch_all, update_docket_item_selection
from incident_pipeline.acquisition.ntsb.manifests import append_jsonl_record
from incident_pipeline.acquisition.ntsb.models import DocketItem, SelectionDecision, SelectionManifestRecord
from incident_pipeline.acquisition.ntsb.runtime import RunContext

SELECTION_SOURCE = "selection"
SELECTION_ACTION = "evaluate_selection"
SELECTION_RULE_VERSION = "selection_v1"

HIGH_VALUE_TITLE_KEYWORDS = (
    "report",
    "factual",
    "transcript",
    "interview",
    "attachment",
    "exhibit",
    "analysis",
    "summary",
    "chairman",
    "maintenance records",
)

NEGATIVE_TITLE_KEYWORDS = (
    "photo",
    "image",
    "video",
    "audio",
    "gallery",
    "correspondence",
    "invoice",
    "receipt",
    "cover sheet",
    "administrative",
)


def _has_pdf_signal(docket_item: DocketItem) -> tuple[bool, list[str]]:
    matched_rules: list[str] = []
    download_url = (docket_item.download_url or "").lower()
    display_type = (docket_item.display_type or "").lower()
    if download_url.endswith(".pdf"):
        matched_rules.append("pdf_download_url")
    if "pdf" in display_type:
        matched_rules.append("pdf_display_type")
    return bool(matched_rules), matched_rules


def _high_value_title_match(title: str) -> tuple[bool, list[str]]:
    lowered = title.lower()
    matched_rules = [
        f"title:{keyword}"
        for keyword in HIGH_VALUE_TITLE_KEYWORDS
        if keyword in lowered
    ]
    return bool(matched_rules), matched_rules


def _negative_title_match(title: str) -> tuple[bool, list[str]]:
    lowered = title.lower()
    matched_rules = [
        f"reject_title:{keyword}"
        for keyword in NEGATIVE_TITLE_KEYWORDS
        if keyword in lowered
    ]
    return bool(matched_rules), matched_rules


def evaluate_docket_item(docket_item: DocketItem) -> SelectionDecision:
    pdf_signal, pdf_rules = _has_pdf_signal(docket_item)
    high_value, high_value_rules = _high_value_title_match(docket_item.title)
    negative_title, negative_rules = _negative_title_match(docket_item.title)

    matched_rules = [*pdf_rules, *high_value_rules]
    if docket_item.photo_count and not docket_item.page_count:
        return SelectionDecision(
            selected=False,
            rule_version=SELECTION_RULE_VERSION,
            matched_rules=[*matched_rules, "reject_photo_only"],
            rationale=(
                "Rejected because the docket item appears to be photo-only rather "
                "than a document PDF."
            ),
        )

    if negative_title:
        return SelectionDecision(
            selected=False,
            rule_version=SELECTION_RULE_VERSION,
            matched_rules=[*matched_rules, *negative_rules],
            rationale=(
                "Rejected because the title matches a known low-value or "
                "non-document pattern."
            ),
        )

    if not pdf_signal:
        return SelectionDecision(
            selected=False,
            rule_version=SELECTION_RULE_VERSION,
            matched_rules=matched_rules,
            rationale=(
                "Rejected because the docket item does not present a clear PDF "
                "signal."
            ),
        )

    if not high_value:
        return SelectionDecision(
            selected=False,
            rule_version=SELECTION_RULE_VERSION,
            matched_rules=matched_rules,
            rationale=(
                "Rejected because the title does not meet the conservative "
                "high-value document rule set."
            ),
        )

    return SelectionDecision(
        selected=True,
        rule_version=SELECTION_RULE_VERSION,
        matched_rules=matched_rules,
        rationale=(
            "Selected because the docket item is a PDF with a high-value "
            "investigative document title."
        ),
    )


def _row_to_docket_item(row: sqlite3.Row) -> DocketItem:
    return DocketItem(
        docket_item_id=row["docket_item_id"],
        ntsb_number=row["ntsb_number"],
        ordinal=row["ordinal"],
        normalized_title_slug=row["normalized_title_slug"],
        title=row["title"],
        page_count=row["page_count"],
        photo_count=row["photo_count"],
        display_type=row["display_type"],
        view_url=row["view_url"],
        download_url=row["download_url"],
        source_fingerprint=row["source_fingerprint"],
        is_active=bool(row["is_active"]),
    )


@dataclass(frozen=True)
class SelectionResult:
    evaluated: int
    selected: int
    rejected: int


def apply_selection(
    connection: sqlite3.Connection,
    *,
    ntsb_number: str,
    run_context: RunContext,
    manifest_path: Path,
) -> SelectionResult:
    rows = fetch_all(
        connection,
        """
        SELECT *
        FROM docket_items_current
        WHERE ntsb_number = ?
          AND is_active = 1
        ORDER BY ordinal
        """,
        (ntsb_number,),
    )
    evaluated = 0
    selected = 0
    observed_at = run_context.started_at

    for row in rows:
        docket_item = _row_to_docket_item(row)
        decision = evaluate_docket_item(docket_item)
        update_docket_item_selection(
            connection,
            docket_item_id=docket_item.docket_item_id,
            decision=decision,
            observed_at=observed_at,
            run_id=run_context.run_id,
        )
        append_jsonl_record(
            manifest_path,
            SelectionManifestRecord(
                run_id=run_context.run_id,
                source=SELECTION_SOURCE,
                action=SELECTION_ACTION,
                ntsb_number=docket_item.ntsb_number,
                docket_item_id=docket_item.docket_item_id,
                selected=decision.selected,
                rule_version=decision.rule_version,
                matched_rules=decision.matched_rules,
                rationale=decision.rationale,
                decided_at=observed_at,
            ),
        )
        evaluated += 1
        if decision.selected:
            selected += 1

    connection.commit()
    return SelectionResult(
        evaluated=evaluated,
        selected=selected,
        rejected=evaluated - selected,
    )
