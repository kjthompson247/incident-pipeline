from __future__ import annotations

import json
import shutil
import sqlite3
from dataclasses import dataclass
from pathlib import Path

from incident_pipeline.acquisition.ntsb.db import upsert_promotion_current
from incident_pipeline.acquisition.ntsb.hashing import sha256_bytes
from incident_pipeline.acquisition.ntsb.manifests import append_jsonl_record
from incident_pipeline.acquisition.ntsb.models import PromotionManifestRecord, PromotionRecord
from incident_pipeline.acquisition.ntsb.runtime import RunContext


@dataclass(frozen=True)
class PromotionResult:
    evaluated: int
    promoted: int
    reused: int


def promotion_destination_path(
    *,
    downstream_raw_root: Path,
    ntsb_number: str,
    docket_item_id: str,
    blob_sha256: str,
) -> Path:
    filename = f"{docket_item_id}__{blob_sha256[:16]}.pdf"
    return downstream_raw_root / ntsb_number / filename


def _selected_download_rows(
    connection: sqlite3.Connection,
    *,
    ntsb_number: str,
) -> list[sqlite3.Row]:
    rows = connection.execute(
        """
        SELECT
            docket_items_current.ntsb_number,
            docket_items_current.docket_item_id,
            docket_items_current.selection_rule_version,
            docket_items_current.selection_matched_rules_json,
            docket_items_current.selection_rationale,
            downloads_current.blob_sha256,
            downloads_current.blob_path
        FROM docket_items_current
        JOIN downloads_current
          ON downloads_current.docket_item_id = docket_items_current.docket_item_id
        WHERE docket_items_current.ntsb_number = ?
          AND docket_items_current.is_active = 1
          AND docket_items_current.selection_selected = 1
        ORDER BY docket_items_current.ordinal
        """,
        (ntsb_number,),
    )
    return list(rows.fetchall())


def promote_selected(
    connection: sqlite3.Connection,
    *,
    ntsb_number: str,
    run_context: RunContext,
    manifest_path: Path,
) -> PromotionResult:
    evaluated = 0
    promoted = 0
    reused = 0
    observed_at = run_context.started_at

    for row in _selected_download_rows(connection, ntsb_number=ntsb_number):
        evaluated += 1
        destination = promotion_destination_path(
            downstream_raw_root=run_context.paths.downstream_raw_root,
            ntsb_number=row["ntsb_number"],
            docket_item_id=row["docket_item_id"],
            blob_sha256=row["blob_sha256"],
        )
        source_blob_path = Path(row["blob_path"])
        destination.parent.mkdir(parents=True, exist_ok=True)

        changed = True
        if destination.exists():
            destination_hash = sha256_bytes(destination.read_bytes())
            if destination_hash == row["blob_sha256"]:
                changed = False
            else:
                raise ValueError(
                    "Promotion destination already exists with different content: "
                    f"{destination}"
                )
        if changed:
            shutil.copyfile(source_blob_path, destination)
            promoted += 1
        else:
            reused += 1

        rule_version = row["selection_rule_version"] or "selection_v1"
        matched_rules_json = row["selection_matched_rules_json"] or "[]"
        matched_rules = json.loads(matched_rules_json)
        rationale = row["selection_rationale"] or ""
        upsert_promotion_current(
            connection,
            PromotionRecord(
                docket_item_id=row["docket_item_id"],
                ntsb_number=row["ntsb_number"],
                blob_sha256=row["blob_sha256"],
                promoted_path=str(destination),
                rule_version=rule_version,
                matched_rules=matched_rules,
                rationale=rationale,
                promoted_at=observed_at,
            ),
            run_id=run_context.run_id,
        )
        append_jsonl_record(
            manifest_path,
            PromotionManifestRecord(
                run_id=run_context.run_id,
                ntsb_number=row["ntsb_number"],
                docket_item_id=row["docket_item_id"],
                blob_sha256=row["blob_sha256"],
                source_blob_path=str(source_blob_path),
                promoted_path=str(destination),
                rule_version=rule_version,
                matched_rules=matched_rules,
                rationale=rationale,
                promoted_at=observed_at,
                downstream_registration_status="not_attempted",
            ),
        )

    connection.commit()
    return PromotionResult(
        evaluated=evaluated,
        promoted=promoted,
        reused=reused,
    )
