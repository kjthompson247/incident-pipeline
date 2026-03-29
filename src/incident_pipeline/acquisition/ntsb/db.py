from __future__ import annotations

import os
import sqlite3
from collections.abc import Mapping
from pathlib import Path
from typing import Any, cast

from pydantic import BaseModel

from incident_pipeline.acquisition.ntsb.hashing import canonical_json
from incident_pipeline.acquisition.ntsb.models import (
    Docket,
    DocketItem,
    DocketSearchResult,
    DownloadRecord,
    Investigation,
    PromotionRecord,
)
from incident_pipeline.acquisition.ntsb.normalize import slugify

PACKAGE_ROOT = Path(__file__).resolve().parent
DEFAULT_SCHEMA_PATH = PACKAGE_ROOT / "sql" / "init.sql"


def connect_db(database: Path | str) -> sqlite3.Connection:
    connection = sqlite3.connect(os.fspath(database))
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA foreign_keys = ON;")
    return connection


def init_db(database: Path | str, schema_path: Path | str | None = None) -> None:
    db_path = Path(os.fspath(database))
    db_path.parent.mkdir(parents=True, exist_ok=True)
    resolved_schema_path = (
        Path(os.fspath(schema_path)) if schema_path is not None else DEFAULT_SCHEMA_PATH
    )
    schema_sql = resolved_schema_path.read_text(encoding="utf-8")

    connection = connect_db(db_path)
    try:
        with connection:
            connection.executescript(schema_sql)
    finally:
        connection.close()


def _as_mapping(value: BaseModel | Mapping[str, Any]) -> dict[str, Any]:
    if isinstance(value, BaseModel):
        return value.model_dump(mode="python")
    return dict(value)


def _json_text(value: Any) -> str:
    if value is None:
        return canonical_json({})
    if isinstance(value, str):
        return value
    return canonical_json(value)


def _bool_to_int(value: bool | None) -> int | None:
    if value is None:
        return None
    return 1 if value else 0


def _row_dict(row: sqlite3.Row | None) -> dict[str, Any] | None:
    if row is None:
        return None
    return dict(row)


def _row_material_matches(
    row: Mapping[str, Any],
    expected: Mapping[str, Any],
    keys: list[str],
) -> bool:
    return all(row.get(key) == expected.get(key) for key in keys)


def _fetch_current_row(
    connection: sqlite3.Connection,
    table: str,
    key_column: str,
    key_value: Any,
) -> dict[str, Any] | None:
    cursor = connection.execute(
        f"SELECT * FROM {table} WHERE {key_column} = ?",
        (key_value,),
    )
    return _row_dict(cursor.fetchone())


def _fetch_current_item(
    connection: sqlite3.Connection,
    ntsb_number: str,
    ordinal: int,
) -> dict[str, Any] | None:
    cursor = connection.execute(
        """
        SELECT *
        FROM docket_items_current
        WHERE ntsb_number = ? AND ordinal = ?
        """,
        (ntsb_number, ordinal),
    )
    return _row_dict(cursor.fetchone())


def _docket_item_payload(item: DocketItem | Mapping[str, Any]) -> dict[str, Any]:
    payload = _as_mapping(item)
    title = str(payload["title"]).strip()
    if not title:
        raise ValueError("docket item title is required")

    ntsb_number = str(payload["ntsb_number"]).strip()
    ordinal = int(payload["ordinal"])
    normalized_title_slug = (
        str(payload.get("normalized_title_slug") or "").strip() or slugify(title)
    )
    docket_item_id = str(
        payload.get("docket_item_id")
        or f"ntsb:docket_item:{ntsb_number}:{ordinal}:{normalized_title_slug}"
    )
    selection = payload.get("selection")
    selection_payload = (
        _as_mapping(selection) if isinstance(selection, BaseModel) else dict(selection or {})
    )
    raw_is_active = payload.get("is_active", True)
    is_active = 1 if raw_is_active is None else _bool_to_int(bool(raw_is_active))

    return {
        "docket_item_id": docket_item_id,
        "ntsb_number": ntsb_number,
        "ordinal": ordinal,
        "normalized_title_slug": normalized_title_slug,
        "title": title,
        "page_count": payload.get("page_count"),
        "photo_count": payload.get("photo_count"),
        "display_type": payload.get("display_type"),
        "view_url": payload.get("view_url"),
        "download_url": payload.get("download_url"),
        "source_fingerprint": payload.get("source_fingerprint"),
        "selection_selected": _bool_to_int(selection_payload.get("selected")),
        "selection_rule_version": selection_payload.get("rule_version"),
        "selection_matched_rules_json": (
            canonical_json(selection_payload.get("matched_rules", []))
            if selection_payload
            else None
        ),
        "selection_rationale": selection_payload.get("rationale") if selection_payload else None,
        "is_active": is_active,
    }


def upsert_investigation(
    connection: sqlite3.Connection,
    investigation: Investigation | Mapping[str, Any],
    observed_at: str,
    run_id: str,
) -> bool:
    payload = _as_mapping(investigation)
    ntsb_number = str(payload["ntsb_number"]).strip()
    values = {
        "investigation_id": str(payload["investigation_id"]),
        "project_id": payload.get("project_id"),
        "mode": payload.get("mode"),
        "event_date": payload.get("event_date"),
        "title": payload.get("title"),
        "case_type": payload.get("case_type"),
        "location_json": _json_text(payload.get("location")),
        "carol_metadata_json": _json_text(payload.get("carol_metadata")),
        "source_fingerprint": payload.get("source_fingerprint"),
        "first_seen_at": observed_at,
        "last_seen_at": observed_at,
        "last_run_id": run_id,
    }
    material_keys = [
        "investigation_id",
        "project_id",
        "mode",
        "event_date",
        "title",
        "case_type",
        "location_json",
        "carol_metadata_json",
        "source_fingerprint",
    ]

    existing = _fetch_current_row(connection, "investigations_current", "ntsb_number", ntsb_number)
    if existing is None:
        with connection:
            connection.execute(
                """
                INSERT INTO investigations_current (
                    ntsb_number,
                    investigation_id,
                    project_id,
                    mode,
                    event_date,
                    title,
                    case_type,
                    location_json,
                    carol_metadata_json,
                    source_fingerprint,
                    first_seen_at,
                    last_seen_at,
                    last_run_id
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    ntsb_number,
                    values["investigation_id"],
                    values["project_id"],
                    values["mode"],
                    values["event_date"],
                    values["title"],
                    values["case_type"],
                    values["location_json"],
                    values["carol_metadata_json"],
                    values["source_fingerprint"],
                    observed_at,
                    observed_at,
                    run_id,
                ),
            )
        return True

    changed = not _row_material_matches(existing, values, material_keys)
    with connection:
        connection.execute(
            """
            UPDATE investigations_current
            SET investigation_id = ?,
                project_id = ?,
                mode = ?,
                event_date = ?,
                title = ?,
                case_type = ?,
                location_json = ?,
                carol_metadata_json = ?,
                source_fingerprint = ?,
                last_seen_at = ?,
                last_run_id = ?
            WHERE ntsb_number = ?
            """,
            (
                values["investigation_id"],
                values["project_id"],
                values["mode"],
                values["event_date"],
                values["title"],
                values["case_type"],
                values["location_json"],
                values["carol_metadata_json"],
                values["source_fingerprint"],
                observed_at,
                run_id,
                ntsb_number,
            ),
        )
    return changed


def upsert_docket(
    connection: sqlite3.Connection,
    docket: Docket | Mapping[str, Any],
    observed_at: str,
    run_id: str,
) -> bool:
    payload = _as_mapping(docket)
    ntsb_number = str(payload["ntsb_number"]).strip()
    values = {
        "docket_id": str(payload["docket_id"]),
        "project_id": payload.get("project_id"),
        "mode": payload.get("mode"),
        "public_url": payload.get("public_url"),
        "item_count": int(payload.get("item_count", 0)),
        "creation_date": payload.get("creation_date"),
        "last_modified": payload.get("last_modified"),
        "public_release_at": payload.get("public_release_at"),
        "raw_html_path": payload.get("raw_html_path"),
        "retrieved_at": payload.get("retrieved_at"),
        "source_fingerprint": payload.get("source_fingerprint"),
        "first_seen_at": observed_at,
        "last_seen_at": observed_at,
        "last_run_id": run_id,
    }
    material_keys = [
        "docket_id",
        "project_id",
        "mode",
        "public_url",
        "item_count",
        "creation_date",
        "last_modified",
        "public_release_at",
        "raw_html_path",
        "retrieved_at",
        "source_fingerprint",
    ]

    existing = _fetch_current_row(connection, "dockets_current", "ntsb_number", ntsb_number)
    if existing is None:
        with connection:
            connection.execute(
                """
                INSERT INTO dockets_current (
                    ntsb_number,
                    docket_id,
                    project_id,
                    mode,
                    public_url,
                    item_count,
                    creation_date,
                    last_modified,
                    public_release_at,
                    raw_html_path,
                    retrieved_at,
                    source_fingerprint,
                    first_seen_at,
                    last_seen_at,
                    last_run_id
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    ntsb_number,
                    values["docket_id"],
                    values["project_id"],
                    values["mode"],
                    values["public_url"],
                    values["item_count"],
                    values["creation_date"],
                    values["last_modified"],
                    values["public_release_at"],
                    values["raw_html_path"],
                    values["retrieved_at"],
                    values["source_fingerprint"],
                    observed_at,
                    observed_at,
                    run_id,
                ),
            )
        return True

    changed = not _row_material_matches(existing, values, material_keys)
    with connection:
        connection.execute(
            """
            UPDATE dockets_current
            SET docket_id = ?,
                project_id = ?,
                mode = ?,
                public_url = ?,
                item_count = ?,
                creation_date = ?,
                last_modified = ?,
                public_release_at = ?,
                raw_html_path = ?,
                retrieved_at = ?,
                source_fingerprint = ?,
                last_seen_at = ?,
                last_run_id = ?
            WHERE ntsb_number = ?
            """,
            (
                values["docket_id"],
                values["project_id"],
                values["mode"],
                values["public_url"],
                values["item_count"],
                values["creation_date"],
                values["last_modified"],
                values["public_release_at"],
                values["raw_html_path"],
                values["retrieved_at"],
                values["source_fingerprint"],
                observed_at,
                run_id,
                ntsb_number,
            ),
        )
    return changed


def upsert_docket_search_result(
    connection: sqlite3.Connection,
    result: DocketSearchResult | Mapping[str, Any],
    observed_at: str,
    run_id: str,
) -> bool:
    payload = _as_mapping(result)
    ntsb_number = str(payload["ntsb_number"]).strip()
    values = {
        "project_id": str(payload["project_id"]).strip(),
        "result_url": str(payload["result_url"]).strip(),
        "event_date": payload.get("event_date"),
        "city": payload.get("city"),
        "state_or_region": payload.get("state_or_region"),
        "docket_details": payload.get("docket_details"),
        "accident_description": payload.get("accident_description"),
        "source_fingerprint": payload.get("source_fingerprint"),
        "first_seen_at": observed_at,
        "last_seen_at": observed_at,
        "last_run_id": run_id,
    }
    material_keys = [
        "project_id",
        "result_url",
        "event_date",
        "city",
        "state_or_region",
        "docket_details",
        "accident_description",
        "source_fingerprint",
    ]

    existing = _fetch_current_row(
        connection,
        "docket_search_results_current",
        "ntsb_number",
        ntsb_number,
    )
    if existing is None:
        with connection:
            connection.execute(
                """
                INSERT INTO docket_search_results_current (
                    ntsb_number,
                    project_id,
                    result_url,
                    event_date,
                    city,
                    state_or_region,
                    docket_details,
                    accident_description,
                    source_fingerprint,
                    first_seen_at,
                    last_seen_at,
                    last_run_id
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    ntsb_number,
                    values["project_id"],
                    values["result_url"],
                    values["event_date"],
                    values["city"],
                    values["state_or_region"],
                    values["docket_details"],
                    values["accident_description"],
                    values["source_fingerprint"],
                    observed_at,
                    observed_at,
                    run_id,
                ),
            )
        return True

    changed = not _row_material_matches(existing, values, material_keys)
    with connection:
        connection.execute(
            """
            UPDATE docket_search_results_current
            SET project_id = ?,
                result_url = ?,
                event_date = ?,
                city = ?,
                state_or_region = ?,
                docket_details = ?,
                accident_description = ?,
                source_fingerprint = ?,
                last_seen_at = ?,
                last_run_id = ?
            WHERE ntsb_number = ?
            """,
            (
                values["project_id"],
                values["result_url"],
                values["event_date"],
                values["city"],
                values["state_or_region"],
                values["docket_details"],
                values["accident_description"],
                values["source_fingerprint"],
                observed_at,
                run_id,
                ntsb_number,
            ),
        )
    return changed


def replace_docket_items(
    connection: sqlite3.Connection,
    ntsb_number: str,
    items: list[DocketItem | Mapping[str, Any]],
    observed_at: str,
    run_id: str,
) -> dict[str, int]:
    normalized_items = [_docket_item_payload(item) for item in items]
    normalized_items.sort(key=lambda item: item["ordinal"])
    seen_ordinals: set[int] = set()
    summary = {
        "inserted": 0,
        "updated": 0,
        "replaced": 0,
        "reactivated": 0,
        "deactivated": 0,
        "unchanged": 0,
        "active_count": len(normalized_items),
    }

    existing_rows = {
        row["ordinal"]: dict(row)
        for row in connection.execute(
            """
            SELECT *
            FROM docket_items_current
            WHERE ntsb_number = ?
            """,
            (ntsb_number,),
        )
    }

    with connection:
        for item in normalized_items:
            ordinal = item["ordinal"]
            if ordinal in seen_ordinals:
                raise ValueError(f"duplicate docket item ordinal: {ordinal}")
            seen_ordinals.add(ordinal)

            existing = existing_rows.get(ordinal)
            if existing is None:
                connection.execute(
                    """
                    INSERT INTO docket_items_current (
                        docket_item_id,
                        ntsb_number,
                        ordinal,
                        normalized_title_slug,
                        title,
                        page_count,
                        photo_count,
                        display_type,
                        view_url,
                        download_url,
                        source_fingerprint,
                        selection_selected,
                        selection_rule_version,
                        selection_matched_rules_json,
                        selection_rationale,
                        is_active,
                        first_seen_at,
                        last_seen_at,
                        last_run_id
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        item["docket_item_id"],
                        item["ntsb_number"],
                        item["ordinal"],
                        item["normalized_title_slug"],
                        item["title"],
                        item["page_count"],
                        item["photo_count"],
                        item["display_type"],
                        item["view_url"],
                        item["download_url"],
                        item["source_fingerprint"],
                        item["selection_selected"],
                        item["selection_rule_version"],
                        item["selection_matched_rules_json"],
                        item["selection_rationale"],
                        item["is_active"],
                        observed_at,
                        observed_at,
                        run_id,
                    ),
                )
                summary["inserted"] += 1
                continue

            current_id = str(existing["docket_item_id"])
            new_id = item["docket_item_id"]
            changed_values = {
                "normalized_title_slug": item["normalized_title_slug"],
                "title": item["title"],
                "page_count": item["page_count"],
                "photo_count": item["photo_count"],
                "display_type": item["display_type"],
                "view_url": item["view_url"],
                "download_url": item["download_url"],
                "source_fingerprint": item["source_fingerprint"],
                "selection_selected": item["selection_selected"],
                "selection_rule_version": item["selection_rule_version"],
                "selection_matched_rules_json": item["selection_matched_rules_json"],
                "selection_rationale": item["selection_rationale"],
                "is_active": item["is_active"],
            }
            material_keys = list(changed_values)
            material_keys.append("docket_item_id")
            material_matches = current_id == new_id and _row_material_matches(
                existing,
                {**changed_values, "docket_item_id": new_id},
                material_keys,
            )

            if current_id != new_id:
                connection.execute(
                    """
                    UPDATE docket_items_current
                    SET docket_item_id = ?,
                        normalized_title_slug = ?,
                        title = ?,
                        page_count = ?,
                        photo_count = ?,
                        display_type = ?,
                        view_url = ?,
                        download_url = ?,
                        source_fingerprint = ?,
                        selection_selected = ?,
                        selection_rule_version = ?,
                        selection_matched_rules_json = ?,
                        selection_rationale = ?,
                        is_active = ?,
                        last_seen_at = ?,
                        last_run_id = ?
                    WHERE docket_item_id = ?
                    """,
                    (
                        new_id,
                        item["normalized_title_slug"],
                        item["title"],
                        item["page_count"],
                        item["photo_count"],
                        item["display_type"],
                        item["view_url"],
                        item["download_url"],
                        item["source_fingerprint"],
                        item["selection_selected"],
                        item["selection_rule_version"],
                        item["selection_matched_rules_json"],
                        item["selection_rationale"],
                        item["is_active"],
                        observed_at,
                        run_id,
                        current_id,
                    ),
                )
                summary["replaced"] += 1
                if existing["is_active"] == 0 and item["is_active"] == 1:
                    summary["reactivated"] += 1
                elif existing["is_active"] == 1 and item["is_active"] == 0:
                    summary["deactivated"] += 1
                else:
                    summary["updated"] += 1
                continue

            if (
                material_matches
                and existing.get("last_seen_at") == observed_at
                and existing.get("last_run_id") == run_id
            ):
                summary["unchanged"] += 1
                continue

            connection.execute(
                """
                UPDATE docket_items_current
                SET normalized_title_slug = ?,
                    title = ?,
                    page_count = ?,
                    photo_count = ?,
                    display_type = ?,
                    view_url = ?,
                    download_url = ?,
                    source_fingerprint = ?,
                    selection_selected = ?,
                    selection_rule_version = ?,
                    selection_matched_rules_json = ?,
                    selection_rationale = ?,
                    is_active = ?,
                    last_seen_at = ?,
                    last_run_id = ?
                WHERE docket_item_id = ?
                """,
                (
                    item["normalized_title_slug"],
                    item["title"],
                    item["page_count"],
                    item["photo_count"],
                    item["display_type"],
                    item["view_url"],
                    item["download_url"],
                    item["source_fingerprint"],
                    item["selection_selected"],
                    item["selection_rule_version"],
                    item["selection_matched_rules_json"],
                    item["selection_rationale"],
                    item["is_active"],
                    observed_at,
                    run_id,
                    current_id,
                ),
            )
            if existing["is_active"] == 0 and item["is_active"] == 1:
                summary["reactivated"] += 1
            elif existing["is_active"] == 1 and item["is_active"] == 0:
                summary["deactivated"] += 1
            elif material_matches:
                summary["unchanged"] += 1
            else:
                summary["updated"] += 1

        for ordinal, existing in existing_rows.items():
            if ordinal in seen_ordinals:
                continue
            if existing["is_active"] == 0:
                continue
            connection.execute(
                """
                UPDATE docket_items_current
                SET is_active = 0,
                    last_run_id = ?
                WHERE docket_item_id = ?
                """,
                (run_id, existing["docket_item_id"]),
            )
            summary["deactivated"] += 1

    return summary


def register_blob(
    connection: sqlite3.Connection,
    *,
    blob_sha256: str,
    blob_path: str,
    size_bytes: int,
    media_type: str | None,
    downloaded_at: str,
    run_id: str,
) -> bool:
    existing = _fetch_current_row(connection, "blobs_current", "blob_sha256", blob_sha256)
    values = {
        "blob_path": blob_path,
        "media_type": media_type,
        "size_bytes": int(size_bytes),
    }
    changed = existing is None or not _row_material_matches(
        existing or {},
        values,
        ["blob_path", "media_type", "size_bytes"],
    )
    with connection:
        connection.execute(
            """
            INSERT INTO blobs_current (
                blob_sha256,
                blob_path,
                media_type,
                size_bytes,
                first_downloaded_at,
                first_run_id,
                last_verified_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(blob_sha256) DO UPDATE SET
                blob_path = excluded.blob_path,
                media_type = excluded.media_type,
                size_bytes = excluded.size_bytes,
                last_verified_at = excluded.last_verified_at
            """,
            (
                blob_sha256,
                blob_path,
                media_type,
                int(size_bytes),
                downloaded_at,
                run_id,
                downloaded_at,
            ),
        )
    return changed


def record_download(
    connection: sqlite3.Connection,
    download: DownloadRecord | Mapping[str, Any],
    *,
    downloaded_at: str,
    run_id: str,
) -> bool:
    payload = _as_mapping(download)
    record = {
        "ntsb_number": str(payload["ntsb_number"]),
        "source_url": str(payload["source_url"]),
        "source_fingerprint": payload.get("source_fingerprint"),
        "blob_sha256": str(payload["blob_sha256"]),
        "blob_path": str(payload["blob_path"]),
        "media_type": payload.get("media_type"),
        "size_bytes": int(payload["size_bytes"]),
        "http_status": payload.get("http_status"),
    }
    existing = _fetch_current_row(
        connection,
        "downloads_current",
        "docket_item_id",
        str(payload["docket_item_id"]),
    )
    material_keys = [
        "ntsb_number",
        "source_url",
        "source_fingerprint",
        "blob_sha256",
        "blob_path",
        "media_type",
        "size_bytes",
        "http_status",
    ]

    if existing is None:
        with connection:
            connection.execute(
                """
                INSERT INTO downloads_current (
                    docket_item_id,
                    ntsb_number,
                    source_url,
                    source_fingerprint,
                    blob_sha256,
                    blob_path,
                    media_type,
                    size_bytes,
                    http_status,
                    downloaded_at,
                    last_run_id
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    str(payload["docket_item_id"]),
                    record["ntsb_number"],
                    record["source_url"],
                    record["source_fingerprint"],
                    record["blob_sha256"],
                    record["blob_path"],
                    record["media_type"],
                    record["size_bytes"],
                    record["http_status"],
                    downloaded_at,
                    run_id,
                ),
            )
        return True

    changed = not _row_material_matches(existing, record, material_keys)
    with connection:
        connection.execute(
            """
            UPDATE downloads_current
            SET ntsb_number = ?,
                source_url = ?,
                source_fingerprint = ?,
                blob_sha256 = ?,
                blob_path = ?,
                media_type = ?,
                size_bytes = ?,
                http_status = ?,
                downloaded_at = ?,
                last_run_id = ?
            WHERE docket_item_id = ?
            """,
            (
                record["ntsb_number"],
                record["source_url"],
                record["source_fingerprint"],
                record["blob_sha256"],
                record["blob_path"],
                record["media_type"],
                record["size_bytes"],
                record["http_status"],
                downloaded_at,
                run_id,
                str(payload["docket_item_id"]),
            ),
        )
    return changed


def record_promotion(
    connection: sqlite3.Connection,
    promotion: PromotionRecord | Mapping[str, Any],
    *,
    promoted_at: str,
    run_id: str,
) -> bool:
    payload = _as_mapping(promotion)
    record = {
        "ntsb_number": str(payload["ntsb_number"]),
        "blob_sha256": str(payload["blob_sha256"]),
        "promoted_path": str(payload["promoted_path"]),
        "rule_version": str(payload["rule_version"]),
        "matched_rules_json": canonical_json(payload.get("matched_rules", [])),
        "rationale": str(payload["rationale"]),
    }
    existing = _fetch_current_row(
        connection,
        "promotions_current",
        "docket_item_id",
        str(payload["docket_item_id"]),
    )
    material_keys = [
        "ntsb_number",
        "blob_sha256",
        "promoted_path",
        "rule_version",
        "matched_rules_json",
        "rationale",
    ]

    if existing is None:
        with connection:
            connection.execute(
                """
                INSERT INTO promotions_current (
                    docket_item_id,
                    ntsb_number,
                    blob_sha256,
                    promoted_path,
                    rule_version,
                    matched_rules_json,
                    rationale,
                    promoted_at,
                    last_run_id
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    str(payload["docket_item_id"]),
                    record["ntsb_number"],
                    record["blob_sha256"],
                    record["promoted_path"],
                    record["rule_version"],
                    record["matched_rules_json"],
                    record["rationale"],
                    promoted_at,
                    run_id,
                ),
            )
        return True

    changed = not _row_material_matches(existing, record, material_keys)
    with connection:
        connection.execute(
            """
            UPDATE promotions_current
            SET ntsb_number = ?,
                blob_sha256 = ?,
                promoted_path = ?,
                rule_version = ?,
                matched_rules_json = ?,
                rationale = ?,
                promoted_at = ?,
                last_run_id = ?
            WHERE docket_item_id = ?
            """,
            (
                record["ntsb_number"],
                record["blob_sha256"],
                record["promoted_path"],
                record["rule_version"],
                record["matched_rules_json"],
                record["rationale"],
                promoted_at,
                run_id,
                str(payload["docket_item_id"]),
            ),
        )
    return changed


def connect_sqlite(sqlite_path: Path) -> sqlite3.Connection:
    return connect_db(sqlite_path)


def fetch_all(
    connection: sqlite3.Connection,
    query: str,
    parameters: tuple[Any, ...] = (),
) -> list[sqlite3.Row]:
    cursor = connection.execute(query, parameters)
    return list(cursor.fetchall())


def fetch_one(
    connection: sqlite3.Connection,
    query: str,
    parameters: tuple[Any, ...] = (),
) -> sqlite3.Row | None:
    cursor = connection.execute(query, parameters)
    return cast(sqlite3.Row | None, cursor.fetchone())


def upsert_investigation_current(
    connection: sqlite3.Connection,
    investigation: Investigation | Mapping[str, Any],
    *,
    observed_at: str,
    run_id: str,
) -> None:
    upsert_investigation(connection, investigation, observed_at, run_id)


def upsert_docket_current(
    connection: sqlite3.Connection,
    docket: Docket | Mapping[str, Any],
    *,
    observed_at: str,
    run_id: str,
) -> None:
    upsert_docket(connection, docket, observed_at, run_id)


def upsert_docket_search_result_current(
    connection: sqlite3.Connection,
    result: DocketSearchResult | Mapping[str, Any],
    *,
    observed_at: str,
    run_id: str,
) -> bool:
    return upsert_docket_search_result(connection, result, observed_at, run_id)


def upsert_docket_item_current(
    connection: sqlite3.Connection,
    docket_item: DocketItem | Mapping[str, Any],
    *,
    observed_at: str,
    run_id: str,
) -> None:
    item = _docket_item_payload(docket_item)
    existing = _fetch_current_item(connection, item["ntsb_number"], item["ordinal"])

    if existing is None:
        with connection:
            connection.execute(
                """
                INSERT INTO docket_items_current (
                    docket_item_id,
                    ntsb_number,
                    ordinal,
                    normalized_title_slug,
                    title,
                    page_count,
                    photo_count,
                    display_type,
                    view_url,
                    download_url,
                    source_fingerprint,
                    selection_selected,
                    selection_rule_version,
                    selection_matched_rules_json,
                    selection_rationale,
                    is_active,
                    first_seen_at,
                    last_seen_at,
                    last_run_id
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    item["docket_item_id"],
                    item["ntsb_number"],
                    item["ordinal"],
                    item["normalized_title_slug"],
                    item["title"],
                    item["page_count"],
                    item["photo_count"],
                    item["display_type"],
                    item["view_url"],
                    item["download_url"],
                    item["source_fingerprint"],
                    item["selection_selected"],
                    item["selection_rule_version"],
                    item["selection_matched_rules_json"],
                    item["selection_rationale"],
                    item["is_active"],
                    observed_at,
                    observed_at,
                    run_id,
                ),
            )
        return

    with connection:
        connection.execute(
            """
            UPDATE docket_items_current
            SET docket_item_id = ?,
                normalized_title_slug = ?,
                title = ?,
                page_count = ?,
                photo_count = ?,
                display_type = ?,
                view_url = ?,
                download_url = ?,
                source_fingerprint = ?,
                selection_selected = ?,
                selection_rule_version = ?,
                selection_matched_rules_json = ?,
                selection_rationale = ?,
                is_active = ?,
                last_seen_at = ?,
                last_run_id = ?
            WHERE ntsb_number = ? AND ordinal = ?
            """,
            (
                item["docket_item_id"],
                item["normalized_title_slug"],
                item["title"],
                item["page_count"],
                item["photo_count"],
                item["display_type"],
                item["view_url"],
                item["download_url"],
                item["source_fingerprint"],
                item["selection_selected"],
                item["selection_rule_version"],
                item["selection_matched_rules_json"],
                item["selection_rationale"],
                item["is_active"],
                observed_at,
                run_id,
                item["ntsb_number"],
                item["ordinal"],
            ),
        )


def deactivate_missing_docket_items(
    connection: sqlite3.Connection,
    *,
    ntsb_number: str,
    active_docket_item_ids: list[str],
    run_id: str,
) -> None:
    if active_docket_item_ids:
        placeholders = ", ".join("?" for _ in active_docket_item_ids)
        with connection:
            connection.execute(
                f"""
                UPDATE docket_items_current
                SET is_active = 0,
                    last_run_id = ?
                WHERE ntsb_number = ?
                  AND docket_item_id NOT IN ({placeholders})
                """,
                (run_id, ntsb_number, *active_docket_item_ids),
            )
        return

    with connection:
        connection.execute(
            """
            UPDATE docket_items_current
            SET is_active = 0,
                last_run_id = ?
            WHERE ntsb_number = ?
            """,
            (run_id, ntsb_number),
        )


def upsert_blob_current(
    connection: sqlite3.Connection,
    blob: Mapping[str, Any] | BaseModel,
    *,
    observed_at: str,
    run_id: str,
) -> None:
    payload = _as_mapping(blob)
    register_blob(
        connection,
        blob_sha256=str(payload["blob_sha256"]),
        blob_path=str(payload["blob_path"]),
        size_bytes=int(payload["size_bytes"]),
        media_type=payload.get("media_type"),
        downloaded_at=observed_at,
        run_id=run_id,
    )


def upsert_download_current(
    connection: sqlite3.Connection,
    download: DownloadRecord | Mapping[str, Any],
    *,
    run_id: str,
) -> None:
    payload = _as_mapping(download)
    record_download(
        connection,
        payload,
        downloaded_at=str(payload["downloaded_at"]),
        run_id=run_id,
    )


def upsert_promotion_current(
    connection: sqlite3.Connection,
    promotion: PromotionRecord | Mapping[str, Any],
    *,
    run_id: str,
) -> None:
    payload = _as_mapping(promotion)
    record_promotion(
        connection,
        payload,
        promoted_at=str(payload["promoted_at"]),
        run_id=run_id,
    )


def update_docket_item_selection(
    connection: sqlite3.Connection,
    *,
    docket_item_id: str,
    decision: BaseModel | Mapping[str, Any],
    observed_at: str,
    run_id: str,
) -> None:
    payload = _as_mapping(decision)
    with connection:
        connection.execute(
            """
            UPDATE docket_items_current
            SET selection_selected = ?,
                selection_rule_version = ?,
                selection_matched_rules_json = ?,
                selection_rationale = ?,
                last_seen_at = ?,
                last_run_id = ?
            WHERE docket_item_id = ?
            """,
            (
                _bool_to_int(bool(payload.get("selected"))),
                payload.get("rule_version"),
                canonical_json(payload.get("matched_rules", [])),
                payload.get("rationale"),
                observed_at,
                run_id,
                docket_item_id,
            ),
        )
