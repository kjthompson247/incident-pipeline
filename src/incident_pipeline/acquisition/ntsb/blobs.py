from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from pathlib import Path

from incident_pipeline.acquisition.ntsb.db import fetch_one, upsert_blob_current, upsert_download_current
from incident_pipeline.acquisition.ntsb.hashing import sha256_bytes
from incident_pipeline.acquisition.ntsb.http import HttpClient
from incident_pipeline.acquisition.ntsb.identifiers import blob_relative_path
from incident_pipeline.acquisition.ntsb.manifests import append_jsonl_record
from incident_pipeline.acquisition.ntsb.models import AcquisitionManifestRecord, BlobRecord, DownloadRecord
from incident_pipeline.acquisition.ntsb.runtime import RunContext

DOWNLOAD_SOURCE = "download"
DOWNLOAD_ACTION = "download_selected_artifact"
DOWNLOAD_PARSER_VERSION = "download_v1"


@dataclass(frozen=True)
class DownloadSelectedResult:
    evaluated: int
    downloaded: int
    reused: int


def _existing_download_row(
    connection: sqlite3.Connection,
    *,
    docket_item_id: str,
) -> sqlite3.Row | None:
    return fetch_one(
        connection,
        """
        SELECT source_fingerprint, blob_sha256, blob_path
        FROM downloads_current
        WHERE docket_item_id = ?
        """,
        (docket_item_id,),
    )


def _selected_rows(connection: sqlite3.Connection, *, ntsb_number: str) -> list[sqlite3.Row]:
    rows = connection.execute(
        """
        SELECT *
        FROM docket_items_current
        WHERE ntsb_number = ?
          AND is_active = 1
          AND selection_selected = 1
        ORDER BY ordinal
        """,
        (ntsb_number,),
    )
    return list(rows.fetchall())


def download_selected(
    connection: sqlite3.Connection,
    http_client: HttpClient,
    *,
    ntsb_number: str,
    run_context: RunContext,
    manifest_path: Path,
) -> DownloadSelectedResult:
    evaluated = 0
    downloaded = 0
    reused = 0
    observed_at = run_context.started_at

    for row in _selected_rows(connection, ntsb_number=ntsb_number):
        evaluated += 1
        docket_item_id = row["docket_item_id"]
        source_url = row["download_url"]
        if not source_url:
            continue

        existing = _existing_download_row(connection, docket_item_id=docket_item_id)
        if (
            existing is not None
            and existing["source_fingerprint"] == row["source_fingerprint"]
            and Path(existing["blob_path"]).exists()
        ):
            append_jsonl_record(
                manifest_path,
                AcquisitionManifestRecord(
                    run_id=run_context.run_id,
                    source=DOWNLOAD_SOURCE,
                    ntsb_number=row["ntsb_number"],
                    action=DOWNLOAD_ACTION,
                    input_fingerprint=row["source_fingerprint"],
                    output_path=existing["blob_path"],
                    content_hash=existing["blob_sha256"],
                    parser_version=DOWNLOAD_PARSER_VERSION,
                    retrieved_at=observed_at,
                    changed=False,
                    note="reused_existing_download",
                ),
            )
            reused += 1
            continue

        response = http_client.request("GET", source_url)
        content = response.content
        blob_sha256 = sha256_bytes(content)
        blob_path = run_context.paths.acquisition_blobs_root / blob_relative_path(blob_sha256)
        blob_path.parent.mkdir(parents=True, exist_ok=True)
        if not blob_path.exists():
            blob_path.write_bytes(content)
        media_type = response.headers.get("content-type")

        upsert_blob_current(
            connection,
            BlobRecord(
                blob_sha256=blob_sha256,
                blob_path=str(blob_path),
                media_type=media_type,
                size_bytes=len(content),
            ),
            observed_at=observed_at,
            run_id=run_context.run_id,
        )
        upsert_download_current(
            connection,
            DownloadRecord(
                docket_item_id=docket_item_id,
                ntsb_number=row["ntsb_number"],
                source_url=source_url,
                source_fingerprint=row["source_fingerprint"],
                blob_sha256=blob_sha256,
                blob_path=str(blob_path),
                media_type=media_type,
                size_bytes=len(content),
                http_status=response.status_code,
                downloaded_at=observed_at,
            ),
            run_id=run_context.run_id,
        )
        append_jsonl_record(
            manifest_path,
            AcquisitionManifestRecord(
                run_id=run_context.run_id,
                source=DOWNLOAD_SOURCE,
                ntsb_number=row["ntsb_number"],
                action=DOWNLOAD_ACTION,
                input_fingerprint=row["source_fingerprint"],
                output_path=str(blob_path),
                content_hash=blob_sha256,
                http_status=response.status_code,
                parser_version=DOWNLOAD_PARSER_VERSION,
                retrieved_at=observed_at,
                changed=True,
            ),
        )
        downloaded += 1

    connection.commit()
    return DownloadSelectedResult(
        evaluated=evaluated,
        downloaded=downloaded,
        reused=reused,
    )
