from __future__ import annotations

import hashlib
import os
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from incident_pipeline.common.paths import DEFAULT_SETTINGS_PATH, resolve_repo_path
from incident_pipeline.common.settings import load_settings
from incident_pipeline.ingestion.manifest_reader import iter_manifest_records

CONFIG_PATH = DEFAULT_SETTINGS_PATH
LINEAGE_COLUMNS = {
    "acquisition_run_id": "TEXT",
    "acquisition_manifest_path": "TEXT",
    "acquisition_blob_path": "TEXT",
    "project_id": "TEXT",
    "ntsb_number": "TEXT",
    "docket_item_id": "TEXT",
    "view_url": "TEXT",
    "blob_sha256": "TEXT",
}
PDF_MEDIA_TYPE = "application/pdf"


def load_config():
    config_path = None if CONFIG_PATH == DEFAULT_SETTINGS_PATH else CONFIG_PATH
    return load_settings(config_path)


# ---------- Data Model ----------

@dataclass
class ReportRecord:
    report_id: str
    jurisdiction: str | None
    case_number: str | None
    filing_date: str | None
    report_type: str | None
    source_url: str | None
    raw_path: str
    sha256: str
    file_size: int
    ingested_at: str
    status: str
    stage: str
    acquisition_run_id: str | None = None
    acquisition_manifest_path: str | None = None
    acquisition_blob_path: str | None = None
    project_id: str | None = None
    ntsb_number: str | None = None
    docket_item_id: str | None = None
    view_url: str | None = None
    blob_sha256: str | None = None


@dataclass(frozen=True)
class AcquisitionLineage:
    acquisition_run_id: str | None
    acquisition_manifest_path: str
    acquisition_blob_path: str
    project_id: str
    ntsb_number: str
    docket_item_id: str
    view_url: str
    source_url: str
    blob_sha256: str
    media_type: str


# ---------- Helpers ----------

def sha256_file(path: Path, chunk_size: int = 1024 * 1024) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(chunk_size):
            digest.update(chunk)
    return digest.hexdigest()


def infer_report_type(path: Path, *, media_type: str | None = None) -> str:
    normalized_media_type = (media_type or "").split(";", 1)[0].strip().lower()
    if normalized_media_type == PDF_MEDIA_TYPE:
        return "pdf"
    return path.suffix.lower().lstrip(".") or "unknown"


def infer_jurisdiction(path: Path, raw_root: Path) -> str | None:
    try:
        relative = path.relative_to(raw_root)
    except ValueError:
        return None

    parts = relative.parts
    return parts[0] if len(parts) >= 2 else None


def now_utc() -> str:
    return datetime.now(timezone.utc).isoformat()


def build_report_id(sha256: str) -> str:
    # Registration IDs must be stable across clean DB rebuilds; the pipeline
    # already treats content hash as the governing uniqueness key for documents.
    return f"doc:sha256:{sha256}"


def connect_db(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL;")
    return conn


def get_document_columns(conn: sqlite3.Connection) -> set[str]:
    rows = conn.execute("PRAGMA table_info(documents)").fetchall()
    return {row["name"] for row in rows}


def ensure_document_columns(
    conn: sqlite3.Connection,
    required_columns: dict[str, str],
) -> set[str]:
    existing_columns = get_document_columns(conn)
    missing_columns = [
        (name, definition)
        for name, definition in required_columns.items()
        if name not in existing_columns
    ]
    for name, definition in missing_columns:
        conn.execute(f"ALTER TABLE documents ADD COLUMN {name} {definition}")
    if missing_columns:
        conn.commit()
        existing_columns = get_document_columns(conn)
    return existing_columns


def get_document_by_sha(conn: sqlite3.Connection, sha256: str) -> sqlite3.Row | None:
    cur = conn.execute("SELECT * FROM documents WHERE sha256 = ? LIMIT 1", (sha256,))
    return cur.fetchone()


def get_document_by_path(conn: sqlite3.Connection, raw_path: str) -> sqlite3.Row | None:
    cur = conn.execute("SELECT * FROM documents WHERE raw_path = ? LIMIT 1", (raw_path,))
    return cur.fetchone()


def resolve_configured_path(path_value: str) -> Path:
    return resolve_repo_path(path_value)


def resolve_lineage_manifest_path(cfg: dict) -> Path | None:
    manifest_value = cfg.get("docket_ingest", {}).get("manifest_path")
    if not manifest_value:
        return None
    return resolve_configured_path(manifest_value)


def load_acquisition_manifest_rows(manifest_path: Path) -> list[AcquisitionLineage]:
    resolved_manifest_path = str(manifest_path.resolve())
    rows: list[AcquisitionLineage] = []
    for record in iter_manifest_records(manifest_path):
        rows.append(
            AcquisitionLineage(
                acquisition_run_id=record.acquisition_run_id,
                acquisition_manifest_path=resolved_manifest_path,
                acquisition_blob_path=record.blob_path,
                project_id=record.project_id,
                ntsb_number=record.ntsb_number,
                docket_item_id=record.docket_item_id,
                view_url=record.view_url,
                source_url=record.source_url,
                blob_sha256=record.blob_sha256,
                media_type=record.media_type,
            )
        )
    return rows


def count_duplicate_manifest_blobs(rows: list[AcquisitionLineage]) -> int:
    seen: set[str] = set()
    duplicates = 0
    for row in rows:
        if row.blob_sha256 in seen:
            duplicates += 1
            continue
        seen.add(row.blob_sha256)
    return duplicates


def blob_store_path(blob_root: Path, blob_sha256: str) -> Path:
    return blob_root / "sha256" / blob_sha256[:2] / blob_sha256[2:4] / blob_sha256


def iter_blob_resolution_candidates(
    lineage: AcquisitionLineage,
    *,
    manifest_path: Path,
    raw_root: Path,
    cfg: dict,
) -> list[Path]:
    candidates: list[Path] = []
    seen: set[Path] = set()

    def add(path_value: str | Path | None) -> None:
        if path_value in (None, ""):
            return
        path = Path(path_value)
        if not path.is_absolute():
            path = resolve_configured_path(path)
        if path in seen:
            return
        seen.add(path)
        candidates.append(path)

    add(lineage.acquisition_blob_path)

    manifest_acquisition_root = manifest_path.parents[1]
    add(blob_store_path(manifest_acquisition_root / "blobs", lineage.blob_sha256))
    add(blob_store_path(raw_root / "acquisition" / "blobs", lineage.blob_sha256))

    acquisition_cfg = cfg.get("ntsb_acquisition", {})
    acquisition_data_root = acquisition_cfg.get("data_root")
    if isinstance(acquisition_data_root, str) and acquisition_data_root:
        add(
            blob_store_path(
                resolve_configured_path(acquisition_data_root) / "acquisition" / "blobs",
                lineage.blob_sha256,
            )
        )

    return candidates


def resolve_manifest_artifact_path(
    lineage: AcquisitionLineage,
    *,
    manifest_path: Path,
    raw_root: Path,
    cfg: dict,
) -> Path | None:
    for candidate in iter_blob_resolution_candidates(
        lineage,
        manifest_path=manifest_path,
        raw_root=raw_root,
        cfg=cfg,
    ):
        if candidate.exists():
            return candidate.resolve()
    return None


def insert_report(conn: sqlite3.Connection, record: ReportRecord) -> None:
    conn.execute(
        """
        INSERT INTO documents (
            doc_id,
            jurisdiction,
            case_number,
            filing_date,
            doc_type,
            source_url,
            acquisition_run_id,
            acquisition_manifest_path,
            acquisition_blob_path,
            project_id,
            ntsb_number,
            docket_item_id,
            view_url,
            blob_sha256,
            raw_path,
            sha256,
            file_size,
            ingested_at,
            status,
            stage
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            record.report_id,
            record.jurisdiction,
            record.case_number,
            record.filing_date,
            record.report_type,
            record.source_url,
            record.acquisition_run_id,
            record.acquisition_manifest_path,
            record.acquisition_blob_path,
            record.project_id,
            record.ntsb_number,
            record.docket_item_id,
            record.view_url,
            record.blob_sha256,
            record.raw_path,
            record.sha256,
            record.file_size,
            record.ingested_at,
            record.status,
            record.stage,
        ),
    )


def iter_candidate_files(root: Path, allowed_extensions: set[str]) -> list[Path]:
    files: list[Path] = []
    for dirpath, _, filenames in os.walk(root):
        for filename in filenames:
            path = Path(dirpath) / filename
            if path.suffix.lower() in allowed_extensions:
                files.append(path)
    return sorted(files)


def get_pending_reports(conn):
    cur = conn.execute(
        """
        SELECT *
        FROM documents
        WHERE status = 'pending'
        AND stage = 'registration'
        ORDER BY ingested_at ASC
        """
    )
    return cur.fetchall()


def build_record_from_path(
    path: Path,
    *,
    raw_root: Path,
    sha256: str,
    lineage: AcquisitionLineage | None,
    report_type: str | None = None,
) -> ReportRecord:
    stat = path.stat()
    jurisdiction = infer_jurisdiction(path, raw_root)
    if jurisdiction is None and lineage is not None:
        jurisdiction = "ntsb"

    effective_report_type = report_type or infer_report_type(
        path,
        media_type=lineage.media_type if lineage is not None else None,
    )
    return ReportRecord(
        report_id=build_report_id(sha256),
        jurisdiction=jurisdiction,
        case_number=None,
        filing_date=None,
        report_type=effective_report_type,
        source_url=lineage.source_url if lineage is not None else None,
        raw_path=str(path.resolve()),
        sha256=sha256,
        file_size=stat.st_size,
        ingested_at=now_utc(),
        status="pending",
        stage="registration",
        acquisition_run_id=lineage.acquisition_run_id if lineage is not None else None,
        acquisition_manifest_path=(
            lineage.acquisition_manifest_path if lineage is not None else None
        ),
        acquisition_blob_path=lineage.acquisition_blob_path if lineage is not None else None,
        project_id=lineage.project_id if lineage is not None else None,
        ntsb_number=lineage.ntsb_number if lineage is not None else None,
        docket_item_id=lineage.docket_item_id if lineage is not None else None,
        view_url=lineage.view_url if lineage is not None else None,
        blob_sha256=lineage.blob_sha256 if lineage is not None else None,
    )


def needs_record_reconciliation(
    document: sqlite3.Row,
    *,
    record: ReportRecord,
    document_columns: set[str],
) -> bool:
    if document["jurisdiction"] != record.jurisdiction:
        return True
    if document["doc_type"] != record.report_type:
        return True
    if document["source_url"] != record.source_url:
        return True
    if document["raw_path"] != record.raw_path:
        return True
    if document["file_size"] != record.file_size:
        return True
    for column in LINEAGE_COLUMNS:
        if column in document_columns and document[column] != getattr(record, column):
            return True
    return False


def reconcile_existing_record(
    conn: sqlite3.Connection,
    *,
    doc_id: str,
    record: ReportRecord,
    document_columns: set[str],
) -> bool:
    assignments = [
        "jurisdiction = ?",
        "doc_type = ?",
        "source_url = ?",
        "raw_path = ?",
        "file_size = ?",
    ]
    values: list[str | int | None] = [
        record.jurisdiction,
        record.report_type,
        record.source_url,
        record.raw_path,
        record.file_size,
    ]
    for column in LINEAGE_COLUMNS:
        if column in document_columns:
            assignments.append(f"{column} = ?")
            values.append(getattr(record, column))
    values.append(doc_id)
    cur = conn.execute(
        f"""
        UPDATE documents
        SET {", ".join(assignments)}
        WHERE doc_id = ?
        """,
        values,
    )
    return cur.rowcount == 1


def register_manifest_candidates(
    conn: sqlite3.Connection,
    *,
    cfg: dict,
    raw_root: Path,
    manifest_path: Path,
    manifest_rows: list[AcquisitionLineage],
    allowed_extensions: set[str],
    document_columns: set[str],
) -> dict[str, int]:
    stats = {
        "manifest_candidates": 0,
        "raw_scan_candidates": 0,
        "inserted": 0,
        "skipped_duplicate_path": 0,
        "skipped_duplicate_sha": 0,
        "skipped_duplicate_manifest_blob": 0,
        "lineage_backfilled": 0,
        "failed": 0,
    }
    seen_manifest_blobs: set[str] = set()

    for lineage in manifest_rows:
        stats["manifest_candidates"] += 1
        report_type = infer_report_type(Path(lineage.acquisition_blob_path), media_type=lineage.media_type)
        if f".{report_type}" not in allowed_extensions:
            stats["failed"] += 1
            print(
                "[UNSUPPORTED] "
                f"{lineage.docket_item_id} :: media_type={lineage.media_type}"
            )
            continue

        if lineage.blob_sha256 in seen_manifest_blobs:
            stats["skipped_duplicate_manifest_blob"] += 1
            print(
                "[SKIP manifest blob] "
                f"{lineage.docket_item_id} :: blob_sha256={lineage.blob_sha256}"
            )
            continue
        seen_manifest_blobs.add(lineage.blob_sha256)

        resolved_path = resolve_manifest_artifact_path(
            lineage,
            manifest_path=manifest_path,
            raw_root=raw_root,
            cfg=cfg,
        )
        if resolved_path is None:
            stats["failed"] += 1
            print(
                "[MISSING ] "
                f"{lineage.docket_item_id} :: blob_sha256={lineage.blob_sha256}"
            )
            continue

        actual_sha = sha256_file(resolved_path)
        if actual_sha != lineage.blob_sha256:
            stats["failed"] += 1
            print(
                "[FAILED ] "
                f"{lineage.docket_item_id} :: hash mismatch for {resolved_path}"
            )
            continue

        record = build_record_from_path(
            resolved_path,
            raw_root=raw_root,
            sha256=actual_sha,
            lineage=lineage,
            report_type=report_type,
        )

        existing_by_path = get_document_by_path(conn, record.raw_path)
        if existing_by_path is not None:
            if existing_by_path["sha256"] != actual_sha:
                stats["failed"] += 1
                print(
                    "[FAILED ] "
                    f"{record.raw_path} :: existing registration row has different sha256"
                )
                continue
            if needs_record_reconciliation(
                existing_by_path,
                record=record,
                document_columns=document_columns,
            ) and reconcile_existing_record(
                conn,
                doc_id=existing_by_path["doc_id"],
                record=record,
                document_columns=document_columns,
            ):
                stats["lineage_backfilled"] += 1
                print(f"[LINEAGE ] {record.raw_path}")
            stats["skipped_duplicate_path"] += 1
            print(f"[SKIP path] {record.raw_path}")
            continue

        existing_by_sha = get_document_by_sha(conn, actual_sha)
        if existing_by_sha is not None:
            if needs_record_reconciliation(
                existing_by_sha,
                record=record,
                document_columns=document_columns,
            ) and reconcile_existing_record(
                conn,
                doc_id=existing_by_sha["doc_id"],
                record=record,
                document_columns=document_columns,
            ):
                stats["lineage_backfilled"] += 1
                print(f"[LINEAGE ] {record.raw_path}")
            stats["skipped_duplicate_sha"] += 1
            print(f"[SKIP sha ] {record.raw_path}")
            continue

        try:
            insert_report(conn, record)
            stats["inserted"] += 1
            print(f"[INSERT  ] {record.raw_path}")
        except Exception as error:
            stats["failed"] += 1
            print(f"[FAILED ] {record.raw_path} :: {error}")

    return stats


def register_raw_scan_candidates(
    conn: sqlite3.Connection,
    *,
    raw_root: Path,
    allowed_extensions: set[str],
    document_columns: set[str],
) -> dict[str, int]:
    del document_columns
    stats = {
        "manifest_candidates": 0,
        "raw_scan_candidates": 0,
        "inserted": 0,
        "skipped_duplicate_path": 0,
        "skipped_duplicate_sha": 0,
        "skipped_duplicate_manifest_blob": 0,
        "lineage_backfilled": 0,
        "failed": 0,
    }

    files = iter_candidate_files(raw_root, allowed_extensions)
    print(f"Found {len(files)} raw-scan candidate files under {raw_root}")

    for path in files:
        stats["raw_scan_candidates"] += 1
        raw_path = str(path.resolve())

        existing_by_path = get_document_by_path(conn, raw_path)
        if existing_by_path is not None:
            stats["skipped_duplicate_path"] += 1
            print(f"[SKIP path] {raw_path}")
            continue

        sha256 = sha256_file(path)
        existing_by_sha = get_document_by_sha(conn, sha256)
        if existing_by_sha is not None:
            stats["skipped_duplicate_sha"] += 1
            print(f"[SKIP sha ] {raw_path}")
            continue

        record = build_record_from_path(path, raw_root=raw_root, sha256=sha256, lineage=None)

        try:
            insert_report(conn, record)
            stats["inserted"] += 1
            print(f"[INSERT  ] {raw_path}")
        except Exception as error:
            stats["failed"] += 1
            print(f"[FAILED ] {raw_path} :: {error}")

    return stats


# ---------- Main ----------

def main() -> None:
    cfg = load_config()

    db_path = resolve_configured_path(cfg["database"]["manifest_path"])
    raw_root = resolve_configured_path(cfg["paths"]["raw"])
    allowed_extensions = {extension.lower() for extension in cfg["ingestion"]["allowed_extensions"]}
    lineage_manifest_path = resolve_lineage_manifest_path(cfg)

    if not db_path.exists():
        raise FileNotFoundError(f"Manifest DB not found: {db_path}")

    if not raw_root.exists():
        raise FileNotFoundError(f"Raw data directory not found: {raw_root}")

    conn = connect_db(db_path)

    try:
        document_columns = ensure_document_columns(conn, LINEAGE_COLUMNS)

        if lineage_manifest_path is not None and lineage_manifest_path.exists():
            manifest_rows = load_acquisition_manifest_rows(lineage_manifest_path)
            duplicate_manifest_blobs = count_duplicate_manifest_blobs(manifest_rows)
            print(
                "Loaded "
                f"{len(manifest_rows)} acquisition manifest rows from "
                f"{lineage_manifest_path}"
            )
            if duplicate_manifest_blobs:
                print(
                    "[INFO   ] "
                    f"{duplicate_manifest_blobs} manifest rows shared an existing blob_sha256; "
                    "registration preserves the first deterministic row per unique blob."
                )
            stats = register_manifest_candidates(
                conn,
                cfg=cfg,
                raw_root=raw_root,
                manifest_path=lineage_manifest_path,
                manifest_rows=manifest_rows,
                allowed_extensions=allowed_extensions,
                document_columns=document_columns,
            )
        else:
            if lineage_manifest_path is not None:
                print(
                    "Acquisition manifest not found at configured path; "
                    f"continuing with raw-scan compatibility mode: {lineage_manifest_path}"
                )
            else:
                print("No acquisition manifest configured; continuing with raw-scan compatibility mode.")
            stats = register_raw_scan_candidates(
                conn,
                raw_root=raw_root,
                allowed_extensions=allowed_extensions,
                document_columns=document_columns,
            )

        conn.commit()

    finally:
        conn.close()

    print("\nDone.")
    print(f"Manifest candidates:          {stats['manifest_candidates']}")
    print(f"Raw-scan candidates:          {stats['raw_scan_candidates']}")
    print(f"Inserted:                     {stats['inserted']}")
    print(f"Skipped duplicate path:       {stats['skipped_duplicate_path']}")
    print(f"Skipped duplicate sha:        {stats['skipped_duplicate_sha']}")
    print(f"Skipped duplicate manifest:   {stats['skipped_duplicate_manifest_blob']}")
    print(f"Lineage backfilled:           {stats['lineage_backfilled']}")
    print(f"Failed:                       {stats['failed']}")


if __name__ == "__main__":
    main()
