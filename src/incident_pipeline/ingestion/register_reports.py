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
    "project_id": "TEXT",
    "ntsb_number": "TEXT",
    "docket_item_id": "TEXT",
    "view_url": "TEXT",
    "blob_sha256": "TEXT",
}


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
    project_id: str | None = None
    ntsb_number: str | None = None
    docket_item_id: str | None = None
    view_url: str | None = None
    blob_sha256: str | None = None


@dataclass(frozen=True)
class AcquisitionLineage:
    acquisition_run_id: str | None
    acquisition_manifest_path: str
    project_id: str
    ntsb_number: str
    docket_item_id: str
    view_url: str
    source_url: str
    blob_sha256: str


# ---------- Helpers ----------

def sha256_file(path: Path, chunk_size: int = 1024 * 1024) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as f:
        while chunk := f.read(chunk_size):
            digest.update(chunk)
    return digest.hexdigest()


def infer_report_type(path: Path) -> str:
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
    # Registration preserves acquisition lineage from the governed export
    # snapshot when one is configured; raw-file scanning remains a fallback.
    return resolve_configured_path(manifest_value)


def load_acquisition_lineage(
    manifest_path: Path,
) -> tuple[dict[str, AcquisitionLineage], int]:
    by_sha: dict[str, AcquisitionLineage] = {}
    duplicate_sha_rows = 0
    resolved_manifest_path = str(manifest_path.resolve())

    for record in iter_manifest_records(manifest_path):
        lineage = AcquisitionLineage(
            acquisition_run_id=record.acquisition_run_id,
            acquisition_manifest_path=resolved_manifest_path,
            project_id=record.project_id,
            ntsb_number=record.ntsb_number,
            docket_item_id=record.docket_item_id,
            view_url=record.view_url,
            source_url=record.source_url,
            blob_sha256=record.blob_sha256,
        )
        if record.blob_sha256 in by_sha:
            duplicate_sha_rows += 1
            continue
        by_sha[record.blob_sha256] = lineage

    return by_sha, duplicate_sha_rows


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
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
            record.stage
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


def needs_lineage_backfill(
    document: sqlite3.Row,
    *,
    lineage: AcquisitionLineage,
    document_columns: set[str],
) -> bool:
    if document["source_url"] != lineage.source_url:
        return True
    for column in LINEAGE_COLUMNS:
        if column in document_columns and document[column] != getattr(lineage, column):
            return True
    return False


def build_record_from_path(
    path: Path,
    *,
    raw_root: Path,
    sha256: str,
    lineage: AcquisitionLineage | None,
) -> ReportRecord:
    stat = path.stat()
    return ReportRecord(
        report_id=build_report_id(sha256),
        jurisdiction=infer_jurisdiction(path, raw_root),
        case_number=None,
        filing_date=None,
        report_type=infer_report_type(path),
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
        project_id=lineage.project_id if lineage is not None else None,
        ntsb_number=lineage.ntsb_number if lineage is not None else None,
        docket_item_id=lineage.docket_item_id if lineage is not None else None,
        view_url=lineage.view_url if lineage is not None else None,
        blob_sha256=lineage.blob_sha256 if lineage is not None else None,
    )


def backfill_lineage(
    conn: sqlite3.Connection,
    *,
    doc_id: str,
    lineage: AcquisitionLineage,
    document_columns: set[str],
) -> bool:
    assignments = ["source_url = ?"]
    values: list[str | None] = [lineage.source_url]
    for column in LINEAGE_COLUMNS:
        if column in document_columns:
            assignments.append(f"{column} = ?")
            values.append(getattr(lineage, column))
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

# ---------- Main ----------

def main() -> None:
    cfg = load_config()

    db_path = resolve_configured_path(cfg["database"]["manifest_path"])
    raw_root = resolve_configured_path(cfg["paths"]["raw"])
    allowed_extensions = set(cfg["ingestion"]["allowed_extensions"])
    lineage_manifest_path = resolve_lineage_manifest_path(cfg)

    if not db_path.exists():
        raise FileNotFoundError(f"Manifest DB not found: {db_path}")

    if not raw_root.exists():
        raise FileNotFoundError(f"Raw data directory not found: {raw_root}")

    conn = connect_db(db_path)

    scanned = 0
    inserted = 0
    skipped_duplicate_sha = 0
    skipped_duplicate_path = 0
    backfilled_lineage_rows = 0
    failed = 0

    try:
        document_columns = ensure_document_columns(conn, LINEAGE_COLUMNS)
        lineage_by_sha: dict[str, AcquisitionLineage] = {}
        duplicate_lineage_rows = 0
        if lineage_manifest_path is not None:
            if lineage_manifest_path.exists():
                lineage_by_sha, duplicate_lineage_rows = load_acquisition_lineage(
                    lineage_manifest_path
                )
                print(
                    "Loaded "
                    f"{len(lineage_by_sha)} acquisition lineage records from "
                    f"{lineage_manifest_path}"
                )
                if duplicate_lineage_rows:
                    print(
                        "[INFO   ] "
                        f"{duplicate_lineage_rows} additional manifest rows shared an existing "
                        "blob_sha256; registration preserved the first deterministic lineage row "
                        "per unique blob."
                    )
            else:
                print(
                    "Acquisition manifest not found at configured path; "
                    f"continuing with raw scan only: {lineage_manifest_path}"
                )

        files = iter_candidate_files(raw_root, allowed_extensions)
        print(f"Found {len(files)} candidate files under {raw_root}")

        for path in files:
            scanned += 1
            raw_path = str(path.resolve())
            existing_by_path = get_document_by_path(conn, raw_path)
            if existing_by_path is not None:
                lineage = lineage_by_sha.get(existing_by_path["sha256"])
                if (
                    lineage is not None
                    and needs_lineage_backfill(
                        existing_by_path,
                        lineage=lineage,
                        document_columns=document_columns,
                    )
                    and backfill_lineage(
                        conn,
                        doc_id=existing_by_path["doc_id"],
                        lineage=lineage,
                        document_columns=document_columns,
                    )
                ):
                    backfilled_lineage_rows += 1
                    print(f"[LINEAGE ] {raw_path}")
                skipped_duplicate_path += 1
                print(f"[SKIP path] {raw_path}")
                continue

            sha256 = sha256_file(path)
            lineage = lineage_by_sha.get(sha256)
            existing_by_sha = get_document_by_sha(conn, sha256)

            if existing_by_sha is not None:
                if (
                    lineage is not None
                    and needs_lineage_backfill(
                        existing_by_sha,
                        lineage=lineage,
                        document_columns=document_columns,
                    )
                    and backfill_lineage(
                        conn,
                        doc_id=existing_by_sha["doc_id"],
                        lineage=lineage,
                        document_columns=document_columns,
                    )
                ):
                    backfilled_lineage_rows += 1
                    print(f"[LINEAGE ] {raw_path}")
                skipped_duplicate_sha += 1
                print(f"[SKIP sha ] {raw_path}")
                continue

            record = build_record_from_path(path, raw_root=raw_root, sha256=sha256, lineage=lineage)

            try:
                insert_report(conn, record)
                inserted += 1
                print(f"[INSERT  ] {raw_path}")

            except Exception as e:
                failed += 1
                conn.execute(
                    """
                    INSERT OR REPLACE INTO documents (
                        doc_id, raw_path, sha256, status, stage, error_message, last_processed_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        record.report_id,
                        raw_path,
                        sha256,
                        "failed",
                        "registration",
                        str(e),
                        now_utc(),
                    ),
                )
                print(f"[FAILED ] {raw_path} :: {e}")

        conn.commit()

    finally:
        conn.close()

    print("\nDone.")
    print(f"Scanned:                {scanned}")
    print(f"Inserted:               {inserted}")
    print(f"Skipped duplicate path: {skipped_duplicate_path}")
    print(f"Skipped duplicate sha:  {skipped_duplicate_sha}")
    print(f"Lineage backfilled:     {backfilled_lineage_rows}")
    print(f"Failed:                 {failed}")
    


if __name__ == "__main__":
    main()
