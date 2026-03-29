from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
import sqlite3

from incident_pipeline.common.paths import DEFAULT_SETTINGS_PATH, resolve_repo_path
from incident_pipeline.common.settings import load_settings


CONFIG_PATH = DEFAULT_SETTINGS_PATH


def load_config() -> dict:
    config_path = None if CONFIG_PATH == DEFAULT_SETTINGS_PATH else CONFIG_PATH
    return load_settings(config_path)


def resolve_path(path_value: str) -> Path:
    return resolve_repo_path(path_value)


def now_utc() -> str:
    return datetime.now(timezone.utc).isoformat()


def connect_db(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL;")
    return conn


def get_document_columns(conn: sqlite3.Connection) -> set[str]:
    rows = conn.execute("PRAGMA table_info(documents)").fetchall()
    return {row["name"] for row in rows}


def get_pending_documents(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    cur = conn.execute(
        """
        SELECT doc_id, raw_path
        FROM documents
        WHERE status = 'pending'
        AND stage = 'registration'
        ORDER BY ingested_at ASC
        """
    )
    return cur.fetchall()


def claim_document(conn: sqlite3.Connection, doc_id: str) -> bool:
    cur = conn.execute(
        """
        UPDATE documents
        SET status = ?, stage = ?, error_message = ?
        WHERE doc_id = ?
        AND status = 'pending'
        AND stage = 'registration'
        """,
        ("processing", "extraction", None, doc_id),
    )
    conn.commit()
    return cur.rowcount == 1


def finalize_document(
    conn: sqlite3.Connection,
    doc_id: str,
    *,
    status: str,
    stage: str,
    error_message: str | None = None,
    document_columns: set[str] | None = None,
    output_updates: dict[str, str] | None = None,
) -> None:
    assignments = [
        "status = ?",
        "stage = ?",
        "last_processed_at = ?",
        "error_message = ?",
    ]
    parameters: list[str | None] = [status, stage, now_utc(), error_message]
    if document_columns is not None and output_updates is not None:
        for column_name, value in output_updates.items():
            if column_name in document_columns:
                assignments.append(f"{column_name} = ?")
                parameters.append(value)
    parameters.append(doc_id)
    conn.execute(
        f"""
        UPDATE documents
        SET {", ".join(assignments)}
        WHERE doc_id = ?
        """,
        parameters,
    )
    conn.commit()


def build_output_path(processed_root: Path, doc_id: str) -> Path:
    return processed_root / "extracted" / f"{doc_id}.txt"


def extract_pdf_text(pdf_path: Path) -> str:
    try:
        from pypdf import PdfReader
    except ImportError as exc:
        raise RuntimeError("pypdf is required for PDF extraction") from exc

    reader = PdfReader(str(pdf_path))
    page_text = [(page.extract_text() or "") for page in reader.pages]
    return "\n\n".join(page_text).strip()


def get_document_text(
    raw_path: Path,
    output_path: Path,
    *,
    overwrite_existing: bool,
) -> tuple[str, bool]:
    output_path.parent.mkdir(parents=True, exist_ok=True)

    if not overwrite_existing and output_path.exists():
        return output_path.read_text(encoding="utf-8"), True

    if not raw_path.exists():
        raise FileNotFoundError(f"Raw PDF not found: {raw_path}")

    text = extract_pdf_text(raw_path)
    output_path.write_text(text, encoding="utf-8")
    return text, False


def make_summary(selected: int = 0) -> dict[str, int]:
    return {
        "selected": selected,
        "completed": 0,
        "queued_for_ocr": 0,
        "reused_existing": 0,
        "failed": 0,
    }


def run_extraction_batch() -> dict[str, int]:
    cfg = load_config()

    db_path = resolve_path(cfg["database"]["manifest_path"])
    processed_root = resolve_path(cfg["paths"]["processed"])
    overwrite_existing = bool(cfg["processing"].get("overwrite_existing", False))
    min_text_threshold = int(cfg["extraction"].get("min_text_threshold", 0))
    ocr_enabled = bool(cfg["ocr"].get("enabled", False))

    if not db_path.exists():
        raise FileNotFoundError(f"Manifest DB not found: {db_path}")

    conn = connect_db(db_path)

    try:
        document_columns = get_document_columns(conn)
        documents = get_pending_documents(conn)
        summary = make_summary(selected=len(documents))

        for document in documents:
            doc_id = document["doc_id"]

            if not claim_document(conn, doc_id):
                continue

            try:
                raw_path = Path(document["raw_path"])
                output_path = build_output_path(processed_root, doc_id)
                text, reused_existing = get_document_text(
                    raw_path,
                    output_path,
                    overwrite_existing=overwrite_existing,
                )

                if reused_existing:
                    summary["reused_existing"] += 1

                if len(text) < min_text_threshold and ocr_enabled:
                    finalize_document(
                        conn,
                        doc_id,
                        status="pending",
                        stage="ocr",
                        document_columns=document_columns,
                        output_updates={"extracted_text_path": str(output_path)},
                    )
                    summary["queued_for_ocr"] += 1
                else:
                    finalize_document(
                        conn,
                        doc_id,
                        status="completed",
                        stage="extraction",
                        document_columns=document_columns,
                        output_updates={"extracted_text_path": str(output_path)},
                    )
                    summary["completed"] += 1
            except Exception as exc:
                finalize_document(
                    conn,
                    doc_id,
                    status="failed",
                    stage="extraction",
                    error_message=str(exc),
                    document_columns=document_columns,
                )
                summary["failed"] += 1

        return summary
    finally:
        conn.close()
