from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path
import sqlite3

from incident_pipeline.common.paths import DEFAULT_SETTINGS_PATH, resolve_repo_path
from incident_pipeline.common.settings import load_settings


CONFIG_PATH = DEFAULT_SETTINGS_PATH
ELIGIBLE_INPUT_STAGES = ("extraction", "ocr")


def load_config(config_path: Path | None = None) -> dict:
    resolved_config_path = config_path or CONFIG_PATH
    effective_path = None if resolved_config_path == DEFAULT_SETTINGS_PATH else resolved_config_path
    return load_settings(effective_path)


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


def get_completed_documents(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    placeholders = ", ".join("?" for _ in ELIGIBLE_INPUT_STAGES)
    cur = conn.execute(
        f"""
        SELECT *
        FROM documents
        WHERE status = 'completed'
        AND stage IN ({placeholders})
        ORDER BY COALESCE(last_processed_at, ingested_at) ASC, ingested_at ASC
        """,
        ELIGIBLE_INPUT_STAGES,
    )
    return cur.fetchall()


def claim_document(conn: sqlite3.Connection, doc_id: str, input_stage: str) -> bool:
    cur = conn.execute(
        """
        UPDATE documents
        SET status = ?, stage = ?, error_message = ?
        WHERE doc_id = ?
        AND status = 'completed'
        AND stage = ?
        """,
        ("processing", "structure", None, doc_id, input_stage),
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
    return processed_root / "structured" / f"{doc_id}.json"


def build_debug_output_path(processed_root: Path, doc_id: str) -> Path:
    return processed_root / "structured_debug" / f"{doc_id}.json"


def build_fallback_input_path(processed_root: Path, doc_id: str, input_stage: str) -> Path:
    if input_stage == "ocr":
        return processed_root / "ocr" / f"{doc_id}.txt"
    return processed_root / "extracted" / f"{doc_id}.txt"


def get_source_text_path(
    document: sqlite3.Row,
    document_columns: set[str],
    processed_root: Path,
) -> Path:
    if "extracted_text_path" in document_columns:
        manifest_value = document["extracted_text_path"]
        if manifest_value:
            return resolve_path(manifest_value)

    return build_fallback_input_path(processed_root, document["doc_id"], document["stage"])


def read_source_text(source_text_path: Path) -> str:
    if not source_text_path.exists():
        raise FileNotFoundError(f"Source text artifact not found: {source_text_path}")

    return source_text_path.read_text(encoding="utf-8")


def clean_optional_text(value: str | None) -> str | None:
    if value is None:
        return None

    cleaned = value.strip()
    return cleaned or None


def build_structure_payload(document: sqlite3.Row, text: str) -> tuple[dict, dict]:
    from incident_pipeline.extract.structure_state_machine import (
        parse_structure_document,
        render_sections,
        serialize_trace,
    )

    parse_result = parse_structure_document(text)
    sections = render_sections(parse_result)
    probable_cause = clean_optional_text(sections.get("probable_cause"))
    recommendations = clean_optional_text(sections.get("recommendations"))
    if recommendations is None:
        recommendations = clean_optional_text(sections.get("what_we_recommended"))
    payload = {
        "doc_id": document["doc_id"],
        "raw_path": document["raw_path"],
        "report_type": document["doc_type"],
        "title": parse_result.title,
        "sections": sections,
        "probable_cause": probable_cause,
        "recommendations": recommendations,
        "stats": {
            "source_text_length": len(text),
            "section_count": len(sections),
        },
    }
    debug_payload = serialize_trace(parse_result, doc_id=document["doc_id"])
    return payload, debug_payload


def write_json_artifact(
    output_path: Path,
    payload: dict,
) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)

    output_path.write_text(
        json.dumps(payload, indent=2, ensure_ascii=True) + "\n",
        encoding="utf-8",
    )


def make_summary(selected: int = 0) -> dict[str, int]:
    return {
        "selected": selected,
        "completed": 0,
        "reused_existing": 0,
        "failed": 0,
    }


def run_structure_batch(config_path: Path | None = None) -> dict[str, int]:
    cfg = load_config(config_path)

    db_path = resolve_path(cfg["database"]["manifest_path"])
    processed_root = resolve_path(cfg["paths"]["processed"])
    overwrite_existing = bool(cfg["processing"].get("overwrite_existing", False))

    if not db_path.exists():
        raise FileNotFoundError(f"Manifest DB not found: {db_path}")

    conn = connect_db(db_path)

    try:
        document_columns = get_document_columns(conn)
        documents = get_completed_documents(conn)
        summary = make_summary(selected=len(documents))

        for document in documents:
            doc_id = document["doc_id"]
            input_stage = document["stage"]

            if not claim_document(conn, doc_id, input_stage):
                continue

            try:
                output_path = build_output_path(processed_root, doc_id)
                debug_output_path = build_debug_output_path(processed_root, doc_id)
                reused_existing = False

                if overwrite_existing or not output_path.exists():
                    source_text_path = get_source_text_path(document, document_columns, processed_root)
                    source_text = read_source_text(source_text_path)
                    payload, debug_payload = build_structure_payload(document, source_text)
                    write_json_artifact(output_path, payload)
                    write_json_artifact(debug_output_path, debug_payload)
                else:
                    reused_existing = True

                finalize_document(
                    conn,
                    doc_id,
                    status="completed",
                    stage="structure",
                    document_columns=document_columns,
                    output_updates={
                        "structured_json_path": str(output_path),
                        "structured_debug_path": str(debug_output_path),
                    },
                )
                summary["completed"] += 1

                if reused_existing:
                    summary["reused_existing"] += 1
            except Exception as exc:
                finalize_document(
                    conn,
                    doc_id,
                    status="failed",
                    stage="structure",
                    error_message=str(exc),
                    document_columns=document_columns,
                )
                summary["failed"] += 1

        return summary
    finally:
        conn.close()
