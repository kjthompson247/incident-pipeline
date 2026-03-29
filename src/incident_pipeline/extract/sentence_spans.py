from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import re
import sqlite3
from time import perf_counter
from typing import Any

from incident_pipeline.common.paths import DEFAULT_SETTINGS_PATH, resolve_repo_path
from incident_pipeline.common.settings import load_settings
from incident_pipeline.common.stage_runs import (
    StageFailure,
    StageFailurePolicy,
    ValidationRule,
    create_stage_run_context,
    isoformat_utc,
    parse_jsonl,
    read_json,
    rule,
    sha256_file,
    utc_now,
    write_json,
    write_jsonl,
)
from incident_pipeline.extract.atomic_contract import SentenceSpan


CONFIG_PATH = DEFAULT_SETTINGS_PATH
STAGE_NAME = "sentence_span_generation"
STAGE_VERSION = "sentence_span_generation_v1"
RULE_SET_VERSION = "sentence_span_generation_v1"
SEGMENTATION_VERSION = "sentence-split-v1"
ELIGIBLE_INPUT_STAGES = ("extraction", "ocr")
FAILURE_POLICY = StageFailurePolicy(max_failure_count=0, max_failure_rate=0.0)
REQUIRED_RUN_FILES = (
    "sentence_spans.jsonl",
    "sentence_span_failures.jsonl",
    "sentence_span_metrics.json",
    "sentence_span_run_summary.json",
    "sentence_span_validation_report.json",
)

ABBREVIATION_PATTERNS = (
    "Mr.",
    "Mrs.",
    "Ms.",
    "Dr.",
    "Prof.",
    "Sr.",
    "Jr.",
    "St.",
    "U.S.",
    "U.K.",
    "No.",
    "Inc.",
    "Ltd.",
    "e.g.",
    "i.e.",
    "etc.",
    "a.m.",
    "p.m.",
    "Q.",
    "A.",
)
BULLET_OR_TRANSCRIPT_PATTERN = re.compile(
    r"^\s*(?:[-*•]|\d+[.)]|[A-Z][A-Z0-9 .'/()-]{0,40}:)\s+"
)
MULTI_INITIAL_PATTERN = re.compile(r"\b(?:[A-Z]\.){2,}")


def load_config(config_path: Path | None = None) -> dict[str, Any]:
    resolved_config_path = config_path or CONFIG_PATH
    effective_path = None if resolved_config_path == DEFAULT_SETTINGS_PATH else resolved_config_path
    return load_settings(effective_path)


def resolve_path(path_value: str | Path) -> Path:
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


def compute_input_digest(entries: list[dict[str, Any]]) -> str:
    payload = json.dumps(entries, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _protect_abbreviations(text: str) -> str:
    protected = text.replace("...", "<ELLIPSIS>")
    protected = re.sub(r"(?<=\d)\.(?=\d)", "<DECIMAL>", protected)

    for abbreviation in ABBREVIATION_PATTERNS:
        protected = protected.replace(abbreviation, abbreviation.replace(".", "<ABBR>"))

    def replace_initials(match: re.Match[str]) -> str:
        return match.group(0).replace(".", "<ABBR>")

    return MULTI_INITIAL_PATTERN.sub(replace_initials, protected)


def _restore_protected_text(text: str) -> str:
    return (
        text.replace("<ELLIPSIS>", "...")
        .replace("<DECIMAL>", ".")
        .replace("<ABBR>", ".")
    )


def normalize_whitespace(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()


def split_structural_blocks(text: str) -> list[str]:
    blocks: list[str] = []
    for raw_block in re.split(r"\n\s*\n+", text.strip()):
        lines = [line.rstrip() for line in raw_block.splitlines() if line.strip()]
        if not lines:
            continue
        if any(BULLET_OR_TRANSCRIPT_PATTERN.match(line) for line in lines):
            for line in lines:
                normalized = normalize_whitespace(line)
                if normalized:
                    blocks.append(normalized)
        else:
            normalized = normalize_whitespace("\n".join(lines))
            if normalized:
                blocks.append(normalized)
    return blocks


def split_sentences(text: str) -> list[str]:
    normalized = normalize_whitespace(text)
    if not normalized:
        return []

    protected = _protect_abbreviations(normalized)
    parts = re.split(r'(?<=[.!?])\s+(?=(?:["\'(\[])?[A-Z0-9])', protected)
    sentences = [_restore_protected_text(part).strip() for part in parts if part.strip()]
    return sentences or [normalized]


def build_parent_structural_span_id(artifact_id: str, paragraph_index: int) -> str:
    return f"pstruct:{artifact_id}:paragraph:{paragraph_index}"


def build_locator_hash(locator_seed: Mapping[str, Any]) -> str:
    payload = json.dumps(locator_seed, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:12]


def build_sentence_span_id(
    *,
    case_id: str,
    artifact_id: str,
    locator_seed: Mapping[str, Any],
    sentence_index: int,
) -> str:
    return (
        f"sspan:{case_id}:{artifact_id}:{build_locator_hash(locator_seed)}:{sentence_index}"
    )


def derive_case_id(document: sqlite3.Row) -> str:
    return str(
        document["ntsb_number"]
        or document["case_number"]
        or document["doc_id"]
    )


def derive_artifact_id(document: sqlite3.Row) -> str:
    return str(document["docket_item_id"] or document["doc_id"])


def derive_text_extraction_version(document: sqlite3.Row, cfg: Mapping[str, Any]) -> str:
    if document["stage"] == "ocr":
        return f"ocr:{cfg.get('ocr', {}).get('engine', 'unknown')}"
    return f"extract:{cfg.get('extraction', {}).get('method', 'unknown')}"


def generate_sentence_spans_for_document(
    document: sqlite3.Row,
    *,
    source_text: str,
    artifact_checksum: str,
    text_extraction_version: str,
    segmentation_version: str,
    include_context: bool,
) -> list[SentenceSpan]:
    artifact_id = derive_artifact_id(document)
    case_id = derive_case_id(document)
    blocks = split_structural_blocks(source_text)
    spans: list[SentenceSpan] = []

    for paragraph_index, block in enumerate(blocks, start=1):
        sentences = split_sentences(block)
        if not sentences:
            continue
        parent_structural_span_id = build_parent_structural_span_id(artifact_id, paragraph_index)
        locator_seed = {
            "type": "paragraph",
            "paragraph": paragraph_index,
            "parent_structural_span_id": parent_structural_span_id,
        }
        for sentence_index, sentence_text in enumerate(sentences, start=1):
            context = None
            if include_context:
                context = {
                    "preceding_text": sentences[sentence_index - 2] if sentence_index > 1 else "",
                    "following_text": (
                        sentences[sentence_index] if sentence_index < len(sentences) else ""
                    ),
                }
            locator = {
                "type": "paragraph_sentence",
                "paragraph": paragraph_index,
                "sentence": sentence_index,
            }
            spans.append(
                SentenceSpan(
                    sentence_span_id=build_sentence_span_id(
                        case_id=case_id,
                        artifact_id=artifact_id,
                        locator_seed=locator_seed,
                        sentence_index=sentence_index,
                    ),
                    artifact_id=artifact_id,
                    case_id=case_id,
                    parent_structural_span_id=parent_structural_span_id,
                    locator=locator,
                    sentence_text=sentence_text,
                    sentence_index=sentence_index,
                    segmentation_version=segmentation_version,
                    provenance={
                        "artifact_checksum": f"sha256:{artifact_checksum}",
                        "text_extraction_version": text_extraction_version,
                        "segmentation_version": segmentation_version,
                    },
                    context=context,
                )
            )

    return spans


def make_summary(selected: int = 0) -> dict[str, int]:
    return {
        "selected": selected,
        "completed": 0,
        "sentences_generated": 0,
        "failed": 0,
    }


def _rules_summary(rules: list[ValidationRule]) -> dict[str, int]:
    warnings = sum(1 for item in rules if item.severity == "warning")
    rules_passed = sum(1 for item in rules if item.result == "pass")
    rules_failed = len(rules) - rules_passed
    return {
        "rules_evaluated": len(rules),
        "rules_passed": rules_passed,
        "rules_failed": rules_failed,
        "warnings": warnings,
    }


def _error_rules_failed(rules: list[ValidationRule]) -> list[ValidationRule]:
    return [item for item in rules if item.severity == "error" and item.result == "fail"]


def _write_sentinel(run_dir: Path, *, certified: bool) -> None:
    certified_path = run_dir / "_CERTIFIED"
    failed_path = run_dir / "_FAILED"
    if certified_path.exists():
        certified_path.unlink()
    if failed_path.exists():
        failed_path.unlink()
    (certified_path if certified else failed_path).write_text("", encoding="utf-8")


def _classify_failure(
    *,
    artifact_id: str,
    source_locator: str,
    exc: Exception,
) -> StageFailure:
    failure_code = "internal_error"
    failure_class = "internal_error"
    if isinstance(exc, FileNotFoundError):
        failure_code = "missing_input_artifact"
        failure_class = "input_contract"
    elif isinstance(exc, ValueError):
        failure_code = "segmentation_failed"
        failure_class = "segmentation_error"

    return StageFailure(
        record_id=artifact_id,
        artifact_id=artifact_id,
        failure_code=failure_code,
        failure_class=failure_class,
        message=str(exc),
        retryable=False,
        blocked_output=True,
        source_locator=source_locator,
        exception_type=type(exc).__name__,
    )


def validate_sentence_spans(
    spans: list[SentenceSpan],
    *,
    input_digest: str,
    output_root: Path,
    primary_output_digest: str,
) -> tuple[list[ValidationRule], dict[str, int], dict[str, dict[str, int]]]:
    empty_sentence_count = 0
    duplicate_count = 0
    missing_artifact_count = 0
    missing_version_count = 0
    sequentiality_violations = 0
    duplicate_ids = [span.sentence_span_id for span in spans]
    duplicate_count = len(duplicate_ids) - len(set(duplicate_ids))

    artifact_counts: dict[str, int] = {}
    span_counts_by_parent: dict[str, list[int]] = {}

    for span in spans:
        if not span.sentence_text.strip():
            empty_sentence_count += 1
        if not span.artifact_id:
            missing_artifact_count += 1
        if not span.segmentation_version:
            missing_version_count += 1
        artifact_counts[span.artifact_id] = artifact_counts.get(span.artifact_id, 0) + 1
        span_counts_by_parent.setdefault(span.parent_structural_span_id, []).append(span.sentence_index)

    for sentence_indexes in span_counts_by_parent.values():
        expected = list(range(1, len(sentence_indexes) + 1))
        if sorted(sentence_indexes) != expected:
            sequentiality_violations += 1

    reproducible = True
    reproducibility_detail = "no comparable prior certified run"
    try:
        from incident_pipeline.common.stage_runs import load_latest_certified_run

        prior = load_latest_certified_run(output_root, expected_stage_name=STAGE_NAME)
        if str(prior.get("input_digest")) == input_digest:
            reproducible = (
                int(prior.get("primary_output_count", -1)) == len(spans)
                and str(prior.get("primary_output_digest")) == primary_output_digest
            )
            reproducibility_detail = (
                f"prior_primary_output_count={prior.get('primary_output_count')} "
                f"prior_primary_output_digest={prior.get('primary_output_digest')}"
            )
    except ValueError:
        pass

    rules = [
        rule(
            rule_id="sentence_spans_non_empty_text",
            severity="error",
            passed=empty_sentence_count == 0,
            detail=f"empty_sentence_count={empty_sentence_count}",
        ),
        rule(
            rule_id="sentence_spans_duplicate_ids_absent",
            severity="error",
            passed=duplicate_count == 0,
            detail=f"duplicate_count={duplicate_count}",
        ),
        rule(
            rule_id="sentence_spans_sequential_indexes",
            severity="error",
            passed=sequentiality_violations == 0,
            detail=f"sequentiality_violations={sequentiality_violations}",
        ),
        rule(
            rule_id="sentence_spans_trace_to_artifact",
            severity="error",
            passed=missing_artifact_count == 0,
            detail=f"missing_artifact_count={missing_artifact_count}",
        ),
        rule(
            rule_id="sentence_spans_segmentation_version_present",
            severity="error",
            passed=missing_version_count == 0,
            detail=f"missing_version_count={missing_version_count}",
        ),
        rule(
            rule_id="sentence_spans_reproducible_for_same_input_digest",
            severity="error",
            passed=reproducible,
            detail=reproducibility_detail,
        ),
    ]
    quality = {
        "missing_required_field_count": empty_sentence_count + missing_artifact_count + missing_version_count,
        "invalid_enum_count": 0,
        "duplicate_key_count": duplicate_count,
    }
    distribution = {
        "artifact_id": dict(sorted(artifact_counts.items())),
    }
    return rules, quality, distribution


def run_sentence_span_batch(config_path: Path | None = None) -> dict[str, int]:
    cfg = load_config(config_path)
    db_path = resolve_path(cfg["database"]["manifest_path"])
    processed_root = resolve_path(cfg["paths"]["processed"])
    sentence_cfg = cfg.get("sentence_span_generation", {})
    output_root = resolve_path(
        sentence_cfg.get("output_root", processed_root / "sentence_spans")
    )
    segmentation_version = str(sentence_cfg.get("segmentation_version", SEGMENTATION_VERSION))
    include_context = bool(sentence_cfg.get("include_context", True))
    runtime_parameters = {
        "db_path": str(db_path),
        "processed_root": str(processed_root),
        "output_root": str(output_root),
        "segmentation_version": segmentation_version,
        "include_context": include_context,
    }
    run_context = create_stage_run_context(
        stage_name=STAGE_NAME,
        stage_version=STAGE_VERSION,
        output_root=output_root,
        config_payload=cfg,
        runtime_parameters=runtime_parameters,
    )
    started_monotonic = perf_counter()

    sentence_spans_path = run_context.run_dir / "sentence_spans.jsonl"
    failures_path = run_context.run_dir / "sentence_span_failures.jsonl"
    metrics_path = run_context.run_dir / "sentence_span_metrics.json"
    run_summary_path = run_context.run_dir / "sentence_span_run_summary.json"
    validation_report_path = run_context.run_dir / "sentence_span_validation_report.json"

    summary = make_summary(selected=0)
    failures: list[StageFailure] = []
    spans: list[SentenceSpan] = []
    input_entries: list[dict[str, Any]] = []
    blocking_issues: list[str] = []
    run_status = "completed"

    try:
        if not db_path.exists():
            raise FileNotFoundError(f"Manifest DB not found: {db_path}")

        conn = connect_db(db_path)
        try:
            document_columns = get_document_columns(conn)
            documents = get_completed_documents(conn)
            summary = make_summary(selected=len(documents))

            for document in documents:
                artifact_id = derive_artifact_id(document)
                try:
                    source_text_path = get_source_text_path(document, document_columns, processed_root)
                    input_entries.append(
                        {
                            "artifact_id": artifact_id,
                            "doc_id": document["doc_id"],
                            "source_text_path": str(source_text_path),
                            "source_text_sha256": (
                                sha256_file(source_text_path) if source_text_path.exists() else None
                            ),
                            "source_stage": document["stage"],
                        }
                    )
                    source_text = read_source_text(source_text_path)
                    generated = generate_sentence_spans_for_document(
                        document,
                        source_text=source_text,
                        artifact_checksum=document["sha256"],
                        text_extraction_version=derive_text_extraction_version(document, cfg),
                        segmentation_version=segmentation_version,
                        include_context=include_context,
                    )
                    spans.extend(generated)
                    summary["completed"] += 1
                    summary["sentences_generated"] += len(generated)
                except Exception as exc:
                    summary["failed"] += 1
                    failures.append(
                        _classify_failure(
                            artifact_id=artifact_id,
                            source_locator=str(get_source_text_path(document, document_columns, processed_root)),
                            exc=exc,
                        )
                    )
        finally:
            conn.close()
    except Exception as exc:
        run_status = "failed"
        blocking_issues.append(str(exc))

    write_jsonl(sentence_spans_path, [span.to_mapping() for span in spans])
    failure_rows = [
        failure.to_mapping(run_id=run_context.run_id, stage_name=run_context.stage_name)
        for failure in failures
    ]
    write_jsonl(failures_path, failure_rows)

    input_digest = compute_input_digest(input_entries)
    primary_output_digest = sha256_file(sentence_spans_path)
    stage_rules, quality, distribution = validate_sentence_spans(
        spans,
        input_digest=input_digest,
        output_root=output_root,
        primary_output_digest=primary_output_digest,
    )

    base_rules = [
        rule(
            rule_id="envelope_run_completed",
            severity="error",
            passed=run_status == "completed",
            detail=f"run_status={run_status}",
        ),
        rule(
            rule_id="envelope_counts_reconcile",
            severity="error",
            passed=summary["selected"] == (summary["completed"] + summary["failed"]),
            detail=(
                f"selected={summary['selected']} completed={summary['completed']} "
                f"failed={summary['failed']}"
            ),
        ),
        rule(
            rule_id="envelope_failures_count_matches",
            severity="error",
            passed=len(failure_rows) == summary["failed"],
            detail=f"failures_jsonl={len(failure_rows)} records_failed={summary['failed']}",
        ),
        rule(
            rule_id="certification_failure_policy_threshold",
            severity="error",
            passed=summary["failed"] <= FAILURE_POLICY.max_failure_count,
            detail=(
                f"records_failed={summary['failed']} "
                f"max_failure_count={FAILURE_POLICY.max_failure_count}"
            ),
        ),
    ]
    provisional_rules = base_rules + stage_rules
    provisional_validation = _rules_summary(provisional_rules)
    wall_clock_seconds = max(perf_counter() - started_monotonic, 0.0)
    metrics_payload = {
        "artifacts_processed": summary["completed"],
        "sentences_generated": summary["sentences_generated"],
        "failures_count": summary["failed"],
        "avg_sentences_per_artifact": round(
            summary["sentences_generated"] / summary["completed"], 6
        )
        if summary["completed"]
        else 0.0,
        "distribution": distribution,
        "timing": {
            "wall_clock_seconds": round(wall_clock_seconds, 6),
            "artifacts_per_second": round(
                (summary["selected"] / wall_clock_seconds) if wall_clock_seconds else float(summary["selected"]),
                6,
            ),
        },
        "validation": provisional_validation,
    }
    write_json(metrics_path, metrics_payload)
    write_json(
        validation_report_path,
        {
            "summary": provisional_validation,
            "rules": [item.to_mapping() for item in provisional_rules],
        },
    )

    run_summary = {
        "run_id": run_context.run_id,
        "stage_name": STAGE_NAME,
        "stage_version": STAGE_VERSION,
        "timestamps": {
            "start": isoformat_utc(run_context.started_at),
            "end": isoformat_utc(utc_now()),
        },
        "run_status": run_status,
        "validation_status": "failed" if _error_rules_failed(provisional_rules) else "passed",
        "certification_status": "failed",
        "input_record_count": len(input_entries),
        "input_digest": input_digest,
        "output_root": str(output_root),
        "primary_output_path": str(sentence_spans_path),
        "primary_output_count": len(spans),
        "primary_output_digest": primary_output_digest,
        "metrics_path": str(metrics_path),
        "failures_path": str(failures_path),
        "validation_report_path": str(validation_report_path),
        "records_seen": summary["selected"],
        "records_succeeded": summary["completed"],
        "records_failed": summary["failed"],
        "records_skipped": 0,
        "failure_mode": FAILURE_POLICY.failure_mode,
        "max_failure_count": FAILURE_POLICY.max_failure_count,
        "max_failure_rate": FAILURE_POLICY.max_failure_rate,
        "code_version": run_context.code_version,
        "config_version": run_context.config_version,
        "runtime_parameters_digest": run_context.runtime_parameters_digest,
        "rule_set_version": RULE_SET_VERSION,
        "certified_at": None,
        "blocking_issues": list(blocking_issues),
        "segmentation_version": segmentation_version,
    }
    write_json(run_summary_path, run_summary)

    file_existence_passed = all((run_context.run_dir / name).exists() for name in REQUIRED_RUN_FILES)
    json_parse_passed = True
    try:
        parse_jsonl(sentence_spans_path)
        parse_jsonl(failures_path)
        read_json(metrics_path)
        read_json(run_summary_path)
        read_json(validation_report_path)
    except (ValueError, json.JSONDecodeError):
        json_parse_passed = False

    final_rules = [
        rule(
            rule_id="envelope_required_files_exist",
            severity="error",
            passed=file_existence_passed,
            detail=", ".join(REQUIRED_RUN_FILES),
        ),
        rule(
            rule_id="envelope_json_artifacts_parse",
            severity="error",
            passed=json_parse_passed,
        ),
    ] + provisional_rules
    final_validation = _rules_summary(final_rules)
    error_failures = _error_rules_failed(final_rules)
    certified = run_status == "completed" and not error_failures and summary["failed"] == 0

    metrics_payload["validation"] = final_validation
    write_json(metrics_path, metrics_payload)
    write_json(
        validation_report_path,
        {
            "summary": final_validation,
            "rules": [item.to_mapping() for item in final_rules],
        },
    )
    final_blocking_issues = list(blocking_issues)
    for item in error_failures:
        if item.detail:
            final_blocking_issues.append(f"{item.rule_id}: {item.detail}")
        else:
            final_blocking_issues.append(item.rule_id)
    write_json(
        run_summary_path,
        {
            **run_summary,
            "validation_status": "failed" if error_failures else "passed",
            "certification_status": "certified" if certified else "failed",
            "certified_at": isoformat_utc(utc_now()) if certified else None,
            "blocking_issues": final_blocking_issues,
        },
    )
    _write_sentinel(run_context.run_dir, certified=certified)

    return summary
