from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
import json
from pathlib import Path
from time import perf_counter, sleep
from typing import Any, Protocol

from incident_pipeline.common.paths import DEFAULT_SETTINGS_PATH
from incident_pipeline.common.settings import load_settings, resolve_storage_setting
from incident_pipeline.common.stage_runs import (
    StageFailure,
    StageFailurePolicy,
    ValidationRule,
    create_stage_run_context,
    isoformat_utc,
    load_latest_certified_run_with_dir,
    load_run_summary_for_dir,
    parse_jsonl,
    read_json,
    rule,
    sha256_file,
    utc_now,
    write_json,
    write_jsonl,
)
from incident_pipeline.extract.atomic_contract import (
    ClaimContextRecord,
    AtomicExtractionResult,
    MAPPING_CONTRACT_VERSION,
    MODEL_CONTRACT_VERSION,
    ONTOLOGY_VERSION,
    PROMPT_VERSION,
    SentenceSpan,
)
from incident_pipeline.extract.openai_atomic_inference import (
    BatchAPIClient,
    BatchJobMetadata,
    BatchRequestTrace,
    LiveRequestTrace,
    OpenAIBatchAPIClient,
    OpenAIConfigurationError,
    OpenAIResponsesAPIClient,
    OpenAIBatchError,
    ResponsesAPIClient,
    TransientOpenAIError,
    build_batch_request_line,
    build_responses_api_request,
    parse_batch_output_line,
    parse_structured_response_payload,
)


CONFIG_PATH = DEFAULT_SETTINGS_PATH
STAGE_NAME = "atomic_extract"
STAGE_VERSION = "atomic_extract_v2"
RULE_SET_VERSION = "atomic_stage_certification_v2"
FAILURE_POLICY = StageFailurePolicy(max_failure_count=0, max_failure_rate=0.0)
UPSTREAM_STAGE_NAME = "sentence_span_generation"
UPSTREAM_INPUT_FILENAME = "sentence_spans.jsonl"
REQUIRED_RUN_FILES = (
    "sentence_spans.jsonl",
    "atomic_extractions.jsonl",
    "atomic_failures.jsonl",
    "atomic_metrics.json",
    "atomic_run_summary.json",
    "atomic_validation_report.json",
)
INFERENCE_METADATA_FILENAME = "inference_metadata.json"
CONTEXT_RECORDS_FILENAME = "atomic_contexts.jsonl"
BATCH_REQUESTS_FILENAME = "batch_requests.jsonl"
DEFAULT_BATCH_COMPLETION_WINDOW = "24h"


class SentenceSpanTransformer(Protocol):
    def __call__(self, sentence_span: SentenceSpan) -> AtomicExtractionResult | Mapping[str, Any]:
        ...


@dataclass(frozen=True)
class AtomicRunArtifacts:
    sentence_spans_path: Path
    context_records_path: Path
    atomic_extractions_path: Path
    atomic_failures_path: Path
    atomic_metrics_path: Path
    atomic_run_summary_path: Path
    atomic_validation_report_path: Path
    inference_metadata_path: Path
    certified: bool


@dataclass(frozen=True)
class UpstreamSentenceSpanInput:
    stage_name: str
    run_id: str
    run_dir: Path
    input_path: Path
    summary: Mapping[str, Any]


@dataclass(frozen=True)
class RawAtomicExecutionResult:
    sentence_span: SentenceSpan
    payload: Mapping[str, Any]


@dataclass(frozen=True)
class AtomicExecutionPayload:
    raw_results: tuple[RawAtomicExecutionResult, ...]
    failures: tuple[StageFailure, ...]
    inference_metadata: dict[str, Any]


def load_config(config_path: Path | None = None) -> dict[str, Any]:
    resolved_config_path = config_path or CONFIG_PATH
    effective_path = None if resolved_config_path == DEFAULT_SETTINGS_PATH else resolved_config_path
    return load_settings(effective_path)


def resolve_output_root(cfg: Mapping[str, Any]) -> Path:
    atomic_cfg = cfg["atomic_extraction"]
    return resolve_storage_setting(dict(cfg), atomic_cfg["output_root"])


def resolve_sentence_span_root(cfg: Mapping[str, Any]) -> Path:
    atomic_cfg = cfg["atomic_extraction"]
    return resolve_storage_setting(
        dict(cfg),
        atomic_cfg.get("sentence_span_root", "extract/sentence_spans"),
    )


def _reject_configured_input_path(atomic_cfg: Mapping[str, Any]) -> None:
    if "input_path" in atomic_cfg:
        raise ValueError(
            "atomic_extraction.input_path is no longer supported. "
            "Atomic extraction now discovers the latest certified sentence span run "
            "automatically. Use --input-path only to pin a specific certified "
            "sentence_spans.jsonl file for debugging or manual replay."
        )


def _validated_certified_input(
    *,
    run_dir: Path,
    input_path: Path,
    summary: Mapping[str, Any],
) -> UpstreamSentenceSpanInput:
    if not (run_dir / "_CERTIFIED").exists():
        raise ValueError(
            f"Sentence span input must come from a certified run, but _CERTIFIED is missing: {run_dir}"
        )
    if summary.get("stage_name") != UPSTREAM_STAGE_NAME:
        raise ValueError(
            f"Expected certified {UPSTREAM_STAGE_NAME} run, found "
            f"{summary.get('stage_name')!r} in {run_dir}"
        )
    if summary.get("certification_status") != "certified":
        raise ValueError(f"Sentence span run is not certified: {run_dir}")
    if not input_path.exists():
        raise FileNotFoundError(
            f"Certified {UPSTREAM_STAGE_NAME} run is missing {UPSTREAM_INPUT_FILENAME}: {input_path}"
        )
    return UpstreamSentenceSpanInput(
        stage_name=str(summary["stage_name"]),
        run_id=str(summary.get("run_id") or run_dir.name),
        run_dir=run_dir,
        input_path=input_path,
        summary=summary,
    )


def resolve_upstream_sentence_span_input(
    cfg: Mapping[str, Any],
    *,
    input_path_override: Path | None = None,
) -> tuple[Path, UpstreamSentenceSpanInput]:
    sentence_span_root = resolve_sentence_span_root(cfg)
    runs_root = (sentence_span_root / "runs").resolve()

    if input_path_override is None:
        run_dir, summary = load_latest_certified_run_with_dir(
            sentence_span_root,
            expected_stage_name=UPSTREAM_STAGE_NAME,
        )
        input_path = run_dir / UPSTREAM_INPUT_FILENAME
        return sentence_span_root, _validated_certified_input(
            run_dir=run_dir,
            input_path=input_path,
            summary=summary,
        )

    resolved_input_path = input_path_override.expanduser().resolve()
    if resolved_input_path.name != UPSTREAM_INPUT_FILENAME:
        raise ValueError(
            "--input-path must point to sentence_spans.jsonl inside a certified "
            "sentence span run directory."
        )
    try:
        resolved_input_path.relative_to(runs_root)
    except ValueError as exc:
        raise ValueError(
            f"--input-path must be under the configured sentence span runs root: {runs_root}"
        ) from exc

    run_dir = resolved_input_path.parent
    summary = load_run_summary_for_dir(run_dir)
    return sentence_span_root, _validated_certified_input(
        run_dir=run_dir,
        input_path=resolved_input_path,
        summary=summary,
    )


def _assert_selected_certified_input(
    upstream_input: UpstreamSentenceSpanInput,
    *,
    current_primary_output_count: int,
    current_primary_output_digest: str,
) -> None:
    expected_count = int(upstream_input.summary["primary_output_count"])
    expected_digest = str(upstream_input.summary["primary_output_digest"])
    if expected_count != current_primary_output_count:
        raise ValueError(
            f"Certified {upstream_input.stage_name} input count mismatch: "
            f"expected {expected_count}, found {current_primary_output_count}"
        )
    if expected_digest != current_primary_output_digest:
        raise ValueError(
            f"Certified {upstream_input.stage_name} input digest mismatch for "
            f"{upstream_input.input_path}: expected {expected_digest}, "
            f"found {current_primary_output_digest}"
        )


def transform_sentence_span(sentence_span: SentenceSpan) -> AtomicExtractionResult:
    raise RuntimeError(
        "Atomic extraction transformer is not configured. "
        "Provide a SentenceSpan transformer callable to run_atomic_extraction_batch()."
    )


def make_summary(selected: int = 0) -> dict[str, int]:
    return {
        "selected": selected,
        "completed": 0,
        "unprocessable": 0,
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
    sentinel_path = certified_path if certified else failed_path
    sentinel_path.write_text("", encoding="utf-8")


def _load_nonempty_lines(path: Path) -> list[tuple[int, str]]:
    lines: list[tuple[int, str]] = []
    with path.open("r", encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, start=1):
            stripped = line.strip()
            if not stripped:
                continue
            lines.append((line_number, stripped))
    return lines


def _as_result_mapping(
    value: AtomicExtractionResult | Mapping[str, Any],
) -> Mapping[str, Any]:
    if isinstance(value, AtomicExtractionResult):
        return value.to_mapping()
    if isinstance(value, Mapping):
        return value
    raise ValueError("Atomic transformer must return a mapping or AtomicExtractionResult")


def _classify_failure(
    *,
    record_id: str,
    artifact_id: str,
    source_locator: str,
    exc: Exception,
) -> StageFailure:
    message = str(exc)
    failure_code = "internal_error"
    failure_class = "internal_error"
    retryable = False

    if isinstance(exc, FileNotFoundError):
        failure_code = "missing_input_artifact"
        failure_class = "io"
        retryable = True
    elif isinstance(exc, json.JSONDecodeError):
        failure_code = "invalid_json"
        failure_class = "schema_validation"
    elif isinstance(exc, (ValueError, OpenAIBatchError)):
        failure_code = "contract_validation_failed"
        failure_class = "schema_validation"
    elif isinstance(exc, OpenAIConfigurationError):
        failure_code = "dependency_not_configured"
        failure_class = "dependency"
    elif isinstance(exc, TransientOpenAIError):
        failure_code = "transient_dependency_failure"
        failure_class = "dependency"
        retryable = True
    elif isinstance(exc, RuntimeError) and "not configured" in message:
        failure_code = "dependency_not_configured"
        failure_class = "dependency"

    return StageFailure(
        record_id=record_id,
        artifact_id=artifact_id,
        failure_code=failure_code,
        failure_class=failure_class,
        message=message,
        retryable=retryable,
        blocked_output=True,
        source_locator=source_locator,
        exception_type=type(exc).__name__,
    )


def _classify_validation_error(detail: str) -> tuple[str, ...]:
    message = detail.lower()
    if "duplicate claim_id" in message:
        return ("duplicate_key_count",)
    if "links unknown claim_id" in message:
        return ("invalid_link_count",)
    if "predicate_status must be one of" in message:
        return ("invalid_predicate_status_count", "invalid_enum_count")
    if "predicate_raw is required" in message:
        return ("missing_predicate_raw_count", "missing_required_field_count")
    if ".sentence_span_id must" in message:
        return ("claim_sentence_span_id_count",)
    if ".parent_structural_span_id must" in message:
        return ("claim_parent_structural_span_id_count",)
    if ".artifact_id must" in message:
        return ("claim_artifact_id_count",)
    if "context_ref must resolve" in message:
        return ("context_ref_resolution_count",)
    if "mechanism_class" in message:
        return ("invalid_mechanism_class_count",)
    if "candidate_type must be one of" in message:
        return ("invalid_ontology_candidate_type_count",)
    if "must be one of" in message:
        return ("invalid_enum_count",)
    if "must be empty when status=unprocessable" in message or "must contain a reason" in message:
        return ("unprocessable_contract_violations",)
    if "requires explicit causal language" in message:
        return ("causal_language_violations",)
    if "subject_text and/or subject_ref" in message:
        return ("missing_required_field_count",)
    if "must be present" in message:
        return ("missing_required_field_count",)
    return ("missing_required_field_count",)


def _build_distribution(
    results: list[AtomicExtractionResult],
) -> tuple[dict[str, dict[str, int]], dict[str, Any]]:
    status_counts: dict[str, int] = {}
    ontology_type_counts: dict[str, int] = {}
    predicate_status_counts: dict[str, int] = {}
    core_predicate_counts: dict[str, int] = {}
    candidate_predicates: set[str] = set()
    total_claims = 0
    core_predicate_count = 0
    candidate_predicate_count = 0
    unresolved_predicate_count = 0

    for result in results:
        status_counts[result.status] = status_counts.get(result.status, 0) + 1
        for claim in result.atomic_claims:
            total_claims += 1
            predicate_status_counts[claim.predicate_status] = (
                predicate_status_counts.get(claim.predicate_status, 0) + 1
            )
            if claim.predicate_status == "core":
                core_predicate_count += 1
                core_predicate_counts[claim.predicate] = (
                    core_predicate_counts.get(claim.predicate, 0) + 1
                )
            elif claim.predicate_status == "candidate":
                candidate_predicate_count += 1
                if claim.predicate_candidate:
                    candidate_predicates.add(claim.predicate_candidate)
            else:
                unresolved_predicate_count += 1
        for candidate in result.ontology_candidates:
            ontology_type_counts[candidate.candidate_type] = (
                ontology_type_counts.get(candidate.candidate_type, 0) + 1
            )

    distribution = {
        "status": dict(sorted(status_counts.items())),
        "predicate_status": dict(sorted(predicate_status_counts.items())),
        "core_predicate": dict(sorted(core_predicate_counts.items())),
        "ontology_candidate_type": dict(sorted(ontology_type_counts.items())),
    }
    claim_metrics = {
        "total_claims": total_claims,
        "core_predicate_count": core_predicate_count,
        "candidate_predicate_count": candidate_predicate_count,
        "unresolved_predicate_count": unresolved_predicate_count,
        "distinct_candidate_predicates": sorted(candidate_predicates),
    }
    return distribution, claim_metrics


def _validate_persisted_results(
    results: list[AtomicExtractionResult],
    sentence_spans_by_id: Mapping[str, SentenceSpan],
    context_records_by_ref: Mapping[str, ClaimContextRecord],
    *,
    initial_counters: Mapping[str, int] | None = None,
) -> tuple[list[ValidationRule], dict[str, int], dict[str, dict[str, int]], dict[str, Any]]:
    counters = {
        "missing_required_field_count": 0,
        "invalid_enum_count": 0,
        "duplicate_key_count": 0,
        "invalid_link_count": 0,
        "invalid_predicate_status_count": 0,
        "missing_predicate_raw_count": 0,
        "claim_sentence_span_id_count": 0,
        "claim_parent_structural_span_id_count": 0,
        "claim_artifact_id_count": 0,
        "context_ref_resolution_count": 0,
        "invalid_ontology_candidate_type_count": 0,
        "invalid_mechanism_class_count": 0,
        "unprocessable_contract_violations": 0,
        "causal_language_violations": 0,
    }
    if initial_counters is not None:
        for key, value in initial_counters.items():
            if key in counters:
                counters[key] += int(value)

    for result in results:
        sentence_span = sentence_spans_by_id.get(result.sentence_span_id)
        if sentence_span is None:
            counters["claim_sentence_span_id_count"] += 1
            continue
        try:
            reparsed = AtomicExtractionResult.from_mapping(
                result.to_mapping(),
                sentence_span=sentence_span,
            )
        except ValueError as exc:
            for key in _classify_validation_error(str(exc)):
                counters[key] += 1
            continue

        for claim in reparsed.atomic_claims:
            if claim.context_ref not in context_records_by_ref:
                counters["context_ref_resolution_count"] += 1

    distribution, claim_metrics = _build_distribution(results)
    validation_failures_by_type = {
        "missing_required_field_count": counters["missing_required_field_count"],
        "invalid_enum_count": counters["invalid_enum_count"],
        "duplicate_key_count": counters["duplicate_key_count"],
        "invalid_link_count": counters["invalid_link_count"],
        "invalid_predicate_status_count": counters["invalid_predicate_status_count"],
        "missing_predicate_raw_count": counters["missing_predicate_raw_count"],
        "claim_sentence_span_id_count": counters["claim_sentence_span_id_count"],
        "claim_parent_structural_span_id_count": counters["claim_parent_structural_span_id_count"],
        "claim_artifact_id_count": counters["claim_artifact_id_count"],
        "context_ref_resolution_count": counters["context_ref_resolution_count"],
        "invalid_ontology_candidate_type_count": counters["invalid_ontology_candidate_type_count"],
        "invalid_mechanism_class_count": counters["invalid_mechanism_class_count"],
        "unprocessable_contract_violations": counters["unprocessable_contract_violations"],
        "causal_language_violations": counters["causal_language_violations"],
    }
    stage_rules = [
        rule(
            rule_id="atomic_required_claim_fields_present",
            severity="error",
            passed=counters["missing_required_field_count"] == 0,
            detail=f"missing_required_field_count={counters['missing_required_field_count']}",
        ),
        rule(
            rule_id="atomic_valid_predicate_status",
            severity="error",
            passed=counters["invalid_predicate_status_count"] == 0,
            detail=f"invalid_predicate_status_count={counters['invalid_predicate_status_count']}",
        ),
        rule(
            rule_id="atomic_predicate_raw_required_for_non_core",
            severity="error",
            passed=counters["missing_predicate_raw_count"] == 0,
            detail=f"missing_predicate_raw_count={counters['missing_predicate_raw_count']}",
        ),
        rule(
            rule_id="atomic_claim_sentence_span_id_present",
            severity="error",
            passed=counters["claim_sentence_span_id_count"] == 0,
            detail=f"claim_sentence_span_id_count={counters['claim_sentence_span_id_count']}",
        ),
        rule(
            rule_id="atomic_claim_parent_structural_span_id_present",
            severity="error",
            passed=counters["claim_parent_structural_span_id_count"] == 0,
            detail=(
                "claim_parent_structural_span_id_count="
                f"{counters['claim_parent_structural_span_id_count']}"
            ),
        ),
        rule(
            rule_id="atomic_claim_artifact_id_present",
            severity="error",
            passed=counters["claim_artifact_id_count"] == 0,
            detail=f"claim_artifact_id_count={counters['claim_artifact_id_count']}",
        ),
        rule(
            rule_id="atomic_context_refs_resolve",
            severity="error",
            passed=counters["context_ref_resolution_count"] == 0,
            detail=f"context_ref_resolution_count={counters['context_ref_resolution_count']}",
        ),
        rule(
            rule_id="atomic_duplicate_claim_ids_absent",
            severity="error",
            passed=counters["duplicate_key_count"] == 0,
            detail=f"duplicate_key_count={counters['duplicate_key_count']}",
        ),
        rule(
            rule_id="atomic_ontology_candidate_types_valid",
            severity="error",
            passed=counters["invalid_ontology_candidate_type_count"] == 0,
            detail=(
                "invalid_ontology_candidate_type_count="
                f"{counters['invalid_ontology_candidate_type_count']}"
            ),
        ),
        rule(
            rule_id="atomic_mechanism_class_valid",
            severity="error",
            passed=counters["invalid_mechanism_class_count"] == 0,
            detail=f"invalid_mechanism_class_count={counters['invalid_mechanism_class_count']}",
        ),
        rule(
            rule_id="atomic_ontology_links_reference_valid_claims",
            severity="error",
            passed=counters["invalid_link_count"] == 0,
            detail=f"invalid_link_count={counters['invalid_link_count']}",
        ),
        rule(
            rule_id="atomic_unprocessable_contract_enforced",
            severity="error",
            passed=counters["unprocessable_contract_violations"] == 0,
            detail=(
                "unprocessable_contract_violations="
                f"{counters['unprocessable_contract_violations']}"
            ),
        ),
        rule(
            rule_id="atomic_causal_candidates_require_explicit_language",
            severity="error",
            passed=counters["causal_language_violations"] == 0,
            detail=f"causal_language_violations={counters['causal_language_violations']}",
        ),
    ]
    quality = {
        "missing_required_field_count": counters["missing_required_field_count"],
        "invalid_enum_count": counters["invalid_enum_count"],
        "duplicate_key_count": counters["duplicate_key_count"],
        "validation_failures_by_type": validation_failures_by_type,
    }
    claim_metrics["validation_failures_by_type"] = validation_failures_by_type
    return stage_rules, quality, distribution, claim_metrics


def _build_context_records(sentence_spans: list[SentenceSpan]) -> list[ClaimContextRecord]:
    return [ClaimContextRecord.from_sentence_span(span) for span in sentence_spans]


def _write_inference_metadata(path: Path, payload: Mapping[str, Any]) -> None:
    write_json(path, dict(payload))


def _finalize_atomic_run(
    *,
    run_context: Any,
    upstream_input: UpstreamSentenceSpanInput | None,
    sentence_spans_path: Path,
    context_records_path: Path,
    atomic_extractions_path: Path,
    atomic_failures_path: Path,
    atomic_metrics_path: Path,
    atomic_run_summary_path: Path,
    atomic_validation_report_path: Path,
    inference_metadata_path: Path,
    input_record_count: int,
    records_seen: int,
    records_succeeded: int,
    records_failed: int,
    records_skipped: int,
    results: list[AtomicExtractionResult],
    failures: list[StageFailure],
    stage_rules: list[ValidationRule],
    quality: dict[str, Any],
    distribution: dict[str, dict[str, int]],
    claim_metrics: dict[str, Any],
    blocking_issues: list[str],
    run_status: str,
    failure_policy: StageFailurePolicy,
    model_contract_version: str,
    ontology_version: str,
    mapping_contract_version: str,
    prompt_version: str,
    execution_mode: str,
    model: str | None,
    inference_metadata: Mapping[str, Any],
    started_monotonic: float,
) -> AtomicRunArtifacts:
    ended_at = utc_now()
    wall_clock_seconds = max(perf_counter() - started_monotonic, 0.0)
    failure_rows = [
        failure.to_mapping(run_id=run_context.run_id, stage_name=run_context.stage_name)
        for failure in failures
    ]
    write_jsonl(atomic_failures_path, failure_rows)
    write_jsonl(atomic_extractions_path, [result.to_mapping() for result in results])
    _write_inference_metadata(inference_metadata_path, inference_metadata)

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
            passed=records_seen == (records_succeeded + records_failed + records_skipped),
            detail=(
                f"seen={records_seen} succeeded={records_succeeded} "
                f"failed={records_failed} skipped={records_skipped}"
            ),
        ),
        rule(
            rule_id="envelope_failures_count_matches",
            severity="error",
            passed=len(failure_rows) == records_failed,
            detail=f"failures_jsonl={len(failure_rows)} records_failed={records_failed}",
        ),
        rule(
            rule_id="determinism_code_version_present",
            severity="error",
            passed=bool(run_context.code_version),
            detail=run_context.code_version or "git hash unavailable",
        ),
        rule(
            rule_id="determinism_config_version_present",
            severity="error",
            passed=bool(run_context.config_version),
            detail=run_context.config_version,
        ),
        rule(
            rule_id="determinism_runtime_parameters_digest_present",
            severity="error",
            passed=bool(run_context.runtime_parameters_digest),
            detail=run_context.runtime_parameters_digest,
        ),
        rule(
            rule_id="atomic_certified_upstream_required",
            severity="error",
            passed=upstream_input is not None,
            detail="certified sentence span upstream selected" if upstream_input else "missing certified upstream",
        ),
        rule(
            rule_id="certification_failure_policy_threshold",
            severity="error",
            passed=(
                records_failed <= failure_policy.max_failure_count
                and (
                    records_seen == 0
                    or (records_failed / records_seen) <= failure_policy.max_failure_rate
                )
            ),
            detail=(
                f"records_failed={records_failed} "
                f"max_failure_count={failure_policy.max_failure_count} "
                f"max_failure_rate={failure_policy.max_failure_rate}"
            ),
        ),
    ]
    provisional_rules = base_rules + stage_rules
    provisional_validation = _rules_summary(provisional_rules)
    metrics_payload = {
        "counts": {
            "records_seen": records_seen,
            "records_emitted": len(results),
            "records_failed": records_failed,
            "records_skipped": records_skipped,
        },
        "claims": {
            "total_claims": int(claim_metrics["total_claims"]),
            "core_predicate_count": int(claim_metrics["core_predicate_count"]),
            "candidate_predicate_count": int(claim_metrics["candidate_predicate_count"]),
            "unresolved_predicate_count": int(claim_metrics["unresolved_predicate_count"]),
            "distinct_candidate_predicates": list(claim_metrics["distinct_candidate_predicates"]),
        },
        "quality": {
            "missing_required_field_count": int(quality["missing_required_field_count"]),
            "invalid_enum_count": int(quality["invalid_enum_count"]),
            "duplicate_key_count": int(quality["duplicate_key_count"]),
            "validation_failures_by_type": dict(quality["validation_failures_by_type"]),
        },
        "distribution": distribution,
        "timing": {
            "wall_clock_seconds": round(wall_clock_seconds, 6),
            "records_per_second": round(
                (records_seen / wall_clock_seconds) if wall_clock_seconds else float(records_seen),
                6,
            ),
        },
        "validation": provisional_validation,
    }
    write_json(atomic_metrics_path, metrics_payload)
    write_json(
        atomic_validation_report_path,
        {
            "summary": provisional_validation,
            "rules": [item.to_mapping() for item in provisional_rules],
        },
    )

    primary_output_digest = sha256_file(atomic_extractions_path)
    input_digest = sha256_file(sentence_spans_path)
    provisional_summary = {
        "run_id": run_context.run_id,
        "stage_name": run_context.stage_name,
        "stage_version": STAGE_VERSION,
        "timestamps": {
            "start": isoformat_utc(run_context.started_at),
            "end": isoformat_utc(ended_at),
        },
        "run_status": run_status,
        "validation_status": "failed" if _error_rules_failed(provisional_rules) else "passed",
        "certification_status": "failed",
        "upstream_stage": upstream_input.stage_name if upstream_input else UPSTREAM_STAGE_NAME,
        "upstream_run_id": upstream_input.run_id if upstream_input else None,
        "upstream_run_dir": str(upstream_input.run_dir) if upstream_input else None,
        "upstream_input_path": str(upstream_input.input_path) if upstream_input else None,
        "input_manifest_path": str(upstream_input.input_path) if upstream_input else None,
        "input_record_count": input_record_count,
        "input_digest": input_digest,
        "output_root": str(run_context.output_root),
        "primary_output_path": str(atomic_extractions_path),
        "primary_output_count": len(results),
        "primary_output_digest": primary_output_digest,
        "metrics_path": str(atomic_metrics_path),
        "failures_path": str(atomic_failures_path),
        "validation_report_path": str(atomic_validation_report_path),
        "records_seen": records_seen,
        "records_succeeded": records_succeeded,
        "records_failed": records_failed,
        "records_skipped": records_skipped,
        "failure_mode": failure_policy.failure_mode,
        "max_failure_count": failure_policy.max_failure_count,
        "max_failure_rate": failure_policy.max_failure_rate,
        "code_version": run_context.code_version,
        "config_version": run_context.config_version,
        "runtime_parameters_digest": run_context.runtime_parameters_digest,
        "rule_set_version": RULE_SET_VERSION,
        "certified_at": None,
        "blocking_issues": list(blocking_issues),
        "model_contract_version": model_contract_version,
        "ontology_version": ontology_version,
        "mapping_contract_version": mapping_contract_version,
        "prompt_version": prompt_version,
        "execution_mode": execution_mode,
        "model": model,
        "context_records_path": str(context_records_path),
        "inference_metadata_path": str(inference_metadata_path),
    }
    write_json(atomic_run_summary_path, provisional_summary)

    file_existence_passed = all((run_context.run_dir / name).exists() for name in REQUIRED_RUN_FILES)
    json_parse_passed = True
    try:
        parse_jsonl(sentence_spans_path)
        parse_jsonl(context_records_path)
        parse_jsonl(atomic_extractions_path)
        parse_jsonl(atomic_failures_path)
        read_json(atomic_metrics_path)
        read_json(atomic_run_summary_path)
        read_json(atomic_validation_report_path)
        read_json(inference_metadata_path)
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
    ] + base_rules + stage_rules

    final_validation = _rules_summary(final_rules)
    error_failures = _error_rules_failed(final_rules)
    validation_status = "failed" if error_failures else "passed"

    final_blocking_issues = list(blocking_issues)
    for item in error_failures:
        detail = item.detail or item.rule_id
        final_blocking_issues.append(f"{item.rule_id}: {detail}" if item.detail else detail)

    certified = (
        run_status == "completed"
        and not error_failures
        and records_failed <= failure_policy.max_failure_count
        and (
            records_seen == 0
            or (records_failed / records_seen) <= failure_policy.max_failure_rate
        )
    )

    metrics_payload["validation"] = final_validation
    write_json(atomic_metrics_path, metrics_payload)
    write_json(
        atomic_validation_report_path,
        {
            "summary": final_validation,
            "rules": [item.to_mapping() for item in final_rules],
        },
    )
    write_json(
        atomic_run_summary_path,
        {
            **provisional_summary,
            "validation_status": validation_status,
            "certification_status": "certified" if certified else "failed",
            "blocking_issues": final_blocking_issues,
            "certified_at": isoformat_utc(ended_at) if certified else None,
        },
    )
    _write_sentinel(run_context.run_dir, certified=certified)

    return AtomicRunArtifacts(
        sentence_spans_path=sentence_spans_path,
        context_records_path=context_records_path,
        atomic_extractions_path=atomic_extractions_path,
        atomic_failures_path=atomic_failures_path,
        atomic_metrics_path=atomic_metrics_path,
        atomic_run_summary_path=atomic_run_summary_path,
        atomic_validation_report_path=atomic_validation_report_path,
        inference_metadata_path=inference_metadata_path,
        certified=certified,
    )


def _load_validated_sentence_spans(
    input_path: Path,
    *,
    summary: dict[str, int],
    failures: list[StageFailure],
) -> tuple[list[SentenceSpan], dict[str, SentenceSpan], str | None]:
    validated_sentence_spans: list[SentenceSpan] = []
    sentence_spans_by_id: dict[str, SentenceSpan] = {}
    try:
        lines = _load_nonempty_lines(input_path)
    except Exception as exc:
        return [], {}, str(exc)

    for line_number, line in lines:
        summary["selected"] += 1
        record_id = f"line:{line_number}"
        source_locator = f"{input_path}:{line_number}"
        try:
            raw_span = json.loads(line)
            sentence_span = SentenceSpan.from_mapping(
                raw_span,
                label=f"sentence_spans[{line_number}]",
            )
            sentence_spans_by_id[sentence_span.sentence_span_id] = sentence_span
            validated_sentence_spans.append(sentence_span)
        except Exception as exc:
            summary["failed"] += 1
            failures.append(
                _classify_failure(
                    record_id=record_id,
                    artifact_id=record_id,
                    source_locator=source_locator,
                    exc=exc,
                )
            )
    return validated_sentence_spans, sentence_spans_by_id, None


def _validate_execution_results(
    raw_results: tuple[RawAtomicExecutionResult, ...],
    *,
    summary: dict[str, int],
    failures: list[StageFailure],
) -> tuple[list[AtomicExtractionResult], dict[str, int]]:
    results: list[AtomicExtractionResult] = []
    validation_counters: dict[str, int] = {}
    for index, item in enumerate(raw_results, start=1):
        sentence_span = item.sentence_span
        try:
            result = AtomicExtractionResult.from_mapping(
                dict(item.payload),
                sentence_span=sentence_span,
                label=f"atomic_results[{index}]",
            )
            results.append(result)
            summary["completed"] += 1
            if result.status == "unprocessable":
                summary["unprocessable"] += 1
        except Exception as exc:
            summary["failed"] += 1
            for key in _classify_validation_error(str(exc)):
                validation_counters[key] = validation_counters.get(key, 0) + 1
            failures.append(
                _classify_failure(
                    record_id=sentence_span.sentence_span_id,
                    artifact_id=sentence_span.artifact_id,
                    source_locator=json.dumps(sentence_span.locator, sort_keys=True),
                    exc=exc,
                )
            )
    return results, validation_counters


def _resolve_prompt_version(atomic_cfg: Mapping[str, Any], prompt_version: str | None) -> str:
    return str(prompt_version or atomic_cfg.get("prompt_version", PROMPT_VERSION))


def _resolve_model(atomic_cfg: Mapping[str, Any], model: str | None) -> str:
    selected = model or atomic_cfg.get("model")
    if not isinstance(selected, str) or not selected.strip():
        raise ValueError(
            "An OpenAI model must be provided either as a function argument or "
            "atomic_extraction.model in settings."
        )
    return selected


def _prepare_atomic_run(
    config_path: Path | None,
    *,
    execution_mode: str,
    model: str | None,
    prompt_version: str | None,
    input_path_override: Path | None,
) -> tuple[
    dict[str, Any],
    Mapping[str, Any],
    Any,
    UpstreamSentenceSpanInput | None,
    Path,
    Path,
    Path,
    Path,
    Path,
    Path,
    Path,
    Path,
    dict[str, int],
    list[str],
    list[StageFailure],
    list[SentenceSpan],
    dict[str, SentenceSpan],
    list[ClaimContextRecord],
    str,
    str,
    str,
    str,
]:
    cfg = load_config(config_path)
    atomic_cfg = cfg["atomic_extraction"]
    _reject_configured_input_path(atomic_cfg)
    output_root = resolve_output_root(cfg)
    require_certified_input = bool(atomic_cfg.get("require_certified_input", True))
    model_contract_version = str(
        atomic_cfg.get("model_contract_version", MODEL_CONTRACT_VERSION)
    )
    ontology_version = str(atomic_cfg.get("ontology_version", ONTOLOGY_VERSION))
    mapping_contract_version = str(
        atomic_cfg.get("mapping_contract_version", MAPPING_CONTRACT_VERSION)
    )
    prompt_version_value = _resolve_prompt_version(atomic_cfg, prompt_version)
    sentence_span_root = resolve_sentence_span_root(cfg)

    upstream_input: UpstreamSentenceSpanInput | None = None
    input_path_text: str | None = None
    upstream_run_id: str | None = None
    upstream_run_dir: str | None = None
    try:
        sentence_span_root, upstream_input = resolve_upstream_sentence_span_input(
            cfg,
            input_path_override=input_path_override,
        )
        input_path_text = str(upstream_input.input_path)
        upstream_run_id = upstream_input.run_id
        upstream_run_dir = str(upstream_input.run_dir)
    except Exception as exc:
        upstream_resolution_error = str(exc)
    else:
        upstream_resolution_error = None

    runtime_parameters = {
        "execution_mode": execution_mode,
        "model": model,
        "prompt_version": prompt_version_value,
        "input_path": input_path_text,
        "output_root": str(output_root),
        "require_certified_input": require_certified_input,
        "sentence_span_root": str(sentence_span_root),
        "upstream_run_id": upstream_run_id,
        "upstream_run_dir": upstream_run_dir,
        "input_path_override": str(input_path_override.resolve()) if input_path_override else None,
        "model_contract_version": model_contract_version,
        "ontology_version": ontology_version,
        "mapping_contract_version": mapping_contract_version,
    }
    run_context = create_stage_run_context(
        stage_name=STAGE_NAME,
        stage_version=STAGE_VERSION,
        output_root=output_root,
        config_payload=cfg,
        runtime_parameters=runtime_parameters,
    )
    sentence_spans_path = run_context.run_dir / "sentence_spans.jsonl"
    context_records_path = run_context.run_dir / CONTEXT_RECORDS_FILENAME
    atomic_extractions_path = run_context.run_dir / "atomic_extractions.jsonl"
    atomic_failures_path = run_context.run_dir / "atomic_failures.jsonl"
    atomic_metrics_path = run_context.run_dir / "atomic_metrics.json"
    atomic_run_summary_path = run_context.run_dir / "atomic_run_summary.json"
    atomic_validation_report_path = run_context.run_dir / "atomic_validation_report.json"
    inference_metadata_path = run_context.run_dir / INFERENCE_METADATA_FILENAME
    batch_requests_path = run_context.run_dir / BATCH_REQUESTS_FILENAME

    summary = make_summary(selected=0)
    blocking_issues: list[str] = []
    failures: list[StageFailure] = []
    run_status = "completed"

    if upstream_resolution_error is not None:
        run_status = "failed"
        blocking_issues.append(upstream_resolution_error)
        validated_sentence_spans: list[SentenceSpan] = []
        sentence_spans_by_id: dict[str, SentenceSpan] = {}
    else:
        assert upstream_input is not None
        validated_sentence_spans, sentence_spans_by_id, load_error = _load_validated_sentence_spans(
            upstream_input.input_path,
            summary=summary,
            failures=failures,
        )
        if load_error is not None:
            run_status = "failed"
            blocking_issues.append(load_error)

    write_jsonl(sentence_spans_path, [span.to_mapping() for span in validated_sentence_spans])
    context_records = _build_context_records(validated_sentence_spans)
    write_jsonl(context_records_path, [record.to_mapping() for record in context_records])

    if run_status == "completed" and require_certified_input:
        try:
            assert upstream_input is not None
            _assert_selected_certified_input(
                upstream_input,
                current_primary_output_count=len(validated_sentence_spans),
                current_primary_output_digest=sha256_file(sentence_spans_path),
            )
        except Exception as exc:
            run_status = "failed"
            blocking_issues.append(str(exc))

    return (
        cfg,
        atomic_cfg,
        run_context,
        upstream_input,
        sentence_spans_path,
        context_records_path,
        atomic_extractions_path,
        atomic_failures_path,
        atomic_metrics_path,
        atomic_run_summary_path,
        atomic_validation_report_path,
        inference_metadata_path,
        summary,
        blocking_issues,
        failures,
        validated_sentence_spans,
        sentence_spans_by_id,
        context_records,
        run_status,
        model_contract_version,
        ontology_version,
        mapping_contract_version,
        prompt_version_value,
    )


def _execute_local_transformer(
    sentence_spans: list[SentenceSpan],
    *,
    transformer: SentenceSpanTransformer,
) -> AtomicExecutionPayload:
    raw_results: list[RawAtomicExecutionResult] = []
    failures: list[StageFailure] = []
    for sentence_span in sentence_spans:
        try:
            raw_result = transformer(sentence_span)
            raw_results.append(
                RawAtomicExecutionResult(
                    sentence_span=sentence_span,
                    payload=dict(_as_result_mapping(raw_result)),
                )
            )
        except Exception as exc:
            failures.append(
                _classify_failure(
                    record_id=sentence_span.sentence_span_id,
                    artifact_id=sentence_span.artifact_id,
                    source_locator=json.dumps(sentence_span.locator, sort_keys=True),
                    exc=exc,
                )
            )
    return AtomicExecutionPayload(
        raw_results=tuple(raw_results),
        failures=tuple(failures),
        inference_metadata={
            "execution_mode": "local_transformer",
            "live_requests": [],
            "batch_job": None,
        },
    )


def _execute_live_responses(
    sentence_spans: list[SentenceSpan],
    *,
    client: ResponsesAPIClient,
    model: str,
    prompt_version: str,
    model_contract_version: str,
    ontology_version: str,
    mapping_contract_version: str,
    max_retries: int,
    retry_base_delay_seconds: float,
) -> AtomicExecutionPayload:
    raw_results: list[RawAtomicExecutionResult] = []
    failures: list[StageFailure] = []
    traces: list[LiveRequestTrace] = []
    for sentence_span in sentence_spans:
        retry_status_codes: list[int] = []
        request_id: str | None = None
        attempts = 0
        while True:
            attempts += 1
            try:
                response_payload = client.create_response(
                    build_responses_api_request(
                        sentence_span,
                        model=model,
                        prompt_version=prompt_version,
                        model_contract_version=model_contract_version,
                        ontology_version=ontology_version,
                        mapping_contract_version=mapping_contract_version,
                    )
                )
                request_id_value = response_payload.get("id")
                if isinstance(request_id_value, str) and request_id_value.strip():
                    request_id = request_id_value
                payload = parse_structured_response_payload(response_payload)
                raw_results.append(
                    RawAtomicExecutionResult(sentence_span=sentence_span, payload=payload)
                )
                traces.append(
                    LiveRequestTrace(
                        sentence_span_id=sentence_span.sentence_span_id,
                        request_id=request_id,
                        attempt_count=attempts,
                        retry_status_codes=tuple(retry_status_codes),
                    )
                )
                break
            except TransientOpenAIError as exc:
                if exc.status_code is not None:
                    retry_status_codes.append(exc.status_code)
                if attempts > max_retries:
                    failures.append(
                        _classify_failure(
                            record_id=sentence_span.sentence_span_id,
                            artifact_id=sentence_span.artifact_id,
                            source_locator=json.dumps(sentence_span.locator, sort_keys=True),
                            exc=RuntimeError(
                                f"OpenAI retry budget exhausted after {attempts} attempts: {exc}"
                            ),
                        )
                    )
                    traces.append(
                        LiveRequestTrace(
                            sentence_span_id=sentence_span.sentence_span_id,
                            request_id=request_id,
                            attempt_count=attempts,
                            retry_status_codes=tuple(retry_status_codes),
                        )
                    )
                    break
                sleep(max(retry_base_delay_seconds, 0.0) * (2 ** (attempts - 1)))
            except Exception as exc:
                failures.append(
                    _classify_failure(
                        record_id=sentence_span.sentence_span_id,
                        artifact_id=sentence_span.artifact_id,
                        source_locator=json.dumps(sentence_span.locator, sort_keys=True),
                        exc=exc,
                    )
                )
                traces.append(
                    LiveRequestTrace(
                        sentence_span_id=sentence_span.sentence_span_id,
                        request_id=request_id,
                        attempt_count=attempts,
                        retry_status_codes=tuple(retry_status_codes),
                    )
                )
                break
    return AtomicExecutionPayload(
        raw_results=tuple(raw_results),
        failures=tuple(failures),
        inference_metadata={
            "execution_mode": "responses_api",
            "model": model,
            "prompt_version": prompt_version,
            "live_requests": [trace.to_mapping() for trace in traces],
            "batch_job": None,
        },
    )


def _execute_batch_api(
    sentence_spans: list[SentenceSpan],
    *,
    client: BatchAPIClient,
    model: str,
    prompt_version: str,
    model_contract_version: str,
    ontology_version: str,
    mapping_contract_version: str,
    batch_requests_path: Path,
    completion_window: str,
    poll_interval_seconds: float,
    max_polls: int,
) -> AtomicExecutionPayload:
    request_lines = [
        build_batch_request_line(
            sentence_span,
            model=model,
            prompt_version=prompt_version,
            model_contract_version=model_contract_version,
            ontology_version=ontology_version,
            mapping_contract_version=mapping_contract_version,
        )
        for sentence_span in sentence_spans
    ]
    write_jsonl(batch_requests_path, request_lines)
    submission_payload = client.submit_batch(
        batch_requests_path,
        completion_window=completion_window,
        metadata={
            "stage_name": STAGE_NAME,
            "prompt_version": prompt_version,
            "model": model,
        },
    )
    batch_id_value = submission_payload.get("id")
    if not isinstance(batch_id_value, str) or not batch_id_value.strip():
        raise ValueError("Batch submission did not return a batch id")
    terminal_payload = client.wait_for_batch(
        batch_id_value,
        poll_interval_seconds=poll_interval_seconds,
        max_polls=max_polls,
    )
    status = terminal_payload.get("status")
    if not isinstance(status, str) or not status.strip():
        raise ValueError("Batch terminal payload missing status")
    output_file_id = terminal_payload.get("output_file_id")
    error_file_id = terminal_payload.get("error_file_id")
    metadata = BatchJobMetadata(
        batch_id=batch_id_value,
        status=status,
        input_file_id=(
            terminal_payload.get("input_file_id")
            if isinstance(terminal_payload.get("input_file_id"), str)
            else None
        ),
        output_file_id=output_file_id if isinstance(output_file_id, str) else None,
        error_file_id=error_file_id if isinstance(error_file_id, str) else None,
        endpoint="/v1/responses",
        completion_window=completion_window,
    )
    if status != "completed":
        raise OpenAIBatchError(f"Batch did not complete successfully: {status}")
    if not metadata.output_file_id:
        raise OpenAIBatchError("Completed batch did not return output_file_id")

    output_text = client.download_file_text(metadata.output_file_id)
    error_text = client.download_file_text(metadata.error_file_id) if metadata.error_file_id else ""
    result_by_id: dict[str, RawAtomicExecutionResult] = {}
    failures: list[StageFailure] = []
    traces: list[BatchRequestTrace] = []

    for line in output_text.splitlines():
        if not line.strip():
            continue
        try:
            custom_id, response_body, trace = parse_batch_output_line(line)
            sentence_span = next(
                span for span in sentence_spans if span.sentence_span_id == custom_id
            )
            result_by_id[custom_id] = RawAtomicExecutionResult(
                sentence_span=sentence_span,
                payload=parse_structured_response_payload(response_body),
            )
            traces.append(trace)
        except Exception as exc:
            failures.append(
                _classify_failure(
                    record_id="batch_output",
                    artifact_id="batch_output",
                    source_locator="batch_output",
                    exc=exc,
                )
            )

    for line in error_text.splitlines():
        if not line.strip():
            continue
        try:
            payload = json.loads(line)
            if not isinstance(payload, Mapping):
                raise ValueError("Batch error line must be a JSON object")
            custom_id = payload.get("custom_id")
            if not isinstance(custom_id, str) or not custom_id.strip():
                raise ValueError("Batch error line missing custom_id")
            error_payload = payload.get("error")
            code = ""
            message = ""
            if isinstance(error_payload, Mapping):
                code_value = error_payload.get("code")
                message_value = error_payload.get("message")
                code = code_value if isinstance(code_value, str) else ""
                message = message_value if isinstance(message_value, str) else ""
            sentence_span = next(
                (span for span in sentence_spans if span.sentence_span_id == custom_id),
                None,
            )
            failures.append(
                StageFailure(
                    record_id=custom_id,
                    artifact_id=sentence_span.artifact_id if sentence_span else custom_id,
                    failure_code=code or "batch_request_failed",
                    failure_class="dependency",
                    message=message or f"Batch request failed for {custom_id}",
                    retryable=False,
                    blocked_output=True,
                    source_locator=custom_id,
                    exception_type=None,
                )
            )
        except Exception as exc:
            failures.append(
                _classify_failure(
                    record_id="batch_error_output",
                    artifact_id="batch_error_output",
                    source_locator="batch_error_output",
                    exc=exc,
                )
            )

    raw_results: list[RawAtomicExecutionResult] = []
    for sentence_span in sentence_spans:
        matched = result_by_id.get(sentence_span.sentence_span_id)
        if matched is None:
            failures.append(
                StageFailure(
                    record_id=sentence_span.sentence_span_id,
                    artifact_id=sentence_span.artifact_id,
                    failure_code="missing_batch_result",
                    failure_class="dependency",
                    message=(
                        "Batch output did not include a result for "
                        f"{sentence_span.sentence_span_id}"
                    ),
                    retryable=False,
                    blocked_output=True,
                    source_locator=sentence_span.sentence_span_id,
                    exception_type=None,
                )
            )
            continue
        raw_results.append(matched)

    return AtomicExecutionPayload(
        raw_results=tuple(raw_results),
        failures=tuple(failures),
        inference_metadata={
            "execution_mode": "batch_api",
            "model": model,
            "prompt_version": prompt_version,
            "batch_job": metadata.to_mapping(),
            "batch_requests_path": str(batch_requests_path),
            "batch_requests": [trace.to_mapping() for trace in traces],
        },
    )


def _complete_atomic_run(
    *,
    prepared: tuple[
        dict[str, Any],
        Mapping[str, Any],
        Any,
        UpstreamSentenceSpanInput | None,
        Path,
        Path,
        Path,
        Path,
        Path,
        Path,
        Path,
        Path,
        dict[str, int],
        list[str],
        list[StageFailure],
        list[SentenceSpan],
        dict[str, SentenceSpan],
        list[ClaimContextRecord],
        str,
        str,
        str,
        str,
        str,
    ],
    execution_payload: AtomicExecutionPayload | None,
    execution_mode: str,
    model: str | None,
    prompt_version: str,
    started_monotonic: float,
) -> dict[str, int]:
    (
        _cfg,
        atomic_cfg,
        run_context,
        upstream_input,
        sentence_spans_path,
        context_records_path,
        atomic_extractions_path,
        atomic_failures_path,
        atomic_metrics_path,
        atomic_run_summary_path,
        atomic_validation_report_path,
        inference_metadata_path,
        summary,
        blocking_issues,
        failures,
        validated_sentence_spans,
        sentence_spans_by_id,
        context_records,
        run_status,
        model_contract_version,
        ontology_version,
        mapping_contract_version,
        prepared_prompt_version,
    ) = prepared
    context_records_by_ref = {record.context_ref: record for record in context_records}
    results: list[AtomicExtractionResult] = []

    if execution_payload is not None:
        failures.extend(execution_payload.failures)
        if run_status == "completed":
            results, validation_counters = _validate_execution_results(
                execution_payload.raw_results,
                summary=summary,
                failures=failures,
            )
        else:
            validation_counters = {}
        inference_metadata = execution_payload.inference_metadata
    else:
        validation_counters = {}
        inference_metadata = {
            "execution_mode": execution_mode,
            "model": model,
            "prompt_version": prompt_version,
            "live_requests": [],
            "batch_job": None,
        }

    stage_rules, quality, distribution, claim_metrics = _validate_persisted_results(
        results,
        sentence_spans_by_id,
        context_records_by_ref,
        initial_counters=validation_counters,
    )
    _finalize_atomic_run(
        run_context=run_context,
        upstream_input=upstream_input,
        sentence_spans_path=sentence_spans_path,
        context_records_path=context_records_path,
        atomic_extractions_path=atomic_extractions_path,
        atomic_failures_path=atomic_failures_path,
        atomic_metrics_path=atomic_metrics_path,
        atomic_run_summary_path=atomic_run_summary_path,
        atomic_validation_report_path=atomic_validation_report_path,
        inference_metadata_path=inference_metadata_path,
        input_record_count=len(validated_sentence_spans),
        records_seen=summary["selected"],
        records_succeeded=summary["completed"],
        records_failed=summary["failed"],
        records_skipped=summary["selected"] - summary["completed"] - summary["failed"],
        results=results,
        failures=failures,
        stage_rules=stage_rules,
        quality=quality,
        distribution=distribution,
        claim_metrics=claim_metrics,
        blocking_issues=blocking_issues,
        run_status=run_status,
        failure_policy=FAILURE_POLICY,
        model_contract_version=model_contract_version,
        ontology_version=ontology_version,
        mapping_contract_version=mapping_contract_version,
        prompt_version=prepared_prompt_version,
        execution_mode=execution_mode,
        model=model,
        inference_metadata=inference_metadata,
        started_monotonic=started_monotonic,
    )
    return summary


def run_atomic_extraction_batch(
    config_path: Path | None = None,
    *,
    transform_span: SentenceSpanTransformer | None = None,
    input_path_override: Path | None = None,
) -> dict[str, int]:
    prepared = _prepare_atomic_run(
        config_path,
        execution_mode="local_transformer",
        model=None,
        prompt_version=None,
        input_path_override=input_path_override,
    )
    (
        _cfg,
        _atomic_cfg,
        _run_context,
        _upstream_input,
        _sentence_spans_path,
        _context_records_path,
        _atomic_extractions_path,
        _atomic_failures_path,
        _atomic_metrics_path,
        _atomic_run_summary_path,
        _atomic_validation_report_path,
        _inference_metadata_path,
        _summary,
        _blocking_issues,
        _failures,
        validated_sentence_spans,
        _sentence_spans_by_id,
        _context_records,
        run_status,
        _model_contract_version,
        _ontology_version,
        _mapping_contract_version,
        prompt_version,
    ) = prepared
    started_monotonic = perf_counter()
    execution_payload: AtomicExecutionPayload | None = None
    if run_status == "completed":
        transformer = transform_span or transform_sentence_span
        execution_payload = _execute_local_transformer(
            validated_sentence_spans,
            transformer=transformer,
        )
    return _complete_atomic_run(
        prepared=prepared,
        execution_payload=execution_payload,
        execution_mode="local_transformer",
        model=None,
        prompt_version=prompt_version,
        started_monotonic=started_monotonic,
    )


def run_atomic_extraction_live(
    config_path: Path | None = None,
    *,
    client: ResponsesAPIClient | None = None,
    model: str | None = None,
    prompt_version: str | None = None,
    input_path_override: Path | None = None,
    max_retries: int | None = None,
    retry_base_delay_seconds: float | None = None,
) -> dict[str, int]:
    cfg = load_config(config_path)
    atomic_cfg = cfg["atomic_extraction"]
    selected_model = _resolve_model(atomic_cfg, model)
    prepared = _prepare_atomic_run(
        config_path,
        execution_mode="responses_api",
        model=selected_model,
        prompt_version=prompt_version,
        input_path_override=input_path_override,
    )
    (
        _cfg,
        _atomic_cfg,
        _run_context,
        _upstream_input,
        _sentence_spans_path,
        _context_records_path,
        _atomic_extractions_path,
        _atomic_failures_path,
        _atomic_metrics_path,
        _atomic_run_summary_path,
        _atomic_validation_report_path,
        _inference_metadata_path,
        _summary,
        _blocking_issues,
        _failures,
        validated_sentence_spans,
        _sentence_spans_by_id,
        _context_records,
        run_status,
        model_contract_version,
        ontology_version,
        mapping_contract_version,
        prompt_version_value,
    ) = prepared
    started_monotonic = perf_counter()
    execution_payload: AtomicExecutionPayload | None = None
    if run_status == "completed":
        live_client = client or OpenAIResponsesAPIClient()
        execution_payload = _execute_live_responses(
            validated_sentence_spans,
            client=live_client,
            model=selected_model,
            prompt_version=prompt_version_value,
            model_contract_version=model_contract_version,
            ontology_version=ontology_version,
            mapping_contract_version=mapping_contract_version,
            max_retries=int(max_retries if max_retries is not None else atomic_cfg.get("live_max_retries", 3)),
            retry_base_delay_seconds=float(
                retry_base_delay_seconds
                if retry_base_delay_seconds is not None
                else atomic_cfg.get("live_retry_base_delay_seconds", 1.0)
            ),
        )
    return _complete_atomic_run(
        prepared=prepared,
        execution_payload=execution_payload,
        execution_mode="responses_api",
        model=selected_model,
        prompt_version=prompt_version_value,
        started_monotonic=started_monotonic,
    )


def run_atomic_extraction_openai_batch(
    config_path: Path | None = None,
    *,
    client: BatchAPIClient | None = None,
    model: str | None = None,
    prompt_version: str | None = None,
    input_path_override: Path | None = None,
    completion_window: str | None = None,
    poll_interval_seconds: float | None = None,
    max_polls: int | None = None,
) -> dict[str, int]:
    cfg = load_config(config_path)
    atomic_cfg = cfg["atomic_extraction"]
    selected_model = _resolve_model(atomic_cfg, model)
    prepared = _prepare_atomic_run(
        config_path,
        execution_mode="batch_api",
        model=selected_model,
        prompt_version=prompt_version,
        input_path_override=input_path_override,
    )
    (
        _cfg,
        _atomic_cfg,
        run_context,
        _upstream_input,
        _sentence_spans_path,
        _context_records_path,
        _atomic_extractions_path,
        _atomic_failures_path,
        _atomic_metrics_path,
        _atomic_run_summary_path,
        _atomic_validation_report_path,
        _inference_metadata_path,
        _summary,
        _blocking_issues,
        _failures,
        validated_sentence_spans,
        _sentence_spans_by_id,
        _context_records,
        run_status,
        model_contract_version,
        ontology_version,
        mapping_contract_version,
        prompt_version_value,
    ) = prepared
    started_monotonic = perf_counter()
    execution_payload: AtomicExecutionPayload | None = None
    if run_status == "completed":
        batch_client = client or OpenAIBatchAPIClient()
        execution_payload = _execute_batch_api(
            validated_sentence_spans,
            client=batch_client,
            model=selected_model,
            prompt_version=prompt_version_value,
            model_contract_version=model_contract_version,
            ontology_version=ontology_version,
            mapping_contract_version=mapping_contract_version,
            batch_requests_path=run_context.run_dir / BATCH_REQUESTS_FILENAME,
            completion_window=str(
                completion_window
                or atomic_cfg.get("batch_completion_window", DEFAULT_BATCH_COMPLETION_WINDOW)
            ),
            poll_interval_seconds=float(
                poll_interval_seconds
                if poll_interval_seconds is not None
                else atomic_cfg.get("batch_poll_interval_seconds", 5.0)
            ),
            max_polls=int(max_polls if max_polls is not None else atomic_cfg.get("batch_max_polls", 120)),
        )
    return _complete_atomic_run(
        prepared=prepared,
        execution_payload=execution_payload,
        execution_mode="batch_api",
        model=selected_model,
        prompt_version=prompt_version_value,
        started_monotonic=started_monotonic,
    )
