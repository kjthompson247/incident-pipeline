from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
import json
from pathlib import Path
from time import perf_counter
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
    AtomicExtractionResult,
    MAPPING_CONTRACT_VERSION,
    MODEL_CONTRACT_VERSION,
    ONTOLOGY_VERSION,
    SentenceSpan,
)


CONFIG_PATH = DEFAULT_SETTINGS_PATH
STAGE_NAME = "atomic_extract"
STAGE_VERSION = "atomic_extract_v1"
RULE_SET_VERSION = "atomic_stage_certification_v1"
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


class SentenceSpanTransformer(Protocol):
    def __call__(self, sentence_span: SentenceSpan) -> AtomicExtractionResult | Mapping[str, Any]:
        ...


@dataclass(frozen=True)
class AtomicRunArtifacts:
    sentence_spans_path: Path
    atomic_extractions_path: Path
    atomic_failures_path: Path
    atomic_metrics_path: Path
    atomic_run_summary_path: Path
    atomic_validation_report_path: Path
    certified: bool


@dataclass(frozen=True)
class UpstreamSentenceSpanInput:
    stage_name: str
    run_id: str
    run_dir: Path
    input_path: Path
    summary: Mapping[str, Any]


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
        raise ValueError(
            f"Sentence span run is not certified: {run_dir}"
        )
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
        upstream_input = _validated_certified_input(
            run_dir=run_dir,
            input_path=input_path,
            summary=summary,
        )
        return sentence_span_root, upstream_input

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
    upstream_input = _validated_certified_input(
        run_dir=run_dir,
        input_path=resolved_input_path,
        summary=summary,
    )
    return sentence_span_root, upstream_input


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
            f"{upstream_input.input_path}: expected {expected_digest}, found {current_primary_output_digest}"
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
    elif isinstance(exc, ValueError):
        failure_code = "contract_validation_failed"
        failure_class = "schema_validation"
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


def _build_distribution(
    results: list[AtomicExtractionResult],
) -> dict[str, dict[str, int]]:
    status_counts: dict[str, int] = {}
    assertion_mode_counts: dict[str, int] = {}
    polarity_counts: dict[str, int] = {}
    claim_type_counts: dict[str, int] = {}
    ontology_type_counts: dict[str, int] = {}

    for result in results:
        status_counts[result.status] = status_counts.get(result.status, 0) + 1
        for claim in result.atomic_claims:
            assertion_mode_counts[claim.assertion_mode] = (
                assertion_mode_counts.get(claim.assertion_mode, 0) + 1
            )
            polarity_counts[claim.polarity] = polarity_counts.get(claim.polarity, 0) + 1
            claim_type_counts[claim.claim_type] = claim_type_counts.get(claim.claim_type, 0) + 1
        for candidate in result.ontology_candidates:
            ontology_type_counts[candidate.candidate_type] = (
                ontology_type_counts.get(candidate.candidate_type, 0) + 1
            )

    return {
        "status": dict(sorted(status_counts.items())),
        "assertion_mode": dict(sorted(assertion_mode_counts.items())),
        "polarity": dict(sorted(polarity_counts.items())),
        "claim_type": dict(sorted(claim_type_counts.items())),
        "ontology_candidate_type": dict(sorted(ontology_type_counts.items())),
    }


def _validate_persisted_results(
    results: list[AtomicExtractionResult],
    sentence_spans_by_id: Mapping[str, SentenceSpan],
) -> tuple[list[ValidationRule], dict[str, int], dict[str, dict[str, int]]]:
    missing_required_field_count = 0
    invalid_enum_count = 0
    duplicate_key_count = 0
    invalid_link_count = 0
    mismatch_count = 0
    unprocessable_contract_violations = 0
    causal_language_violations = 0

    for result in results:
        sentence_span = sentence_spans_by_id.get(result.sentence_span_id)
        if sentence_span is None:
            mismatch_count += 1
            continue
        try:
            reparsed = AtomicExtractionResult.from_mapping(
                result.to_mapping(),
                sentence_span=sentence_span,
            )
        except ValueError as exc:
            detail = str(exc)
            if "must match input span" in detail:
                mismatch_count += 1
            elif "duplicate atomic_claim_id" in detail:
                duplicate_key_count += 1
            elif "links unknown atomic_claim_id" in detail:
                invalid_link_count += 1
            elif "must be one of" in detail:
                invalid_enum_count += 1
            elif "must be empty when status=unprocessable" in detail or "must contain a reason" in detail:
                unprocessable_contract_violations += 1
            elif "require explicit causal language" in detail:
                causal_language_violations += 1
            else:
                missing_required_field_count += 1
            continue

        claim_ids = [claim.atomic_claim_id for claim in reparsed.atomic_claims]
        duplicate_key_count += len(claim_ids) - len(set(claim_ids))

    stage_rules = [
        rule(
            rule_id="atomic_sentence_span_ids_match_input",
            severity="error",
            passed=mismatch_count == 0,
            detail=f"mismatch_count={mismatch_count}",
        ),
        rule(
            rule_id="atomic_required_fields_present",
            severity="error",
            passed=missing_required_field_count == 0,
            detail=f"missing_required_field_count={missing_required_field_count}",
        ),
        rule(
            rule_id="atomic_enum_values_valid",
            severity="error",
            passed=invalid_enum_count == 0,
            detail=f"invalid_enum_count={invalid_enum_count}",
        ),
        rule(
            rule_id="atomic_duplicate_atomic_claim_ids_absent",
            severity="error",
            passed=duplicate_key_count == 0,
            detail=f"duplicate_key_count={duplicate_key_count}",
        ),
        rule(
            rule_id="atomic_ontology_links_reference_valid_claims",
            severity="error",
            passed=invalid_link_count == 0,
            detail=f"invalid_link_count={invalid_link_count}",
        ),
        rule(
            rule_id="atomic_unprocessable_contract_enforced",
            severity="error",
            passed=unprocessable_contract_violations == 0,
            detail=f"unprocessable_contract_violations={unprocessable_contract_violations}",
        ),
        rule(
            rule_id="atomic_causal_candidates_require_explicit_language",
            severity="error",
            passed=causal_language_violations == 0,
            detail=f"causal_language_violations={causal_language_violations}",
        ),
    ]
    quality = {
        "missing_required_field_count": missing_required_field_count,
        "invalid_enum_count": invalid_enum_count,
        "duplicate_key_count": duplicate_key_count,
    }
    return stage_rules, quality, _build_distribution(results)


def _finalize_atomic_run(
    *,
    run_context: Any,
    upstream_input: UpstreamSentenceSpanInput | None,
    sentence_spans_path: Path,
    atomic_extractions_path: Path,
    atomic_failures_path: Path,
    atomic_metrics_path: Path,
    atomic_run_summary_path: Path,
    atomic_validation_report_path: Path,
    input_record_count: int,
    records_seen: int,
    records_succeeded: int,
    records_failed: int,
    records_skipped: int,
    results: list[AtomicExtractionResult],
    failures: list[StageFailure],
    stage_rules: list[ValidationRule],
    quality: dict[str, int],
    distribution: dict[str, dict[str, int]],
    blocking_issues: list[str],
    run_status: str,
    failure_policy: StageFailurePolicy,
    model_contract_version: str,
    ontology_version: str,
    mapping_contract_version: str,
    batch_size: int,
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
        "quality": {
            "missing_required_field_count": int(quality.get("missing_required_field_count", 0)),
            "invalid_enum_count": int(quality.get("invalid_enum_count", 0)),
            "duplicate_key_count": int(quality.get("duplicate_key_count", 0)),
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
        "upstream_stage": upstream_input.stage_name if upstream_input is not None else UPSTREAM_STAGE_NAME,
        "upstream_run_id": upstream_input.run_id if upstream_input is not None else None,
        "upstream_run_dir": str(upstream_input.run_dir) if upstream_input is not None else None,
        "upstream_input_path": str(upstream_input.input_path) if upstream_input is not None else None,
        "input_manifest_path": (
            str(upstream_input.input_path) if upstream_input is not None else None
        ),
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
        "batch_size": batch_size,
    }
    write_json(atomic_run_summary_path, provisional_summary)

    file_existence_passed = all((run_context.run_dir / name).exists() for name in REQUIRED_RUN_FILES)
    json_parse_passed = True
    try:
        parse_jsonl(sentence_spans_path)
        parse_jsonl(atomic_extractions_path)
        parse_jsonl(atomic_failures_path)
        read_json(atomic_metrics_path)
        read_json(atomic_run_summary_path)
        read_json(atomic_validation_report_path)
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
        if item.detail:
            final_blocking_issues.append(f"{item.rule_id}: {detail}")
        else:
            final_blocking_issues.append(detail)

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
        atomic_extractions_path=atomic_extractions_path,
        atomic_failures_path=atomic_failures_path,
        atomic_metrics_path=atomic_metrics_path,
        atomic_run_summary_path=atomic_run_summary_path,
        atomic_validation_report_path=atomic_validation_report_path,
        certified=certified,
    )


def run_atomic_extraction_batch(
    config_path: Path | None = None,
    *,
    transform_span: SentenceSpanTransformer | None = None,
    input_path_override: Path | None = None,
) -> dict[str, int]:
    cfg = load_config(config_path)
    atomic_cfg = cfg["atomic_extraction"]
    _reject_configured_input_path(atomic_cfg)
    output_root = resolve_output_root(cfg)
    batch_size = int(atomic_cfg.get("batch_size", 50))
    require_certified_input = bool(atomic_cfg.get("require_certified_input", True))
    model_contract_version = str(
        atomic_cfg.get("model_contract_version", MODEL_CONTRACT_VERSION)
    )
    ontology_version = str(atomic_cfg.get("ontology_version", ONTOLOGY_VERSION))
    mapping_contract_version = str(
        atomic_cfg.get("mapping_contract_version", MAPPING_CONTRACT_VERSION)
    )
    sentence_span_root = resolve_sentence_span_root(cfg)
    transformer = transform_span or transform_sentence_span
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
        input_path_text = None
        upstream_run_id = None
        upstream_run_dir = None
        upstream_resolution_error = str(exc)
    else:
        upstream_resolution_error = None

    runtime_parameters = {
        "input_path": input_path_text,
        "output_root": str(output_root),
        "batch_size": batch_size,
        "require_certified_input": require_certified_input,
        "sentence_span_root": str(sentence_span_root),
        "upstream_run_id": upstream_run_id,
        "upstream_run_dir": upstream_run_dir,
        "input_path_override": str(input_path_override.resolve()) if input_path_override is not None else None,
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
    started_monotonic = perf_counter()

    sentence_spans_path = run_context.run_dir / "sentence_spans.jsonl"
    atomic_extractions_path = run_context.run_dir / "atomic_extractions.jsonl"
    atomic_failures_path = run_context.run_dir / "atomic_failures.jsonl"
    atomic_metrics_path = run_context.run_dir / "atomic_metrics.json"
    atomic_run_summary_path = run_context.run_dir / "atomic_run_summary.json"
    atomic_validation_report_path = run_context.run_dir / "atomic_validation_report.json"

    summary = make_summary(selected=0)
    blocking_issues: list[str] = []
    failures: list[StageFailure] = []
    results: list[AtomicExtractionResult] = []
    validated_sentence_spans: list[SentenceSpan] = []
    sentence_spans_by_id: dict[str, SentenceSpan] = {}
    run_status = "completed"

    try:
        if upstream_resolution_error is not None:
            raise ValueError(upstream_resolution_error)
        assert upstream_input is not None
        input_path = upstream_input.input_path

        for line_number, line in _load_nonempty_lines(input_path):
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
                record_id = sentence_span.sentence_span_id
                source_locator = json.dumps(sentence_span.locator, sort_keys=True)
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
    except Exception as exc:
        run_status = "failed"
        blocking_issues.append(str(exc))

    write_jsonl(sentence_spans_path, [span.to_mapping() for span in validated_sentence_spans])

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

    if run_status == "completed":
        for index, sentence_span in enumerate(validated_sentence_spans, start=1):
            try:
                raw_result = transformer(sentence_span)
                result = AtomicExtractionResult.from_mapping(
                    _as_result_mapping(raw_result),
                    sentence_span=sentence_span,
                    label=f"atomic_results[{index}]",
                )
                results.append(result)
                summary["completed"] += 1
                if result.status == "unprocessable":
                    summary["unprocessable"] += 1
            except Exception as exc:
                summary["failed"] += 1
                failures.append(
                    _classify_failure(
                        record_id=sentence_span.sentence_span_id,
                        artifact_id=sentence_span.artifact_id,
                        source_locator=json.dumps(sentence_span.locator, sort_keys=True),
                        exc=exc,
                    )
                )

    stage_rules, quality, distribution = _validate_persisted_results(
        results,
        sentence_spans_by_id,
    )
    _finalize_atomic_run(
        run_context=run_context,
        upstream_input=upstream_input,
        sentence_spans_path=sentence_spans_path,
        atomic_extractions_path=atomic_extractions_path,
        atomic_failures_path=atomic_failures_path,
        atomic_metrics_path=atomic_metrics_path,
        atomic_run_summary_path=atomic_run_summary_path,
        atomic_validation_report_path=atomic_validation_report_path,
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
        blocking_issues=blocking_issues,
        run_status=run_status,
        failure_policy=FAILURE_POLICY,
        model_contract_version=model_contract_version,
        ontology_version=ontology_version,
        mapping_contract_version=mapping_contract_version,
        batch_size=batch_size,
        started_monotonic=started_monotonic,
    )

    return summary
