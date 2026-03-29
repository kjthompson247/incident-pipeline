from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
import hashlib
import json
from pathlib import Path
import subprocess
from typing import Any

from incident_pipeline.common.paths import REPO_ROOT

RULE_SET_VERSION = "stage_certification_v1"
REQUIRED_RUN_FILES = (
    "run_summary.json",
    "metrics.json",
    "failures.jsonl",
    "validation_report.json",
)


def _canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"))


def sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def sha256_mapping(value: Any) -> str:
    return sha256_text(_canonical_json(value))


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=True) + "\n", encoding="utf-8")


def write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    lines = [json.dumps(row, ensure_ascii=True, sort_keys=True) for row in rows]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def read_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    if not isinstance(payload, dict):
        raise ValueError(f"Expected JSON object in {path}")
    return payload


def parse_jsonl(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, start=1):
            stripped = line.strip()
            if not stripped:
                continue
            payload = json.loads(stripped)
            if not isinstance(payload, dict):
                raise ValueError(f"Expected JSON object at {path}:{line_number}")
            rows.append(payload)
    return rows


def utc_now() -> datetime:
    return datetime.now(UTC)


def isoformat_utc(value: datetime) -> str:
    return value.astimezone(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def git_code_version(repo_root: Path = REPO_ROOT) -> str | None:
    try:
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=repo_root,
            capture_output=True,
            text=True,
            check=True,
        )
    except (FileNotFoundError, subprocess.CalledProcessError):
        return None

    value = result.stdout.strip()
    return value or None


def build_run_id(
    *,
    stage_name: str,
    started_at: datetime,
    config_version: str,
    runtime_parameters_digest: str,
) -> str:
    started_text = isoformat_utc(started_at)
    seed = sha256_text(f"{stage_name}|{started_text}|{config_version}|{runtime_parameters_digest}")
    return f"{stage_name}_{started_text}_{seed[:8]}"


def build_logical_output_digest(entries: list[dict[str, Any]]) -> str:
    return sha256_mapping(entries)


def build_input_manifest(
    run_dir: Path,
    *,
    input_entries: list[dict[str, Any]],
) -> tuple[Path, int, str]:
    input_manifest_path = run_dir / "input_manifest.jsonl"
    write_jsonl(input_manifest_path, input_entries)
    return (
        input_manifest_path,
        len(input_entries),
        sha256_file(input_manifest_path),
    )


@dataclass(frozen=True)
class StageFailurePolicy:
    failure_mode: str = "fail_on_any_record_error"
    max_failure_count: int = 0
    max_failure_rate: float = 0.0


@dataclass(frozen=True)
class StageFailure:
    record_id: str
    artifact_id: str
    failure_code: str
    failure_class: str
    message: str
    retryable: bool
    blocked_output: bool
    source_locator: str
    exception_type: str | None = None

    def to_mapping(self, *, run_id: str, stage_name: str) -> dict[str, Any]:
        return {
            "run_id": run_id,
            "stage_name": stage_name,
            "record_id": self.record_id,
            "artifact_id": self.artifact_id,
            "failure_code": self.failure_code,
            "failure_class": self.failure_class,
            "message": self.message,
            "retryable": self.retryable,
            "blocked_output": self.blocked_output,
            "source_locator": self.source_locator,
            "exception_type": self.exception_type,
        }


@dataclass(frozen=True)
class ValidationRule:
    rule_id: str
    severity: str
    result: str
    detail: str | None = None

    def to_mapping(self) -> dict[str, Any]:
        payload = {
            "rule_id": self.rule_id,
            "severity": self.severity,
            "result": self.result,
        }
        if self.detail:
            payload["detail"] = self.detail
        return payload


@dataclass(frozen=True)
class StageRunContext:
    stage_name: str
    stage_version: str
    output_root: Path
    run_id: str
    run_dir: Path
    started_at: datetime
    code_version: str | None
    config_version: str
    runtime_parameters_digest: str
    rule_set_version: str = RULE_SET_VERSION


@dataclass(frozen=True)
class StageRunArtifacts:
    run_summary_path: Path
    metrics_path: Path
    failures_path: Path
    validation_report_path: Path
    input_manifest_path: Path
    certified: bool


def create_stage_run_context(
    *,
    stage_name: str,
    stage_version: str,
    output_root: Path,
    config_payload: dict[str, Any],
    runtime_parameters: dict[str, Any],
) -> StageRunContext:
    started_at = utc_now()
    config_version = sha256_mapping(config_payload)
    runtime_parameters_digest = sha256_mapping(runtime_parameters)
    run_id = build_run_id(
        stage_name=stage_name,
        started_at=started_at,
        config_version=config_version,
        runtime_parameters_digest=runtime_parameters_digest,
    )
    run_dir = output_root / "runs" / run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    return StageRunContext(
        stage_name=stage_name,
        stage_version=stage_version,
        output_root=output_root,
        run_id=run_id,
        run_dir=run_dir,
        started_at=started_at,
        code_version=git_code_version(),
        config_version=config_version,
        runtime_parameters_digest=runtime_parameters_digest,
    )


def rule(*, rule_id: str, severity: str, passed: bool, detail: str | None = None) -> ValidationRule:
    return ValidationRule(
        rule_id=rule_id,
        severity=severity,
        result="pass" if passed else "fail",
        detail=detail,
    )


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
    return [
        item
        for item in rules
        if item.severity == "error" and item.result == "fail"
    ]


def _records_per_second(*, records_seen: int, started_at: datetime, ended_at: datetime) -> float:
    wall_clock_seconds = max((ended_at - started_at).total_seconds(), 0.0)
    if wall_clock_seconds == 0.0:
        return float(records_seen) if records_seen else 0.0
    return records_seen / wall_clock_seconds


def _write_sentinel(run_dir: Path, *, certified: bool) -> None:
    certified_path = run_dir / "_CERTIFIED"
    failed_path = run_dir / "_FAILED"
    if certified_path.exists():
        certified_path.unlink()
    if failed_path.exists():
        failed_path.unlink()
    sentinel_path = certified_path if certified else failed_path
    sentinel_path.write_text("", encoding="utf-8")


def finalize_stage_run(
    context: StageRunContext,
    *,
    upstream_stage: str,
    input_manifest_path: Path,
    input_record_count: int,
    input_digest: str,
    primary_output_count: int,
    primary_output_digest: str,
    records_seen: int,
    records_succeeded: int,
    records_failed: int,
    records_skipped: int,
    failure_policy: StageFailurePolicy,
    failures: list[StageFailure],
    quality: dict[str, int],
    distribution: dict[str, dict[str, int]],
    stage_rules: list[ValidationRule],
    blocking_issues: list[str],
    run_status: str,
) -> StageRunArtifacts:
    metrics_path = context.run_dir / "metrics.json"
    failures_path = context.run_dir / "failures.jsonl"
    validation_report_path = context.run_dir / "validation_report.json"
    run_summary_path = context.run_dir / "run_summary.json"
    ended_at = utc_now()
    wall_clock_seconds = max((ended_at - context.started_at).total_seconds(), 0.0)
    failure_rows = [
        failure.to_mapping(run_id=context.run_id, stage_name=context.stage_name)
        for failure in failures
    ]

    write_jsonl(failures_path, failure_rows)

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
            passed=bool(context.code_version),
            detail=context.code_version or "git hash unavailable",
        ),
        rule(
            rule_id="determinism_config_version_present",
            severity="error",
            passed=bool(context.config_version),
            detail=context.config_version,
        ),
        rule(
            rule_id="determinism_runtime_parameters_digest_present",
            severity="error",
            passed=bool(context.runtime_parameters_digest),
            detail=context.runtime_parameters_digest,
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
            "records_emitted": primary_output_count,
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
                _records_per_second(
                    records_seen=records_seen,
                    started_at=context.started_at,
                    ended_at=ended_at,
                ),
                6,
            ),
        },
        "validation": provisional_validation,
    }
    write_json(metrics_path, metrics_payload)

    provisional_validation_payload = {
        "summary": provisional_validation,
        "rules": [item.to_mapping() for item in provisional_rules],
    }
    write_json(validation_report_path, provisional_validation_payload)

    provisional_summary_payload = {
        "run_id": context.run_id,
        "stage_name": context.stage_name,
        "stage_version": context.stage_version,
        "timestamps": {
            "start": isoformat_utc(context.started_at),
            "end": isoformat_utc(ended_at),
        },
        "run_status": run_status,
        "validation_status": "failed" if _error_rules_failed(provisional_rules) else "passed",
        "certification_status": "failed",
        "upstream_stage": upstream_stage,
        "input_manifest_path": str(input_manifest_path),
        "input_record_count": input_record_count,
        "input_digest": input_digest,
        "output_root": str(context.output_root),
        "primary_output_count": primary_output_count,
        "primary_output_digest": primary_output_digest,
        "metrics_path": str(metrics_path),
        "failures_path": str(failures_path),
        "validation_report_path": str(validation_report_path),
        "records_seen": records_seen,
        "records_succeeded": records_succeeded,
        "records_failed": records_failed,
        "records_skipped": records_skipped,
        "failure_mode": failure_policy.failure_mode,
        "max_failure_count": failure_policy.max_failure_count,
        "max_failure_rate": failure_policy.max_failure_rate,
        "code_version": context.code_version,
        "config_version": context.config_version,
        "runtime_parameters_digest": context.runtime_parameters_digest,
        "rule_set_version": context.rule_set_version,
        "certified_at": None,
        "blocking_issues": blocking_issues,
    }
    write_json(run_summary_path, provisional_summary_payload)

    file_existence_passed = all((context.run_dir / name).exists() for name in REQUIRED_RUN_FILES)
    json_parse_passed = True
    try:
        read_json(metrics_path)
        read_json(validation_report_path)
        read_json(run_summary_path)
        parse_jsonl(failures_path)
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
        if item.detail:
            final_blocking_issues.append(f"{item.rule_id}: {item.detail}")
        else:
            final_blocking_issues.append(item.rule_id)

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
    write_json(metrics_path, metrics_payload)

    write_json(
        validation_report_path,
        {
            "summary": final_validation,
            "rules": [item.to_mapping() for item in final_rules],
        },
    )

    final_summary_payload = {
        **provisional_summary_payload,
        "validation_status": validation_status,
        "certification_status": "certified" if certified else "failed",
        "blocking_issues": final_blocking_issues,
        "certified_at": isoformat_utc(ended_at) if certified else None,
    }
    write_json(run_summary_path, final_summary_payload)
    _write_sentinel(context.run_dir, certified=certified)

    return StageRunArtifacts(
        run_summary_path=run_summary_path,
        metrics_path=metrics_path,
        failures_path=failures_path,
        validation_report_path=validation_report_path,
        input_manifest_path=input_manifest_path,
        certified=certified,
    )


def load_latest_certified_run(stage_root: Path, *, expected_stage_name: str) -> dict[str, Any]:
    runs_root = stage_root / "runs"
    if not runs_root.exists():
        raise ValueError(f"No certified runs directory found for {expected_stage_name}: {runs_root}")

    certified_summaries: list[tuple[str, dict[str, Any]]] = []
    for run_dir in sorted(path for path in runs_root.iterdir() if path.is_dir()):
        summary_path = run_dir / "run_summary.json"
        certified_path = run_dir / "_CERTIFIED"
        if not summary_path.exists() or not certified_path.exists():
            continue
        payload = read_json(summary_path)
        if payload.get("stage_name") != expected_stage_name:
            continue
        if payload.get("certification_status") != "certified":
            continue
        certified_key = str(payload.get("certified_at") or payload.get("timestamps", {}).get("end") or "")
        certified_summaries.append((certified_key, payload))

    if not certified_summaries:
        raise ValueError(f"No certified {expected_stage_name} runs found under {runs_root}")

    certified_summaries.sort(key=lambda item: item[0])
    return certified_summaries[-1][1]


def assert_certified_input(
    stage_root: Path,
    *,
    expected_stage_name: str,
    current_primary_output_count: int,
    current_primary_output_digest: str,
) -> dict[str, Any]:
    payload = load_latest_certified_run(stage_root, expected_stage_name=expected_stage_name)
    expected_count = int(payload["primary_output_count"])
    expected_digest = str(payload["primary_output_digest"])

    if expected_count != current_primary_output_count:
        raise ValueError(
            f"Certified {expected_stage_name} input count mismatch: "
            f"expected {expected_count}, found {current_primary_output_count}"
        )

    if expected_digest != current_primary_output_digest:
        raise ValueError(
            f"Certified {expected_stage_name} input digest mismatch for {stage_root}: "
            f"expected {expected_digest}, found {current_primary_output_digest}"
        )

    return payload
