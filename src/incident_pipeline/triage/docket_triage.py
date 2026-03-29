from __future__ import annotations

import json
from pathlib import Path

from incident_pipeline.common.paths import DEFAULT_SETTINGS_PATH
from incident_pipeline.common.settings import load_settings, resolve_storage_setting
from incident_pipeline.common.stage_runs import (
    StageFailure,
    StageFailurePolicy,
    assert_certified_input,
    build_input_manifest,
    build_logical_output_digest,
    create_stage_run_context,
    finalize_stage_run,
    rule,
    sha256_file,
)
from incident_pipeline.ingestion.manifest_reader import IngestionRecord
from incident_pipeline.triage.document_type import DocumentTypeInference, infer_document_type

CONFIG_PATH = DEFAULT_SETTINGS_PATH
DEFAULT_TEXT_READ_CHARS = 4000
STAGE_NAME = "triage"
STAGE_VERSION = "triage_v1"
FAILURE_POLICY = StageFailurePolicy(max_failure_count=0, max_failure_rate=0.0)
VALID_DOCUMENT_TYPES = {
    "attachment_appendix",
    "emergency_response_report",
    "external_reference",
    "factual_report",
    "interview_transcript",
    "investigation_report",
    "materials_report",
    "preliminary_report",
    "regulatory_report",
    "unknown",
}
REQUIRED_OUTPUT_FIELDS = (
    "docket_item_id",
    "ntsb_number",
    "title",
    "blob_sha256",
    "inferred_document_type",
    "inference_basis",
    "source_metadata_path",
    "source_text_path",
)


def load_config(config_path: Path | None = None) -> dict:
    resolved_config_path = config_path or CONFIG_PATH
    effective_path = None if resolved_config_path == DEFAULT_SETTINGS_PATH else resolved_config_path
    return load_settings(effective_path)


def make_summary(selected: int = 0) -> dict[str, int]:
    return {
        "selected": selected,
        "completed": 0,
        "reused_existing": 0,
        "failed": 0,
    }


def iter_metadata_paths(metadata_root: Path) -> list[Path]:
    return sorted(metadata_root.glob("*.json"))


def load_metadata_record(metadata_path: Path) -> IngestionRecord:
    with metadata_path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)

    if not isinstance(payload, dict):
        raise ValueError(f"Metadata file must contain a JSON object: {metadata_path}")

    return IngestionRecord.from_mapping(payload, line_number=1)


def ensure_supported_record(record: IngestionRecord, metadata_path: Path) -> None:
    if not record.docket_item_id or Path(record.docket_item_id).name != record.docket_item_id:
        raise ValueError(f"Invalid docket_item_id for output key: {record.docket_item_id}")

    if metadata_path.stem != record.docket_item_id:
        raise ValueError(
            f"Metadata filename does not match docket_item_id for {metadata_path}: "
            f"{record.docket_item_id}"
        )


def build_text_path(text_root: Path, docket_item_id: str) -> Path:
    return text_root / f"{docket_item_id}.txt"


def build_output_path(output_root: Path, docket_item_id: str) -> Path:
    return output_root / f"{docket_item_id}.json"


def read_text_excerpt(text_path: Path, *, char_limit: int) -> str:
    with text_path.open("r", encoding="utf-8", errors="replace") as handle:
        return handle.read(char_limit)


def build_triage_payload(
    record: IngestionRecord,
    inference: DocumentTypeInference,
    *,
    metadata_path: Path,
    text_path: Path,
) -> dict[str, str]:
    return {
        "docket_item_id": record.docket_item_id,
        "ntsb_number": record.ntsb_number,
        "title": record.title,
        "blob_sha256": record.blob_sha256,
        "inferred_document_type": inference.inferred_document_type,
        "inference_basis": inference.inference_basis,
        "source_metadata_path": str(metadata_path),
        "source_text_path": str(text_path),
    }


def write_json_artifact(path: Path, payload: dict[str, str], *, overwrite_existing: bool) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)

    if path.exists() and not overwrite_existing:
        return

    path.write_text(json.dumps(payload, indent=2, ensure_ascii=True) + "\n", encoding="utf-8")


def emit_error(docket_item_id: str, message: str) -> None:
    print(f"[ERROR] {docket_item_id}: {message}")


def build_input_entries(metadata_root: Path, text_root: Path) -> list[dict[str, str]]:
    entries: list[dict[str, str]] = []
    for metadata_path in iter_metadata_paths(metadata_root):
        docket_item_id = metadata_path.stem
        text_path = build_text_path(text_root, docket_item_id)
        entry = {
            "record_id": docket_item_id,
            "metadata_path": str(metadata_path.resolve()),
            "text_path": str(text_path.resolve()),
            "metadata_sha256": sha256_file(metadata_path),
        }
        if text_path.exists():
            entry["text_sha256"] = sha256_file(text_path)
        entries.append(entry)
    return entries


def build_output_entries(output_root: Path, docket_item_ids: list[str]) -> list[dict[str, str]]:
    entries: list[dict[str, str]] = []
    for docket_item_id in sorted(set(docket_item_ids)):
        output_path = build_output_path(output_root, docket_item_id)
        entry = {
            "record_id": docket_item_id,
            "output_path": str(output_path.resolve()),
        }
        if output_path.exists():
            entry["output_sha256"] = sha256_file(output_path)
        entries.append(entry)
    return entries


def validate_outputs(
    output_root: Path,
    docket_item_ids: list[str],
    *,
    expected_output_count: int,
) -> tuple[list, dict[str, int], dict[str, dict[str, int]]]:
    missing_required_field_count = 0
    invalid_enum_count = 0
    duplicate_key_count = max(0, len(docket_item_ids) - len(set(docket_item_ids)))
    distribution_counts: dict[str, int] = {}
    actual_output_count = 0

    for docket_item_id in sorted(set(docket_item_ids)):
        output_path = build_output_path(output_root, docket_item_id)
        if output_path.exists():
            actual_output_count += 1
        else:
            missing_required_field_count += len(REQUIRED_OUTPUT_FIELDS)
            continue

        with output_path.open("r", encoding="utf-8") as handle:
            payload = json.load(handle)

        for field in REQUIRED_OUTPUT_FIELDS:
            value = payload.get(field)
            if value in (None, ""):
                missing_required_field_count += 1

        inferred_document_type = str(payload.get("inferred_document_type") or "")
        if inferred_document_type not in VALID_DOCUMENT_TYPES:
            invalid_enum_count += 1
        distribution_counts[inferred_document_type or "missing"] = (
            distribution_counts.get(inferred_document_type or "missing", 0) + 1
        )

    stage_rules = [
        rule(
            rule_id="triage_output_count_matches_expected",
            severity="error",
            passed=actual_output_count == expected_output_count,
            detail=f"actual={actual_output_count} expected={expected_output_count}",
        ),
        rule(
            rule_id="triage_required_fields_present",
            severity="error",
            passed=missing_required_field_count == 0,
            detail=f"missing_required_field_count={missing_required_field_count}",
        ),
        rule(
            rule_id="triage_enum_values_valid",
            severity="error",
            passed=invalid_enum_count == 0,
            detail=f"invalid_enum_count={invalid_enum_count}",
        ),
        rule(
            rule_id="triage_duplicate_keys_absent",
            severity="error",
            passed=duplicate_key_count == 0,
            detail=f"duplicate_key_count={duplicate_key_count}",
        ),
    ]
    quality = {
        "missing_required_field_count": missing_required_field_count,
        "invalid_enum_count": invalid_enum_count,
        "duplicate_key_count": duplicate_key_count,
    }
    distribution = {
        "inferred_document_type": dict(sorted(distribution_counts.items())),
    }
    return stage_rules, quality, distribution


def classify_failure(
    *,
    docket_item_id: str,
    metadata_path: Path,
    exc: Exception,
) -> StageFailure:
    message = str(exc)
    failure_code = "internal_error"
    failure_class = "internal_error"
    retryable = False
    source_locator = str(metadata_path)

    if isinstance(exc, FileNotFoundError):
        failure_code = "missing_input_artifact"
        failure_class = "io"
        retryable = True
    elif isinstance(exc, ValueError):
        failure_code = "schema_validation_failed"
        failure_class = "schema_validation"

    return StageFailure(
        record_id=docket_item_id,
        artifact_id=docket_item_id,
        failure_code=failure_code,
        failure_class=failure_class,
        message=message,
        retryable=retryable,
        blocked_output=True,
        source_locator=source_locator,
        exception_type=type(exc).__name__,
    )


def run_docket_triage_batch(config_path: Path | None = None) -> dict[str, int]:
    cfg = load_config(config_path)
    triage_cfg = cfg["docket_triage"]
    metadata_root = resolve_storage_setting(cfg, triage_cfg["metadata_root"])
    text_root = resolve_storage_setting(cfg, triage_cfg["text_root"])
    output_root = resolve_storage_setting(cfg, triage_cfg["output_root"])
    overwrite_existing = bool(triage_cfg.get("overwrite_existing", False))
    text_read_chars = int(triage_cfg.get("text_read_chars", DEFAULT_TEXT_READ_CHARS))
    require_certified_input = bool(triage_cfg.get("require_certified_input", True))

    runtime_parameters = {
        "metadata_root": str(metadata_root),
        "text_root": str(text_root),
        "output_root": str(output_root),
        "overwrite_existing": overwrite_existing,
        "text_read_chars": text_read_chars,
        "require_certified_input": require_certified_input,
    }
    run_context = create_stage_run_context(
        stage_name=STAGE_NAME,
        stage_version=STAGE_VERSION,
        output_root=output_root,
        config_payload=cfg,
        runtime_parameters=runtime_parameters,
    )

    summary = make_summary(selected=0)
    failures: list[StageFailure] = []
    blocking_issues: list[str] = []
    effective_output_ids: list[str] = []
    input_entries: list[dict[str, str]] = []
    records_seen = 0
    run_status = "completed"

    try:
        if not metadata_root.exists():
            raise FileNotFoundError(f"Triage metadata directory not found: {metadata_root}")

        if not text_root.exists():
            raise FileNotFoundError(f"Triage text directory not found: {text_root}")

        input_entries = build_input_entries(metadata_root, text_root)
        summary = make_summary(selected=len(input_entries))

        if require_certified_input:
            ingestion_root = metadata_root.parent
            assert_certified_input(
                ingestion_root,
                expected_stage_name="ingestion",
                current_primary_output_count=len(input_entries),
                current_primary_output_digest=build_logical_output_digest(input_entries),
            )

        for metadata_path in iter_metadata_paths(metadata_root):
            docket_item_id = metadata_path.stem
            records_seen += 1

            try:
                record = load_metadata_record(metadata_path)
                ensure_supported_record(record, metadata_path)
                docket_item_id = record.docket_item_id
                output_path = build_output_path(output_root, docket_item_id)

                if not overwrite_existing and output_path.exists():
                    summary["completed"] += 1
                    summary["reused_existing"] += 1
                    effective_output_ids.append(docket_item_id)
                    continue

                print(f"[START] {docket_item_id}")
                text_path = build_text_path(text_root, docket_item_id)
                if not text_path.exists():
                    raise FileNotFoundError(f"Source text artifact not found: {text_path}")

                text_excerpt = read_text_excerpt(text_path, char_limit=text_read_chars)
                inference = infer_document_type(record.title, text_excerpt)
                payload = build_triage_payload(
                    record,
                    inference,
                    metadata_path=metadata_path,
                    text_path=text_path,
                )
                write_json_artifact(output_path, payload, overwrite_existing=overwrite_existing)
                summary["completed"] += 1
                effective_output_ids.append(docket_item_id)
            except Exception as exc:
                emit_error(docket_item_id, str(exc))
                summary["failed"] += 1
                failures.append(
                    classify_failure(
                        docket_item_id=docket_item_id,
                        metadata_path=metadata_path,
                        exc=exc,
                    )
                )
    except Exception as exc:
        run_status = "failed"
        blocking_issues.append(str(exc))

    input_manifest_path, input_record_count, input_digest = build_input_manifest(
        run_context.run_dir,
        input_entries=input_entries,
    )
    output_entries = build_output_entries(output_root, effective_output_ids)
    stage_rules, quality, distribution = validate_outputs(
        output_root,
        effective_output_ids,
        expected_output_count=summary["completed"],
    )
    finalize_stage_run(
        run_context,
        upstream_stage="ingestion",
        input_manifest_path=input_manifest_path,
        input_record_count=input_record_count,
        input_digest=input_digest,
        primary_output_count=len(output_entries),
        primary_output_digest=build_logical_output_digest(output_entries),
        records_seen=records_seen,
        records_succeeded=summary["completed"] - summary["reused_existing"],
        records_failed=summary["failed"],
        records_skipped=summary["reused_existing"],
        failure_policy=FAILURE_POLICY,
        failures=failures,
        quality=quality,
        distribution=distribution,
        stage_rules=stage_rules,
        blocking_issues=blocking_issues,
        run_status=run_status,
    )

    return summary
