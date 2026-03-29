from __future__ import annotations

import json
from pathlib import Path

from incident_pipeline.common.paths import DEFAULT_SETTINGS_PATH, resolve_repo_path
from incident_pipeline.common.settings import load_settings
from incident_pipeline.common.stage_runs import (
    StageFailure,
    StageFailurePolicy,
    build_input_manifest,
    build_logical_output_digest,
    create_stage_run_context,
    finalize_stage_run,
    rule,
    sha256_file,
)
from incident_pipeline.ingestion.manifest_reader import IngestionRecord, iter_manifest_records
from incident_pipeline.ingestion.pdf_extract import extract_pdf_text_with_warnings
from incident_pipeline.ingestion.writer import (
    build_metadata_path,
    build_text_path,
    outputs_exist,
    write_metadata,
    write_text,
)

CONFIG_PATH = DEFAULT_SETTINGS_PATH
STAGE_NAME = "ingestion"
STAGE_VERSION = "ingestion_v1"
FAILURE_POLICY = StageFailurePolicy(max_failure_count=0, max_failure_rate=0.0)
REQUIRED_METADATA_FIELDS = (
    "project_id",
    "ntsb_number",
    "docket_item_id",
    "ordinal",
    "title",
    "view_url",
    "source_url",
    "blob_sha256",
    "blob_path",
    "media_type",
)


def load_config() -> dict:
    config_path = None if CONFIG_PATH == DEFAULT_SETTINGS_PATH else CONFIG_PATH
    return load_settings(config_path)


def resolve_path(path_value: str) -> Path:
    return resolve_repo_path(path_value)


def make_summary() -> dict[str, int]:
    return {
        "selected": 0,
        "completed": 0,
        "reused_existing": 0,
        "failed": 0,
    }


def record_to_mapping(record: IngestionRecord) -> dict[str, object]:
    payload: dict[str, object] = {
        "project_id": record.project_id,
        "ntsb_number": record.ntsb_number,
        "docket_item_id": record.docket_item_id,
        "ordinal": record.ordinal,
        "title": record.title,
        "view_url": record.view_url,
        "source_url": record.source_url,
        "blob_sha256": record.blob_sha256,
        "blob_path": record.blob_path,
        "media_type": record.media_type,
    }
    if record.acquisition_run_id is not None:
        payload["acquisition_run_id"] = record.acquisition_run_id
    return payload


def ensure_supported_record(record: IngestionRecord) -> None:
    if record.media_type.lower() != "application/pdf":
        raise ValueError(
            f"Unsupported media_type for {record.docket_item_id}: {record.media_type}"
        )

    if not record.docket_item_id or Path(record.docket_item_id).name != record.docket_item_id:
        raise ValueError(f"Invalid docket_item_id for output key: {record.docket_item_id}")


def emit_warning(docket_item_id: str, warning_lines: tuple[str, ...] | list[str]) -> None:
    if not warning_lines:
        return

    print(f"[WARN] {docket_item_id}")
    for line in warning_lines:
        print(f"  {line}")


def emit_error(docket_item_id: str, message: str) -> None:
    print(f"[ERROR] {docket_item_id}: {message}")


def build_output_entries(output_root: Path, docket_item_ids: list[str]) -> list[dict[str, str]]:
    entries: list[dict[str, str]] = []
    for docket_item_id in sorted(set(docket_item_ids)):
        metadata_path = build_metadata_path(output_root, docket_item_id)
        text_path = build_text_path(output_root, docket_item_id)
        entry = {
            "record_id": docket_item_id,
            "metadata_path": str(metadata_path.resolve()),
            "text_path": str(text_path.resolve()),
        }
        if metadata_path.exists():
            entry["metadata_sha256"] = sha256_file(metadata_path)
        if text_path.exists():
            entry["text_sha256"] = sha256_file(text_path)
        entries.append(entry)
    return entries


def validate_outputs(
    output_root: Path,
    docket_item_ids: list[str],
    *,
    expected_output_count: int,
) -> tuple[list, dict[str, int], dict[str, dict[str, int]]]:
    missing_required_field_count = 0
    duplicate_key_count = max(0, len(docket_item_ids) - len(set(docket_item_ids)))
    media_type_counts: dict[str, int] = {}
    text_pair_missing_count = 0
    actual_output_count = 0

    for docket_item_id in sorted(set(docket_item_ids)):
        metadata_path = build_metadata_path(output_root, docket_item_id)
        text_path = build_text_path(output_root, docket_item_id)
        if metadata_path.exists() and text_path.exists():
            actual_output_count += 1
        if not text_path.exists():
            text_pair_missing_count += 1

        if not metadata_path.exists():
            missing_required_field_count += len(REQUIRED_METADATA_FIELDS)
            continue

        with metadata_path.open("r", encoding="utf-8") as handle:
            payload = json.load(handle)

        for field in REQUIRED_METADATA_FIELDS:
            value = payload.get(field)
            if value in (None, ""):
                missing_required_field_count += 1
        media_type = str(payload.get("media_type") or "unknown")
        media_type_counts[media_type] = media_type_counts.get(media_type, 0) + 1

    stage_rules = [
        rule(
            rule_id="ingestion_output_count_matches_expected",
            severity="error",
            passed=actual_output_count == expected_output_count,
            detail=f"actual={actual_output_count} expected={expected_output_count}",
        ),
        rule(
            rule_id="ingestion_required_fields_present",
            severity="error",
            passed=missing_required_field_count == 0,
            detail=f"missing_required_field_count={missing_required_field_count}",
        ),
        rule(
            rule_id="ingestion_duplicate_keys_absent",
            severity="error",
            passed=duplicate_key_count == 0,
            detail=f"duplicate_key_count={duplicate_key_count}",
        ),
        rule(
            rule_id="ingestion_text_pairs_exist",
            severity="error",
            passed=text_pair_missing_count == 0,
            detail=f"text_pair_missing_count={text_pair_missing_count}",
        ),
    ]
    quality = {
        "missing_required_field_count": missing_required_field_count,
        "invalid_enum_count": 0,
        "duplicate_key_count": duplicate_key_count,
    }
    distribution = {
        "media_type": dict(sorted(media_type_counts.items())),
    }
    return stage_rules, quality, distribution


def classify_failure(record: IngestionRecord, exc: Exception) -> StageFailure:
    message = str(exc)
    failure_code = "internal_error"
    failure_class = "internal_error"
    retryable = False

    if isinstance(exc, FileNotFoundError):
        failure_code = "blob_missing"
        failure_class = "io"
        retryable = True
    elif isinstance(exc, ValueError):
        failure_code = "input_contract_violation"
        failure_class = "input_contract"
    elif isinstance(exc, RuntimeError):
        if "pypdf is required" in message:
            failure_code = "dependency_missing"
            failure_class = "dependency"
            retryable = True
        else:
            failure_code = "pdf_extract_failed"
            failure_class = "data_contract"

    return StageFailure(
        record_id=record.docket_item_id,
        artifact_id=record.docket_item_id,
        failure_code=failure_code,
        failure_class=failure_class,
        message=message,
        retryable=retryable,
        blocked_output=True,
        source_locator=record.blob_path,
        exception_type=type(exc).__name__,
    )


def run_docket_ingest_batch() -> dict[str, int]:
    cfg = load_config()
    ingest_cfg = cfg["docket_ingest"]
    manifest_path = resolve_path(ingest_cfg["manifest_path"])
    output_root = resolve_path(ingest_cfg["output_root"])
    overwrite_existing = bool(ingest_cfg.get("overwrite_existing", False))
    runtime_parameters = {
        "manifest_path": str(manifest_path),
        "output_root": str(output_root),
        "overwrite_existing": overwrite_existing,
    }
    run_context = create_stage_run_context(
        stage_name=STAGE_NAME,
        stage_version=STAGE_VERSION,
        output_root=output_root,
        config_payload=cfg,
        runtime_parameters=runtime_parameters,
    )

    summary = make_summary()
    failures: list[StageFailure] = []
    blocking_issues: list[str] = []
    input_entries: list[dict[str, object]] = []
    effective_output_ids: list[str] = []
    records_seen = 0
    run_status = "completed"

    try:
        if not manifest_path.exists():
            raise FileNotFoundError(f"Ingestion manifest not found: {manifest_path}")

        records = list(iter_manifest_records(manifest_path))
        input_entries = [record_to_mapping(record) for record in records]

        for record in records:
            summary["selected"] += 1
            records_seen += 1

            try:
                ensure_supported_record(record)

                if not overwrite_existing and outputs_exist(output_root, record.docket_item_id):
                    summary["completed"] += 1
                    summary["reused_existing"] += 1
                    effective_output_ids.append(record.docket_item_id)
                    continue

                print(f"[START] {record.docket_item_id}")

                blob_path = Path(record.blob_path)
                if not blob_path.exists():
                    raise FileNotFoundError(
                        f"Blob not found for {record.docket_item_id}: {blob_path}"
                    )

                extraction = extract_pdf_text_with_warnings(blob_path)
                emit_warning(record.docket_item_id, extraction.warnings)
                text_path = build_text_path(output_root, record.docket_item_id)
                metadata_path = build_metadata_path(output_root, record.docket_item_id)

                write_text(text_path, extraction.text, overwrite_existing=overwrite_existing)
                write_metadata(metadata_path, record, overwrite_existing=overwrite_existing)
                summary["completed"] += 1
                effective_output_ids.append(record.docket_item_id)
            except Exception as exc:
                emit_warning(record.docket_item_id, getattr(exc, "warnings", ()))
                emit_error(record.docket_item_id, str(exc))
                summary["failed"] += 1
                failures.append(classify_failure(record, exc))
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
        upstream_stage="acquisition",
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
