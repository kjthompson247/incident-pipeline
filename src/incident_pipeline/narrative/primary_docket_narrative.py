from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path

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
from incident_pipeline.triage.docket_triage import load_config, resolve_path, write_json_artifact


TYPE_PRECEDENCE = (
    "investigation_report",
    "factual_report",
)
STAGE_NAME = "narrative"
STAGE_VERSION = "narrative_v1"
FAILURE_POLICY = StageFailurePolicy(max_failure_count=0, max_failure_rate=0.0)
REQUIRED_OUTPUT_FIELDS = (
    "docket_id",
    "selected_document_id",
    "inferred_document_type",
    "selection_reason",
)


@dataclass(frozen=True)
class NarrativeCandidate:
    docket_id: str
    docket_item_id: str
    inferred_document_type: str
    title: str
    source_text_path: Path | None


def make_summary(dockets: int = 0) -> dict[str, int]:
    return {
        "dockets": dockets,
        "selected": 0,
        "reused_existing": 0,
        "failed": 0,
    }


def iter_triage_artifact_paths(input_root: Path) -> list[Path]:
    return sorted(input_root.glob("*.json"))


def extract_docket_id(docket_item_id: str) -> str:
    parts = docket_item_id.split(":")
    if len(parts) < 3 or parts[0] != "ntsb" or parts[1] != "docket_item" or not parts[2]:
        raise ValueError(f"Invalid docket_item_id: {docket_item_id}")

    return parts[2]


def load_triage_candidate(path: Path) -> NarrativeCandidate:
    with path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)

    if not isinstance(payload, dict):
        raise ValueError(f"Triage artifact must contain a JSON object: {path}")

    docket_item_id = payload.get("docket_item_id")
    inferred_document_type = payload.get("inferred_document_type")
    if not docket_item_id or not isinstance(docket_item_id, str):
        raise ValueError(f"Missing docket_item_id in triage artifact: {path}")

    if not inferred_document_type or not isinstance(inferred_document_type, str):
        raise ValueError(f"Missing inferred_document_type in triage artifact: {path}")

    if path.stem != docket_item_id:
        raise ValueError(
            f"Triage artifact filename does not match docket_item_id for {path}: {docket_item_id}"
        )

    source_text_path_value = payload.get("source_text_path")
    source_text_path = None
    if isinstance(source_text_path_value, str) and source_text_path_value:
        source_text_path = Path(source_text_path_value)

    return NarrativeCandidate(
        docket_id=extract_docket_id(docket_item_id),
        docket_item_id=docket_item_id,
        inferred_document_type=inferred_document_type,
        title=str(payload.get("title") or ""),
        source_text_path=source_text_path,
    )


def group_candidates_by_docket(input_root: Path) -> dict[str, list[NarrativeCandidate]]:
    grouped: dict[str, list[NarrativeCandidate]] = {}

    for path in iter_triage_artifact_paths(input_root):
        candidate = load_triage_candidate(path)
        grouped.setdefault(candidate.docket_id, []).append(candidate)

    return grouped


def build_output_path(output_root: Path, docket_id: str) -> Path:
    return output_root / f"{docket_id}.json"


def _read_text_length(path: Path | None) -> int:
    if path is None or not path.exists():
        return 0

    return len(path.read_text(encoding="utf-8", errors="replace"))


def _title_length(candidate: NarrativeCandidate) -> int:
    return len(candidate.title.strip())


def _candidate_sort_key(candidate: NarrativeCandidate) -> tuple[int, int, str]:
    return (
        -_title_length(candidate),
        -_read_text_length(candidate.source_text_path),
        candidate.docket_item_id,
    )


def _select_first(candidates: list[NarrativeCandidate]) -> NarrativeCandidate:
    return min(candidates, key=_candidate_sort_key)


def select_primary_candidate(
    candidates: list[NarrativeCandidate],
) -> tuple[NarrativeCandidate, str]:
    if not candidates:
        raise ValueError("Cannot select primary docket narrative from an empty candidate list")

    for document_type in TYPE_PRECEDENCE:
        typed_candidates = [
            candidate
            for candidate in candidates
            if candidate.inferred_document_type == document_type
        ]
        if typed_candidates:
            return (
                _select_first(typed_candidates),
                f"selected highest-priority document_type {document_type}",
            )

    selected = _select_first(candidates)
    if _title_length(selected) > 0:
        return selected, "selected fallback longest title"

    if _read_text_length(selected.source_text_path) > 0:
        return selected, "selected fallback available text"

    return selected, "selected fallback first document"


def build_selection_payload(
    candidate: NarrativeCandidate,
    *,
    selection_reason: str,
) -> dict[str, str]:
    return {
        "docket_id": candidate.docket_id,
        "selected_document_id": candidate.docket_item_id,
        "inferred_document_type": candidate.inferred_document_type,
        "selection_reason": selection_reason,
    }


def emit_error(docket_id: str, message: str) -> None:
    print(f"[ERROR] {docket_id}: {message}")


def build_input_entries(input_root: Path) -> list[dict[str, str]]:
    entries: list[dict[str, str]] = []
    for path in iter_triage_artifact_paths(input_root):
        entries.append(
            {
                "record_id": path.stem,
                "artifact_path": str(path.resolve()),
                "artifact_sha256": sha256_file(path),
            }
        )
    return entries


def build_output_entries(output_root: Path, docket_ids: list[str]) -> list[dict[str, str]]:
    entries: list[dict[str, str]] = []
    for docket_id in sorted(set(docket_ids)):
        output_path = build_output_path(output_root, docket_id)
        entry = {
            "record_id": docket_id,
            "output_path": str(output_path.resolve()),
        }
        if output_path.exists():
            entry["output_sha256"] = sha256_file(output_path)
        entries.append(entry)
    return entries


def validate_outputs(
    output_root: Path,
    docket_ids: list[str],
    *,
    grouped_candidates: dict[str, list[NarrativeCandidate]],
    expected_output_count: int,
) -> tuple[list, dict[str, int], dict[str, dict[str, int]]]:
    missing_required_field_count = 0
    invalid_enum_count = 0
    duplicate_key_count = max(0, len(docket_ids) - len(set(docket_ids)))
    anchor_missing_count = 0
    actual_output_count = 0
    distribution_counts: dict[str, int] = {}

    for docket_id in sorted(set(docket_ids)):
        output_path = build_output_path(output_root, docket_id)
        if output_path.exists():
            actual_output_count += 1
        else:
            missing_required_field_count += len(REQUIRED_OUTPUT_FIELDS)
            anchor_missing_count += 1
            continue

        with output_path.open("r", encoding="utf-8") as handle:
            payload = json.load(handle)

        for field in REQUIRED_OUTPUT_FIELDS:
            value = payload.get(field)
            if value in (None, ""):
                missing_required_field_count += 1

        inferred_document_type = str(payload.get("inferred_document_type") or "")
        if not inferred_document_type:
            invalid_enum_count += 1
        distribution_counts[inferred_document_type or "missing"] = (
            distribution_counts.get(inferred_document_type or "missing", 0) + 1
        )

        selected_document_id = str(payload.get("selected_document_id") or "")
        candidate_ids = {
            candidate.docket_item_id
            for candidate in grouped_candidates.get(docket_id, [])
        }
        if selected_document_id not in candidate_ids:
            anchor_missing_count += 1

    stage_rules = [
        rule(
            rule_id="narrative_output_count_matches_expected",
            severity="error",
            passed=actual_output_count == expected_output_count,
            detail=f"actual={actual_output_count} expected={expected_output_count}",
        ),
        rule(
            rule_id="narrative_required_fields_present",
            severity="error",
            passed=missing_required_field_count == 0,
            detail=f"missing_required_field_count={missing_required_field_count}",
        ),
        rule(
            rule_id="narrative_duplicate_keys_absent",
            severity="error",
            passed=duplicate_key_count == 0,
            detail=f"duplicate_key_count={duplicate_key_count}",
        ),
        rule(
            rule_id="narrative_anchor_exists_in_input_set",
            severity="error",
            passed=anchor_missing_count == 0,
            detail=f"anchor_missing_count={anchor_missing_count}",
        ),
    ]
    quality = {
        "missing_required_field_count": missing_required_field_count,
        "invalid_enum_count": invalid_enum_count,
        "duplicate_key_count": duplicate_key_count,
    }
    distribution = {
        "selected_document_type": dict(sorted(distribution_counts.items())),
    }
    return stage_rules, quality, distribution


def classify_failure(docket_id: str, message: str) -> StageFailure:
    return StageFailure(
        record_id=docket_id,
        artifact_id=docket_id,
        failure_code="narrative_selection_failed",
        failure_class="data_contract",
        message=message,
        retryable=False,
        blocked_output=True,
        source_locator=docket_id,
        exception_type="ValueError",
    )


def run_primary_docket_narrative_batch(config_path: Path | None = None) -> dict[str, int]:
    cfg = load_config(config_path)
    narrative_cfg = cfg["primary_docket_narrative"]
    input_root = resolve_path(narrative_cfg["input_root"])
    output_root = resolve_path(narrative_cfg["output_root"])
    overwrite_existing = bool(narrative_cfg.get("overwrite_existing", False))
    require_certified_input = bool(narrative_cfg.get("require_certified_input", True))

    runtime_parameters = {
        "input_root": str(input_root),
        "output_root": str(output_root),
        "overwrite_existing": overwrite_existing,
        "require_certified_input": require_certified_input,
    }
    run_context = create_stage_run_context(
        stage_name=STAGE_NAME,
        stage_version=STAGE_VERSION,
        output_root=output_root,
        config_payload=cfg,
        runtime_parameters=runtime_parameters,
    )

    summary = make_summary(dockets=0)
    failures: list[StageFailure] = []
    blocking_issues: list[str] = []
    effective_output_ids: list[str] = []
    input_entries: list[dict[str, str]] = []
    grouped_candidates: dict[str, list[NarrativeCandidate]] = {}
    records_seen = 0
    run_status = "completed"

    try:
        if not input_root.exists():
            raise FileNotFoundError(f"Primary narrative input directory not found: {input_root}")

        input_entries = build_input_entries(input_root)
        if require_certified_input:
            triage_root = input_root.parent
            assert_certified_input(
                triage_root,
                expected_stage_name="triage",
                current_primary_output_count=len(input_entries),
                current_primary_output_digest=build_logical_output_digest(input_entries),
            )

        grouped_candidates = group_candidates_by_docket(input_root)
        summary = make_summary(dockets=len(grouped_candidates))

        for docket_id in sorted(grouped_candidates):
            records_seen += 1
            try:
                output_path = build_output_path(output_root, docket_id)
                if not overwrite_existing and output_path.exists():
                    summary["selected"] += 1
                    summary["reused_existing"] += 1
                    effective_output_ids.append(docket_id)
                    continue

                print(f"[START] {docket_id}")
                candidate, selection_reason = select_primary_candidate(grouped_candidates[docket_id])
                payload = build_selection_payload(candidate, selection_reason=selection_reason)
                write_json_artifact(output_path, payload, overwrite_existing=overwrite_existing)
                summary["selected"] += 1
                effective_output_ids.append(docket_id)
            except Exception as exc:
                emit_error(docket_id, str(exc))
                summary["failed"] += 1
                failures.append(classify_failure(docket_id, str(exc)))
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
        grouped_candidates=grouped_candidates,
        expected_output_count=summary["selected"],
    )
    finalize_stage_run(
        run_context,
        upstream_stage="triage",
        input_manifest_path=input_manifest_path,
        input_record_count=input_record_count,
        input_digest=input_digest,
        primary_output_count=len(output_entries),
        primary_output_digest=build_logical_output_digest(output_entries),
        records_seen=records_seen,
        records_succeeded=summary["selected"] - summary["reused_existing"],
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
