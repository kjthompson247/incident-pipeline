from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path

from incident_pipeline.triage.docket_triage import load_config, resolve_path, write_json_artifact


TYPE_PRECEDENCE = (
    "investigation_report",
    "factual_report",
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


def run_primary_docket_narrative_batch(config_path: Path | None = None) -> dict[str, int]:
    cfg = load_config(config_path)
    narrative_cfg = cfg["primary_docket_narrative"]
    input_root = resolve_path(narrative_cfg["input_root"])
    output_root = resolve_path(narrative_cfg["output_root"])
    overwrite_existing = bool(narrative_cfg.get("overwrite_existing", False))

    if not input_root.exists():
        raise FileNotFoundError(f"Primary narrative input directory not found: {input_root}")

    grouped_candidates = group_candidates_by_docket(input_root)
    summary = make_summary(dockets=len(grouped_candidates))

    for docket_id in sorted(grouped_candidates):
        try:
            output_path = build_output_path(output_root, docket_id)
            if not overwrite_existing and output_path.exists():
                summary["selected"] += 1
                summary["reused_existing"] += 1
                continue

            print(f"[START] {docket_id}")
            candidate, selection_reason = select_primary_candidate(grouped_candidates[docket_id])
            payload = build_selection_payload(candidate, selection_reason=selection_reason)
            write_json_artifact(output_path, payload, overwrite_existing=overwrite_existing)
            summary["selected"] += 1
        except Exception as exc:
            emit_error(docket_id, str(exc))
            summary["failed"] += 1

    return summary
