from __future__ import annotations

import json
from pathlib import Path

import yaml

from incident_pipeline.narrative.primary_docket_narrative import (
    NarrativeCandidate,
    run_primary_docket_narrative_batch,
    select_primary_candidate,
)


def write_config(
    config_path: Path,
    input_root: Path,
    output_root: Path,
    *,
    overwrite_existing: bool,
) -> None:
    config = {
        "primary_docket_narrative": {
            "input_root": str(input_root),
            "output_root": str(output_root),
            "overwrite_existing": overwrite_existing,
        }
    }
    config_path.write_text(yaml.safe_dump(config), encoding="utf-8")


def make_candidate(
    *,
    docket_id: str,
    docket_item_id: str,
    inferred_document_type: str,
    title: str,
    source_text_path: Path | None = None,
) -> NarrativeCandidate:
    return NarrativeCandidate(
        docket_id=docket_id,
        docket_item_id=docket_item_id,
        inferred_document_type=inferred_document_type,
        title=title,
        source_text_path=source_text_path,
    )


def write_triage_artifact(
    input_root: Path,
    *,
    docket_item_id: str,
    inferred_document_type: str,
    title: str,
    source_text_path: Path | None = None,
) -> Path:
    input_root.mkdir(parents=True, exist_ok=True)
    payload = {
        "docket_item_id": docket_item_id,
        "inferred_document_type": inferred_document_type,
        "title": title,
    }
    if source_text_path is not None:
        payload["source_text_path"] = str(source_text_path)

    artifact_path = input_root / f"{docket_item_id}.json"
    artifact_path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    return artifact_path


def write_text(path: Path, text: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")
    return path


def test_select_primary_candidate_prefers_investigation_then_factual() -> None:
    candidates = [
        make_candidate(
            docket_id="DCA00FP008",
            docket_item_id="ntsb:docket_item:DCA00FP008:1:unknown",
            inferred_document_type="unknown",
            title="Unknown Notes",
        ),
        make_candidate(
            docket_id="DCA00FP008",
            docket_item_id="ntsb:docket_item:DCA00FP008:2:factual",
            inferred_document_type="factual_report",
            title="Operations Group - Factual Report",
        ),
        make_candidate(
            docket_id="DCA00FP008",
            docket_item_id="ntsb:docket_item:DCA00FP008:3:investigation",
            inferred_document_type="investigation_report",
            title="Pipeline Investigation Report PIR-22/03",
        ),
    ]

    selected, selection_reason = select_primary_candidate(candidates)

    assert selected.docket_item_id == "ntsb:docket_item:DCA00FP008:3:investigation"
    assert selected.inferred_document_type == "investigation_report"
    assert selection_reason == "selected highest-priority document_type investigation_report"


def test_select_primary_candidate_falls_back_to_longest_title() -> None:
    candidates = [
        make_candidate(
            docket_id="DCA00FP009",
            docket_item_id="ntsb:docket_item:DCA00FP009:1:short",
            inferred_document_type="unknown",
            title="Short",
        ),
        make_candidate(
            docket_id="DCA00FP009",
            docket_item_id="ntsb:docket_item:DCA00FP009:2:long",
            inferred_document_type="regulatory_report",
            title="Longest Title Available For This Docket",
        ),
    ]

    selected, selection_reason = select_primary_candidate(candidates)

    assert selected.docket_item_id == "ntsb:docket_item:DCA00FP009:2:long"
    assert selected.inferred_document_type == "regulatory_report"
    assert selection_reason == "selected fallback longest title"


def test_select_primary_candidate_falls_back_to_available_text(tmp_path: Path) -> None:
    text_path = write_text(tmp_path / "texts" / "candidate.txt", "Available fallback text")
    candidates = [
        make_candidate(
            docket_id="DCA00FP010",
            docket_item_id="ntsb:docket_item:DCA00FP010:1:blank",
            inferred_document_type="unknown",
            title="",
        ),
        make_candidate(
            docket_id="DCA00FP010",
            docket_item_id="ntsb:docket_item:DCA00FP010:2:text",
            inferred_document_type="external_reference",
            title="",
            source_text_path=text_path,
        ),
    ]

    selected, selection_reason = select_primary_candidate(candidates)

    assert selected.docket_item_id == "ntsb:docket_item:DCA00FP010:2:text"
    assert selected.inferred_document_type == "external_reference"
    assert selection_reason == "selected fallback available text"


def test_run_primary_docket_narrative_batch_writes_one_selection_per_docket_and_reuses_existing(
    tmp_path: Path,
) -> None:
    input_root = tmp_path / "triage" / "document_types"
    output_root = tmp_path / "triage" / "primary_docket_narratives"
    text_root = tmp_path / "ingestion" / "extracted"
    config_path = tmp_path / "settings.yaml"

    write_text(text_root / "ntsb:docket_item:DCA00FP011:2:text.txt", "Available narrative text")

    write_triage_artifact(
        input_root,
        docket_item_id="ntsb:docket_item:DCA00FP008:1:factual",
        inferred_document_type="factual_report",
        title="Operations Group - Factual Report",
    )
    write_triage_artifact(
        input_root,
        docket_item_id="ntsb:docket_item:DCA00FP008:2:investigation",
        inferred_document_type="investigation_report",
        title="Pipeline Investigation Report PIR-22/03",
    )
    write_triage_artifact(
        input_root,
        docket_item_id="ntsb:docket_item:DCA00FP009:1:unknown",
        inferred_document_type="unknown",
        title="Short",
    )
    write_triage_artifact(
        input_root,
        docket_item_id="ntsb:docket_item:DCA00FP009:2:long",
        inferred_document_type="regulatory_report",
        title="Longest Title Available For This Docket",
    )
    write_triage_artifact(
        input_root,
        docket_item_id="ntsb:docket_item:DCA00FP010:1:regulatory",
        inferred_document_type="regulatory_report",
        title="Texas Railroad Commission Report",
    )
    write_triage_artifact(
        input_root,
        docket_item_id="ntsb:docket_item:DCA00FP010:2:factual",
        inferred_document_type="factual_report",
        title="Operations Group - Factual Report",
    )
    write_triage_artifact(
        input_root,
        docket_item_id="ntsb:docket_item:DCA00FP011:1:blank",
        inferred_document_type="unknown",
        title="",
    )
    write_triage_artifact(
        input_root,
        docket_item_id="ntsb:docket_item:DCA00FP011:2:text",
        inferred_document_type="external_reference",
        title="",
        source_text_path=text_root / "ntsb:docket_item:DCA00FP011:2:text.txt",
    )

    write_config(config_path, input_root, output_root, overwrite_existing=False)

    first_summary = run_primary_docket_narrative_batch(config_path)

    assert first_summary == {
        "dockets": 4,
        "selected": 4,
        "reused_existing": 0,
        "failed": 0,
    }

    output_paths = sorted(output_root.glob("*.json"))
    assert [path.stem for path in output_paths] == [
        "DCA00FP008",
        "DCA00FP009",
        "DCA00FP010",
        "DCA00FP011",
    ]

    assert json.loads((output_root / "DCA00FP008.json").read_text(encoding="utf-8")) == {
        "docket_id": "DCA00FP008",
        "selected_document_id": "ntsb:docket_item:DCA00FP008:2:investigation",
        "inferred_document_type": "investigation_report",
        "selection_reason": "selected highest-priority document_type investigation_report",
    }
    assert json.loads((output_root / "DCA00FP009.json").read_text(encoding="utf-8")) == {
        "docket_id": "DCA00FP009",
        "selected_document_id": "ntsb:docket_item:DCA00FP009:2:long",
        "inferred_document_type": "regulatory_report",
        "selection_reason": "selected fallback longest title",
    }
    assert json.loads((output_root / "DCA00FP010.json").read_text(encoding="utf-8")) == {
        "docket_id": "DCA00FP010",
        "selected_document_id": "ntsb:docket_item:DCA00FP010:2:factual",
        "inferred_document_type": "factual_report",
        "selection_reason": "selected highest-priority document_type factual_report",
    }
    assert json.loads((output_root / "DCA00FP011.json").read_text(encoding="utf-8")) == {
        "docket_id": "DCA00FP011",
        "selected_document_id": "ntsb:docket_item:DCA00FP011:2:text",
        "inferred_document_type": "external_reference",
        "selection_reason": "selected fallback available text",
    }

    second_summary = run_primary_docket_narrative_batch(config_path)

    assert second_summary == {
        "dockets": 4,
        "selected": 4,
        "reused_existing": 4,
        "failed": 0,
    }
