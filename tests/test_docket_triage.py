from __future__ import annotations

import json
from pathlib import Path

import pytest
import yaml

from incident_pipeline.triage.document_type import infer_document_type
from incident_pipeline.triage.docket_triage import run_docket_triage_batch


def write_config(
    config_path: Path,
    metadata_root: Path,
    text_root: Path,
    output_root: Path,
    *,
    overwrite_existing: bool,
    text_read_chars: int = 4000,
) -> None:
    config = {
        "docket_triage": {
            "metadata_root": str(metadata_root),
            "text_root": str(text_root),
            "output_root": str(output_root),
            "overwrite_existing": overwrite_existing,
            "text_read_chars": text_read_chars,
        }
    }
    config_path.write_text(yaml.safe_dump(config), encoding="utf-8")


def write_metadata(
    metadata_root: Path,
    *,
    docket_item_id: str,
    title: str,
    ntsb_number: str = "DCA00FP008",
    blob_sha256: str = "abc123",
) -> Path:
    metadata_root.mkdir(parents=True, exist_ok=True)
    payload = {
        "project_id": "53416",
        "ntsb_number": ntsb_number,
        "docket_item_id": docket_item_id,
        "ordinal": 1,
        "title": title,
        "view_url": "https://example.test/view",
        "source_url": "https://example.test/source",
        "blob_sha256": blob_sha256,
        "blob_path": "/tmp/blob.pdf",
        "media_type": "application/pdf",
    }
    metadata_path = metadata_root / f"{docket_item_id}.json"
    metadata_path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    return metadata_path


def write_text(text_root: Path, *, docket_item_id: str, text: str) -> Path:
    text_root.mkdir(parents=True, exist_ok=True)
    text_path = text_root / f"{docket_item_id}.txt"
    text_path.write_text(text, encoding="utf-8")
    return text_path


@pytest.mark.parametrize(
    ("title", "text_excerpt", "expected_type", "expected_basis"),
    [
        (
            "Preliminary Report",
            "",
            "preliminary_report",
            "title matched 'preliminary report'",
        ),
        (
            "Operations Group - Factual Report",
            "",
            "factual_report",
            "title matched 'factual report'",
        ),
        (
            "Materials Laboratory Factual Report",
            "",
            "materials_report",
            "title matched 'materials laboratory'",
        ),
        (
            "Emergency Response Group Chairman's Factual Report",
            "",
            "emergency_response_report",
            "title matched 'emergency response'",
        ),
        (
            "Transcript of Interview -- UGI -- Ops Mgr",
            "",
            "interview_transcript",
            "title matched 'interview'",
        ),
        (
            "Appendix H - Site Drawing",
            "",
            "attachment_appendix",
            "title matched 'appendix'",
        ),
        (
            "Final Accident Report NTSB/PAR-19/01",
            "",
            "investigation_report",
            "title matched 'final accident report'",
        ),
        (
            "Final Accident Report",
            "",
            "investigation_report",
            "title matched 'final accident report'",
        ),
        (
            "Corrected Final Report",
            "",
            "investigation_report",
            "title matched 'corrected final report'",
        ),
        (
            "NTSB PIR-22/03",
            "",
            "investigation_report",
            "title matched 'PIR-'",
        ),
        (
            "NTSB PAR-19/01",
            "",
            "investigation_report",
            "title matched 'PAR-'",
        ),
        (
            "Michigan Public Service Commission Report",
            "",
            "regulatory_report",
            "title matched 'public service commission report'",
        ),
        (
            "Texas Railroad Commission Final Report",
            "",
            "regulatory_report",
            "title matched 'railroad commission' + 'report'",
        ),
        (
            "PHMSA Field Report Excavation Section",
            "",
            "regulatory_report",
            "title matched 'phmsa field report'",
        ),
        (
            "NRC Report",
            "",
            "regulatory_report",
            "title matched 'nrc report'",
        ),
        (
            "National Response Center Report of Incident",
            "",
            "regulatory_report",
            "title matched 'national response center report'",
        ),
        (
            "Report to the Montana Governor by the Oil Pipeline Safety Review Council",
            "",
            "regulatory_report",
            "title matched 'governor' + 'review council' + 'report'",
        ),
        (
            "DOT Accident Report",
            "",
            "external_reference",
            "title matched 'accident report'",
        ),
        (
            "Daily Activity Sheet",
            "",
            "unknown",
            "fallback to unknown",
        ),
        (
            "Untitled",
            "MATERIALS LABORATORY FACTUAL REPORT",
            "materials_report",
            "text matched 'materials laboratory'",
        ),
        (
            "Reports from State Agencies",
            "Transcript of interview with field responder",
            "unknown",
            "fallback to unknown",
        ),
        (
            "OPS TASK FORCE REPORT-COLONIAL PIPELINE FATIGUE FAILURES-1990",
            "Attachment 4 pipeline drawing and supporting notes",
            "unknown",
            "fallback to unknown",
        ),
        (
            "PSE Consultant Summary Report",
            "Appendix A consultant notes",
            "unknown",
            "fallback to unknown",
        ),
        (
            "SCADA Factual Report Reference Material #8 (Interview - Enterprise Controller)",
            "",
            "factual_report",
            "title matched 'factual report'",
        ),
        (
            "Unknown Working Notes",
            "Transcript of interview with operations manager",
            "unknown",
            "fallback to unknown",
        ),
        (
            "State Agency Correspondence",
            "",
            "unknown",
            "fallback to unknown",
        ),
    ],
)
def test_infer_document_type(title: str, text_excerpt: str, expected_type: str, expected_basis: str) -> None:
    inference = infer_document_type(title, text_excerpt)

    assert inference.inferred_document_type == expected_type
    assert inference.inference_basis == expected_basis


def test_investigation_report_precedence_over_regulatory_cues() -> None:
    inference = infer_document_type(
        "Texas Railroad Commission Investigation Report",
        "",
    )

    assert inference.inferred_document_type == "investigation_report"
    assert inference.inference_basis == "title matched 'investigation report'"


def test_interview_transcript_is_not_overridden_by_investigation_heuristics() -> None:
    inference = infer_document_type(
        "Transcript of Interview - Final Report Discussion",
        "",
    )

    assert inference.inferred_document_type == "interview_transcript"
    assert inference.inference_basis == "title matched 'interview'"


def test_factual_report_does_not_become_investigation_report() -> None:
    inference = infer_document_type(
        "Operations Group Final Factual Report",
        "",
    )

    assert inference.inferred_document_type == "factual_report"
    assert inference.inference_basis == "title matched 'factual report'"


def test_investigation_report_wins_over_attachment_cues() -> None:
    inference = infer_document_type(
        "Appendix A - Pipeline Investigation Report PIR-22/03",
        "",
    )

    assert inference.inferred_document_type == "investigation_report"
    assert inference.inference_basis == "title matched 'investigation report'"


def test_strong_title_match_wins_over_weaker_text_fallback() -> None:
    inference = infer_document_type(
        "Operations Group - Factual Report",
        "Appendix A interview transcript and exhibit list",
    )

    assert inference.inferred_document_type == "factual_report"
    assert inference.inference_basis == "title matched 'factual report'"


def test_run_docket_triage_batch_writes_sidecar_and_reuses_existing(tmp_path: Path) -> None:
    metadata_root = tmp_path / "metadata"
    text_root = tmp_path / "extracted"
    output_root = tmp_path / "triage" / "document_types"
    config_path = tmp_path / "settings.yaml"
    docket_item_id = "ntsb:docket_item:DCA00FP008:1:preliminary_report"

    metadata_path = write_metadata(
        metadata_root,
        docket_item_id=docket_item_id,
        title="Preliminary Report",
        blob_sha256="triage-sha",
    )
    text_path = write_text(
        text_root,
        docket_item_id=docket_item_id,
        text="PRELIMINARY REPORT\nOpening paragraph",
    )
    write_config(config_path, metadata_root, text_root, output_root, overwrite_existing=False)

    first_summary = run_docket_triage_batch(config_path)

    assert first_summary == {
        "selected": 1,
        "completed": 1,
        "reused_existing": 0,
        "failed": 0,
    }

    output_path = output_root / f"{docket_item_id}.json"
    payload = json.loads(output_path.read_text(encoding="utf-8"))

    assert payload == {
        "docket_item_id": docket_item_id,
        "ntsb_number": "DCA00FP008",
        "title": "Preliminary Report",
        "blob_sha256": "triage-sha",
        "inferred_document_type": "preliminary_report",
        "inference_basis": "title matched 'preliminary report'",
        "source_metadata_path": str(metadata_path),
        "source_text_path": str(text_path),
    }

    second_summary = run_docket_triage_batch(config_path)

    assert second_summary == {
        "selected": 1,
        "completed": 1,
        "reused_existing": 1,
        "failed": 0,
    }


def test_run_docket_triage_batch_reports_batch_counts_and_failures(
    tmp_path: Path, capsys
) -> None:
    metadata_root = tmp_path / "metadata"
    text_root = tmp_path / "extracted"
    output_root = tmp_path / "triage" / "document_types"
    config_path = tmp_path / "settings.yaml"

    completed_id = "ntsb:docket_item:DCA00FP008:1:factual_report"
    reused_id = "ntsb:docket_item:DCA00FP008:2:attachment"
    failed_id = "ntsb:docket_item:DCA00FP008:3:missing_text"

    write_metadata(metadata_root, docket_item_id=completed_id, title="Operations Group - Factual Report")
    write_text(text_root, docket_item_id=completed_id, text="Operations Group Chairman's Factual Report")

    write_metadata(metadata_root, docket_item_id=reused_id, title="Appendix H - Site Drawing")
    write_text(text_root, docket_item_id=reused_id, text="Appendix H")
    output_root.mkdir(parents=True, exist_ok=True)
    existing_output = output_root / f"{reused_id}.json"
    existing_output.write_text('{"status": "existing"}\n', encoding="utf-8")

    write_metadata(metadata_root, docket_item_id=failed_id, title="Unknown Document")

    write_config(config_path, metadata_root, text_root, output_root, overwrite_existing=False)

    summary = run_docket_triage_batch(config_path)
    output = capsys.readouterr().out

    assert summary == {
        "selected": 3,
        "completed": 2,
        "reused_existing": 1,
        "failed": 1,
    }
    assert existing_output.read_text(encoding="utf-8") == '{"status": "existing"}\n'
    assert f"[START] {completed_id}" in output
    assert (
        f"[ERROR] {failed_id}: Source text artifact not found: {text_root / f'{failed_id}.txt'}"
        in output
    )
