from __future__ import annotations

import importlib
import json
from pathlib import Path

import pytest


FIXTURE_DIR = Path(__file__).resolve().parent / "fixtures" / "structure"
def load_parser_module():
    required_symbols = ("parse_structure_document", "render_sections", "serialize_trace")
    for module_name in (
        "incident_pipeline.extract.structure_state_machine",
        "incident_pipeline.extract.structure_extract",
    ):
        try:
            module = importlib.import_module(module_name)
        except Exception:
            continue

        if all(hasattr(module, name) for name in required_symbols):
            return module

    pytest.skip(
        "parser source does not yet expose parse_structure_document, render_sections, and serialize_trace",
        allow_module_level=True,
    )


parser = load_parser_module()

parse_structure_document = parser.parse_structure_document
render_sections = parser.render_sections
serialize_trace = parser.serialize_trace


def load_fixture(name: str) -> str:
    return (FIXTURE_DIR / name).read_text(encoding="utf-8")


def call_parse(text: str, *, doc_id: str | None = None):
    try:
        return parse_structure_document(text, doc_id=doc_id)
    except TypeError:
        try:
            return parse_structure_document(text, doc_id)
        except TypeError:
            return parse_structure_document(text)


def trace_get(trace, name: str):
    if isinstance(trace, dict):
        return trace[name]
    return getattr(trace, name)


def normalize_trace(trace, *, doc_id: str):
    try:
        serialized = serialize_trace(trace, doc_id=doc_id)
    except TypeError:
        serialized = serialize_trace(trace)
    if isinstance(serialized, str):
        serialized = json.loads(serialized)
    return serialized


def line_assignments(trace) -> list[dict]:
    return trace_get(trace, "line_assignments")


def trace_sections(trace) -> list[dict]:
    return trace_get(trace, "sections")


def trace_line_map(trace) -> dict[int, dict]:
    return {entry["line_index"]: entry for entry in line_assignments(trace)}


def assert_single_owner_trace(trace, source_text: str) -> None:
    lines = source_text.splitlines()
    assignments = line_assignments(trace)
    assert len(assignments) == len(lines)

    seen: set[int] = set()
    for entry in assignments:
        line_index = entry["line_index"]
        assert line_index not in seen
        seen.add(line_index)
        assert entry["text"] == lines[line_index]
        if entry["kind"] == "noise":
            assert entry["section_key"] is None
        else:
            assert entry["section_key"] in {section["section_key"] for section in trace_sections(trace)} | {"unclassified"}

    assert seen == set(range(len(lines)))


def assert_no_overlapping_section_ownership(trace) -> None:
    owned_indexes: set[int] = set()
    for section in trace_sections(trace):
        indexes = set(section["header_line_indexes"]) | set(section["body_line_indexes"])
        assert not indexes & owned_indexes
        owned_indexes |= indexes


def reconstruct_section(trace, section_key: str) -> str:
    chunks = [section for section in trace_sections(trace) if section["section_key"] == section_key]
    rendered_chunks: list[str] = []

    for chunk in chunks:
        rendered_lines: list[str] = []
        for body_index in chunk["body_line_indexes"]:
            rendered_lines.append(trace_line_map(trace)[body_index]["text"])
        rendered_chunks.append("\n".join(rendered_lines).strip())

    return "\n\n".join(part for part in rendered_chunks if part)


def test_single_owner_line_assignment_and_trace_serialization() -> None:
    source_text = load_fixture("pipeline_report.txt")
    parse_result = call_parse(source_text, doc_id="doc-pipeline")
    serialized = normalize_trace(parse_result, doc_id="doc-pipeline")
    rendered_sections = render_sections(parse_result)

    assert serialized["doc_id"] == "doc-pipeline"
    assert serialized["title_line_index"] == 0
    assert serialized["section_order"][:6] == [
        "abstract",
        "summary",
        "executive_summary",
        "what_happened",
        "what_we_found",
        "what_we_recommended",
    ]
    assert set(serialized) == {
        "doc_id",
        "title_line_index",
        "section_order",
        "sections",
        "unclassified_line_indexes",
        "line_assignments",
    }

    assert_single_owner_trace(serialized, source_text)
    assert_no_overlapping_section_ownership(serialized)
    assert serialized["unclassified_line_indexes"] == []

    assert rendered_sections["summary"] == "Short summary of the pipeline accident."
    assert rendered_sections["executive_summary"] == ""
    assert rendered_sections["what_happened"] == "Gas was released from the pipeline."
    assert rendered_sections["what_we_found"] == "Investigators found incomplete integrity management controls."
    assert rendered_sections["what_we_recommended"] == "Revise operator procedures."
    assert rendered_sections["factual_information"] == "Detailed factual record."
    assert rendered_sections["conclusions"] == "The board identified a maintenance program breakdown that was not corrected in time."
    assert rendered_sections["probable_cause"] == "Failure to address known geohazard threats."
    assert rendered_sections["lessons_learned"] == "Operators should evaluate geohazard exposure earlier."

    assert reconstruct_section(serialized, "summary") == rendered_sections["summary"]
    assert reconstruct_section(serialized, "factual_information") == rendered_sections["factual_information"]
    assert reconstruct_section(serialized, "probable_cause") == rendered_sections["probable_cause"]


def test_trace_separates_headers_from_body_lines() -> None:
    source_text = load_fixture("pipeline_report.txt")
    parse_result = call_parse(source_text, doc_id="doc-pipeline-2")
    trace = normalize_trace(parse_result, doc_id="doc-pipeline-2")
    rendered_sections = render_sections(parse_result)

    factual = next(section for section in trace_sections(trace) if section["section_key"] == "factual_information")
    probable_cause = next(section for section in trace_sections(trace) if section["section_key"] == "probable_cause")

    assert factual["header_line_indexes"]
    assert factual["body_line_indexes"]
    assert probable_cause["header_line_indexes"]
    assert probable_cause["body_line_indexes"]

    line_map = trace_line_map(trace)
    assert all(line_map[idx]["kind"] == "section_header" for idx in factual["header_line_indexes"])
    assert all(line_map[idx]["kind"] == "section_body" for idx in factual["body_line_indexes"])
    assert all(line_map[idx]["kind"] == "section_header" for idx in probable_cause["header_line_indexes"])
    assert all(line_map[idx]["kind"] == "section_body" for idx in probable_cause["body_line_indexes"])

    assert rendered_sections["factual_information"] == "Detailed factual record."


def test_probable_cause_stops_before_signoff() -> None:
    source_text = load_fixture("probable_cause_signoff.txt")
    parse_result = call_parse(source_text, doc_id="doc-pc-signoff")
    trace = normalize_trace(parse_result, doc_id="doc-pc-signoff")
    rendered_sections = render_sections(parse_result)

    assert rendered_sections["probable_cause"] == "A control-room monitoring failure allowed pressure to rise undetected."
    assert "BY THE NATIONAL TRANSPORTATION SAFETY BOARD" not in rendered_sections["probable_cause"]

    line_map = trace_line_map(trace)
    stop_index = next(
        idx for idx, entry in line_map.items() if entry["text"] == "BY THE NATIONAL TRANSPORTATION SAFETY BOARD"
    )
    assert line_map[stop_index]["kind"] == "stop_header"
    assert line_map[stop_index]["section_key"] == "unclassified"
    assert line_map[stop_index]["active_section_after"] is None
    assert all(
        entry["kind"] == "unclassified"
        for entry in line_assignments(trace)[stop_index + 1 :]
        if entry["text"].strip()
    )
    assert_single_owner_trace(trace, source_text)


@pytest.mark.parametrize(
    ("stop_line", "normalized_prefix"),
    [
        (
            "For more details about this accident, visit www.ntsb.gov.",
            "FOR MORE DETAILS ABOUT THIS ACCIDENT",
        ),
        (
            "The NTSB has authority to investigate pipeline accidents under federal law.",
            "THE NTSB HAS AUTHORITY TO INVESTIGATE",
        ),
        (
            "The NTSB is an independent federal agency charged by Congress with investigating transportation accidents.",
            "THE NTSB IS AN INDEPENDENT FEDERAL AGENCY",
        ),
        (
            "The National Transportation Safety Board (NTSB) is an independent federal agency charged by Congress with investigating transportation accidents.",
            "THE NATIONAL TRANSPORTATION SAFETY BOARD NTSB IS AN INDEPENDENT FEDERAL AGENCY",
        ),
    ],
)
def test_probable_cause_stops_on_approved_hard_stop_prefixes(
    stop_line: str,
    normalized_prefix: str,
) -> None:
    source_text = f"""Focused Pipeline Report

ABSTRACT
Brief overview of the event.

PROBABLE CAUSE
A failed valve released gas into the distribution line.
{stop_line}
Chairman
"""
    parse_result = call_parse(source_text, doc_id="doc-pc-prefix")
    trace = normalize_trace(parse_result, doc_id="doc-pc-prefix")
    rendered_sections = render_sections(parse_result)

    assert rendered_sections["probable_cause"] == "A failed valve released gas into the distribution line."
    assert stop_line not in rendered_sections["probable_cause"]

    line_map = trace_line_map(trace)
    stop_index = next(idx for idx, entry in line_map.items() if entry["text"] == stop_line)
    assert line_map[stop_index]["kind"] == "stop_header"
    assert line_map[stop_index]["section_key"] == "unclassified"
    assert line_map[stop_index]["reason"] == f"probable_cause hard stop -> {normalized_prefix}"
    assert line_map[stop_index]["active_section_after"] is None
    assert all(
        entry["kind"] == "unclassified"
        for entry in line_assignments(trace)[stop_index + 1 :]
        if entry["text"].strip()
    )
    assert_single_owner_trace(trace, source_text)


def test_probable_cause_ignores_repeated_title_and_report_id_bleed() -> None:
    source_text = """Marathon Pipe Line LLC Rupture and Release

ABSTRACT
Brief overview of the event.

PROBABLE CAUSE
Failure to address known geohazard threats.
Marathon Pipe Line LLC Rupture and Release
PIR-23/02
The NTSB is an independent federal agency charged by Congress with investigating transportation accidents.
Chairman
"""
    parse_result = call_parse(source_text, doc_id="doc-pc-noise")
    trace = normalize_trace(parse_result, doc_id="doc-pc-noise")
    rendered_sections = render_sections(parse_result)

    assert rendered_sections["probable_cause"] == "Failure to address known geohazard threats."
    assert "Marathon Pipe Line LLC Rupture and Release" not in rendered_sections["probable_cause"]
    assert "PIR-23/02" not in rendered_sections["probable_cause"]

    line_map = trace_line_map(trace)
    repeated_title_index = next(
        idx
        for idx, entry in line_map.items()
        if entry["text"] == "Marathon Pipe Line LLC Rupture and Release" and idx != trace["title_line_index"]
    )
    report_id_index = next(idx for idx, entry in line_map.items() if entry["text"] == "PIR-23/02")
    stop_index = next(
        idx
        for idx, entry in line_map.items()
        if entry["text"]
        == "The NTSB is an independent federal agency charged by Congress with investigating transportation accidents."
    )

    assert line_map[repeated_title_index]["kind"] == "noise"
    assert line_map[repeated_title_index]["reason"] == "repeated document title bleed after probable cause"
    assert line_map[report_id_index]["kind"] == "noise"
    assert line_map[report_id_index]["reason"] == "report-id running header bleed after probable cause"
    assert line_map[stop_index]["kind"] == "stop_header"
    assert_single_owner_trace(trace, source_text)


def test_probable_cause_stops_on_wrapped_approved_prefix() -> None:
    source_text = """Focused Pipeline Report

ABSTRACT
Brief overview of the event.

PROBABLE CAUSE
A failed valve released gas into the distribution line.
The National Transportation Safety Board (NTSB) is an independent federal
agency charged by Congress with investigating transportation accidents.
Chairman
"""
    parse_result = call_parse(source_text, doc_id="doc-pc-wrapped-prefix")
    trace = normalize_trace(parse_result, doc_id="doc-pc-wrapped-prefix")
    rendered_sections = render_sections(parse_result)

    assert rendered_sections["probable_cause"] == "A failed valve released gas into the distribution line."
    assert "The National Transportation Safety Board (NTSB) is an independent federal" not in rendered_sections["probable_cause"]
    assert "agency charged by Congress with investigating transportation accidents." not in rendered_sections["probable_cause"]

    line_map = trace_line_map(trace)
    stop_index = next(
        idx
        for idx, entry in line_map.items()
        if entry["text"] == "The National Transportation Safety Board (NTSB) is an independent federal"
    )
    wrapped_continuation_index = stop_index + 1

    assert line_map[stop_index]["kind"] == "stop_header"
    assert line_map[stop_index]["reason"] == (
        "probable_cause hard stop -> THE NATIONAL TRANSPORTATION SAFETY BOARD NTSB IS AN INDEPENDENT FEDERAL AGENCY"
    )
    assert line_map[wrapped_continuation_index]["kind"] == "unclassified"
    assert line_map[wrapped_continuation_index]["section_key"] == "unclassified"
    assert_single_owner_trace(trace, source_text)


def test_recommendations_stop_before_appendix_and_references() -> None:
    source_text = load_fixture("recommendations_appendix.txt")
    parse_result = call_parse(source_text, doc_id="doc-rec-appendix")
    trace = normalize_trace(parse_result, doc_id="doc-rec-appendix")
    rendered_sections = render_sections(parse_result)

    assert rendered_sections["recommendations"] == "Inspect similar valves within 30 days."
    assert "Appendix A: Investigation" not in rendered_sections["recommendations"]
    assert "Reference list." not in rendered_sections["recommendations"]

    line_map = trace_line_map(trace)
    appendix_index = next(idx for idx, entry in line_map.items() if entry["text"] == "Appendix A: Investigation")
    references_index = next(idx for idx, entry in line_map.items() if entry["text"] == "References")

    assert line_map[appendix_index]["kind"] == "stop_header"
    assert line_map[references_index]["kind"] == "stop_header"
    assert line_map[appendix_index]["section_key"] == "unclassified"
    assert line_map[references_index]["section_key"] == "unclassified"
    assert all(
        entry["kind"] in {"stop_header", "unclassified"}
        for entry in line_assignments(trace)[appendix_index + 1 :]
        if entry["text"].strip()
    )
    assert_single_owner_trace(trace, source_text)


def test_numbered_heading_recognition_and_section_reconstruction() -> None:
    source_text = load_fixture("pipeline_report.txt")
    parse_result = call_parse(source_text, doc_id="doc-numbered")
    trace = normalize_trace(parse_result, doc_id="doc-numbered")
    rendered_sections = render_sections(parse_result)

    assert trace_get(trace, "section_order")[:6] == [
        "abstract",
        "summary",
        "executive_summary",
        "what_happened",
        "what_we_found",
        "what_we_recommended",
    ]
    assert rendered_sections["analysis"] == "Analysis of the pipeline failure."
    assert rendered_sections["findings"] == "Maintenance tracking was incomplete."
    assert rendered_sections["probable_cause"] == "Failure to address known geohazard threats."
    assert rendered_sections["lessons_learned"] == "Operators should evaluate geohazard exposure earlier."

    assert reconstruct_section(trace, "analysis") == rendered_sections["analysis"]
    assert reconstruct_section(trace, "findings") == rendered_sections["findings"]
    assert reconstruct_section(trace, "probable_cause") == rendered_sections["probable_cause"]
    assert_single_owner_trace(trace, source_text)


def test_only_last_non_empty_probable_cause_chunk_is_rendered() -> None:
    source_text = """Focused Accident Report

ABSTRACT
Brief overview of the event.

PROBABLE CAUSE
Early summary probable cause statement.

ANALYSIS
Investigators reviewed the failed monitoring response.

3.2 Probable Cause
Final probable cause statement.
"""
    parse_result = call_parse(source_text, doc_id="doc-pc-last-chunk")
    trace = normalize_trace(parse_result, doc_id="doc-pc-last-chunk")
    rendered_sections = render_sections(parse_result)

    probable_cause_chunks = [
        section for section in trace_sections(trace) if section["section_key"] == "probable_cause"
    ]

    assert len(probable_cause_chunks) == 2
    assert reconstruct_section(trace, "probable_cause") == (
        "Early summary probable cause statement.\n\nFinal probable cause statement."
    )
    assert rendered_sections["probable_cause"] == "Final probable cause statement."
    assert_single_owner_trace(trace, source_text)


def test_recommendation_heading_variant_is_captured_as_recommendations() -> None:
    source_text = load_fixture("recommendation_variant.txt")
    parse_result = call_parse(source_text, doc_id="doc-variant")
    trace = normalize_trace(parse_result, doc_id="doc-variant")
    rendered_sections = render_sections(parse_result)

    assert "recommendations" in rendered_sections
    assert "Review emergency valve placement." in rendered_sections["recommendations"]
    assert "Update pressure monitoring procedures." in rendered_sections["recommendations"]
    assert rendered_sections["probable_cause"] == "Failure to monitor abnormal pressure conditions."
    assert "BY THE NATIONAL TRANSPORTATION SAFETY BOARD" not in rendered_sections["probable_cause"]
    assert reconstruct_section(trace, "recommendations") == rendered_sections["recommendations"]
    assert_single_owner_trace(trace, source_text)
