from __future__ import annotations

from dataclasses import dataclass, field
import json
import re
from pathlib import Path
from typing import Literal, Mapping


LineKind = Literal[
    "noise",
    "title",
    "section_header",
    "stop_header",
    "section_body",
    "unclassified",
]


UNCLASSIFIED_SECTION_KEY = "unclassified"

HEADING_TO_SECTION = {
    "ABSTRACT": "abstract",
    "SUMMARY": "summary",
    "SYNOPSIS": "abstract",
    "EXECUTIVE SUMMARY": "executive_summary",
    "WHAT HAPPENED": "what_happened",
    "WHAT WE FOUND": "what_we_found",
    "WHAT WE RECOMMENDED": "what_we_recommended",
    "FACTUAL INFORMATION": "factual_information",
    "HISTORY OF FLIGHT": "factual_information",
    "PERSONNEL INFORMATION": "factual_information",
    "AIRCRAFT INFORMATION": "factual_information",
    "METEOROLOGICAL INFORMATION": "factual_information",
    "AIRPORT INFORMATION": "factual_information",
    "AERODROME INFORMATION": "factual_information",
    "FLIGHT RECORDERS": "factual_information",
    "WRECKAGE AND IMPACT INFORMATION": "factual_information",
    "MEDICAL AND PATHOLOGICAL INFORMATION": "factual_information",
    "FIRE": "factual_information",
    "SURVIVAL ASPECTS": "factual_information",
    "TESTS AND RESEARCH": "factual_information",
    "ORGANIZATIONAL AND MANAGEMENT INFORMATION": "factual_information",
    "ADDITIONAL INFORMATION": "factual_information",
    "ANALYSIS": "analysis",
    "CONCLUSIONS": "conclusions",
    "FINDINGS": "findings",
    "PROBABLE CAUSE": "probable_cause",
    "PROBABLE CAUSE AND FINDINGS": "probable_cause",
    "SAFETY RECOMMENDATIONS": "recommendations",
    "RECOMMENDATIONS": "recommendations",
    "LESSONS LEARNED": "lessons_learned",
}

HEADING_VARIANTS_TO_SECTION = {
    "NEW RECOMMENDATIONS": "recommendations",
    "PREVIOUSLY ISSUED RECOMMENDATIONS": "recommendations",
    "PREVIOUSLY ISSUED SAFETY RECOMMENDATIONS": "recommendations",
    "PREVIOUSLY ISSUED RECOMMENDATION REITERATED IN THIS REPORT": "recommendations",
    "PREVIOUSLY ISSUED RECOMMENDATIONS REITERATED IN THIS REPORT": "recommendations",
}

SECTION_ORDER = (
    "abstract",
    "summary",
    "executive_summary",
    "what_happened",
    "what_we_found",
    "what_we_recommended",
    "factual_information",
    "analysis",
    "conclusions",
    "findings",
    "probable_cause",
    "recommendations",
    "lessons_learned",
)

STOP_HEADING_MARKERS = {
    "BY THE NATIONAL TRANSPORTATION SAFETY BOARD",
    "APPENDIX",
    "APPENDIXES",
    "REFERENCES",
}

PROBABLE_CAUSE_HARD_STOP_PREFIXES = (
    "FOR MORE DETAILS ABOUT THIS ACCIDENT",
    "THE NTSB HAS AUTHORITY TO INVESTIGATE",
    "THE NTSB IS AN INDEPENDENT FEDERAL AGENCY",
    "THE NATIONAL TRANSPORTATION SAFETY BOARD NTSB IS AN INDEPENDENT FEDERAL AGENCY",
)

__all__ = [
    "LineAssignment",
    "SectionChunk",
    "StructureParseResult",
    "parse_structure_document",
    "parse_structure_text",
    "render_sections",
    "select_probable_cause",
    "select_recommendations",
    "serialize_trace",
    "write_trace_artifact",
]

_SEPARATOR_RE = re.compile(r"^[\s\-_=*•·.]+$")
_PAGE_NUMBER_RE = re.compile(r"^(?:PAGE\s+)?\(?\d+\)?$")
_RUNNING_HEADER_FOOTER_RE = re.compile(
    r"^(?:NTSB\s+)?(?:PIPELINE|AVIATION|HIGHWAY|RAILROAD|MARINE)\s+"
    r"(?:ACCIDENT|INVESTIGATION)\s+REPORT"
    r"(?:\s+REPORT\s+NUMBER\s+[A-Z0-9./_-]+)?"
    r"(?:\s+\d+)?$"
)
_CORRECTED_COPY_RE = re.compile(r"^CORRECTED\s+COPY$")
_REPORT_ID_ONLY_RE = re.compile(r"^(?:PAB|PAR|PIR|PSR)\d{4,5}$")


def normalize_heading(line: str) -> str:
    collapsed = re.sub(r"\s+", " ", line.strip())
    without_numbering = re.sub(r"^\d+(?:\.\d+)*[.)]?\s*", "", collapsed)
    cleaned = without_numbering.strip(":- ")
    normalized = re.sub(r"[^A-Z0-9 ]+", "", cleaned.upper())
    return re.sub(r"\s+", " ", normalized).strip()


def resolve_section_key(heading: str) -> str | None:
    normalized_heading = normalize_heading(heading)
    if normalized_heading in HEADING_TO_SECTION:
        return HEADING_TO_SECTION[normalized_heading]
    return HEADING_VARIANTS_TO_SECTION.get(normalized_heading)


def is_section_heading(line: str) -> bool:
    return resolve_section_key(line) is not None


def is_stop_heading(line: str) -> bool:
    normalized_heading = normalize_heading(line)
    if not normalized_heading:
        return False

    if normalized_heading in STOP_HEADING_MARKERS:
        return True

    return normalized_heading.startswith("APPENDIX ") or normalized_heading.startswith("APPENDIXES ")


def probable_cause_stop_prefix(line: str, next_line: str | None = None) -> str | None:
    candidates = [normalize_heading(line)]
    if next_line and next_line.strip():
        candidates.append(normalize_heading(f"{line.strip()} {next_line.strip()}"))

    for candidate in candidates:
        if not candidate:
            continue

        for prefix in PROBABLE_CAUSE_HARD_STOP_PREFIXES:
            if candidate.startswith(prefix):
                return prefix

    return None


def is_plausible_title(line: str) -> bool:
    candidate = line.strip()
    if not candidate or len(candidate) > 160:
        return False

    words = candidate.split()
    return 1 <= len(words) <= 20


def is_noise_line(line: str) -> bool:
    candidate = line.strip()
    if not candidate:
        return True

    normalized = normalize_heading(candidate)
    if not normalized:
        return True

    if _SEPARATOR_RE.fullmatch(candidate):
        return True

    if _PAGE_NUMBER_RE.fullmatch(normalized):
        return True

    if _CORRECTED_COPY_RE.fullmatch(normalized):
        return True

    if _RUNNING_HEADER_FOOTER_RE.fullmatch(normalized):
        return True

    return False


def probable_cause_noise_reason(line: str, *, title: str | None) -> str | None:
    candidate = line.strip()
    if not candidate:
        return None

    normalized = normalize_heading(candidate)
    if not normalized:
        return None

    normalized_title = normalize_heading(title or "")
    if normalized_title and len(normalized_title.split()) >= 3 and normalized == normalized_title:
        return "repeated document title bleed after probable cause"

    if _REPORT_ID_ONLY_RE.fullmatch(normalized):
        return "report-id running header bleed after probable cause"

    return None


def find_first_section_header_index(lines: list[str]) -> int | None:
    return next((index for index, line in enumerate(lines) if is_section_heading(line)), None)


def find_title_line_index(lines: list[str]) -> int | None:
    for index, line in enumerate(lines):
        if not line.strip():
            continue

        if is_plausible_title(line):
            return index

        break

    return None


@dataclass(frozen=True)
class LineAssignment:
    line_index: int
    text: str
    kind: LineKind
    section_key: str | None
    reason: str
    active_section_before: str | None
    active_section_after: str | None

    def to_dict(self) -> dict[str, object]:
        return {
            "line_index": self.line_index,
            "text": self.text,
            "kind": self.kind,
            "section_key": self.section_key,
            "reason": self.reason,
            "active_section_before": self.active_section_before,
            "active_section_after": self.active_section_after,
        }


@dataclass(frozen=True)
class SectionChunk:
    section_key: str
    source_headings: list[str]
    header_line_indexes: list[int]
    body_line_indexes: list[int]
    start_line_index: int | None = None
    end_line_index: int | None = None

    def to_dict(self) -> dict[str, object]:
        payload: dict[str, object] = {
            "section_key": self.section_key,
            "source_headings": list(self.source_headings),
            "header_line_indexes": list(self.header_line_indexes),
            "body_line_indexes": list(self.body_line_indexes),
        }
        if self.start_line_index is not None:
            payload["start_line_index"] = self.start_line_index
        if self.end_line_index is not None:
            payload["end_line_index"] = self.end_line_index
        return payload


@dataclass
class StructureParseResult:
    title: str | None
    title_line_index: int | None
    section_order: list[str] = field(default_factory=list)
    sections: list[SectionChunk] = field(default_factory=list)
    unclassified_line_indexes: list[int] = field(default_factory=list)
    line_assignments: list[LineAssignment] = field(default_factory=list)
    line_count: int = 0

    def to_trace_dict(self) -> dict[str, object]:
        return {
            "title_line_index": self.title_line_index,
            "section_order": list(self.section_order),
            "sections": [section.to_dict() for section in self.sections],
            "unclassified_line_indexes": list(self.unclassified_line_indexes),
            "line_assignments": [assignment.to_dict() for assignment in self.line_assignments],
        }

    def to_dict(self) -> dict[str, object]:
        return {
            "title": self.title,
            "title_line_index": self.title_line_index,
            "section_order": list(self.section_order),
            "sections": [section.to_dict() for section in self.sections],
            "unclassified_line_indexes": list(self.unclassified_line_indexes),
            "line_assignments": [assignment.to_dict() for assignment in self.line_assignments],
            "line_count": self.line_count,
        }

    def to_json(self, *, indent: int = 2) -> str:
        return json.dumps(self.to_dict(), indent=indent, ensure_ascii=True)


@dataclass
class _SectionChunkBuilder:
    section_key: str
    source_headings: list[str] = field(default_factory=list)
    header_line_indexes: list[int] = field(default_factory=list)
    body_line_indexes: list[int] = field(default_factory=list)

    def add_header(self, heading: str, line_index: int) -> None:
        self.source_headings.append(heading.strip())
        self.header_line_indexes.append(line_index)

    def add_body_line(self, line_index: int) -> None:
        self.body_line_indexes.append(line_index)

    def finalize(self) -> SectionChunk:
        all_indexes = self.header_line_indexes + self.body_line_indexes
        start_line_index = min(all_indexes) if all_indexes else None
        end_line_index = max(all_indexes) if all_indexes else None
        return SectionChunk(
            section_key=self.section_key,
            source_headings=list(self.source_headings),
            header_line_indexes=list(self.header_line_indexes),
            body_line_indexes=list(self.body_line_indexes),
            start_line_index=start_line_index,
            end_line_index=end_line_index,
        )


def _section_sort_key(section_key: str) -> tuple[int, str]:
    try:
        return (SECTION_ORDER.index(section_key), section_key)
    except ValueError:
        return (len(SECTION_ORDER), section_key)


def _line_text(parse_result: StructureParseResult, line_index: int) -> str:
    return parse_result.line_assignments[line_index].text


def _render_chunk_text(parse_result: StructureParseResult, chunk: SectionChunk) -> str:
    body_text = "\n".join(
        _line_text(parse_result, index).rstrip() for index in chunk.body_line_indexes
    ).strip()
    if chunk.section_key == "factual_information" and chunk.source_headings:
        heading_labels = [
            heading
            for heading in chunk.source_headings
            if normalize_heading(heading) != "FACTUAL INFORMATION"
        ]
        if heading_labels:
            heading_text = "\n".join(label.strip() for label in heading_labels if label.strip())
            if body_text:
                return f"{heading_text}\n{body_text}"
            return heading_text

    return body_text


def render_sections(parse_result: StructureParseResult) -> dict[str, str]:
    rendered_chunks: dict[str, list[str]] = {}
    for chunk in parse_result.sections:
        rendered_chunks.setdefault(chunk.section_key, [])
        rendered_chunks[chunk.section_key].append(_render_chunk_text(parse_result, chunk))

    present_keys = set(rendered_chunks)
    ordered_keys: list[str] = [section_key for section_key in SECTION_ORDER if section_key in present_keys]
    seen_keys: set[str] = set(ordered_keys)

    for section_key in parse_result.section_order:
        if section_key in seen_keys or section_key in SECTION_ORDER:
            continue
        ordered_keys.append(section_key)
        seen_keys.add(section_key)

    for section_key in sorted(rendered_chunks):
        if section_key not in seen_keys:
            ordered_keys.append(section_key)
            seen_keys.add(section_key)

    ordered_sections: dict[str, str] = {}
    for section_key in ordered_keys:
        chunk_texts = [chunk for chunk in rendered_chunks.get(section_key, []) if chunk is not None]
        if not chunk_texts:
            continue

        non_empty_chunks = [chunk for chunk in chunk_texts if chunk]
        if section_key == "probable_cause" and non_empty_chunks:
            ordered_sections[section_key] = non_empty_chunks[-1]
        else:
            ordered_sections[section_key] = "\n\n".join(non_empty_chunks) if non_empty_chunks else ""

    return ordered_sections


def select_probable_cause(rendered_sections: Mapping[str, str]) -> str | None:
    value = rendered_sections.get("probable_cause")
    if value is None:
        return None

    cleaned = value.strip()
    return cleaned or None


def select_recommendations(rendered_sections: Mapping[str, str]) -> str | None:
    for section_key in ("recommendations", "what_we_recommended"):
        value = rendered_sections.get(section_key)
        if value is None:
            continue

        cleaned = value.strip()
        if cleaned:
            return cleaned

    return None


def _append_unclassified(result: StructureParseResult, line_index: int) -> None:
    result.unclassified_line_indexes.append(line_index)


def parse_structure_document(text: str) -> StructureParseResult:
    lines = text.splitlines()
    first_section_index = find_first_section_header_index(lines)
    prefix_lines = lines[:first_section_index] if first_section_index is not None else lines
    title_line_offset = find_title_line_index(prefix_lines)
    title_line_index = title_line_offset
    title = prefix_lines[title_line_offset].strip() if title_line_offset is not None else None

    result = StructureParseResult(
        title=title,
        title_line_index=title_line_index,
        line_count=len(lines),
    )

    active_chunk: _SectionChunkBuilder | None = None
    active_section_key: str | None = None
    blocked_by_stop_heading = False
    seen_first_section_header = False
    abstract_chunk: _SectionChunkBuilder | None = None

    def finalize_active_chunk() -> None:
        nonlocal active_chunk, active_section_key
        if active_chunk is None:
            active_section_key = None
            return

        result.sections.append(active_chunk.finalize())
        active_chunk = None
        active_section_key = None

    def open_chunk(section_key: str) -> None:
        nonlocal active_chunk, active_section_key
        active_chunk = _SectionChunkBuilder(section_key=section_key)
        active_section_key = section_key
        result.section_order.append(section_key)

    def ensure_abstract_chunk() -> None:
        nonlocal abstract_chunk, active_chunk, active_section_key
        if abstract_chunk is not None:
            return

        abstract_chunk = _SectionChunkBuilder(section_key="abstract")
        active_chunk = abstract_chunk
        active_section_key = "abstract"
        result.section_order.append("abstract")

    for line_index, line in enumerate(lines):
        active_before = active_section_key

        if title_line_index is not None and line_index == title_line_index:
            result.line_assignments.append(
                LineAssignment(
                    line_index=line_index,
                    text=line,
                    kind="title",
                    section_key=UNCLASSIFIED_SECTION_KEY,
                    reason="selected as document title",
                    active_section_before=active_before,
                    active_section_after=active_section_key,
                )
            )
            continue

        if active_section_key == "probable_cause":
            probable_cause_noise = probable_cause_noise_reason(line, title=title)
            if probable_cause_noise is not None:
                result.line_assignments.append(
                    LineAssignment(
                        line_index=line_index,
                        text=line,
                        kind="noise",
                        section_key=None,
                        reason=probable_cause_noise,
                        active_section_before=active_before,
                        active_section_after=active_section_key,
                    )
                )
                continue

        if is_noise_line(line):
            result.line_assignments.append(
                LineAssignment(
                    line_index=line_index,
                    text=line,
                    kind="noise",
                    section_key=None,
                    reason="noise line",
                    active_section_before=active_before,
                    active_section_after=active_section_key,
                )
            )
            continue

        if active_section_key == "probable_cause":
            next_line = lines[line_index + 1] if line_index + 1 < len(lines) else None
            stop_prefix = probable_cause_stop_prefix(line, next_line)
            if stop_prefix is not None:
                finalize_active_chunk()
                blocked_by_stop_heading = True
                result.line_assignments.append(
                    LineAssignment(
                        line_index=line_index,
                        text=line,
                        kind="stop_header",
                        section_key=UNCLASSIFIED_SECTION_KEY,
                        reason=f"probable_cause hard stop -> {stop_prefix}",
                        active_section_before=active_before,
                        active_section_after=active_section_key,
                    )
                )
                _append_unclassified(result, line_index)
                continue

        if is_stop_heading(line):
            finalize_active_chunk()
            blocked_by_stop_heading = True
            result.line_assignments.append(
                LineAssignment(
                    line_index=line_index,
                    text=line,
                    kind="stop_header",
                    section_key=UNCLASSIFIED_SECTION_KEY,
                    reason=f"stop heading -> {normalize_heading(line)}",
                    active_section_before=active_before,
                    active_section_after=active_section_key,
                )
            )
            _append_unclassified(result, line_index)
            continue

        if is_section_heading(line):
            section_key = resolve_section_key(line)
            assert section_key is not None

            blocked_by_stop_heading = False
            finalize_active_chunk()

            open_chunk(section_key)
            active_chunk.add_header(line, line_index)
            seen_first_section_header = True

            result.line_assignments.append(
                LineAssignment(
                    line_index=line_index,
                    text=line,
                    kind="section_header",
                    section_key=section_key,
                    reason=f"recognized section header -> {section_key}",
                    active_section_before=active_before,
                    active_section_after=active_section_key,
                )
            )
            continue

        if blocked_by_stop_heading:
            result.line_assignments.append(
                LineAssignment(
                    line_index=line_index,
                    text=line,
                    kind="unclassified",
                    section_key=UNCLASSIFIED_SECTION_KEY,
                    reason="line after stop heading",
                    active_section_before=active_before,
                    active_section_after=active_section_key,
                )
            )
            _append_unclassified(result, line_index)
            continue

        if not seen_first_section_header:
            ensure_abstract_chunk()
            abstract_chunk.add_body_line(line_index)
            result.line_assignments.append(
                LineAssignment(
                    line_index=line_index,
                    text=line,
                    kind="section_body",
                    section_key="abstract",
                    reason="preamble line routed to synthetic abstract",
                    active_section_before=active_before,
                    active_section_after=active_section_key,
                )
            )
            continue

        if active_chunk is not None:
            active_chunk.add_body_line(line_index)
            result.line_assignments.append(
                LineAssignment(
                    line_index=line_index,
                    text=line,
                    kind="section_body",
                    section_key=active_section_key,
                    reason=f"body line within active section -> {active_section_key}",
                    active_section_before=active_before,
                    active_section_after=active_section_key,
                )
            )
            continue

        result.line_assignments.append(
            LineAssignment(
                line_index=line_index,
                text=line,
                kind="unclassified",
                section_key=UNCLASSIFIED_SECTION_KEY,
                reason="non-noise line outside an active section",
                active_section_before=active_before,
                active_section_after=active_section_key,
            )
        )
        _append_unclassified(result, line_index)

    if active_chunk is not None:
        result.sections.append(active_chunk.finalize())

    return result


parse_structure_text = parse_structure_document


def serialize_trace(parse_result: StructureParseResult, *, doc_id: str) -> dict[str, object]:
    return {
        "doc_id": doc_id,
        **parse_result.to_trace_dict(),
    }


def write_trace_artifact(
    output_path: Path,
    parse_result: StructureParseResult,
    *,
    doc_id: str,
    overwrite_existing: bool = True,
) -> bool:
    output_path.parent.mkdir(parents=True, exist_ok=True)

    if not overwrite_existing and output_path.exists():
        return True

    payload = serialize_trace(parse_result, doc_id=doc_id)
    output_path.write_text(json.dumps(payload, indent=2, ensure_ascii=True) + "\n", encoding="utf-8")
    return False
