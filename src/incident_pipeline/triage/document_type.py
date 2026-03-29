from __future__ import annotations

from dataclasses import dataclass
import re


NORMALIZE_RE = re.compile(r"[^a-z0-9]+")
INVESTIGATION_CODE_PREFIX_RE = re.compile(r"\b(?P<code>pir|par)-", re.IGNORECASE)
REPORT_CODE_RE = re.compile(r"\b(?P<code>pir|par|pab|psr)[-/ ]\d", re.IGNORECASE)

ATTACHMENT_APPENDIX_CUES = (
    "appendix",
    "attachment",
    "exhibit",
)
INTERVIEW_TRANSCRIPT_CUES = (
    "interview",
    "transcript",
)
PRELIMINARY_REPORT_CUES = ("preliminary report",)
MATERIALS_REPORT_CUES = (
    "materials laboratory",
    "materials lab",
    "metallurgical",
)
EMERGENCY_RESPONSE_CUES = (
    "emergency response",
    "emergency preparedness",
)
FACTUAL_REPORT_CUES = ("factual report",)
INVESTIGATION_REPORT_CUES = (
    "corrected final report",
    "final accident report",
    "investigation report",
    "investigative report",
    "pipeline accident report",
)
REGULATORY_REPORT_CUES = (
    "public service commission report",
    "railroad commission report",
    "phmsa field report",
    "phmsa report",
    "nrc report",
    "national response center report",
    "incident notification report",
    "police report",
)
REGULATORY_REPORT_CUE_SETS = (
    ("railroad commission", "report"),
    ("state fire marshal", "report"),
    ("state fire marshall", "report"),
    ("governor", "review council", "report"),
)
INVESTIGATION_ACCIDENT_REPORT_CONTEXT_CUES = (
    "ntsb",
    "pipeline",
    "aviation",
)
NON_INVESTIGATION_ACCIDENT_REPORT_CUES = ("dot accident report",)
EXTERNAL_REFERENCE_CUES = (
    "reference material",
    "reference",
    "accident report",
    "incident report",
    "weather report",
    "manual",
    "procedure",
    "training",
    "inspection report",
    "reporting form",
    "cover letter",
    "letter",
    "memo",
    "email",
)
INTERVIEW_NEGATIVE_TITLE_CUES = (
    "reports from",
    "summary report",
    "analysis report",
    "commission report",
    "agency report",
)


@dataclass(frozen=True)
class DocumentTypeInference:
    inferred_document_type: str
    inference_basis: str


def normalize_text(value: str | None) -> str:
    if not value:
        return ""

    collapsed = NORMALIZE_RE.sub(" ", value.lower())
    return " ".join(collapsed.split())


def _match_phrase(normalized_text: str, phrases: tuple[str, ...]) -> str | None:
    for phrase in phrases:
        if phrase in normalized_text:
            return phrase
    return None


def _build_match(source: str, phrase: str, inferred_document_type: str) -> DocumentTypeInference:
    return DocumentTypeInference(
        inferred_document_type=inferred_document_type,
        inference_basis=f"{source} matched '{phrase}'",
    )


def _title_has_report_code(raw_text: str) -> str | None:
    match = REPORT_CODE_RE.search(raw_text)
    if not match:
        return None

    return match.group("code").upper()


def _title_has_investigation_code_prefix(raw_text: str) -> str | None:
    match = INVESTIGATION_CODE_PREFIX_RE.search(raw_text)
    if not match:
        return None

    return f"{match.group('code').upper()}-"


def _build_multi_match(
    source: str,
    phrases: tuple[str, ...],
    inferred_document_type: str,
) -> DocumentTypeInference:
    phrase_basis = " + ".join(f"'{phrase}'" for phrase in phrases)
    return DocumentTypeInference(
        inferred_document_type=inferred_document_type,
        inference_basis=f"{source} matched {phrase_basis}",
    )


def _match_investigation_report(
    normalized_text: str,
    raw_text: str,
    *,
    source: str,
    allow_report_code: bool,
) -> DocumentTypeInference | None:
    phrase = _match_phrase(normalized_text, INVESTIGATION_REPORT_CUES)
    if phrase:
        return _build_match(source, phrase, "investigation_report")

    if not allow_report_code:
        return None

    code_prefix = _title_has_investigation_code_prefix(raw_text)
    if code_prefix:
        return _build_match(source, code_prefix, "investigation_report")

    if "final report" in normalized_text and "factual report" not in normalized_text:
        regulatory_match = _match_regulatory_report(normalized_text, source=source)
        if not regulatory_match:
            return _build_match(source, "final report", "investigation_report")

    if (
        "accident report" in normalized_text
        and not _match_phrase(normalized_text, NON_INVESTIGATION_ACCIDENT_REPORT_CUES)
        and _match_phrase(normalized_text, INVESTIGATION_ACCIDENT_REPORT_CONTEXT_CUES)
    ):
        return _build_match(source, "accident report", "investigation_report")

    report_code = _title_has_report_code(raw_text)
    if not report_code:
        return None

    return DocumentTypeInference(
        inferred_document_type="investigation_report",
        inference_basis=f"{source} matched report code '{report_code}'",
    )


def _match_regulatory_report(
    normalized_text: str,
    *,
    source: str,
) -> DocumentTypeInference | None:
    phrase = _match_phrase(normalized_text, REGULATORY_REPORT_CUES)
    if phrase:
        return _build_match(source, phrase, "regulatory_report")

    for cue_set in REGULATORY_REPORT_CUE_SETS:
        if all(cue in normalized_text for cue in cue_set):
            return _build_multi_match(source, cue_set, "regulatory_report")

    return None


def _is_interview_title_guarded(normalized_text: str, raw_text: str) -> bool:
    if _match_phrase(normalized_text, INTERVIEW_NEGATIVE_TITLE_CUES):
        return True

    if _match_phrase(normalized_text, PRELIMINARY_REPORT_CUES):
        return True

    if _match_phrase(normalized_text, FACTUAL_REPORT_CUES):
        return True

    return False


def _match_title_type(
    normalized_text: str,
    raw_text: str,
) -> DocumentTypeInference | None:
    phrase = _match_phrase(normalized_text, PRELIMINARY_REPORT_CUES)
    if phrase:
        return _build_match("title", phrase, "preliminary_report")

    phrase = _match_phrase(normalized_text, MATERIALS_REPORT_CUES)
    if phrase:
        return _build_match("title", phrase, "materials_report")

    phrase = _match_phrase(normalized_text, EMERGENCY_RESPONSE_CUES)
    if phrase:
        return _build_match("title", phrase, "emergency_response_report")

    phrase = _match_phrase(normalized_text, FACTUAL_REPORT_CUES)
    if phrase:
        return _build_match("title", phrase, "factual_report")

    phrase = _match_phrase(normalized_text, INTERVIEW_TRANSCRIPT_CUES)
    if phrase and not _is_interview_title_guarded(normalized_text, raw_text):
        return _build_match("title", phrase, "interview_transcript")

    investigation_match = _match_investigation_report(
        normalized_text,
        raw_text,
        source="title",
        allow_report_code=True,
    )
    if investigation_match:
        return investigation_match

    phrase = _match_phrase(normalized_text, ATTACHMENT_APPENDIX_CUES)
    if phrase:
        return _build_match("title", phrase, "attachment_appendix")

    regulatory_match = _match_regulatory_report(normalized_text, source="title")
    if regulatory_match:
        return regulatory_match

    phrase = _match_phrase(normalized_text, EXTERNAL_REFERENCE_CUES)
    if phrase:
        return _build_match("title", phrase, "external_reference")

    return None


def _match_text_fallback(
    normalized_text: str,
    raw_text: str,
) -> DocumentTypeInference | None:
    phrase = _match_phrase(normalized_text, PRELIMINARY_REPORT_CUES)
    if phrase:
        return _build_match("text", phrase, "preliminary_report")

    phrase = _match_phrase(normalized_text, MATERIALS_REPORT_CUES)
    if phrase:
        return _build_match("text", phrase, "materials_report")

    phrase = _match_phrase(normalized_text, EMERGENCY_RESPONSE_CUES)
    if phrase:
        return _build_match("text", phrase, "emergency_response_report")

    phrase = _match_phrase(normalized_text, FACTUAL_REPORT_CUES)
    if phrase:
        return _build_match("text", phrase, "factual_report")

    return _match_investigation_report(
        normalized_text,
        raw_text,
        source="text",
        allow_report_code=False,
    )


def infer_document_type(title: str, text_excerpt: str | None = None) -> DocumentTypeInference:
    normalized_title = normalize_text(title)
    title_match = _match_title_type(normalized_title, title)
    if title_match:
        return title_match

    normalized_excerpt = normalize_text(text_excerpt)
    text_match = _match_text_fallback(normalized_excerpt, text_excerpt or "")
    if text_match:
        return text_match

    return DocumentTypeInference(
        inferred_document_type="unknown",
        inference_basis="fallback to unknown",
    )
