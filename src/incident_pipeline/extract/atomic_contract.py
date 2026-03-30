from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any


MODEL_CONTRACT_VERSION = "sentence-to-claims-v1"
ONTOLOGY_VERSION = "claims-ontology-v1"
MAPPING_CONTRACT_VERSION = "claim-mapping-v1"
PROMPT_VERSION = "claim-inference-v1"

STATUS_VALUES = {"ok", "unprocessable"}
ASSERTION_MODE_VALUES = {"stated", "observed", "concluded", "recommended"}
POLARITY_VALUES = {"affirmed", "negated", "uncertain"}
PREDICATE_STATUS_VALUES = {"core", "candidate", "unresolved"}
CORE_PREDICATES = {
    "exists",
    "has",
    "located_at",
    "associated_with",
    "produced",
    "observed",
    "reported",
    "caused",
    "contributed_to",
}
ONTOLOGY_CANDIDATE_TYPE_VALUES = {
    "mechanism",
    "defect",
    "entity",
    "event",
    "condition",
}
MECHANISM_CLASS_VALUES = {"demand", "degradation"}
CAUSAL_LANGUAGE_MARKERS = (
    "because",
    "caused",
    "causes",
    "cause",
    "due to",
    "led to",
    "resulted in",
    "contributed to",
    "as a result",
    "probable cause",
)


def _require_mapping(payload: Any, *, label: str) -> Mapping[str, Any]:
    if not isinstance(payload, Mapping):
        raise ValueError(f"{label} must be a JSON object")
    return payload


def _require_string(payload: Mapping[str, Any], field_name: str, *, label: str) -> str:
    value = payload.get(field_name)
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{label}.{field_name} must be a non-empty string")
    return value


def _require_optional_string(
    payload: Mapping[str, Any],
    field_name: str,
    *,
    label: str,
) -> str | None:
    if field_name not in payload:
        raise ValueError(f"{label}.{field_name} must be present")
    value = payload.get(field_name)
    if value is None:
        return None
    if not isinstance(value, str):
        raise ValueError(f"{label}.{field_name} must be a string or null")
    stripped = value.strip()
    return stripped or None


def _require_bool(payload: Mapping[str, Any], field_name: str, *, label: str) -> bool:
    value = payload.get(field_name)
    if not isinstance(value, bool):
        raise ValueError(f"{label}.{field_name} must be a boolean")
    return value


def _require_list(payload: Mapping[str, Any], field_name: str, *, label: str) -> list[Any]:
    value = payload.get(field_name)
    if not isinstance(value, list):
        raise ValueError(f"{label}.{field_name} must be a list")
    return value


def _require_string_list(
    payload: Mapping[str, Any],
    field_name: str,
    *,
    label: str,
) -> list[str]:
    values = _require_list(payload, field_name, label=label)
    normalized: list[str] = []
    for index, value in enumerate(values):
        if not isinstance(value, str) or not value.strip():
            raise ValueError(
                f"{label}.{field_name}[{index}] must be a non-empty string"
            )
        normalized.append(value)
    return normalized


def build_context_ref(sentence_span_id: str) -> str:
    return f"context:{sentence_span_id}"


@dataclass(frozen=True)
class SentenceSpan:
    sentence_span_id: str
    artifact_id: str
    case_id: str
    parent_structural_span_id: str
    locator: dict[str, Any]
    sentence_text: str
    sentence_index: int
    segmentation_version: str
    provenance: dict[str, Any]
    context: dict[str, str] | None

    @classmethod
    def from_mapping(
        cls,
        payload: Mapping[str, Any] | Any,
        *,
        label: str = "sentence_span",
    ) -> SentenceSpan:
        mapping = _require_mapping(payload, label=label)
        locator = mapping.get("locator")
        provenance = mapping.get("provenance")
        if not isinstance(locator, Mapping):
            raise ValueError(f"{label}.locator must be a JSON object")
        if not isinstance(provenance, Mapping):
            raise ValueError(f"{label}.provenance must be a JSON object")

        sentence_index = mapping.get("sentence_index")
        if not isinstance(sentence_index, int) or sentence_index < 1:
            raise ValueError(f"{label}.sentence_index must be a positive integer")
        segmentation_version = _require_string(mapping, "segmentation_version", label=label)

        for field_name in (
            "artifact_checksum",
            "text_extraction_version",
            "segmentation_version",
        ):
            value = provenance.get(field_name)
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"{label}.provenance.{field_name} must be a non-empty string")

        context_value = mapping.get("context")
        context: dict[str, str] | None = None
        if context_value is not None:
            if not isinstance(context_value, Mapping):
                raise ValueError(f"{label}.context must be a JSON object when provided")
            context = {}
            for field_name in ("preceding_text", "following_text"):
                value = context_value.get(field_name, "")
                if not isinstance(value, str):
                    raise ValueError(f"{label}.context.{field_name} must be a string")
                context[field_name] = value

        return cls(
            sentence_span_id=_require_string(mapping, "sentence_span_id", label=label),
            artifact_id=_require_string(mapping, "artifact_id", label=label),
            case_id=_require_string(mapping, "case_id", label=label),
            parent_structural_span_id=_require_string(
                mapping,
                "parent_structural_span_id",
                label=label,
            ),
            locator=dict(locator),
            sentence_text=_require_string(mapping, "sentence_text", label=label),
            sentence_index=sentence_index,
            segmentation_version=segmentation_version,
            provenance=dict(provenance),
            context=context,
        )

    def to_mapping(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "sentence_span_id": self.sentence_span_id,
            "artifact_id": self.artifact_id,
            "case_id": self.case_id,
            "parent_structural_span_id": self.parent_structural_span_id,
            "locator": self.locator,
            "sentence_text": self.sentence_text,
            "sentence_index": self.sentence_index,
            "segmentation_version": self.segmentation_version,
            "provenance": self.provenance,
        }
        if self.context is not None:
            payload["context"] = self.context
        return payload


@dataclass(frozen=True)
class ClaimContextRecord:
    context_ref: str
    sentence_span_id: str
    parent_structural_span_id: str
    artifact_id: str
    sentence_text: str
    preceding_sentence_text: str
    following_sentence_text: str
    structural_span_text: str | None
    section_label: str | None

    @classmethod
    def from_sentence_span(cls, sentence_span: SentenceSpan) -> ClaimContextRecord:
        locator_section_label = sentence_span.locator.get("section_label")
        locator_structural_span_text = sentence_span.locator.get("structural_span_text")
        section_label = (
            locator_section_label.strip()
            if isinstance(locator_section_label, str) and locator_section_label.strip()
            else None
        )
        structural_span_text = (
            locator_structural_span_text.strip()
            if isinstance(locator_structural_span_text, str)
            and locator_structural_span_text.strip()
            else None
        )
        preceding = ""
        following = ""
        if sentence_span.context is not None:
            preceding = sentence_span.context.get("preceding_text", "")
            following = sentence_span.context.get("following_text", "")
        return cls(
            context_ref=build_context_ref(sentence_span.sentence_span_id),
            sentence_span_id=sentence_span.sentence_span_id,
            parent_structural_span_id=sentence_span.parent_structural_span_id,
            artifact_id=sentence_span.artifact_id,
            sentence_text=sentence_span.sentence_text,
            preceding_sentence_text=preceding,
            following_sentence_text=following,
            structural_span_text=structural_span_text,
            section_label=section_label,
        )

    @classmethod
    def from_mapping(
        cls,
        payload: Mapping[str, Any] | Any,
        *,
        label: str = "context_record",
    ) -> ClaimContextRecord:
        mapping = _require_mapping(payload, label=label)
        return cls(
            context_ref=_require_string(mapping, "context_ref", label=label),
            sentence_span_id=_require_string(mapping, "sentence_span_id", label=label),
            parent_structural_span_id=_require_string(
                mapping,
                "parent_structural_span_id",
                label=label,
            ),
            artifact_id=_require_string(mapping, "artifact_id", label=label),
            sentence_text=_require_string(mapping, "sentence_text", label=label),
            preceding_sentence_text=_require_optional_string(
                mapping,
                "preceding_sentence_text",
                label=label,
            )
            or "",
            following_sentence_text=_require_optional_string(
                mapping,
                "following_sentence_text",
                label=label,
            )
            or "",
            structural_span_text=_require_optional_string(
                mapping,
                "structural_span_text",
                label=label,
            ),
            section_label=_require_optional_string(mapping, "section_label", label=label),
        )

    def to_mapping(self) -> dict[str, Any]:
        return {
            "context_ref": self.context_ref,
            "sentence_span_id": self.sentence_span_id,
            "parent_structural_span_id": self.parent_structural_span_id,
            "artifact_id": self.artifact_id,
            "sentence_text": self.sentence_text,
            "preceding_sentence_text": self.preceding_sentence_text,
            "following_sentence_text": self.following_sentence_text,
            "structural_span_text": self.structural_span_text,
            "section_label": self.section_label,
        }


@dataclass(frozen=True)
class AtomicClaim:
    claim_id: str
    sentence_span_id: str
    parent_structural_span_id: str
    artifact_id: str
    subject_text: str | None
    subject_ref: str | None
    predicate: str
    object_text: str | None
    object_ref: str | None
    assertion_mode: str
    polarity: str
    predicate_status: str
    predicate_raw: str | None
    predicate_candidate: str | None
    context_ref: str

    @classmethod
    def from_mapping(
        cls,
        payload: Mapping[str, Any] | Any,
        *,
        sentence_span: SentenceSpan,
        label: str,
    ) -> AtomicClaim:
        mapping = _require_mapping(payload, label=label)
        assertion_mode = _require_string(mapping, "assertion_mode", label=label)
        polarity = _require_string(mapping, "polarity", label=label)
        predicate_status = _require_string(mapping, "predicate_status", label=label)
        predicate = _require_string(mapping, "predicate", label=label)

        if assertion_mode not in ASSERTION_MODE_VALUES:
            raise ValueError(f"{label}.assertion_mode must be one of {sorted(ASSERTION_MODE_VALUES)}")
        if polarity not in POLARITY_VALUES:
            raise ValueError(f"{label}.polarity must be one of {sorted(POLARITY_VALUES)}")
        if predicate_status not in PREDICATE_STATUS_VALUES:
            raise ValueError(
                f"{label}.predicate_status must be one of {sorted(PREDICATE_STATUS_VALUES)}"
            )

        sentence_span_id = _require_string(mapping, "sentence_span_id", label=label)
        if sentence_span_id != sentence_span.sentence_span_id:
            raise ValueError(
                f"{label}.sentence_span_id must match input span "
                f"{sentence_span.sentence_span_id}: {sentence_span_id}"
            )

        parent_structural_span_id = _require_string(
            mapping,
            "parent_structural_span_id",
            label=label,
        )
        if parent_structural_span_id != sentence_span.parent_structural_span_id:
            raise ValueError(
                f"{label}.parent_structural_span_id must match input span "
                f"{sentence_span.parent_structural_span_id}: {parent_structural_span_id}"
            )

        artifact_id = _require_string(mapping, "artifact_id", label=label)
        if artifact_id != sentence_span.artifact_id:
            raise ValueError(
                f"{label}.artifact_id must match input span {sentence_span.artifact_id}: {artifact_id}"
            )

        subject_text = _require_optional_string(mapping, "subject_text", label=label)
        subject_ref = _require_optional_string(mapping, "subject_ref", label=label)
        object_text = _require_optional_string(mapping, "object_text", label=label)
        object_ref = _require_optional_string(mapping, "object_ref", label=label)
        predicate_raw = _require_optional_string(mapping, "predicate_raw", label=label)
        predicate_candidate = _require_optional_string(
            mapping,
            "predicate_candidate",
            label=label,
        )
        context_ref = _require_string(mapping, "context_ref", label=label)

        if subject_text is None and subject_ref is None:
            raise ValueError(f"{label} must include subject_text and/or subject_ref")

        expected_context_ref = build_context_ref(sentence_span.sentence_span_id)
        if context_ref != expected_context_ref:
            raise ValueError(
                f"{label}.context_ref must resolve to {expected_context_ref}: {context_ref}"
            )

        if predicate_status == "core":
            if predicate not in CORE_PREDICATES:
                raise ValueError(
                    f"{label}.predicate must be one of {sorted(CORE_PREDICATES)} "
                    "when predicate_status=core"
                )
            if predicate_candidate is not None:
                raise ValueError(f"{label}.predicate_candidate must be null when predicate_status=core")
        elif predicate_status == "candidate":
            if predicate_raw is None:
                raise ValueError(f"{label}.predicate_raw is required when predicate_status!=core")
            if predicate_candidate is None:
                raise ValueError(
                    f"{label}.predicate_candidate is required when predicate_status=candidate"
                )
            if predicate != predicate_candidate:
                raise ValueError(
                    f"{label}.predicate must equal predicate_candidate when predicate_status=candidate"
                )
        else:
            if predicate_raw is None:
                raise ValueError(f"{label}.predicate_raw is required when predicate_status!=core")
            if predicate_candidate is not None:
                raise ValueError(
                    f"{label}.predicate_candidate must be null when predicate_status=unresolved"
                )
            if predicate != "unresolved":
                raise ValueError(
                    f"{label}.predicate must be 'unresolved' when predicate_status=unresolved"
                )

        return cls(
            claim_id=_require_string(mapping, "claim_id", label=label),
            sentence_span_id=sentence_span_id,
            parent_structural_span_id=parent_structural_span_id,
            artifact_id=artifact_id,
            subject_text=subject_text,
            subject_ref=subject_ref,
            predicate=predicate,
            object_text=object_text,
            object_ref=object_ref,
            assertion_mode=assertion_mode,
            polarity=polarity,
            predicate_status=predicate_status,
            predicate_raw=predicate_raw,
            predicate_candidate=predicate_candidate,
            context_ref=context_ref,
        )

    def to_mapping(self) -> dict[str, Any]:
        return {
            "claim_id": self.claim_id,
            "sentence_span_id": self.sentence_span_id,
            "parent_structural_span_id": self.parent_structural_span_id,
            "artifact_id": self.artifact_id,
            "subject_text": self.subject_text,
            "subject_ref": self.subject_ref,
            "predicate": self.predicate,
            "object_text": self.object_text,
            "object_ref": self.object_ref,
            "assertion_mode": self.assertion_mode,
            "polarity": self.polarity,
            "predicate_status": self.predicate_status,
            "predicate_raw": self.predicate_raw,
            "predicate_candidate": self.predicate_candidate,
            "context_ref": self.context_ref,
        }


@dataclass(frozen=True)
class OntologyCandidate:
    candidate_id: str
    candidate_type: str
    candidate_text: str
    linked_claim_ids: tuple[str, ...]
    mechanism_class: str | None
    needs_review: bool

    @classmethod
    def from_mapping(
        cls,
        payload: Mapping[str, Any] | Any,
        *,
        label: str,
    ) -> OntologyCandidate:
        mapping = _require_mapping(payload, label=label)
        candidate_type = _require_string(mapping, "candidate_type", label=label)
        if candidate_type not in ONTOLOGY_CANDIDATE_TYPE_VALUES:
            raise ValueError(
                f"{label}.candidate_type must be one of {sorted(ONTOLOGY_CANDIDATE_TYPE_VALUES)}"
            )

        mechanism_class = _require_optional_string(mapping, "mechanism_class", label=label)
        if candidate_type == "mechanism":
            if mechanism_class is None:
                raise ValueError(
                    f"{label}.mechanism_class is required when candidate_type=mechanism"
                )
            if mechanism_class not in MECHANISM_CLASS_VALUES:
                raise ValueError(
                    f"{label}.mechanism_class must be one of {sorted(MECHANISM_CLASS_VALUES)}"
                )
        elif mechanism_class is not None:
            raise ValueError(
                f"{label}.mechanism_class must be null when candidate_type!=\"mechanism\""
            )

        linked_claim_ids = tuple(
            _require_string_list(mapping, "linked_claim_ids", label=label)
        )
        if not linked_claim_ids:
            raise ValueError(f"{label}.linked_claim_ids must not be empty")

        return cls(
            candidate_id=_require_string(mapping, "candidate_id", label=label),
            candidate_type=candidate_type,
            candidate_text=_require_string(mapping, "candidate_text", label=label),
            linked_claim_ids=linked_claim_ids,
            mechanism_class=mechanism_class,
            needs_review=_require_bool(mapping, "needs_review", label=label),
        )

    def to_mapping(self) -> dict[str, Any]:
        return {
            "candidate_id": self.candidate_id,
            "candidate_type": self.candidate_type,
            "candidate_text": self.candidate_text,
            "linked_claim_ids": list(self.linked_claim_ids),
            "mechanism_class": self.mechanism_class,
            "needs_review": self.needs_review,
        }


@dataclass(frozen=True)
class UnresolvedItem:
    code: str
    message: str
    related_claim_ids: tuple[str, ...]

    @classmethod
    def from_mapping(
        cls,
        payload: Mapping[str, Any] | Any,
        *,
        label: str,
    ) -> UnresolvedItem:
        mapping = _require_mapping(payload, label=label)
        related_claim_ids_value = mapping.get("related_claim_ids", [])
        if not isinstance(related_claim_ids_value, list):
            raise ValueError(f"{label}.related_claim_ids must be a list when provided")
        related_claim_ids: list[str] = []
        for index, value in enumerate(related_claim_ids_value):
            if not isinstance(value, str) or not value.strip():
                raise ValueError(
                    f"{label}.related_claim_ids[{index}] must be a non-empty string"
                )
            related_claim_ids.append(value)

        return cls(
            code=_require_string(mapping, "code", label=label),
            message=_require_string(mapping, "message", label=label),
            related_claim_ids=tuple(related_claim_ids),
        )

    def to_mapping(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "code": self.code,
            "message": self.message,
        }
        if self.related_claim_ids:
            payload["related_claim_ids"] = list(self.related_claim_ids)
        return payload


@dataclass(frozen=True)
class AtomicExtractionResult:
    sentence_span_id: str
    status: str
    atomic_claims: tuple[AtomicClaim, ...]
    ontology_candidates: tuple[OntologyCandidate, ...]
    unresolved: tuple[UnresolvedItem, ...]
    warnings: tuple[str, ...]

    @classmethod
    def from_mapping(
        cls,
        payload: Mapping[str, Any] | Any,
        *,
        sentence_span: SentenceSpan,
        label: str = "atomic_result",
    ) -> AtomicExtractionResult:
        mapping = _require_mapping(payload, label=label)
        status = _require_string(mapping, "status", label=label)
        if status not in STATUS_VALUES:
            raise ValueError(f"{label}.status must be one of {sorted(STATUS_VALUES)}")

        sentence_span_id = _require_string(mapping, "sentence_span_id", label=label)
        if sentence_span_id != sentence_span.sentence_span_id:
            raise ValueError(
                f"{label}.sentence_span_id must match input span "
                f"{sentence_span.sentence_span_id}: {sentence_span_id}"
            )

        claim_mappings = _require_list(mapping, "atomic_claims", label=label)
        atomic_claims = tuple(
            AtomicClaim.from_mapping(
                item,
                sentence_span=sentence_span,
                label=f"{label}.atomic_claims[{index}]",
            )
            for index, item in enumerate(claim_mappings)
        )

        claim_ids: set[str] = set()
        for claim in atomic_claims:
            if claim.claim_id in claim_ids:
                raise ValueError(
                    f"{label}.atomic_claims contains duplicate claim_id: {claim.claim_id}"
                )
            claim_ids.add(claim.claim_id)

        candidate_mappings = _require_list(mapping, "ontology_candidates", label=label)
        ontology_candidates = tuple(
            OntologyCandidate.from_mapping(
                item,
                label=f"{label}.ontology_candidates[{index}]",
            )
            for index, item in enumerate(candidate_mappings)
        )

        for candidate in ontology_candidates:
            for claim_id in candidate.linked_claim_ids:
                if claim_id not in claim_ids:
                    raise ValueError(
                        f"{label}.ontology_candidates links unknown claim_id: {claim_id}"
                    )
            if (
                candidate.candidate_type == "mechanism"
                and candidate.mechanism_class == "degradation"
                and not _has_explicit_causal_language(sentence_span, atomic_claims, candidate)
            ):
                raise ValueError(
                    f"{label}.ontology_candidates mechanism degradation requires explicit causal language"
                )

        unresolved_mappings = _require_list(mapping, "unresolved", label=label)
        unresolved = tuple(
            UnresolvedItem.from_mapping(item, label=f"{label}.unresolved[{index}]")
            for index, item in enumerate(unresolved_mappings)
        )

        warnings = tuple(_require_string_list(mapping, "warnings", label=label))

        if status == "unprocessable":
            if atomic_claims:
                raise ValueError(f"{label}.atomic_claims must be empty when status=unprocessable")
            if ontology_candidates:
                raise ValueError(
                    f"{label}.ontology_candidates must be empty when status=unprocessable"
                )
            if not unresolved:
                raise ValueError(f"{label}.unresolved must contain a reason when status=unprocessable")
        elif not atomic_claims:
            raise ValueError(f"{label}.atomic_claims must not be empty when status=ok")

        return cls(
            sentence_span_id=sentence_span_id,
            status=status,
            atomic_claims=atomic_claims,
            ontology_candidates=ontology_candidates,
            unresolved=unresolved,
            warnings=warnings,
        )

    def to_mapping(self) -> dict[str, Any]:
        return {
            "sentence_span_id": self.sentence_span_id,
            "status": self.status,
            "atomic_claims": [claim.to_mapping() for claim in self.atomic_claims],
            "ontology_candidates": [
                candidate.to_mapping() for candidate in self.ontology_candidates
            ],
            "unresolved": [item.to_mapping() for item in self.unresolved],
            "warnings": list(self.warnings),
        }


def _has_explicit_causal_language(
    sentence_span: SentenceSpan,
    atomic_claims: tuple[AtomicClaim, ...],
    candidate: OntologyCandidate,
) -> bool:
    linked_text = " ".join(
        " ".join(
            part
            for part in (
                claim.subject_text,
                claim.predicate_raw,
                claim.object_text,
            )
            if part
        )
        for claim in atomic_claims
        if claim.claim_id in set(candidate.linked_claim_ids)
    )
    combined = f"{sentence_span.sentence_text}\n{linked_text}".lower()
    return any(marker in combined for marker in CAUSAL_LANGUAGE_MARKERS)


def _nullable_string_schema(*, description: str | None = None) -> dict[str, Any]:
    schema: dict[str, Any] = {
        "anyOf": [
            {"type": "string"},
            {"type": "null"},
        ]
    }
    if description:
        schema["description"] = description
    return schema


def atomic_extraction_result_json_schema() -> dict[str, Any]:
    claim_schema = {
        "type": "object",
        "additionalProperties": False,
        "required": [
            "claim_id",
            "sentence_span_id",
            "parent_structural_span_id",
            "artifact_id",
            "subject_text",
            "subject_ref",
            "predicate",
            "object_text",
            "object_ref",
            "assertion_mode",
            "polarity",
            "predicate_status",
            "predicate_raw",
            "predicate_candidate",
            "context_ref",
        ],
        "properties": {
            "claim_id": {"type": "string"},
            "sentence_span_id": {"type": "string"},
            "parent_structural_span_id": {"type": "string"},
            "artifact_id": {"type": "string"},
            "subject_text": _nullable_string_schema(),
            "subject_ref": _nullable_string_schema(),
            "predicate": {"type": "string"},
            "object_text": _nullable_string_schema(),
            "object_ref": _nullable_string_schema(),
            "assertion_mode": {"type": "string", "enum": sorted(ASSERTION_MODE_VALUES)},
            "polarity": {"type": "string", "enum": sorted(POLARITY_VALUES)},
            "predicate_status": {"type": "string", "enum": sorted(PREDICATE_STATUS_VALUES)},
            "predicate_raw": _nullable_string_schema(),
            "predicate_candidate": _nullable_string_schema(),
            "context_ref": {"type": "string"},
        },
    }
    ontology_candidate_schema = {
        "type": "object",
        "additionalProperties": False,
        "required": [
            "candidate_id",
            "candidate_type",
            "candidate_text",
            "linked_claim_ids",
            "mechanism_class",
            "needs_review",
        ],
        "properties": {
            "candidate_id": {"type": "string"},
            "candidate_type": {
                "type": "string",
                "enum": sorted(ONTOLOGY_CANDIDATE_TYPE_VALUES),
            },
            "candidate_text": {"type": "string"},
            "linked_claim_ids": {
                "type": "array",
                "items": {"type": "string"},
            },
            "mechanism_class": {
                "anyOf": [
                    {"type": "string", "enum": sorted(MECHANISM_CLASS_VALUES)},
                    {"type": "null"},
                ]
            },
            "needs_review": {"type": "boolean"},
        },
    }
    unresolved_schema = {
        "type": "object",
        "additionalProperties": False,
        "required": ["code", "message", "related_claim_ids"],
        "properties": {
            "code": {"type": "string"},
            "message": {"type": "string"},
            "related_claim_ids": {
                "type": "array",
                "items": {"type": "string"},
            },
        },
    }
    return {
        "type": "object",
        "additionalProperties": False,
        "required": [
            "sentence_span_id",
            "status",
            "atomic_claims",
            "ontology_candidates",
            "unresolved",
            "warnings",
        ],
        "properties": {
            "sentence_span_id": {"type": "string"},
            "status": {"type": "string", "enum": sorted(STATUS_VALUES)},
            "atomic_claims": {
                "type": "array",
                "items": claim_schema,
            },
            "ontology_candidates": {
                "type": "array",
                "items": ontology_candidate_schema,
            },
            "unresolved": {
                "type": "array",
                "items": unresolved_schema,
            },
            "warnings": {
                "type": "array",
                "items": {"type": "string"},
            },
        },
    }


def build_api_request(
    *,
    job_id: str,
    items: list[SentenceSpan],
    prompt_version: str = PROMPT_VERSION,
    model_contract_version: str = MODEL_CONTRACT_VERSION,
    ontology_version: str = ONTOLOGY_VERSION,
    mapping_contract_version: str = MAPPING_CONTRACT_VERSION,
) -> dict[str, Any]:
    return {
        "job_id": job_id,
        "prompt_version": prompt_version,
        "model_contract_version": model_contract_version,
        "ontology_version": ontology_version,
        "mapping_contract_version": mapping_contract_version,
        "items": [item.to_mapping() for item in items],
    }


def build_api_response(
    *,
    job_id: str,
    results: list[AtomicExtractionResult],
) -> dict[str, Any]:
    return {
        "job_id": job_id,
        "results": [result.to_mapping() for result in results],
    }
