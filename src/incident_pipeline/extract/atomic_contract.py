from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any


MODEL_CONTRACT_VERSION = "sentence-to-atomic-v1"
ONTOLOGY_VERSION = "minimal-ontology-v0.2"
MAPPING_CONTRACT_VERSION = "mapping-contract-v0.1"

STATUS_VALUES = {"ok", "unprocessable"}
ASSERTION_MODE_VALUES = {"stated", "observed", "concluded", "recommended"}
POLARITY_VALUES = {"affirmed", "negated", "uncertain"}
CLAIM_TYPE_VALUES = {
    "statement",
    "observation",
    "finding",
    "conclusion",
    "recommendation",
}
ONTOLOGY_CANDIDATE_TYPE_VALUES = {
    "entities",
    "events",
    "conditions",
    "causal_factors",
}
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

        provenance_required_fields = (
            "artifact_checksum",
            "text_extraction_version",
            "segmentation_version",
        )
        for field_name in provenance_required_fields:
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
class AtomicClaim:
    atomic_claim_id: str
    claim_text: str
    assertion_mode: str
    polarity: str
    claim_type: str
    needs_review: bool

    @classmethod
    def from_mapping(
        cls,
        payload: Mapping[str, Any] | Any,
        *,
        label: str,
    ) -> AtomicClaim:
        mapping = _require_mapping(payload, label=label)
        assertion_mode = _require_string(mapping, "assertion_mode", label=label)
        polarity = _require_string(mapping, "polarity", label=label)
        claim_type = _require_string(mapping, "claim_type", label=label)
        if assertion_mode not in ASSERTION_MODE_VALUES:
            raise ValueError(f"{label}.assertion_mode must be one of {sorted(ASSERTION_MODE_VALUES)}")
        if polarity not in POLARITY_VALUES:
            raise ValueError(f"{label}.polarity must be one of {sorted(POLARITY_VALUES)}")
        if claim_type not in CLAIM_TYPE_VALUES:
            raise ValueError(f"{label}.claim_type must be one of {sorted(CLAIM_TYPE_VALUES)}")

        return cls(
            atomic_claim_id=_require_string(mapping, "atomic_claim_id", label=label),
            claim_text=_require_string(mapping, "claim_text", label=label),
            assertion_mode=assertion_mode,
            polarity=polarity,
            claim_type=claim_type,
            needs_review=_require_bool(mapping, "needs_review", label=label),
        )

    def to_mapping(self) -> dict[str, Any]:
        return {
            "atomic_claim_id": self.atomic_claim_id,
            "claim_text": self.claim_text,
            "assertion_mode": self.assertion_mode,
            "polarity": self.polarity,
            "claim_type": self.claim_type,
            "needs_review": self.needs_review,
        }


@dataclass(frozen=True)
class OntologyCandidate:
    candidate_id: str
    candidate_type: str
    candidate_text: str
    linked_claim_ids: tuple[str, ...]
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
            needs_review=_require_bool(mapping, "needs_review", label=label),
        )

    def to_mapping(self) -> dict[str, Any]:
        return {
            "candidate_id": self.candidate_id,
            "candidate_type": self.candidate_type,
            "candidate_text": self.candidate_text,
            "linked_claim_ids": list(self.linked_claim_ids),
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
            AtomicClaim.from_mapping(item, label=f"{label}.atomic_claims[{index}]")
            for index, item in enumerate(claim_mappings)
        )

        claim_ids: set[str] = set()
        for claim in atomic_claims:
            if claim.atomic_claim_id in claim_ids:
                raise ValueError(
                    f"{label}.atomic_claims contains duplicate atomic_claim_id: "
                    f"{claim.atomic_claim_id}"
                )
            claim_ids.add(claim.atomic_claim_id)

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
                        f"{label}.ontology_candidates links unknown atomic_claim_id: {claim_id}"
                    )
            if (
                candidate.candidate_type == "causal_factors"
                and not _has_explicit_causal_language(sentence_span, atomic_claims, candidate)
            ):
                raise ValueError(
                    f"{label}.ontology_candidates causal_factors require explicit causal language"
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
        claim.claim_text
        for claim in atomic_claims
        if claim.atomic_claim_id in set(candidate.linked_claim_ids)
    )
    combined = f"{sentence_span.sentence_text}\n{linked_text}".lower()
    return any(marker in combined for marker in CAUSAL_LANGUAGE_MARKERS)


def build_api_request(
    *,
    job_id: str,
    items: list[SentenceSpan],
    model_contract_version: str = MODEL_CONTRACT_VERSION,
    ontology_version: str = ONTOLOGY_VERSION,
    mapping_contract_version: str = MAPPING_CONTRACT_VERSION,
) -> dict[str, Any]:
    return {
        "job_id": job_id,
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
