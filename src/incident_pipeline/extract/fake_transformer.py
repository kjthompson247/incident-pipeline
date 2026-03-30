from __future__ import annotations

from incident_pipeline.extract.atomic_contract import SentenceSpan


def transform_sentence_span(sentence_span: SentenceSpan) -> dict:
    text = sentence_span.sentence_text.strip()

    return {
        "sentence_span_id": sentence_span.sentence_span_id,
        "status": "ok",
        "atomic_claims": [
            {
                "claim_id": "claim:01",
                "sentence_span_id": sentence_span.sentence_span_id,
                "parent_structural_span_id": sentence_span.parent_structural_span_id,
                "artifact_id": sentence_span.artifact_id,
                "subject_text": text,
                "subject_ref": None,
                "predicate": "reported",
                "object_text": None,
                "object_ref": None,
                "assertion_mode": "stated",
                "polarity": "affirmed",
                "predicate_status": "core",
                "predicate_raw": None,
                "predicate_candidate": None,
                "context_ref": f"context:{sentence_span.sentence_span_id}",
            }
        ],
        "ontology_candidates": [],
        "unresolved": [],
        "warnings": [],
    }
