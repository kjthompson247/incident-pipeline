from __future__ import annotations

from incident_pipeline.extract.atomic_contract import SentenceSpan


def transform_sentence_span(sentence_span: SentenceSpan) -> dict:
    text = sentence_span.sentence_text.strip()

    return {
        "sentence_span_id": sentence_span.sentence_span_id,
        "status": "ok",
        "atomic_claims": [
            {
                "atomic_claim_id": "ac:01",
                "claim_text": text,
                "assertion_mode": "stated",
                "polarity": "affirmed",
                "claim_type": "statement",
                "needs_review": False,
            }
        ],
        "ontology_candidates": [],
        "unresolved": [],
        "warnings": [],
    }