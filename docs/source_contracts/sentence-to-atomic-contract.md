# SentenceSpan To Atomic Claim Contract

## Rule

Atomic transformation is model-assisted but pipeline-governed.
The pipeline owns:

- SentenceSpan inputs
- batching
- schema validation
- persistence
- certification

The model owns bounded transformation of one governed `SentenceSpan`.

## Input

Atomic extraction consumes certified SentenceSpan artifacts.
By default the stage rejects uncertified upstream SentenceSpan runs.

## Output

Certified run artifacts live under:

`{atomic_output_root}/runs/{run_id}/`

Required artifacts:

- `sentence_spans.jsonl`
- `atomic_extractions.jsonl`
- `atomic_failures.jsonl`
- `atomic_metrics.json`
- `atomic_run_summary.json`
- `atomic_validation_report.json`
- `_CERTIFIED` or `_FAILED`

## AtomicExtractionResult Schema

Required top-level fields:

- `sentence_span_id`
- `status`
- `atomic_claims`
- `ontology_candidates`
- `unresolved`
- `warnings`

Allowed `status` values:

- `ok`
- `unprocessable`

If `status=unprocessable`:

- `atomic_claims` must be empty
- `ontology_candidates` must be empty
- `unresolved` must contain the reason

## Atomic Claim Schema

Each atomic claim must include:

- `atomic_claim_id`
- `claim_text`
- `assertion_mode`
- `polarity`
- `claim_type`
- `needs_review`

The implementation enforces unique `atomic_claim_id` values within each
SentenceSpan result.

## Ontology Candidate Schema

Each ontology candidate must include:

- `candidate_id`
- `candidate_type`
- `candidate_text`
- `linked_claim_ids`
- `needs_review`

Allowed `candidate_type` values:

- `entities`
- `events`
- `conditions`
- `causal_factors`

`linked_claim_ids` must reference valid claim IDs in the same result.
`causal_factors` require explicit causal language in the supporting sentence or
linked claim text.

## Current Implementation Notes

- The repo now enforces the contract and run certification for atomic
  extraction.
- The repo does not ship a production model adapter. Callers must provide an
  explicit SentenceSpan transformer callable to the batch runner.
- Invalid model payloads are converted into governed failure rows rather than
  being silently persisted as outputs.
