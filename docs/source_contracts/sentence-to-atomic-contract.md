# SentenceSpan To Claim Inference Contract

## Rule

Claim inference is model-assisted but pipeline-governed.
The pipeline owns:

- certified SentenceSpan inputs
- batching
- request construction
- schema validation
- persistence
- certification

The model owns bounded transformation of one governed `SentenceSpan`.

## Input

Atomic extraction consumes certified SentenceSpan artifacts.
By default the stage rejects uncertified upstream SentenceSpan runs and
discovers the latest certified SentenceSpan run automatically.

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

Additional governed review artifacts may be written alongside them, including:

- `atomic_contexts.jsonl`
- `inference_metadata.json`
- `batch_requests.jsonl` when Batch API execution is used

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

## Claim Schema

Each claim is a lightweight structured assertion.

Required claim fields:

- `claim_id`
- `sentence_span_id`
- `parent_structural_span_id`
- `artifact_id`
- `subject_text`
- `subject_ref`
- `predicate`
- `object_text`
- `object_ref`
- `assertion_mode`
- `polarity`
- `predicate_status`
- `predicate_raw`
- `predicate_candidate`
- `context_ref`

Rules:

- Every claim must carry sentence, parent-structure, and artifact lineage.
- Every claim must reference parent context through `context_ref`.
- `predicate` is always required.
- `predicate_status` must be one of `core`, `candidate`, or `unresolved`.
- `predicate_raw` is required when `predicate_status != core`.
- `predicate_candidate` is required when `predicate_status = candidate`.
- Candidate predicate exploration is allowed only when the exploratory value is
  captured explicitly in `predicate_candidate`.

Core predicates are locked to:

- `exists`
- `has`
- `located_at`
- `associated_with`
- `produced`
- `observed`
- `reported`
- `caused`
- `contributed_to`

## Context / Review Support

Primary atomic outputs stay lean, but the run must preserve resolvable parent
context for review and governance. Context records support:

- `sentence_text`
- `preceding_sentence_text`
- `following_sentence_text`
- `structural_span_text`
- `section_label`

Claims reference those records via `context_ref`.

## Ontology Candidate Schema

Ontology candidates are adjacent to claims, not a replacement for them.

Each ontology candidate must include:

- `candidate_id`
- `candidate_type`
- `candidate_text`
- `linked_claim_ids`
- `mechanism_class`
- `needs_review`

Allowed `candidate_type` values:

- `mechanism`
- `defect`
- `entity`
- `event`
- `condition`

Rules:

- `linked_claim_ids` must reference valid claim IDs in the same result.
- `mechanism_class` is required only when `candidate_type = mechanism`.
- Allowed `mechanism_class` values are `demand` and `degradation`.
- Higher-order systemic synthesis is outside this base extraction layer.

## Validation And Certification

Certification is rule-derived from the persisted run artifacts.

Current atomic validation enforces:

- required claim fields present
- valid `predicate_status`
- `predicate_raw` present when `predicate_status != core`
- `sentence_span_id` present and input-aligned
- `parent_structural_span_id` present and input-aligned
- `artifact_id` present and input-aligned
- all `context_ref` values resolve
- no duplicate `claim_id` values inside a result item
- ontology candidate types valid
- `mechanism_class` valid when `candidate_type = mechanism`
- uncertified upstream input rejected by default

## Live And Batch API Execution

The repo now supports one governed contract across both OpenAI execution modes:

- live Responses API execution
- asynchronous Batch API execution

Shared rules:

- same JSON schema
- same validator
- same certification logic
- same lineage fields
- same certified-upstream policy

Live mode validates every response immediately and preserves request and retry
metadata in run artifacts.

Batch mode writes a governed request manifest, submits `/v1/responses` batch
requests, reconciles results by `custom_id = sentence_span_id`, and preserves
batch job metadata in run artifacts.

## Current Implementation Notes

- The repo now enforces the claim/predicate/context contract and run
  certification for atomic extraction.
- The repo still does not silently ship a hidden production model adapter for
  the local CLI path; the explicit transformer boundary remains intact.
- OpenAI live and batch execution are implemented as explicit library runners:
  `run_atomic_extraction_live(...)` and `run_atomic_extraction_openai_batch(...)`.
