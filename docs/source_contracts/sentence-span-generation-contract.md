# SentenceSpan Generation Contract

## Rule

SentenceSpan generation is a deterministic, pipeline-owned extract substage.
It does not call external models.

## Position

Within extract, SentenceSpan generation is the first governed semantic-interface
substage:

1. structural segmentation when available
2. sentence span generation
3. atomic transformation
4. validation and certification

## Input

The current implementation consumes completed extract/ocr text artifacts from
the manifest DB and reads the resolved extracted text path for each document.

Required operational inputs:

- `artifact_id`
- `case_id`
- extracted text
- text extraction version
- artifact checksum

When a richer structural span is not already available, the pipeline falls back
to deterministic paragraph-based structural spans.

## Output

Certified run artifacts live under:

`{sentence_span_output_root}/runs/{run_id}/`

Required artifacts:

- `sentence_spans.jsonl`
- `sentence_span_failures.jsonl`
- `sentence_span_metrics.json`
- `sentence_span_run_summary.json`
- `sentence_span_validation_report.json`
- `_CERTIFIED` or `_FAILED`

## SentenceSpan Schema

Each `sentence_spans.jsonl` row contains:

- `sentence_span_id`
- `artifact_id`
- `case_id`
- `parent_structural_span_id`
- `locator`
- `sentence_text`
- `sentence_index`
- `segmentation_version`
- `provenance`
- optional `context`

The canonical ID format is:

`sspan:{case_id}:{artifact_id}:{locator_hash}:{sentence_index}`

No UUIDs are used.

## Determinism Rules

- same input text must yield the same sentence spans
- segmentation is rule-based and versioned as `sentence-split-v1`
- IDs are derived from stable document identity plus deterministic locator data
- reruns against the same input digest are validated against the latest
  certified run

## Current Implementation Notes

- Structural blocks are currently paragraph-based when no richer upstream
  structure is available.
- Bullet and transcript-style lines are treated as deterministic structural
  blocks.
- Sentence segmentation protects common abbreviations, decimals, ellipses, and
  transcript markers before splitting.
- Context, when included, is limited to the previous and next sentence in the
  same parent structural span.

## Failure Handling

Segmentation failures are written to `sentence_span_failures.jsonl`.
There are no silent drops.
