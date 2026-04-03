# Statement Execution and Caching — Contract

Status: ACTIVE  
Version: v0.1  

## Purpose

Define deterministic, idempotent statement-level extraction execution with:
- validation gating
- caching
- selective replay

---

## Core Rule

A statement extraction result is reusable only if all are unchanged:

- normalized input text
- extractor_version
- prompt_version
- ontology_version
- mapping_version
- validator_version

Otherwise, it must be reprocessed.

---

## Statement Work Unit

Each statement must have:

- statement_id (stable)
- document_id
- span_id
- source_text
- source_text_normalized
- input_hash

---

## Execution Record

Each run must persist:

- statement_id
- run_id
- cache_key
- status
- extractor_version
- prompt_version
- ontology_version
- mapping_version
- validator_version
- error_code
- updated_at

Statuses:
- succeeded
- failed_validation
- failed_runtime
- skipped_cached
- needs_review

---

## Cache Key

```text
cache_key = hash(
  source_text_normalized +
  extractor_version +
  prompt_version +
  ontology_version +
  mapping_version +
  validator_version
)