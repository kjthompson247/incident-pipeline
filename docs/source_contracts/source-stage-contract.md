# Source / Stage Contract

## Rule

Every source uses the same stage names, even if the underlying artifacts differ.

## Sources

- ntsb
- phmsa
- courts
- news

## Required stage meanings

### raw
Original source-derived artifacts with preserved lineage.

### ingestion
Deterministic derived artifacts suitable for later routing and extraction.

### triage
Coarse routing labels only.

### narrative
One primary narrative anchor per case/docket/investigation where applicable.

### extract
Structured evidence fields.
This stage may include deterministic structural parsing, deterministic
SentenceSpan generation, and schema-bound atomic extraction, but only governed
stage records are authoritative.

### index
Prepared artifacts for retrieval or graph/indexing systems.

## Notes

Not every source must implement every stage immediately.
The stage scaffold still exists to keep the architecture uniform.

Each implemented stage should expose one governed stage record layer that downstream code can rely on.
Raw artifacts, derived content files, debug traces, and operator conveniences may coexist, but they are not co-equal authority unless they are explicitly documented as the stage record.
