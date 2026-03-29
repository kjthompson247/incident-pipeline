# Metadata Authority

## Decision

The authoritative metadata layer for this repository is the governed stage record layer.

Stage records may be persisted as:

- append-only JSONL manifests
- SQLite rows in an authoritative stage-state database
- per-record JSON artifacts in a stage output directory

These are all acceptable authoritative forms when they are explicitly stage-owned,
schema-governed, and consumed as the official handoff for downstream work.

Filesystem-adjacent convenience files are not co-equal authority.

## Storage Contract

The pipeline depends on a configured data root, not on any specific machine,
mount point, or external drive.

- The repo-local default is `data/` so a clean checkout can run without machine-specific edits.
- Operators may point the pipeline at external storage by configuring a different data root.
- External SSD usage is an operator profile, not the architectural truth.
- The configured NTSB source root must be shared consistently across acquisition and downstream stages.

Supported configuration surfaces today:

- `INCIDENT_PIPELINE_DATA_ROOT`
- `INCIDENT_PIPELINE_NTSB_SOURCE_ROOT`
- `INCIDENT_PIPELINE_SETTINGS_PATH`
- `NTSB_ACQUIRE_*` acquisition overrides

For reproducible downstream reruns, operators should prefer pinned snapshot inputs over mutable aliases.

## Stage Authority

### Acquisition

Authoritative operational truth:

- acquisition SQLite current-state tables under `acquisition/state/acquisition.db`
- append-only acquisition manifests under `acquisition/manifests/*.jsonl`

Authoritative handoff to ingestion:

- `acquisition/exports/ingestion_manifest_<run_id>.jsonl`

Required lineage carried by that handoff:

- `acquisition_run_id`
- `project_id`
- `ntsb_number`
- `docket_item_id`
- `source_url`
- `view_url`
- `blob_sha256`

Convenience only:

- `acquisition/exports/ingestion_manifest_latest.jsonl`
- HTML feedback reports
- materialized case views

### Ingestion

Authoritative stage record:

- `ingestion/metadata/<docket_item_id>.json`

Paired derived artifact:

- `ingestion/extracted/<docket_item_id>.txt`

The ingestion JSON file is a governed stage record even though it is artifact-adjacent.
It is not a convenience sidecar.

### Triage

Authoritative stage record:

- `triage/document_types/<docket_item_id>.json`

### Narrative

Authoritative stage record:

- `narrative/primary_docket_narratives/<docket_id>.json`

### Extract / Structure

Authoritative operational truth:

- `documents` rows in the manifest SQLite database

When registration consumes an acquisition export snapshot, authoritative
`documents` rows should preserve the upstream acquisition identifiers and the
resolved `acquisition_manifest_path` used for the handoff.

Derived artifacts:

- `extract/extracted/<doc_id>.txt`
- `extract/structured/<doc_id>.json`

Convenience / debug artifacts:

- `extract/structured_debug/<doc_id>.json`

When the schema supports them, authoritative `documents` rows should record the output paths of derived artifacts.

## Sidecar Policy

Allowed sidecar or convenience roles:

- human-readable inspection aids
- debug traces
- local materializations or browseable views
- mutable aliases that point at immutable authoritative artifacts

Forbidden sidecar-only roles:

- sole copy of artifact identity
- sole copy of stage status
- sole copy of downstream routing decisions
- sole copy of provenance needed for replay
- sole copy of authoritative output paths

If downstream logic depends on a file, that file must be a governed stage record or be referenced by one.

## Required Authoritative Fields

Every authoritative stage record should ultimately carry, directly or by stable reference:

- stable record identity
- stage name
- status or outcome
- source linkage
- output path or stable derivation rule
- checksum or fingerprint where relevant
- enough provenance to replay or audit the transition

Fields still needing normalization in later work:

- explicit run identity on downstream stage records
- explicit recorded/processed timestamps on downstream stage records
- multi-record lineage support when multiple acquisition items resolve to the
  same content hash

## Current Normalization Direction

- Treat acquisition manifests and current-state tables as governed authority.
- Treat ingestion / triage / narrative JSON outputs as governed stage records, not informal sidecars.
- Treat the extract / structure manifest DB as the governing operational store for those stages.
- Keep convenience outputs clearly secondary and documented as non-authoritative.
