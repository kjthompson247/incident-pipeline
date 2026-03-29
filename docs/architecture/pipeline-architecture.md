# Pipeline Architecture

## Design Rule

Code is organized by stage under one canonical package:

- `src/incident_pipeline/acquisition/<source>/`
- `src/incident_pipeline/ingestion/`
- `src/incident_pipeline/triage/`
- `src/incident_pipeline/narrative/`
- `src/incident_pipeline/extract/`
- `src/incident_pipeline/index/`
- `src/incident_pipeline/common/`

## Canonical Data Root

By default the NTSB source root is:

`data/ntsb`

Override it with `INCIDENT_PIPELINE_DATA_ROOT` or `INCIDENT_PIPELINE_NTSB_SOURCE_ROOT` when an operator needs data outside the repository working tree.

## Canonical NTSB Stage Roots

- `raw/`
- `ingestion/`
- `triage/`
- `narrative/`
- `extract/`
- `index/`

## NTSB Acquisition Working Roots

To preserve current acquisition contracts, the acquisition package keeps its operational state beneath the same source root:

- `acquisition/state/`
- `acquisition/raw/`
- `acquisition/normalized/`
- `acquisition/blobs/`
- `acquisition/manifests/`
- `acquisition/logs/`
- `acquisition/exports/`

The promoted downstream raw files still land in:

- `raw/`

This keeps existing acquisition behavior deterministic while giving the unified repository clean stage boundaries for the downstream pipeline.

## Shared Path Foundation

`incident_pipeline.common.paths` is the canonical place for:

- repository root resolution
- canonical config path resolution
- canonical NTSB source root resolution
- computed NTSB stage and acquisition working directories

The configured data root is the architectural contract. Repo-local `data/` is the portable default, and external storage is an operator configuration choice rather than a hard-coded assumption.

## Compatibility Strategy

- The canonical Python import surface is `incident_pipeline.*`.
- `incident_pipeline.triage.primary_docket_narrative` re-exports the narrative selector while the canonical implementation lives under `incident_pipeline.narrative`.

## Metadata Authority

Metadata authority is defined in [metadata-authority.md](/Users/kellythompson/projects/incident-pipeline/docs/architecture/metadata-authority.md).
The short version is:

- acquisition uses governed current-state tables plus append-only manifests
- downstream JSON stage outputs are governed stage records
- extract / structure use the manifest SQLite DB as operational authority
- extract sentence-span and atomic runs write certified run artifacts beneath
  their stage output roots
- convenience views, debug outputs, and mutable aliases are not co-equal truth

## Current Implementation Status

- NTSB acquisition is implemented under `incident_pipeline.acquisition.ntsb`.
- NTSB ingestion, triage, narrative selection, and extract stages are implemented under their canonical stage packages.
- Extract now includes deterministic SentenceSpan generation and a
  schema-bound atomic extraction contract. The pipeline contract is implemented
  in-code, but the repo does not yet ship a production model adapter for atomic
  transformation.
- PHMSA, courts, news, and index are scaffold-only for now.
