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

## Canonical Roots

There are exactly two path bases in the repository:

- `repo_root`: implicit, discovered by code, used only for repo-owned assets
- `storage_root`: explicit, loaded from `INCIDENT_PIPELINE_DATA_ROOT`, used for all pipeline data

Pipeline data then resolves through one required namespace layer:

- `storage_namespace`: a single relative namespace segment such as `ntsb`

The NTSB source root is therefore derived as:

`{storage_root}/{storage_namespace}`

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

`incident_pipeline.common.paths` and `incident_pipeline.common.settings` are the canonical places for:

- repository root resolution
- storage root validation
- storage-relative config resolution
- computed NTSB stage and acquisition working directories

All storage-owned settings must be relative to `paths.storage_root / paths.storage_namespace`. Unresolved `${VAR}` placeholders fail fast, and data paths do not fall back to `repo_root`.

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
