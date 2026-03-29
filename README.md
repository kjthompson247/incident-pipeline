# incident-pipeline

A deterministic multi-source incident evidence pipeline.

This repository is the constrained consolidation of `ntsb-acquire` and `ntsb-report-flow`.
The canonical package root is `src/incident_pipeline`.
The canonical Python import surface is `incident_pipeline.*`.

## Current Scope

The current migration preserves NTSB acquisition, ingestion, triage, narrative selection, and extract stages without redesigning business logic or metadata contracts.

Current and planned sources include:
- NTSB
- PHMSA
- courts
- news

The extract layer now includes two governed substages:

- deterministic SentenceSpan generation
- schema-bound atomic extraction

The repository implements the contracts, validators, and certified run
artifacts for those substages. It does not currently ship a production model
adapter for atomic extraction.

## Bootstrap

Supported runtime: Python `3.12.13`

Supported environment workflow: `uv`

From a clean checkout:

```bash
uv python pin 3.12.13
uv sync
uv run pytest -q
```

The repository ships with `.python-version` pinned to `3.12.13`, so `uv sync` will provision and use that interpreter automatically when needed.
If you do not already have Python `3.12.13` on your machine, use a current `uv` release that can install it before running `uv sync`.
The first `uv sync` also requires access to PyPI or a configured internal package mirror so `uv` can resolve and download pinned dependencies.

## Configuration

The default operator config is [configs/settings.yaml](/Users/kellythompson/projects/incident-pipeline/configs/settings.yaml). By default it uses a repo-local data root at `data/ntsb`, not a machine-specific external disk path.

Supported overrides:

- `INCIDENT_PIPELINE_DATA_ROOT`
  Sets the repo-wide default data root used by acquisition defaults.
- `INCIDENT_PIPELINE_SETTINGS_PATH`
  Points the downstream stage scripts at an alternate YAML settings file.
- `NTSB_ACQUIRE_*`
  Acquisition CLI overrides such as `NTSB_ACQUIRE_DATA_ROOT`, `NTSB_ACQUIRE_SQLITE_PATH`, and `NTSB_ACQUIRE_DOWNSTREAM_RAW_ROOT`.

Example:

```bash
export INCIDENT_PIPELINE_DATA_ROOT=/absolute/path/to/pipeline-data
uv run ntsb-acquire doctor
```

External-drive usage is supported by pointing the configured data root at that mount. The code depends on a configured storage root, not on any specific device name or one operator's machine.

## Metadata Authority

The canonical metadata model is governed stage records, not ad hoc sidecars.

- Acquisition authority lives in acquisition SQLite current-state tables plus append-only acquisition manifests.
- Registration is manifest-driven: it should consume the immutable acquisition export snapshot as
  the primary candidate source and preserve acquisition lineage in manifest DB `documents` rows.
- Ingestion, triage, and narrative JSON outputs are governed stage records consumed downstream.
- Extract and structure authority lives in the manifest SQLite database, with file outputs treated as derived artifacts referenced by that DB.
- Mutable convenience files such as `ingestion_manifest_latest.jsonl`, HTML feedback reports, and debug outputs are not co-equal sources of truth.

For reproducible ingestion reruns, point `INCIDENT_PIPELINE_INGESTION_MANIFEST_PATH` at a specific `ingestion_manifest_<run_id>.jsonl` snapshot when possible instead of relying on the mutable `ingestion_manifest_latest.jsonl` alias.

## Operator Flow

Acquisition:

```bash
uv run ntsb-acquire doctor
uv run ntsb-acquire init-db
uv run ntsb-acquire collect
uv run ntsb-acquire export-ingestion-manifest
```

Downstream stages:

```bash
uv run python scripts/run_docket_ingest.py
uv run python scripts/run_docket_triage.py
uv run python scripts/run_primary_docket_narrative.py
uv run python scripts/run_extract.py
uv run python scripts/run_structure.py
```

Extract and structure use the manifest database flow. Initialize and populate it before running those stages:

```bash
uv run python scripts/init_db.py
uv run python -m incident_pipeline.ingestion.register_reports
uv run python scripts/run_extract.py
uv run python scripts/run_structure.py
```

When `docket_ingest.manifest_path` points at an acquisition export snapshot,
`incident_pipeline.ingestion.register_reports` uses that governed manifest as
its primary registration input. It resolves SHA/blob-backed artifacts from the
manifest, registers them even when there is no convenient report filename under
`raw/`, and preserves acquisition identifiers such as `project_id`,
`ntsb_number`, `docket_item_id`, `source_url`, and `acquisition_run_id` in the
manifest DB for downstream auditability.

If no acquisition export snapshot is configured or available, registration
falls back to a raw filesystem scan for compatibility only. That fallback is
lower-authority behavior and should not be treated as the architectural source
of truth.

## Stage Certification

Current governed run-artifact support is implemented for:

- ingestion
- triage
- narrative

Each run writes deterministic execution evidence under:

```text
{stage_output_root}/runs/{run_id}/
```

Required artifacts:

- `run_summary.json`
- `metrics.json`
- `failures.jsonl`
- `validation_report.json`
- `_CERTIFIED` or `_FAILED`

`run_id` uses the format `{stage}_{ISO8601}_{short_hash}`.
Certification is rule-derived from `validation_report.json`; downstream stages
reject uncertified upstream runs by default.

At present, triage requires a certified ingestion run by default, and narrative
requires a certified triage run by default. Extract and index should adopt the
same run-artifact contract as they are normalized.

## Supported vs Out Of Scope

Supported in this repository:

- NTSB acquisition
- docket ingestion
- docket triage
- primary docket narrative selection
- extract
- structure extraction

Out of scope:

- retrieve
- serve
- OCR execution
- embed
- chunk
- index integration

OCR is disabled by default in the shipped config. If you explicitly enable it, queued OCR work is outside the supported standalone workflow.

## See Also

- `docs/architecture/pipeline-architecture.md`
- `docs/architecture/metadata-authority.md`
- `docs/migration/repo-consolidation-plan.md`
- `docs/source_contracts/source-stage-contract.md`
- `docs/source_contracts/sentence-span-generation-contract.md`
- `docs/source_contracts/sentence-to-atomic-contract.md`
- `MIGRATION_STATUS.md`
# incident-pipeline
