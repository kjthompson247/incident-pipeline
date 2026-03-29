# Repo Consolidation Plan

## Objective

Create one unified repository in `/Users/kellythompson/projects/incident-pipeline` without modifying the source repos:

- `/Users/kellythompson/projects/ntsb-acquire`
- `/Users/kellythompson/projects/ntsb-report-flow`

## Scope Guardrails

- Old repos are read-only.
- Business logic stays intact except for import and path normalization.
- Metadata contracts stay intact.
- External SSD data is not moved or rewritten.
- When a destination is unclear, leave a TODO instead of guessing.

## Source Inventory

### `ntsb-acquire`

- Legacy acquisition package modules
- Package-local schema: `sql/init.sql`
- Acquisition tests: `tests/*.py`
- Supporting docs: `docs/acquisition_architecture.md`

### `ntsb-report-flow`

- Ingestion stage: `src/ingest/*.py`
- Triage stage: `src/triage/document_type.py`, `src/triage/docket_triage.py`
- Narrative selection: `src/triage/primary_docket_narrative.py`
- Extract stage: `src/parse/*.py`
- Manifest schema: `sql/init_manifest.sql`
- Pipeline scripts: `scripts/*.py`
- Downstream tests: `tests/*.py`
- Baseline config: `configs/settings.yaml`

## Canonical Mapping

| Source repo path | Unified repo destination | Notes |
| --- | --- | --- |
| `ntsb-acquire` legacy acquisition modules | `src/incident_pipeline/acquisition/ntsb/*.py` | Canonical NTSB acquisition package |
| `ntsb-acquire/sql/init.sql` | `src/incident_pipeline/acquisition/ntsb/sql/init.sql` | Keep acquisition schema package-local |
| `ntsb-report-flow/src/ingest/*.py` | `src/incident_pipeline/ingestion/*.py` | Ingestion stage |
| `ntsb-report-flow/src/triage/document_type.py` | `src/incident_pipeline/triage/document_type.py` | Triage classifier |
| `ntsb-report-flow/src/triage/docket_triage.py` | `src/incident_pipeline/triage/docket_triage.py` | Triage runner |
| `ntsb-report-flow/src/triage/primary_docket_narrative.py` | `src/incident_pipeline/narrative/primary_docket_narrative.py` | Narrative stage split out explicitly |
| `ntsb-report-flow/src/parse/*.py` | `src/incident_pipeline/extract/*.py` | Extract stage |
| `ntsb-report-flow/sql/init_manifest.sql` | `sql/init_manifest.sql` | Kept at repo level for scripts and tests |
| `ntsb-acquire` tests | `tests/` | Updated to canonical acquisition imports |
| `ntsb-report-flow` tests | `tests/` | Updated toward canonical imports |
| `ntsb-report-flow/scripts/*.py` | `scripts/*.py` | Updated to import canonical packages |
| `ntsb-report-flow/configs/settings.yaml` | `configs/settings.yaml` | Rebased to canonical repo-local, env-overridable root |

## Import Surface

- The canonical Python import surface is `incident_pipeline.*`.
- `src/incident_pipeline/triage/primary_docket_narrative.py` re-exports the narrative selector for compatibility while the canonical implementation lives under `incident_pipeline.narrative`.

## Out Of Scope In This Migration

- `ntsb-report-flow/src/retrieve`
- `ntsb-report-flow/src/serve`
- `ntsb-report-flow/src/ocr`
- `ntsb-report-flow/src/embed`
- `ntsb-report-flow/src/chunk`

Those areas do not map cleanly into the requested tight scope, so the unified repo keeps only the target stage scaffolds and leaves future integration as TODO work.

## Phase Summaries

### Phase 1

- inventoried both source repos
- identified a partially consolidated starting workspace
- mapped the acquisition, ingestion, triage, narrative, and extract modules into canonical destinations

### Phase 2

- scaffolded `src/incident_pipeline/...`
- added package `__init__` files
- created the migration and architecture docs
- started `MIGRATION_STATUS.md`

### Phase 3

- copied acquisition code into `src/incident_pipeline/acquisition/ntsb`
- copied downstream NTSB stages into `src/incident_pipeline/ingestion`, `triage`, `narrative`, and `extract`
- added minimal compatibility wrappers instead of redesigning interfaces
