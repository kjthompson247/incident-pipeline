# Migration Status

## Completed

- Phase 1 inventory and mapping for `ntsb-acquire` and `ntsb-report-flow`
- Phase 2 repository scaffold for `docs/`, `src/incident_pipeline/`, `sql/`, and package `__init__` files
- Phase 3 code copy into canonical stage/source packages for acquisition, ingestion, triage, narrative, and extract
- Phase 4 shared path/config normalization through `incident_pipeline.common.paths` and canonical `configs/settings.yaml`
- Phase 5 validation run with `134` passing tests

## In Progress

- Phase 6 bootstrap hardening to a single `uv` workflow on Python `3.12.13`, repo-local defaults, and OCR-disabled config defaults
- Generating `uv.lock` and validating a full clean `uv sync` in an environment with package index access

## Blocked

- Clean `uv lock` and `uv sync` validation require access to PyPI or an internal package mirror; this workspace cannot reach `https://pypi.org/simple/`

## Open Questions

- `retrieve`, `serve`, `ocr`, `embed`, and `chunk` from `ntsb-report-flow` remain intentionally out of scope for this constrained migration
- `index/` is scaffolded only until a later phase decides how chunking and embedding should map into the unified repository
