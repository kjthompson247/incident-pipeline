# Replay Reconciliation — Contract

Status: ACTIVE  
Version: v0.1  

## Purpose

Define how replayed extraction outputs supersede failed records from prior runs without mutating original artifacts.

---

## Core Rules

1. Runs are immutable
- No prior run artifacts may be modified after completion

2. Replay is selective
- Only failed records from a prior run are eligible for replay

3. Supersession
A replayed record supersedes a prior failed record only if:
- record_id matches exactly
- replay output passes validation
- replay run is certified

4. No implicit overwrite
- Original failed records remain in place
- Supersession is recorded explicitly

---

## Reconciliation Artifact

A reconciliation manifest must record:

- original_run_id
- replay_run_id
- record_id
- original_status
- replay_status
- reconciliation_status
- canonical_source_run_id
- timestamp

---

## Canonical Output View

Canonical outputs are constructed as:

- original successful records
- plus replay-certified replacements
- excluding superseded failed records

---

## Provenance

Each canonical record must retain:

- original_run_id
- replay_run_id (if applicable)
- canonical_source_run_id
- reconciliation_status

---

## Non-Negotiable

- No mutation of prior runs
- No use of uncertified replay outputs
- No loss of provenance
- No implicit merging without manifest