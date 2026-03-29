from __future__ import annotations

import json
from pathlib import Path

import yaml

from incident_pipeline.common.stage_runs import sha256_file
from incident_pipeline.extract.atomic_contract import SentenceSpan, build_api_request
from incident_pipeline.extract.atomic_extract import run_atomic_extraction_batch


def namespace_root(storage_root: Path) -> Path:
    return storage_root / "ntsb"


def relpath(path: Path, storage_root: Path) -> str:
    return str(path.relative_to(namespace_root(storage_root)))


def write_sentence_spans(path: Path, rows: list[dict[str, object] | str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    rendered: list[str] = []
    for row in rows:
        if isinstance(row, str):
            rendered.append(row)
        else:
            rendered.append(json.dumps(row, sort_keys=True))
    path.write_text("\n".join(rendered) + "\n", encoding="utf-8")


def write_config(
    config_path: Path,
    sentence_span_root: Path,
    output_root: Path,
    *,
    require_certified_input: bool = True,
) -> None:
    storage_root = config_path.parent
    config = {
        "paths": {
            "storage_root": str(storage_root),
            "storage_namespace": "ntsb",
        },
        "atomic_extraction": {
            "output_root": relpath(output_root, storage_root),
            "sentence_span_root": relpath(sentence_span_root, storage_root),
            "batch_size": 2,
            "require_certified_input": require_certified_input,
            "model_contract_version": "sentence-to-atomic-v1",
            "ontology_version": "minimal-ontology-v0.2",
            "mapping_contract_version": "mapping-contract-v0.1",
        }
    }
    config_path.write_text(yaml.safe_dump(config), encoding="utf-8")


def write_certified_sentence_span_run(
    sentence_span_root: Path,
    rows: list[dict[str, object]],
    *,
    run_id: str = "sentence_span_generation_2026-03-29T12:00:00.000000Z_deadbeef",
) -> Path:
    run_dir = sentence_span_root / "runs" / run_id
    sentence_spans_path = run_dir / "sentence_spans.jsonl"
    write_sentence_spans(sentence_spans_path, rows)
    run_summary = {
        "run_id": run_id,
        "stage_name": "sentence_span_generation",
        "stage_version": "sentence_spans_v1",
        "timestamps": {
            "start": "2026-03-29T12:00:00Z",
            "end": "2026-03-29T12:00:01Z",
        },
        "run_status": "completed",
        "validation_status": "passed",
        "certification_status": "certified",
        "input_record_count": len(rows),
        "input_digest": "fixture-input-digest",
        "output_root": str(sentence_span_root),
        "primary_output_path": str(sentence_spans_path),
        "primary_output_count": len(rows),
        "primary_output_digest": sha256_file(sentence_spans_path),
        "metrics_path": str(run_dir / "sentence_span_metrics.json"),
        "failures_path": str(run_dir / "sentence_span_failures.jsonl"),
        "validation_report_path": str(run_dir / "sentence_span_validation_report.json"),
        "records_seen": len(rows),
        "records_succeeded": len(rows),
        "records_failed": 0,
        "records_skipped": 0,
        "failure_mode": "fail_on_any_record_error",
        "max_failure_count": 0,
        "max_failure_rate": 0.0,
        "code_version": "deadbeef",
        "config_version": "config-digest",
        "runtime_parameters_digest": "runtime-digest",
        "rule_set_version": "sentence_span_stage_certification_v1",
        "certified_at": "2026-03-29T12:00:01Z",
        "blocking_issues": [],
        "segmentation_version": "sentence-split-v1",
    }
    (run_dir / "sentence_span_run_summary.json").write_text(
        json.dumps(run_summary, indent=2) + "\n",
        encoding="utf-8",
    )
    (run_dir / "sentence_span_metrics.json").write_text(
        json.dumps({"sentences_generated": len(rows)}, indent=2) + "\n",
        encoding="utf-8",
    )
    (run_dir / "sentence_span_validation_report.json").write_text(
        json.dumps({"summary": {"rules_evaluated": 0, "rules_passed": 0, "rules_failed": 0, "warnings": 0}, "rules": []}, indent=2)
        + "\n",
        encoding="utf-8",
    )
    (run_dir / "sentence_span_failures.jsonl").write_text("", encoding="utf-8")
    (run_dir / "_CERTIFIED").write_text("", encoding="utf-8")
    return run_dir


def make_sentence_span(
    *,
    sentence_span_id: str,
    sentence_text: str,
) -> dict[str, object]:
    return {
        "sentence_span_id": sentence_span_id,
        "artifact_id": "artifact-1",
        "case_id": "case-1",
        "parent_structural_span_id": "section-1",
        "locator": {"page": 1, "sentence_index": 0},
        "sentence_text": sentence_text,
        "sentence_index": 1,
        "segmentation_version": "sentence-split-v1",
        "provenance": {
            "artifact_checksum": "sha256:artifact-1",
            "text_extraction_version": "extract:pypdf",
            "segmentation_version": "sentence-split-v1",
        },
        "context": {"preceding_text": "", "following_text": ""},
    }


def read_json(path: Path) -> dict[str, object]:
    return json.loads(path.read_text(encoding="utf-8"))


def read_jsonl(path: Path) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if line.strip():
            rows.append(json.loads(line))
    return rows


def test_run_atomic_extraction_batch_writes_certified_run_artifacts(tmp_path: Path) -> None:
    namespaced_root = namespace_root(tmp_path)
    sentence_span_root = namespaced_root / "extract" / "sentence_spans"
    output_root = namespaced_root / "extract" / "atomic"
    config_path = tmp_path / "settings.yaml"
    upstream_run_dir = write_certified_sentence_span_run(
        sentence_span_root,
        [
            make_sentence_span(
                sentence_span_id="span-1",
                sentence_text="The pipe failed because corrosion weakened the wall.",
            ),
            make_sentence_span(
                sentence_span_id="span-2",
                sentence_text="Valve sequencing is ambiguous in this sentence.",
            ),
        ],
    )
    write_config(config_path, sentence_span_root, output_root)

    def fake_transform(span):  # type: ignore[no-untyped-def]
        if span.sentence_span_id == "span-1":
            return {
                "sentence_span_id": span.sentence_span_id,
                "status": "ok",
                "atomic_claims": [
                    {
                        "atomic_claim_id": "claim-1",
                        "claim_text": "The pipe failed because corrosion weakened the wall.",
                        "assertion_mode": "concluded",
                        "polarity": "affirmed",
                        "claim_type": "conclusion",
                        "needs_review": False,
                    }
                ],
                "ontology_candidates": [
                    {
                        "candidate_id": "candidate-1",
                        "candidate_type": "causal_factors",
                        "candidate_text": "corrosion weakened the wall",
                        "linked_claim_ids": ["claim-1"],
                        "needs_review": False,
                    }
                ],
                "unresolved": [],
                "warnings": [],
            }

        return {
            "sentence_span_id": span.sentence_span_id,
            "status": "unprocessable",
            "atomic_claims": [],
            "ontology_candidates": [],
            "unresolved": [
                {
                    "code": "ambiguous_coordination",
                    "message": "Coordination is ambiguous and cannot be safely split.",
                }
            ],
            "warnings": ["manual-review"],
        }

    summary = run_atomic_extraction_batch(config_path, transform_span=fake_transform)

    assert summary == {
        "selected": 2,
        "completed": 2,
        "unprocessable": 1,
        "failed": 0,
    }
    run_dirs = sorted(path for path in (output_root / "runs").iterdir() if path.is_dir())
    assert len(run_dirs) == 1
    run_dir = run_dirs[0]
    assert (run_dir / "_CERTIFIED").exists()

    run_summary = read_json(run_dir / "atomic_run_summary.json")
    assert run_summary["stage_name"] == "atomic_extract"
    assert run_summary["validation_status"] == "passed"
    assert run_summary["certification_status"] == "certified"
    assert run_summary["upstream_stage"] == "sentence_span_generation"
    assert run_summary["upstream_run_id"] == upstream_run_dir.name
    assert run_summary["upstream_run_dir"] == str(upstream_run_dir)
    assert run_summary["upstream_input_path"] == str(upstream_run_dir / "sentence_spans.jsonl")
    assert run_summary["primary_output_count"] == 2

    metrics = read_json(run_dir / "atomic_metrics.json")
    assert metrics["counts"] == {
        "records_seen": 2,
        "records_emitted": 2,
        "records_failed": 0,
        "records_skipped": 0,
    }
    assert metrics["distribution"]["status"] == {"ok": 1, "unprocessable": 1}

    sentence_spans = read_jsonl(run_dir / "sentence_spans.jsonl")
    extractions = read_jsonl(run_dir / "atomic_extractions.jsonl")
    failures = read_jsonl(run_dir / "atomic_failures.jsonl")
    assert len(sentence_spans) == 2
    assert len(extractions) == 2
    assert failures == []
    assert extractions[1]["status"] == "unprocessable"


def test_run_atomic_extraction_batch_records_invalid_model_payload_as_failure(
    tmp_path: Path,
) -> None:
    namespaced_root = namespace_root(tmp_path)
    sentence_span_root = namespaced_root / "extract" / "sentence_spans"
    output_root = namespaced_root / "extract" / "atomic"
    config_path = tmp_path / "settings.yaml"
    write_certified_sentence_span_run(
        sentence_span_root,
        [
            make_sentence_span(
                sentence_span_id="span-1",
                sentence_text="The operator observed a pressure excursion.",
            )
        ],
    )
    write_config(config_path, sentence_span_root, output_root)

    def invalid_transform(span):  # type: ignore[no-untyped-def]
        return {
            "sentence_span_id": span.sentence_span_id,
            "status": "ok",
            "atomic_claims": [
                {
                    "atomic_claim_id": "claim-1",
                    "claim_text": "The operator observed a pressure excursion.",
                    "assertion_mode": "observed",
                    "polarity": "affirmed",
                    "claim_type": "observation",
                    "needs_review": False,
                },
                {
                    "atomic_claim_id": "claim-1",
                    "claim_text": "Duplicate claim id should fail validation.",
                    "assertion_mode": "stated",
                    "polarity": "affirmed",
                    "claim_type": "statement",
                    "needs_review": False,
                },
            ],
            "ontology_candidates": [],
            "unresolved": [],
            "warnings": [],
        }

    summary = run_atomic_extraction_batch(config_path, transform_span=invalid_transform)

    assert summary == {
        "selected": 1,
        "completed": 0,
        "unprocessable": 0,
        "failed": 1,
    }
    run_dir = next(path for path in (output_root / "runs").iterdir() if path.is_dir())
    assert (run_dir / "_FAILED").exists()

    run_summary = read_json(run_dir / "atomic_run_summary.json")
    assert run_summary["validation_status"] == "failed"
    assert run_summary["certification_status"] == "failed"

    failures = read_jsonl(run_dir / "atomic_failures.jsonl")
    assert len(failures) == 1
    assert failures[0]["failure_class"] == "schema_validation"
    assert failures[0]["record_id"] == "span-1"

    validation_report = read_json(run_dir / "atomic_validation_report.json")
    failed_rule_ids = {
        item["rule_id"]
        for item in validation_report["rules"]
        if item["result"] == "fail"
    }
    assert "certification_failure_policy_threshold" in failed_rule_ids


def test_run_atomic_extraction_batch_fails_when_no_sentence_span_runs_exist(
    tmp_path: Path,
) -> None:
    namespaced_root = namespace_root(tmp_path)
    sentence_span_root = namespaced_root / "extract" / "sentence_spans"
    output_root = namespaced_root / "extract" / "atomic"
    config_path = tmp_path / "settings.yaml"
    write_config(config_path, sentence_span_root, output_root)

    summary = run_atomic_extraction_batch(config_path, transform_span=lambda span: {})  # type: ignore[no-untyped-call]

    assert summary == {
        "selected": 0,
        "completed": 0,
        "unprocessable": 0,
        "failed": 0,
    }
    run_dir = next(path for path in (output_root / "runs").iterdir() if path.is_dir())
    run_summary = read_json(run_dir / "atomic_run_summary.json")
    assert run_summary["validation_status"] == "failed"
    assert run_summary["certification_status"] == "failed"
    assert "No sentence_span_generation runs directory found" in "\n".join(
        str(item) for item in run_summary["blocking_issues"]
    )


def test_run_atomic_extraction_batch_fails_when_no_certified_sentence_span_runs_exist(
    tmp_path: Path,
) -> None:
    namespaced_root = namespace_root(tmp_path)
    sentence_span_root = namespaced_root / "extract" / "sentence_spans"
    output_root = namespaced_root / "extract" / "atomic"
    config_path = tmp_path / "settings.yaml"
    uncertified_run_dir = sentence_span_root / "runs" / "sentence_span_generation_2026-03-29T12:00:00.000000Z_uncert"
    write_sentence_spans(
        uncertified_run_dir / "sentence_spans.jsonl",
        [
            make_sentence_span(
                sentence_span_id="span-1",
                sentence_text="A governed sentence span should require certified upstream evidence.",
            )
        ],
    )
    (uncertified_run_dir / "_FAILED").write_text("", encoding="utf-8")
    write_config(config_path, sentence_span_root, output_root, require_certified_input=True)

    summary = run_atomic_extraction_batch(config_path, transform_span=lambda span: {})  # type: ignore[no-untyped-call]

    assert summary == {
        "selected": 0,
        "completed": 0,
        "unprocessable": 0,
        "failed": 0,
    }
    run_dir = next(path for path in (output_root / "runs").iterdir() if path.is_dir())
    run_summary = read_json(run_dir / "atomic_run_summary.json")
    assert run_summary["validation_status"] == "failed"
    assert run_summary["certification_status"] == "failed"
    blocking_issues = "\n".join(run_summary["blocking_issues"])
    assert "No certified" in blocking_issues
    assert "sentence_span_generation" in blocking_issues
    assert (run_dir / "_FAILED").exists()


def test_run_atomic_extraction_batch_fails_when_certified_run_is_missing_input_file(
    tmp_path: Path,
) -> None:
    namespaced_root = namespace_root(tmp_path)
    sentence_span_root = namespaced_root / "extract" / "sentence_spans"
    output_root = namespaced_root / "extract" / "atomic"
    config_path = tmp_path / "settings.yaml"
    certified_run_dir = sentence_span_root / "runs" / "sentence_span_generation_2026-03-29T12:00:00.000000Z_missing"
    certified_run_dir.mkdir(parents=True, exist_ok=True)
    (certified_run_dir / "sentence_span_run_summary.json").write_text(
        json.dumps(
            {
                "run_id": certified_run_dir.name,
                "stage_name": "sentence_span_generation",
                "certification_status": "certified",
                "certified_at": "2026-03-29T12:00:01Z",
                "primary_output_count": 1,
                "primary_output_digest": "missing",
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    (certified_run_dir / "_CERTIFIED").write_text("", encoding="utf-8")
    write_config(config_path, sentence_span_root, output_root)

    summary = run_atomic_extraction_batch(config_path, transform_span=lambda span: {})  # type: ignore[no-untyped-call]

    assert summary == {
        "selected": 0,
        "completed": 0,
        "unprocessable": 0,
        "failed": 0,
    }
    run_dir = next(path for path in (output_root / "runs").iterdir() if path.is_dir())
    run_summary = read_json(run_dir / "atomic_run_summary.json")
    assert run_summary["validation_status"] == "failed"
    assert run_summary["certification_status"] == "failed"
    assert "missing sentence_spans.jsonl" in "\n".join(
        str(item) for item in run_summary["blocking_issues"]
    )


def test_run_atomic_extraction_batch_rejects_configured_input_path(
    tmp_path: Path,
) -> None:
    namespaced_root = namespace_root(tmp_path)
    sentence_span_root = namespaced_root / "extract" / "sentence_spans"
    output_root = namespaced_root / "extract" / "atomic"
    config_path = tmp_path / "settings.yaml"
    config = {
        "paths": {
            "storage_root": str(tmp_path),
            "storage_namespace": "ntsb",
        },
        "atomic_extraction": {
            "output_root": relpath(output_root, tmp_path),
            "sentence_span_root": relpath(sentence_span_root, tmp_path),
            "input_path": "extract/sentence_spans/runs/legacy/sentence_spans.jsonl",
            "batch_size": 2,
            "require_certified_input": True,
            "model_contract_version": "sentence-to-atomic-v1",
            "ontology_version": "minimal-ontology-v0.2",
            "mapping_contract_version": "mapping-contract-v0.1",
        },
    }
    config_path.write_text(yaml.safe_dump(config), encoding="utf-8")

    try:
        run_atomic_extraction_batch(config_path, transform_span=lambda span: {})  # type: ignore[no-untyped-call]
    except ValueError as exc:
        message = str(exc)
    else:
        raise AssertionError("Expected ValueError for deprecated atomic_extraction.input_path")

    assert "atomic_extraction.input_path is no longer supported" in message


def test_build_api_request_uses_governed_contract_versions() -> None:
    payload = build_api_request(
        job_id="job-1",
        items=[
            SentenceSpan.from_mapping(
                make_sentence_span(
                    sentence_span_id="span-1",
                    sentence_text="One claim sentence.",
                )
            )
        ],
    )

    assert payload["job_id"] == "job-1"
    assert payload["model_contract_version"] == "sentence-to-atomic-v1"
    assert payload["ontology_version"] == "minimal-ontology-v0.2"
    assert payload["mapping_contract_version"] == "mapping-contract-v0.1"
    assert len(payload["items"]) == 1
