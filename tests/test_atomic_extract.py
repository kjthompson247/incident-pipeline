from __future__ import annotations

import json
from pathlib import Path

import yaml

from incident_pipeline.extract.atomic_contract import SentenceSpan, build_api_request
from incident_pipeline.extract.atomic_extract import run_atomic_extraction_batch


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
    input_path: Path,
    output_root: Path,
    *,
    require_certified_input: bool = False,
) -> None:
    config = {
        "atomic_extraction": {
            "input_path": str(input_path),
            "output_root": str(output_root),
            "batch_size": 2,
            "require_certified_input": require_certified_input,
            "model_contract_version": "sentence-to-atomic-v1",
            "ontology_version": "minimal-ontology-v0.2",
            "mapping_contract_version": "mapping-contract-v0.1",
        }
    }
    config_path.write_text(yaml.safe_dump(config), encoding="utf-8")


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
    input_path = tmp_path / "sentence_spans.jsonl"
    output_root = tmp_path / "extract" / "atomic"
    config_path = tmp_path / "settings.yaml"
    write_sentence_spans(
        input_path,
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
    write_config(config_path, input_path, output_root)

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
    input_path = tmp_path / "sentence_spans.jsonl"
    output_root = tmp_path / "extract" / "atomic"
    config_path = tmp_path / "settings.yaml"
    write_sentence_spans(
        input_path,
        [
            make_sentence_span(
                sentence_span_id="span-1",
                sentence_text="The operator observed a pressure excursion.",
            )
        ],
    )
    write_config(config_path, input_path, output_root)

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


def test_run_atomic_extraction_batch_preserves_only_valid_sentence_spans_in_run_artifact(
    tmp_path: Path,
) -> None:
    input_path = tmp_path / "sentence_spans.jsonl"
    output_root = tmp_path / "extract" / "atomic"
    config_path = tmp_path / "settings.yaml"
    write_sentence_spans(
        input_path,
        [
            '{"sentence_span_id": "broken"',
            make_sentence_span(
                sentence_span_id="span-2",
                sentence_text="Inspectors observed coating damage.",
            ),
        ],
    )
    write_config(config_path, input_path, output_root)

    def fake_transform(span):  # type: ignore[no-untyped-def]
        return {
            "sentence_span_id": span.sentence_span_id,
            "status": "ok",
            "atomic_claims": [
                {
                    "atomic_claim_id": "claim-1",
                    "claim_text": "Inspectors observed coating damage.",
                    "assertion_mode": "observed",
                    "polarity": "affirmed",
                    "claim_type": "observation",
                    "needs_review": False,
                }
            ],
            "ontology_candidates": [],
            "unresolved": [],
            "warnings": [],
        }

    summary = run_atomic_extraction_batch(config_path, transform_span=fake_transform)

    assert summary == {
        "selected": 2,
        "completed": 1,
        "unprocessable": 0,
        "failed": 1,
    }
    run_dir = next(path for path in (output_root / "runs").iterdir() if path.is_dir())
    sentence_spans = read_jsonl(run_dir / "sentence_spans.jsonl")
    failures = read_jsonl(run_dir / "atomic_failures.jsonl")
    assert [row["sentence_span_id"] for row in sentence_spans] == ["span-2"]
    assert len(failures) == 1
    assert failures[0]["record_id"] == "line:1"


def test_run_atomic_extraction_batch_rejects_uncertified_sentence_spans_by_default(
    tmp_path: Path,
) -> None:
    input_path = tmp_path / "sentence_spans.jsonl"
    output_root = tmp_path / "extract" / "atomic"
    config_path = tmp_path / "settings.yaml"
    write_sentence_spans(
        input_path,
        [
            make_sentence_span(
                sentence_span_id="span-1",
                sentence_text="A governed sentence span should require certified upstream evidence.",
            )
        ],
    )
    write_config(
        config_path,
        input_path,
        output_root,
        require_certified_input=True,
    )

    summary = run_atomic_extraction_batch(config_path, transform_span=lambda span: {})  # type: ignore[no-untyped-call]

    assert summary == {
        "selected": 1,
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
