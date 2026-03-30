from __future__ import annotations

import json
from pathlib import Path

import yaml

from incident_pipeline.common.stage_runs import sha256_file
from incident_pipeline.extract.atomic_contract import PROMPT_VERSION, SentenceSpan
from incident_pipeline.extract.atomic_extract import (
    run_atomic_extraction_live,
    run_atomic_extraction_openai_batch,
)
from incident_pipeline.extract.openai_atomic_inference import (
    TransientOpenAIError,
    build_batch_request_line,
    build_responses_api_request,
)


def namespace_root(storage_root: Path) -> Path:
    return storage_root / "ntsb"


def relpath(path: Path, storage_root: Path) -> str:
    return str(path.relative_to(namespace_root(storage_root)))


def write_config(
    config_path: Path,
    sentence_span_root: Path,
    output_root: Path,
    *,
    model: str,
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
            "require_certified_input": True,
            "prompt_version": PROMPT_VERSION,
            "model": model,
            "model_contract_version": "sentence-to-claims-v1",
            "ontology_version": "claims-ontology-v1",
            "mapping_contract_version": "claim-mapping-v1",
        },
    }
    config_path.write_text(yaml.safe_dump(config), encoding="utf-8")


def write_sentence_spans(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "\n".join(json.dumps(row, sort_keys=True) for row in rows) + "\n",
        encoding="utf-8",
    )


def write_certified_sentence_span_run(
    sentence_span_root: Path,
    rows: list[dict[str, object]],
    *,
    run_id: str = "sentence_span_generation_2026-03-29T12:00:00.000000Z_deadbeef",
) -> Path:
    run_dir = sentence_span_root / "runs" / run_id
    sentence_spans_path = run_dir / "sentence_spans.jsonl"
    write_sentence_spans(sentence_spans_path, rows)
    (run_dir / "sentence_span_run_summary.json").write_text(
        json.dumps(
            {
                "run_id": run_id,
                "stage_name": "sentence_span_generation",
                "certification_status": "certified",
                "certified_at": "2026-03-29T12:00:01Z",
                "primary_output_count": len(rows),
                "primary_output_digest": sha256_file(sentence_spans_path),
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    (run_dir / "_CERTIFIED").write_text("", encoding="utf-8")
    return run_dir


def make_sentence_span(*, sentence_span_id: str, sentence_text: str) -> dict[str, object]:
    return {
        "sentence_span_id": sentence_span_id,
        "artifact_id": "artifact-1",
        "case_id": "case-1",
        "parent_structural_span_id": "section-1",
        "locator": {"page": 1, "sentence_index": 0, "section_label": "Findings"},
        "sentence_text": sentence_text,
        "sentence_index": 1,
        "segmentation_version": "sentence-split-v1",
        "provenance": {
            "artifact_checksum": "sha256:artifact-1",
            "text_extraction_version": "extract:pypdf",
            "segmentation_version": "sentence-split-v1",
        },
        "context": {
            "preceding_text": "Previous sentence.",
            "following_text": "Following sentence.",
        },
    }


def make_core_claim(
    *,
    sentence_span_id: str,
    claim_id: str = "claim-1",
    predicate: str = "caused",
    subject_text: str = "The pipe",
    object_text: str | None = "the failure",
) -> dict[str, object]:
    return {
        "claim_id": claim_id,
        "sentence_span_id": sentence_span_id,
        "parent_structural_span_id": "section-1",
        "artifact_id": "artifact-1",
        "subject_text": subject_text,
        "subject_ref": None,
        "predicate": predicate,
        "object_text": object_text,
        "object_ref": None,
        "assertion_mode": "concluded",
        "polarity": "affirmed",
        "predicate_status": "core",
        "predicate_raw": None,
        "predicate_candidate": None,
        "context_ref": f"context:{sentence_span_id}",
    }


def make_candidate_claim(*, sentence_span_id: str, claim_id: str = "claim-2") -> dict[str, object]:
    return {
        "claim_id": claim_id,
        "sentence_span_id": sentence_span_id,
        "parent_structural_span_id": "section-1",
        "artifact_id": "artifact-1",
        "subject_text": "Corrosion",
        "subject_ref": None,
        "predicate": "weakened",
        "object_text": "the wall",
        "object_ref": None,
        "assertion_mode": "observed",
        "polarity": "affirmed",
        "predicate_status": "candidate",
        "predicate_raw": "weakened the wall",
        "predicate_candidate": "weakened",
        "context_ref": f"context:{sentence_span_id}",
    }


def read_json(path: Path) -> dict[str, object]:
    return json.loads(path.read_text(encoding="utf-8"))


def _response_payload(result_mapping: dict[str, object], *, request_id: str) -> dict[str, object]:
    return {
        "id": request_id,
        "status": "completed",
        "output": [
            {
                "type": "message",
                "content": [
                    {
                        "type": "output_text",
                        "text": json.dumps(result_mapping, sort_keys=True),
                    }
                ],
            }
        ],
    }


class SequenceResponsesClient:
    def __init__(self, responses: dict[str, list[object]]) -> None:
        self._responses = responses

    def create_response(self, request_body):  # type: ignore[no-untyped-def]
        sentence_span_id = request_body["metadata"]["sentence_span_id"]
        item = self._responses[sentence_span_id].pop(0)
        if isinstance(item, Exception):
            raise item
        return item


class StaticBatchClient:
    def __init__(self, *, output_text: str, error_text: str = "") -> None:
        self.output_text = output_text
        self.error_text = error_text
        self.submitted_path: Path | None = None

    def submit_batch(self, request_manifest_path, *, completion_window, metadata):  # type: ignore[no-untyped-def]
        self.submitted_path = request_manifest_path
        return {
            "id": "batch-123",
            "input_file_id": "file-input-123",
            "status": "validating",
            "metadata": dict(metadata),
        }

    def wait_for_batch(self, batch_id, *, poll_interval_seconds, max_polls):  # type: ignore[no-untyped-def]
        return {
            "id": batch_id,
            "status": "completed",
            "input_file_id": "file-input-123",
            "output_file_id": "file-output-123",
            "error_file_id": "file-error-123" if self.error_text else None,
        }

    def download_file_text(self, file_id):  # type: ignore[no-untyped-def]
        if file_id == "file-output-123":
            return self.output_text
        return self.error_text


def test_live_and_batch_requests_share_the_same_schema() -> None:
    sentence_span = make_sentence_span(
        sentence_span_id="span-1",
        sentence_text="Corrosion weakened the wall because the coating failed.",
    )
    live_request = build_responses_api_request(
        build_span(sentence_span),
        model="gpt-test",
    )
    batch_request = build_batch_request_line(
        build_span(sentence_span),
        model="gpt-test",
    )

    assert live_request["text"]["format"]["schema"] == batch_request["body"]["text"]["format"]["schema"]


def build_span(mapping: dict[str, object]):
    return SentenceSpan.from_mapping(mapping)


def test_run_atomic_extraction_live_retries_transient_failures_and_certifies(
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
                sentence_text="Corrosion weakened the wall because the coating failed.",
            )
        ],
    )
    write_config(config_path, sentence_span_root, output_root, model="gpt-test")

    result_mapping = {
        "sentence_span_id": "span-1",
        "status": "ok",
        "atomic_claims": [
            make_core_claim(sentence_span_id="span-1"),
            make_candidate_claim(sentence_span_id="span-1"),
        ],
        "ontology_candidates": [
            {
                "candidate_id": "candidate-1",
                "candidate_type": "mechanism",
                "candidate_text": "coating failure",
                "linked_claim_ids": ["claim-1"],
                "mechanism_class": "degradation",
                "needs_review": False,
            }
        ],
        "unresolved": [],
        "warnings": [],
    }
    client = SequenceResponsesClient(
        {
            "span-1": [
                TransientOpenAIError("rate limited", status_code=429),
                _response_payload(result_mapping, request_id="resp-1"),
            ]
        }
    )

    summary = run_atomic_extraction_live(
        config_path,
        client=client,
        max_retries=1,
        retry_base_delay_seconds=0.0,
    )

    assert summary == {
        "selected": 1,
        "completed": 1,
        "unprocessable": 0,
        "failed": 0,
    }
    run_dir = next(path for path in (output_root / "runs").iterdir() if path.is_dir())
    run_summary = read_json(run_dir / "atomic_run_summary.json")
    assert run_summary["certification_status"] == "certified"
    assert run_summary["execution_mode"] == "responses_api"
    assert run_summary["model"] == "gpt-test"
    inference_metadata = read_json(run_dir / "inference_metadata.json")
    assert inference_metadata["execution_mode"] == "responses_api"
    assert inference_metadata["live_requests"][0]["attempt_count"] == 2
    assert inference_metadata["live_requests"][0]["retry_status_codes"] == [429]


def test_run_atomic_extraction_openai_batch_reconciles_results_and_preserves_metadata(
    tmp_path: Path,
) -> None:
    namespaced_root = namespace_root(tmp_path)
    sentence_span_root = namespaced_root / "extract" / "sentence_spans"
    output_root = namespaced_root / "extract" / "atomic"
    config_path = tmp_path / "settings.yaml"
    upstream_run_dir = write_certified_sentence_span_run(
        sentence_span_root,
        [
            make_sentence_span(
                sentence_span_id="span-1",
                sentence_text="Corrosion weakened the wall because the coating failed.",
            ),
            make_sentence_span(
                sentence_span_id="span-2",
                sentence_text="Inspectors reported paint loss.",
            ),
        ],
    )
    write_config(config_path, sentence_span_root, output_root, model="gpt-test")

    output_lines = [
        json.dumps(
            {
                "id": "batch_req_1",
                "custom_id": "span-2",
                "response": {
                    "status_code": 200,
                    "request_id": "req-2",
                    "body": _response_payload(
                        {
                            "sentence_span_id": "span-2",
                            "status": "ok",
                            "atomic_claims": [
                                make_core_claim(
                                    sentence_span_id="span-2",
                                    predicate="reported",
                                    subject_text="Inspectors",
                                    object_text="paint loss",
                                )
                            ],
                            "ontology_candidates": [],
                            "unresolved": [],
                            "warnings": [],
                        },
                        request_id="resp-2",
                    ),
                },
                "error": None,
            }
        ),
        json.dumps(
            {
                "id": "batch_req_2",
                "custom_id": "span-1",
                "response": {
                    "status_code": 200,
                    "request_id": "req-1",
                    "body": _response_payload(
                        {
                            "sentence_span_id": "span-1",
                            "status": "ok",
                            "atomic_claims": [
                                make_core_claim(sentence_span_id="span-1")
                            ],
                            "ontology_candidates": [],
                            "unresolved": [],
                            "warnings": [],
                        },
                        request_id="resp-1",
                    ),
                },
                "error": None,
            }
        ),
    ]
    client = StaticBatchClient(output_text="\n".join(output_lines) + "\n")

    summary = run_atomic_extraction_openai_batch(config_path, client=client)

    assert summary == {
        "selected": 2,
        "completed": 2,
        "unprocessable": 0,
        "failed": 0,
    }
    assert client.submitted_path is not None
    run_dir = next(path for path in (output_root / "runs").iterdir() if path.is_dir())
    run_summary = read_json(run_dir / "atomic_run_summary.json")
    assert run_summary["certification_status"] == "certified"
    assert run_summary["execution_mode"] == "batch_api"
    assert run_summary["upstream_run_id"] == upstream_run_dir.name
    inference_metadata = read_json(run_dir / "inference_metadata.json")
    assert inference_metadata["execution_mode"] == "batch_api"
    assert inference_metadata["batch_job"]["batch_id"] == "batch-123"
    batch_requests_path = Path(inference_metadata["batch_requests_path"])
    assert batch_requests_path.exists()
    assert sha256_file(batch_requests_path)
