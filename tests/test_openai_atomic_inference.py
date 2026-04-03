from __future__ import annotations

import json
import logging
from pathlib import Path
from threading import Lock
import time

import httpx
import yaml

from incident_pipeline.common.stage_runs import sha256_file
from incident_pipeline.extract.atomic_contract import PROMPT_VERSION, SentenceSpan
from incident_pipeline.extract.atomic_extract import (
    run_atomic_extraction_live,
    run_atomic_extraction_openai_batch,
)
from incident_pipeline.extract.openai_atomic_inference import (
    OpenAIResponsesAPIClient,
    TransientOpenAIError,
    build_atomic_prompt,
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
    live_requests_per_minute: float = 1000000.0,
    live_max_workers: int = 1,
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
            "live_requests_per_minute": live_requests_per_minute,
            "live_max_workers": live_max_workers,
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
        self._lock = Lock()

    def create_response(self, request_body):  # type: ignore[no-untyped-def]
        sentence_span_id = request_body["metadata"]["sentence_span_id"]
        with self._lock:
            item = self._responses[sentence_span_id].pop(0)
        if isinstance(item, Exception):
            raise item
        return item


class ConcurrentResponsesClient:
    def __init__(
        self,
        responses: dict[str, list[object]],
        *,
        per_call_delay_seconds: float = 0.0,
    ) -> None:
        self._responses = responses
        self._per_call_delay_seconds = per_call_delay_seconds
        self._lock = Lock()
        self.active_calls = 0
        self.max_active_calls = 0

    def create_response(self, request_body):  # type: ignore[no-untyped-def]
        sentence_span_id = request_body["metadata"]["sentence_span_id"]
        with self._lock:
            self.active_calls += 1
            if self.active_calls > self.max_active_calls:
                self.max_active_calls = self.active_calls
            item = self._responses[sentence_span_id].pop(0)
        try:
            if self._per_call_delay_seconds > 0:
                time.sleep(self._per_call_delay_seconds)
            if isinstance(item, Exception):
                raise item
            return item
        finally:
            with self._lock:
                self.active_calls -= 1


class StaticResponsesTransportClient:
    def __init__(self, response: httpx.Response) -> None:
        self._response = response

    def post(self, url: str, json):  # type: ignore[no-untyped-def]
        return self._response


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


def test_atomic_prompt_explicitly_requires_unprocessable_for_non_assertive_fragments() -> None:
    prompt = build_atomic_prompt()

    assert "label, heading, checkbox option, form field title" in prompt
    assert "If no explicit subject can be grounded" in prompt
    assert "predicate must exactly equal predicate_candidate" in prompt


def build_span(mapping: dict[str, object]):
    return SentenceSpan.from_mapping(mapping)


def test_openai_responses_client_parses_retry_after_and_request_id() -> None:
    request = httpx.Request("POST", "https://api.openai.com/v1/responses")
    response = httpx.Response(
        429,
        request=request,
        headers={
            "Retry-After": "7",
            "x-request-id": "req-429",
        },
        json={"error": {"message": "Too many requests"}},
    )
    client = OpenAIResponsesAPIClient(api_key="test-key")
    client._client = StaticResponsesTransportClient(response)  # type: ignore[assignment]

    try:
        client.create_response({"model": "gpt-test"})
    except TransientOpenAIError as exc:
        assert exc.status_code == 429
        assert exc.request_id == "req-429"
        assert exc.retry_after_seconds == 7
        assert exc.error_kind == "rate_limited_429"
        assert "Too many requests" in str(exc)
    else:
        raise AssertionError("Expected TransientOpenAIError for 429 response")


def test_openai_responses_client_parses_retry_after_from_provider_message() -> None:
    request = httpx.Request("POST", "https://api.openai.com/v1/responses")
    response = httpx.Response(
        429,
        request=request,
        headers={
            "x-request-id": "req-429-message",
        },
        json={
            "error": {
                "message": "Rate limit exceeded for requests per minute. Please try again in 20s."
            }
        },
    )
    client = OpenAIResponsesAPIClient(api_key="test-key")
    client._client = StaticResponsesTransportClient(response)  # type: ignore[assignment]

    try:
        client.create_response({"model": "gpt-test"})
    except TransientOpenAIError as exc:
        assert exc.status_code == 429
        assert exc.request_id == "req-429-message"
        assert exc.retry_after_seconds == 20
        assert exc.provider_message == (
            "Rate limit exceeded for requests per minute. Please try again in 20s."
        )
    else:
        raise AssertionError("Expected TransientOpenAIError for provider message retry guidance")


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


def test_run_atomic_extraction_live_honors_retry_after_and_records_retry_history(
    tmp_path: Path,
    monkeypatch,
    caplog,
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
        "atomic_claims": [make_core_claim(sentence_span_id="span-1")],
        "ontology_candidates": [],
        "unresolved": [],
        "warnings": [],
    }
    client = SequenceResponsesClient(
        {
            "span-1": [
                TransientOpenAIError(
                    "rate limited",
                    status_code=429,
                    request_id="req-429",
                    retry_after_seconds=5,
                    error_kind="rate_limited_429",
                ),
                _response_payload(result_mapping, request_id="resp-1"),
            ]
        }
    )
    sleeps: list[float] = []
    monkeypatch.setattr(
        "incident_pipeline.extract.atomic_extract.sleep",
        lambda value: sleeps.append(value),
    )
    monkeypatch.setattr(
        "incident_pipeline.extract.atomic_extract.random.uniform",
        lambda _low, _high: 0.25,
    )
    caplog.set_level(logging.INFO, logger="incident_pipeline.extract.live")

    summary = run_atomic_extraction_live(
        config_path,
        client=client,
        max_retries=1,
        retry_base_delay_seconds=1.0,
    )

    assert summary == {
        "selected": 1,
        "completed": 1,
        "unprocessable": 0,
        "failed": 0,
    }
    assert sleeps == [5.0]
    run_dir = next(path for path in (output_root / "runs").iterdir() if path.is_dir())
    inference_metadata = read_json(run_dir / "inference_metadata.json")
    assert inference_metadata["max_retries"] == 1
    assert inference_metadata["retry_base_delay_seconds"] == 1.0
    request_trace = inference_metadata["live_requests"][0]
    assert request_trace["retry_status_codes"] == [429]
    assert request_trace["retry_after_seconds_seen"] == [5]
    assert request_trace["final_outcome_classification"] == "success"
    assert request_trace["retry_history"] == [
        {
            "attempt_number": 1,
            "status_code": 429,
            "request_id": "req-429",
            "error_kind": "rate_limited_429",
            "retry_after_seconds": 5,
            "provider_retry_after_seconds": 5,
            "provider_retry_after_applied": True,
            "computed_backoff_seconds": 1.0,
            "jitter_seconds": 0.25,
            "sleep_seconds_applied": 5.0,
        }
    ]
    retry_logs = [
        json.loads(record.getMessage())
        for record in caplog.records
        if '"event": "live_request_retry_scheduled"' in record.getMessage()
    ]
    assert len(retry_logs) == 1
    assert retry_logs[0]["artifact_id"] == "artifact-1"
    assert retry_logs[0]["attempt"] == 1
    assert retry_logs[0]["computed_backoff_seconds"] == 1.0
    assert retry_logs[0]["event"] == "live_request_retry_scheduled"
    assert retry_logs[0]["jitter_seconds"] == 0.25
    assert retry_logs[0]["provider_retry_after_applied"] is True
    assert retry_logs[0]["provider_retry_after_seconds"] == 5
    assert retry_logs[0]["request_id"] == "req-429"
    assert retry_logs[0]["retry_after_seconds"] == 5
    assert retry_logs[0]["sentence_span_id"] == "span-1"
    assert retry_logs[0]["sleep_seconds_applied"] == 5.0
    assert retry_logs[0]["status_code"] == 429
    assert retry_logs[0]["worker_name"] == "atomic-live_0"
    success_logs = [
        json.loads(record.getMessage())
        for record in caplog.records
        if '"event": "live_request_succeeded"' in record.getMessage()
    ]
    assert success_logs[0]["total_attempts"] == 2
    assert success_logs[0]["provider_retry_guidance_seen"] is True
    assert success_logs[0]["outcome"] == "succeeded"


def test_run_atomic_extraction_live_falls_back_to_computed_backoff_when_provider_value_missing(
    tmp_path: Path,
    monkeypatch,
    caplog,
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
        "atomic_claims": [make_core_claim(sentence_span_id="span-1")],
        "ontology_candidates": [],
        "unresolved": [],
        "warnings": [],
    }
    client = SequenceResponsesClient(
        {
            "span-1": [
                TransientOpenAIError(
                    "rate limited",
                    status_code=429,
                    request_id="req-429",
                    error_kind="rate_limited_429",
                ),
                _response_payload(result_mapping, request_id="resp-1"),
            ]
        }
    )
    sleeps: list[float] = []
    monkeypatch.setattr(
        "incident_pipeline.extract.atomic_extract.sleep",
        lambda value: sleeps.append(value),
    )
    monkeypatch.setattr(
        "incident_pipeline.extract.atomic_extract.random.uniform",
        lambda _low, _high: 0.25,
    )
    caplog.set_level(logging.INFO, logger="incident_pipeline.extract.live")

    summary = run_atomic_extraction_live(
        config_path,
        client=client,
        max_retries=1,
        retry_base_delay_seconds=1.0,
    )

    assert summary == {
        "selected": 1,
        "completed": 1,
        "unprocessable": 0,
        "failed": 0,
    }
    assert sleeps == [1.25]
    retry_logs = [
        json.loads(record.getMessage())
        for record in caplog.records
        if '"event": "live_request_retry_scheduled"' in record.getMessage()
    ]
    assert retry_logs[0]["provider_retry_after_applied"] is False
    assert "provider_retry_after_seconds" not in retry_logs[0]
    assert retry_logs[0]["sleep_seconds_applied"] == 1.25


def test_run_atomic_extraction_live_global_rpm_pacer_spaces_requests_and_records_metadata(
    tmp_path: Path,
    monkeypatch,
    caplog,
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
                sentence_text="Artifact one first sentence.",
            ),
            {
                **make_sentence_span(
                    sentence_span_id="span-2",
                    sentence_text="Artifact two first sentence.",
                ),
                "artifact_id": "artifact-2",
            },
        ],
    )
    write_config(
        config_path,
        sentence_span_root,
        output_root,
        model="gpt-test",
        live_requests_per_minute=3.0,
    )
    client = SequenceResponsesClient(
        {
            "span-1": [
                _response_payload(
                    {
                        "sentence_span_id": "span-1",
                        "status": "ok",
                        "atomic_claims": [make_core_claim(sentence_span_id="span-1")],
                        "ontology_candidates": [],
                        "unresolved": [],
                        "warnings": [],
                    },
                    request_id="resp-1",
                )
            ],
            "span-2": [
                _response_payload(
                    {
                        "sentence_span_id": "span-2",
                        "status": "ok",
                        "atomic_claims": [
                            {
                                **make_core_claim(sentence_span_id="span-2"),
                                "artifact_id": "artifact-2",
                            }
                        ],
                        "ontology_candidates": [],
                        "unresolved": [],
                        "warnings": [],
                    },
                    request_id="resp-2",
                )
            ],
        }
    )
    clock = {"now": 100.0}
    sleeps: list[float] = []

    def fake_perf_counter() -> float:
        return clock["now"]

    def fake_sleep(seconds: float) -> None:
        sleeps.append(seconds)
        clock["now"] += seconds

    monkeypatch.setattr(
        "incident_pipeline.extract.atomic_extract.perf_counter",
        fake_perf_counter,
    )
    monkeypatch.setattr(
        "incident_pipeline.extract.atomic_extract.sleep",
        fake_sleep,
    )
    caplog.set_level(logging.INFO, logger="incident_pipeline.extract.live")

    summary = run_atomic_extraction_live(
        config_path,
        client=client,
    )

    assert summary == {
        "selected": 2,
        "completed": 2,
        "unprocessable": 0,
        "failed": 0,
    }
    assert sleeps == [20.0]
    run_dir = next(path for path in (output_root / "runs").iterdir() if path.is_dir())
    inference_metadata = read_json(run_dir / "inference_metadata.json")
    assert inference_metadata["live_requests_per_minute"] == 3.0
    assert inference_metadata["live_request_interval_seconds"] == 20.0
    request_traces = inference_metadata["live_requests"]
    assert request_traces[0]["pacing_history"] == []
    assert request_traces[1]["pacing_history"] == [
        {
            "attempt_number": 1,
            "requests_per_minute": 3.0,
            "request_interval_seconds": 20.0,
            "sleep_seconds_applied": 20.0,
        }
    ]
    paced_logs = [
        json.loads(record.getMessage())
        for record in caplog.records
        if '"event": "live_request_paced"' in record.getMessage()
    ]
    assert len(paced_logs) == 1
    assert paced_logs[0]["artifact_id"] == "artifact-2"
    assert paced_logs[0]["attempt"] == 1
    assert paced_logs[0]["event"] == "live_request_paced"
    assert paced_logs[0]["request_interval_seconds"] == 20.0
    assert paced_logs[0]["requests_per_minute"] == 3.0
    assert paced_logs[0]["sentence_span_id"] == "span-2"
    assert paced_logs[0]["sleep_seconds_applied"] == 20.0
    assert paced_logs[0]["worker_name"] == "atomic-live_0"


def test_run_atomic_extraction_live_parallel_workers_preserve_deterministic_output_order(
    tmp_path: Path,
) -> None:
    namespaced_root = namespace_root(tmp_path)
    sentence_span_root = namespaced_root / "extract" / "sentence_spans"
    output_root = namespaced_root / "extract" / "atomic"
    config_path = tmp_path / "settings.yaml"
    rows = [
        make_sentence_span(
            sentence_span_id="span-1",
            sentence_text="Artifact one first sentence.",
        ),
        {
            **make_sentence_span(
                sentence_span_id="span-2",
                sentence_text="Artifact two first sentence.",
            ),
            "artifact_id": "artifact-2",
        },
        {
            **make_sentence_span(
                sentence_span_id="span-3",
                sentence_text="Artifact three first sentence.",
            ),
            "artifact_id": "artifact-3",
        },
        {
            **make_sentence_span(
                sentence_span_id="span-4",
                sentence_text="Artifact four first sentence.",
            ),
            "artifact_id": "artifact-4",
        },
    ]
    write_certified_sentence_span_run(sentence_span_root, rows)
    write_config(
        config_path,
        sentence_span_root,
        output_root,
        model="gpt-test",
        live_requests_per_minute=1000000.0,
        live_max_workers=2,
    )
    client = ConcurrentResponsesClient(
        {
            "span-1": [
                _response_payload(
                    {
                        "sentence_span_id": "span-1",
                        "status": "ok",
                        "atomic_claims": [make_core_claim(sentence_span_id="span-1")],
                        "ontology_candidates": [],
                        "unresolved": [],
                        "warnings": [],
                    },
                    request_id="resp-1",
                )
            ],
            "span-2": [
                _response_payload(
                    {
                        "sentence_span_id": "span-2",
                        "status": "ok",
                        "atomic_claims": [
                            {
                                **make_core_claim(sentence_span_id="span-2"),
                                "artifact_id": "artifact-2",
                            }
                        ],
                        "ontology_candidates": [],
                        "unresolved": [],
                        "warnings": [],
                    },
                    request_id="resp-2",
                )
            ],
            "span-3": [
                _response_payload(
                    {
                        "sentence_span_id": "span-3",
                        "status": "ok",
                        "atomic_claims": [
                            {
                                **make_core_claim(sentence_span_id="span-3"),
                                "artifact_id": "artifact-3",
                            }
                        ],
                        "ontology_candidates": [],
                        "unresolved": [],
                        "warnings": [],
                    },
                    request_id="resp-3",
                )
            ],
            "span-4": [
                _response_payload(
                    {
                        "sentence_span_id": "span-4",
                        "status": "ok",
                        "atomic_claims": [
                            {
                                **make_core_claim(sentence_span_id="span-4"),
                                "artifact_id": "artifact-4",
                            }
                        ],
                        "ontology_candidates": [],
                        "unresolved": [],
                        "warnings": [],
                    },
                    request_id="resp-4",
                )
            ],
        },
        per_call_delay_seconds=0.02,
    )

    summary = run_atomic_extraction_live(
        config_path,
        client=client,
    )

    assert summary == {
        "selected": 4,
        "completed": 4,
        "unprocessable": 0,
        "failed": 0,
    }
    assert client.max_active_calls == 2
    run_dir = next(path for path in (output_root / "runs").iterdir() if path.is_dir())
    outputs = [
        json.loads(line)
        for line in (run_dir / "atomic_extractions.jsonl").read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    assert [row["sentence_span_id"] for row in outputs] == [
        "span-1",
        "span-2",
        "span-3",
        "span-4",
    ]
    inference_metadata = read_json(run_dir / "inference_metadata.json")
    assert inference_metadata["live_max_workers"] == 2
    assert inference_metadata["live_worker_pool_size_used"] == 2
    assert inference_metadata["max_active_workers_observed"] == 2


def test_run_atomic_extraction_live_parallel_workers_share_one_rate_limiter(
    tmp_path: Path,
    monkeypatch,
) -> None:
    namespaced_root = namespace_root(tmp_path)
    sentence_span_root = namespaced_root / "extract" / "sentence_spans"
    output_root = namespaced_root / "extract" / "atomic"
    config_path = tmp_path / "settings.yaml"
    write_certified_sentence_span_run(
        sentence_span_root,
        [
            make_sentence_span(sentence_span_id="span-1", sentence_text="Sentence one."),
            {
                **make_sentence_span(
                    sentence_span_id="span-2",
                    sentence_text="Sentence two.",
                ),
                "artifact_id": "artifact-2",
            },
            {
                **make_sentence_span(
                    sentence_span_id="span-3",
                    sentence_text="Sentence three.",
                ),
                "artifact_id": "artifact-3",
            },
        ],
    )
    write_config(
        config_path,
        sentence_span_root,
        output_root,
        model="gpt-test",
        live_requests_per_minute=120.0,
        live_max_workers=3,
    )
    client = ConcurrentResponsesClient(
        {
            sentence_span_id: [
                _response_payload(
                    {
                        "sentence_span_id": sentence_span_id,
                        "status": "ok",
                        "atomic_claims": [
                            {
                                **make_core_claim(sentence_span_id=sentence_span_id),
                                "artifact_id": artifact_id,
                            }
                        ],
                        "ontology_candidates": [],
                        "unresolved": [],
                        "warnings": [],
                    },
                    request_id=f"resp-{sentence_span_id}",
                )
            ]
            for sentence_span_id, artifact_id in (
                ("span-1", "artifact-1"),
                ("span-2", "artifact-2"),
                ("span-3", "artifact-3"),
            )
        },
        per_call_delay_seconds=0.01,
    )
    clock = {"now": 100.0}
    clock_lock = Lock()

    def fake_perf_counter() -> float:
        with clock_lock:
            return clock["now"]

    def fake_sleep(seconds: float) -> None:
        with clock_lock:
            clock["now"] += seconds

    monkeypatch.setattr(
        "incident_pipeline.extract.atomic_extract.perf_counter",
        fake_perf_counter,
    )
    monkeypatch.setattr(
        "incident_pipeline.extract.atomic_extract.sleep",
        fake_sleep,
    )

    summary = run_atomic_extraction_live(
        config_path,
        client=client,
    )

    assert summary == {
        "selected": 3,
        "completed": 3,
        "unprocessable": 0,
        "failed": 0,
    }
    run_dir = next(path for path in (output_root / "runs").iterdir() if path.is_dir())
    inference_metadata = read_json(run_dir / "inference_metadata.json")
    assert inference_metadata["live_requests_per_minute"] == 120.0
    assert inference_metadata["live_request_interval_seconds"] == 0.5
    pacing_sleeps = sorted(
        event["sleep_seconds_applied"]
        for trace in inference_metadata["live_requests"]
        for event in trace["pacing_history"]
    )
    assert pacing_sleeps == [0.5, 0.5]
    assert round(sum(pacing_sleeps), 6) == 1.0
    assert inference_metadata["max_active_workers_observed"] == 3


def test_run_atomic_extraction_live_retries_respect_shared_global_pacing(
    tmp_path: Path,
    monkeypatch,
) -> None:
    namespaced_root = namespace_root(tmp_path)
    sentence_span_root = namespaced_root / "extract" / "sentence_spans"
    output_root = namespaced_root / "extract" / "atomic"
    config_path = tmp_path / "settings.yaml"
    write_certified_sentence_span_run(
        sentence_span_root,
        [
            make_sentence_span(sentence_span_id="span-1", sentence_text="Sentence one."),
            {
                **make_sentence_span(
                    sentence_span_id="span-2",
                    sentence_text="Sentence two.",
                ),
                "artifact_id": "artifact-2",
            },
        ],
    )
    write_config(
        config_path,
        sentence_span_root,
        output_root,
        model="gpt-test",
        live_requests_per_minute=60.0,
        live_max_workers=2,
    )
    client = ConcurrentResponsesClient(
        {
            "span-1": [
                TransientOpenAIError(
                    "rate limited",
                    status_code=429,
                    request_id="req-429",
                    error_kind="rate_limited_429",
                ),
                _response_payload(
                    {
                        "sentence_span_id": "span-1",
                        "status": "ok",
                        "atomic_claims": [make_core_claim(sentence_span_id="span-1")],
                        "ontology_candidates": [],
                        "unresolved": [],
                        "warnings": [],
                    },
                    request_id="resp-1",
                ),
            ],
            "span-2": [
                _response_payload(
                    {
                        "sentence_span_id": "span-2",
                        "status": "ok",
                        "atomic_claims": [
                            {
                                **make_core_claim(sentence_span_id="span-2"),
                                "artifact_id": "artifact-2",
                            }
                        ],
                        "ontology_candidates": [],
                        "unresolved": [],
                        "warnings": [],
                    },
                    request_id="resp-2",
                )
            ],
        },
        per_call_delay_seconds=0.01,
    )
    clock = {"now": 200.0}
    clock_lock = Lock()

    def fake_perf_counter() -> float:
        with clock_lock:
            return clock["now"]

    def fake_sleep(seconds: float) -> None:
        with clock_lock:
            clock["now"] += seconds

    monkeypatch.setattr(
        "incident_pipeline.extract.atomic_extract.perf_counter",
        fake_perf_counter,
    )
    monkeypatch.setattr(
        "incident_pipeline.extract.atomic_extract.sleep",
        fake_sleep,
    )
    monkeypatch.setattr(
        "incident_pipeline.extract.atomic_extract.random.uniform",
        lambda _low, _high: 0.0,
    )

    summary = run_atomic_extraction_live(
        config_path,
        client=client,
        max_retries=1,
        retry_base_delay_seconds=0.0,
    )

    assert summary == {
        "selected": 2,
        "completed": 2,
        "unprocessable": 0,
        "failed": 0,
    }
    run_dir = next(path for path in (output_root / "runs").iterdir() if path.is_dir())
    inference_metadata = read_json(run_dir / "inference_metadata.json")
    pacing_sleeps = sorted(
        event["sleep_seconds_applied"]
        for trace in inference_metadata["live_requests"]
        for event in trace["pacing_history"]
    )
    assert pacing_sleeps == [1.0, 1.0]
    retry_trace = next(
        trace
        for trace in inference_metadata["live_requests"]
        if trace["sentence_span_id"] == "span-1"
    )
    assert retry_trace["retry_status_codes"] == [429]


def test_run_atomic_extraction_live_document_limit_uses_first_distinct_artifact_ids(
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
                sentence_text="Artifact one first sentence.",
            ),
            {
                **make_sentence_span(
                    sentence_span_id="span-2",
                    sentence_text="Artifact one second sentence.",
                ),
                "artifact_id": "artifact-1",
            },
            {
                **make_sentence_span(
                    sentence_span_id="span-3",
                    sentence_text="Artifact two first sentence.",
                ),
                "artifact_id": "artifact-2",
            },
        ],
    )
    write_config(config_path, sentence_span_root, output_root, model="gpt-test")

    client = SequenceResponsesClient(
        {
            "span-1": [
                _response_payload(
                    {
                        "sentence_span_id": "span-1",
                        "status": "ok",
                        "atomic_claims": [make_core_claim(sentence_span_id="span-1")],
                        "ontology_candidates": [],
                        "unresolved": [],
                        "warnings": [],
                    },
                    request_id="resp-1",
                )
            ],
            "span-2": [
                _response_payload(
                    {
                        "sentence_span_id": "span-2",
                        "status": "ok",
                        "atomic_claims": [make_core_claim(sentence_span_id="span-2")],
                        "ontology_candidates": [],
                        "unresolved": [],
                        "warnings": [],
                    },
                    request_id="resp-2",
                )
            ],
        }
    )

    summary = run_atomic_extraction_live(
        config_path,
        client=client,
        document_limit=1,
    )

    assert summary == {
        "selected": 2,
        "completed": 2,
        "unprocessable": 0,
        "failed": 0,
    }
    run_dir = next(path for path in (output_root / "runs").iterdir() if path.is_dir())
    sentence_spans = [
        json.loads(line)
        for line in (run_dir / "sentence_spans.jsonl").read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    assert {row["artifact_id"] for row in sentence_spans} == {"artifact-1"}
    assert [row["sentence_span_id"] for row in sentence_spans] == ["span-1", "span-2"]


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


def test_run_atomic_extraction_live_classifies_retry_exhaustion_as_rate_limit(
    tmp_path: Path,
    monkeypatch,
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
    client = SequenceResponsesClient(
        {
            "span-1": [
                TransientOpenAIError(
                    "rate limited",
                    status_code=429,
                    request_id="req-429-a",
                    retry_after_seconds=2,
                    error_kind="rate_limited_429",
                ),
                TransientOpenAIError(
                    "rate limited",
                    status_code=429,
                    request_id="req-429-b",
                    retry_after_seconds=2,
                    error_kind="rate_limited_429",
                ),
            ]
        }
    )
    monkeypatch.setattr(
        "incident_pipeline.extract.atomic_extract.sleep",
        lambda _value: None,
    )
    monkeypatch.setattr(
        "incident_pipeline.extract.atomic_extract.random.uniform",
        lambda _low, _high: 0.0,
    )

    summary = run_atomic_extraction_live(
        config_path,
        client=client,
        max_retries=1,
        retry_base_delay_seconds=1.0,
    )

    assert summary == {
        "selected": 1,
        "completed": 0,
        "unprocessable": 0,
        "failed": 1,
    }
    run_dir = next(path for path in (output_root / "runs").iterdir() if path.is_dir())
    failures = [
        json.loads(line)
        for line in (run_dir / "atomic_failures.jsonl").read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    assert failures[0]["failure_code"] == "rate_limited_429"
    assert failures[0]["failure_class"] == "dependency"
    assert failures[0]["retryable"] is True
    assert "retry budget exhausted after 2 attempts" in failures[0]["message"]
    inference_metadata = read_json(run_dir / "inference_metadata.json")
    request_trace = inference_metadata["live_requests"][0]
    assert request_trace["attempt_count"] == 2
    assert request_trace["retry_status_codes"] == [429, 429]
    assert request_trace["final_outcome_classification"] == "rate_limited_429"


def test_run_atomic_extraction_live_classifies_network_request_errors(
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
    client = SequenceResponsesClient(
        {
            "span-1": [
                TransientOpenAIError(
                    "transport failure",
                    error_kind="network_request_error",
                )
            ]
        }
    )

    summary = run_atomic_extraction_live(
        config_path,
        client=client,
        max_retries=0,
    )

    assert summary == {
        "selected": 1,
        "completed": 0,
        "unprocessable": 0,
        "failed": 1,
    }
    run_dir = next(path for path in (output_root / "runs").iterdir() if path.is_dir())
    failures = [
        json.loads(line)
        for line in (run_dir / "atomic_failures.jsonl").read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    assert failures[0]["failure_code"] == "network_request_error"
    assert failures[0]["failure_class"] == "io"


def test_run_atomic_extraction_live_classifies_provider_5xx_errors(
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
    client = SequenceResponsesClient(
        {
            "span-1": [
                TransientOpenAIError(
                    "provider unavailable",
                    status_code=503,
                    request_id="req-503",
                    error_kind="provider_5xx",
                )
            ]
        }
    )

    summary = run_atomic_extraction_live(
        config_path,
        client=client,
        max_retries=0,
    )

    assert summary == {
        "selected": 1,
        "completed": 0,
        "unprocessable": 0,
        "failed": 1,
    }
    run_dir = next(path for path in (output_root / "runs").iterdir() if path.is_dir())
    failures = [
        json.loads(line)
        for line in (run_dir / "atomic_failures.jsonl").read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    assert failures[0]["failure_code"] == "provider_5xx"
    assert failures[0]["failure_class"] == "dependency"
