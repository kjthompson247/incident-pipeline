from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
import json
import os
from pathlib import Path
import time
from typing import Any, Protocol

import httpx

from incident_pipeline.extract.atomic_contract import (
    MAPPING_CONTRACT_VERSION,
    MODEL_CONTRACT_VERSION,
    ONTOLOGY_VERSION,
    PROMPT_VERSION,
    SentenceSpan,
    atomic_extraction_result_json_schema,
    build_api_request,
)


DEFAULT_OPENAI_BASE_URL = "https://api.openai.com/v1"
DEFAULT_BATCH_ENDPOINT = "/v1/responses"
TERMINAL_BATCH_STATUSES = {"completed", "failed", "expired", "cancelled"}


class TransientOpenAIError(RuntimeError):
    def __init__(self, message: str, *, status_code: int | None = None) -> None:
        super().__init__(message)
        self.status_code = status_code


class OpenAIConfigurationError(RuntimeError):
    pass


class OpenAIBatchError(RuntimeError):
    pass


@dataclass(frozen=True)
class LiveRequestTrace:
    sentence_span_id: str
    request_id: str | None
    attempt_count: int
    retry_status_codes: tuple[int, ...]

    def to_mapping(self) -> dict[str, Any]:
        return {
            "sentence_span_id": self.sentence_span_id,
            "request_id": self.request_id,
            "attempt_count": self.attempt_count,
            "retry_status_codes": list(self.retry_status_codes),
        }


@dataclass(frozen=True)
class BatchRequestTrace:
    sentence_span_id: str
    request_id: str | None
    status_code: int | None

    def to_mapping(self) -> dict[str, Any]:
        return {
            "sentence_span_id": self.sentence_span_id,
            "request_id": self.request_id,
            "status_code": self.status_code,
        }


@dataclass(frozen=True)
class BatchJobMetadata:
    batch_id: str
    status: str
    input_file_id: str | None
    output_file_id: str | None
    error_file_id: str | None
    endpoint: str
    completion_window: str

    def to_mapping(self) -> dict[str, Any]:
        return {
            "batch_id": self.batch_id,
            "status": self.status,
            "input_file_id": self.input_file_id,
            "output_file_id": self.output_file_id,
            "error_file_id": self.error_file_id,
            "endpoint": self.endpoint,
            "completion_window": self.completion_window,
        }


class ResponsesAPIClient(Protocol):
    def create_response(self, request_body: Mapping[str, Any]) -> Mapping[str, Any]:
        ...


class BatchAPIClient(Protocol):
    def submit_batch(
        self,
        request_manifest_path: Path,
        *,
        completion_window: str,
        metadata: Mapping[str, str],
    ) -> Mapping[str, Any]:
        ...

    def wait_for_batch(
        self,
        batch_id: str,
        *,
        poll_interval_seconds: float,
        max_polls: int,
    ) -> Mapping[str, Any]:
        ...

    def download_file_text(self, file_id: str) -> str:
        ...


def _build_headers(api_key: str) -> dict[str, str]:
    headers = {
        "Authorization": f"Bearer {api_key}",
    }
    organization = os.getenv("OPENAI_ORGANIZATION")
    if organization:
        headers["OpenAI-Organization"] = organization
    project = os.getenv("OPENAI_PROJECT")
    if project:
        headers["OpenAI-Project"] = project
    return headers


def _require_api_key() -> str:
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise OpenAIConfigurationError(
            "OPENAI_API_KEY must be set to use OpenAI atomic extraction execution."
        )
    return api_key


def _base_url() -> str:
    return os.getenv("OPENAI_BASE_URL", DEFAULT_OPENAI_BASE_URL).rstrip("/")


def build_atomic_prompt(*, prompt_version: str = PROMPT_VERSION) -> str:
    if prompt_version != PROMPT_VERSION:
        raise ValueError(
            f"Unsupported prompt_version {prompt_version!r}; expected {PROMPT_VERSION!r}"
        )
    core_predicates = (
        "exists, has, located_at, associated_with, produced, observed, "
        "reported, caused, contributed_to"
    )
    return (
        "Transform exactly one governed SentenceSpan into lightweight structured claims.\n"
        "Rules:\n"
        "- Emit only text-supported claims. Never infer unstated facts.\n"
        "- Every claim must include sentence_span_id, parent_structural_span_id, artifact_id, "
        "predicate, predicate_status, and context_ref.\n"
        "- Core predicates are locked to: "
        f"{core_predicates}.\n"
        "- Use predicate_status=candidate only when you preserve the raw phrase in predicate_raw "
        "and the normalized exploratory predicate in predicate_candidate.\n"
        "- Use predicate_status=unresolved only when no safe predicate can be normalized. "
        "When unresolved, set predicate to 'unresolved' and preserve the raw phrase in predicate_raw.\n"
        "- Preserve parent context linkage with context_ref = context:<sentence_span_id>.\n"
        "- Ontology candidates are optional and adjacent to claims. Allowed types are "
        "mechanism, defect, entity, event, condition.\n"
        "- mechanism_class is required only for mechanism and must be demand or degradation.\n"
        "- Never emit systemic_issue as a base object.\n"
        "- If extraction is unsafe, return status=unprocessable with empty atomic_claims and "
        "ontology_candidates plus a reason in unresolved."
    )


def build_responses_api_request(
    sentence_span: SentenceSpan,
    *,
    model: str,
    prompt_version: str = PROMPT_VERSION,
    model_contract_version: str = MODEL_CONTRACT_VERSION,
    ontology_version: str = ONTOLOGY_VERSION,
    mapping_contract_version: str = MAPPING_CONTRACT_VERSION,
) -> dict[str, Any]:
    if not model.strip():
        raise ValueError("model must be a non-empty string")
    job_request = build_api_request(
        job_id=sentence_span.sentence_span_id,
        items=[sentence_span],
        prompt_version=prompt_version,
        model_contract_version=model_contract_version,
        ontology_version=ontology_version,
        mapping_contract_version=mapping_contract_version,
    )
    return {
        "model": model,
        "input": [
            {
                "role": "system",
                "content": [
                    {
                        "type": "input_text",
                        "text": build_atomic_prompt(prompt_version=prompt_version),
                    }
                ],
            },
            {
                "role": "user",
                "content": [
                    {
                        "type": "input_text",
                        "text": json.dumps(job_request, sort_keys=True),
                    }
                ],
            },
        ],
        "text": {
            "format": {
                "type": "json_schema",
                "name": "atomic_extraction_result",
                "strict": True,
                "schema": atomic_extraction_result_json_schema(),
            }
        },
        "metadata": {
            "sentence_span_id": sentence_span.sentence_span_id,
            "artifact_id": sentence_span.artifact_id,
            "prompt_version": prompt_version,
            "model_contract_version": model_contract_version,
            "ontology_version": ontology_version,
            "mapping_contract_version": mapping_contract_version,
        },
    }


def build_batch_request_line(
    sentence_span: SentenceSpan,
    *,
    model: str,
    prompt_version: str = PROMPT_VERSION,
    model_contract_version: str = MODEL_CONTRACT_VERSION,
    ontology_version: str = ONTOLOGY_VERSION,
    mapping_contract_version: str = MAPPING_CONTRACT_VERSION,
) -> dict[str, Any]:
    return {
        "custom_id": sentence_span.sentence_span_id,
        "method": "POST",
        "url": DEFAULT_BATCH_ENDPOINT,
        "body": build_responses_api_request(
            sentence_span,
            model=model,
            prompt_version=prompt_version,
            model_contract_version=model_contract_version,
            ontology_version=ontology_version,
            mapping_contract_version=mapping_contract_version,
        ),
    }


def _extract_output_text(response_payload: Mapping[str, Any]) -> str:
    output_text = response_payload.get("output_text")
    if isinstance(output_text, str) and output_text.strip():
        return output_text

    fragments: list[str] = []
    output_items = response_payload.get("output", [])
    if isinstance(output_items, list):
        for item in output_items:
            if not isinstance(item, Mapping):
                continue
            content_items = item.get("content", [])
            if not isinstance(content_items, list):
                continue
            for content in content_items:
                if not isinstance(content, Mapping):
                    continue
                text_value = content.get("text")
                if isinstance(text_value, str) and text_value.strip():
                    fragments.append(text_value)

    if fragments:
        return "".join(fragments)
    raise ValueError("OpenAI response did not include structured output text")


def parse_structured_response_payload(response_payload: Mapping[str, Any]) -> dict[str, Any]:
    status = response_payload.get("status")
    if isinstance(status, str) and status not in {"completed", "succeeded"}:
        error = response_payload.get("error")
        if isinstance(error, Mapping):
            message = error.get("message")
            if isinstance(message, str) and message.strip():
                raise ValueError(message)
        raise ValueError(f"OpenAI response did not complete successfully: status={status}")

    text = _extract_output_text(response_payload)
    payload = json.loads(text)
    if not isinstance(payload, Mapping):
        raise ValueError("Structured output must decode to a JSON object")
    return dict(payload)


def parse_batch_output_line(line: str) -> tuple[str, dict[str, Any], BatchRequestTrace]:
    payload = json.loads(line)
    if not isinstance(payload, Mapping):
        raise ValueError("Batch output line must be a JSON object")
    custom_id = payload.get("custom_id")
    if not isinstance(custom_id, str) or not custom_id.strip():
        raise ValueError("Batch output line missing custom_id")

    error_payload = payload.get("error")
    if isinstance(error_payload, Mapping):
        code = error_payload.get("code")
        message = error_payload.get("message")
        raise OpenAIBatchError(
            f"Batch request failed for {custom_id}: {code or 'error'} {message or ''}".strip()
        )

    response = payload.get("response")
    if not isinstance(response, Mapping):
        raise ValueError(f"Batch output line missing response for {custom_id}")
    status_code = response.get("status_code")
    request_id = response.get("request_id")
    if isinstance(status_code, int) and status_code >= 400:
        raise OpenAIBatchError(
            f"Batch request failed for {custom_id} with status_code={status_code}"
        )
    body = response.get("body")
    if not isinstance(body, Mapping):
        raise ValueError(f"Batch response body missing for {custom_id}")
    trace = BatchRequestTrace(
        sentence_span_id=custom_id,
        request_id=request_id if isinstance(request_id, str) else None,
        status_code=status_code if isinstance(status_code, int) else None,
    )
    return custom_id, dict(body), trace


class OpenAIResponsesAPIClient:
    def __init__(
        self,
        *,
        api_key: str | None = None,
        base_url: str | None = None,
        timeout_seconds: float = 60.0,
    ) -> None:
        token = api_key or _require_api_key()
        self._client = httpx.Client(
            base_url=base_url or _base_url(),
            headers=_build_headers(token),
            timeout=timeout_seconds,
        )

    def create_response(self, request_body: Mapping[str, Any]) -> Mapping[str, Any]:
        try:
            response = self._client.post("/responses", json=request_body)
        except httpx.RequestError as exc:
            raise TransientOpenAIError(f"OpenAI request failed: {exc}") from exc
        if response.status_code == 429 or response.status_code >= 500:
            raise TransientOpenAIError(
                f"OpenAI transient error status={response.status_code}",
                status_code=response.status_code,
            )
        response.raise_for_status()
        payload = response.json()
        if not isinstance(payload, Mapping):
            raise ValueError("OpenAI response must be a JSON object")
        return payload


class OpenAIBatchAPIClient:
    def __init__(
        self,
        *,
        api_key: str | None = None,
        base_url: str | None = None,
        timeout_seconds: float = 60.0,
    ) -> None:
        token = api_key or _require_api_key()
        self._client = httpx.Client(
            base_url=base_url or _base_url(),
            headers=_build_headers(token),
            timeout=timeout_seconds,
        )

    def submit_batch(
        self,
        request_manifest_path: Path,
        *,
        completion_window: str,
        metadata: Mapping[str, str],
    ) -> Mapping[str, Any]:
        with request_manifest_path.open("rb") as handle:
            upload = self._client.post(
                "/files",
                data={"purpose": "batch"},
                files={"file": (request_manifest_path.name, handle, "application/jsonl")},
            )
        upload.raise_for_status()
        upload_payload = upload.json()
        if not isinstance(upload_payload, Mapping):
            raise ValueError("OpenAI file upload response must be a JSON object")
        input_file_id = upload_payload.get("id")
        if not isinstance(input_file_id, str) or not input_file_id.strip():
            raise ValueError("OpenAI file upload did not return an id")

        response = self._client.post(
            "/batches",
            json={
                "input_file_id": input_file_id,
                "endpoint": DEFAULT_BATCH_ENDPOINT,
                "completion_window": completion_window,
                "metadata": dict(metadata),
            },
        )
        response.raise_for_status()
        payload = response.json()
        if not isinstance(payload, Mapping):
            raise ValueError("OpenAI batch submission response must be a JSON object")
        return payload

    def wait_for_batch(
        self,
        batch_id: str,
        *,
        poll_interval_seconds: float,
        max_polls: int,
    ) -> Mapping[str, Any]:
        if max_polls < 1:
            raise ValueError("max_polls must be >= 1")
        for _ in range(max_polls):
            response = self._client.get(f"/batches/{batch_id}")
            response.raise_for_status()
            payload = response.json()
            if not isinstance(payload, Mapping):
                raise ValueError("OpenAI batch retrieval response must be a JSON object")
            status = payload.get("status")
            if isinstance(status, str) and status in TERMINAL_BATCH_STATUSES:
                return payload
            time.sleep(max(poll_interval_seconds, 0.0))
        raise OpenAIBatchError(f"OpenAI batch did not reach a terminal state: {batch_id}")

    def download_file_text(self, file_id: str) -> str:
        response = self._client.get(f"/files/{file_id}/content")
        response.raise_for_status()
        return response.text
