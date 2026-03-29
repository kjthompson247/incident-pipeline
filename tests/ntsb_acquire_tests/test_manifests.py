from __future__ import annotations

from pathlib import Path

import pytest
from pydantic import ValidationError

from incident_pipeline.acquisition.ntsb.manifests import append_jsonl_record, serialize_manifest_record
from incident_pipeline.acquisition.ntsb.models import AcquisitionManifestRecord


def make_record(*, run_id: str = "run_test") -> AcquisitionManifestRecord:
    return AcquisitionManifestRecord(
        run_id=run_id,
        source="phase_1_test",
        ntsb_number="ABC123",
        action="noop",
        retrieved_at="2026-03-23T00:00:00Z",
        changed=False,
    )


def test_serialize_manifest_record_is_deterministic() -> None:
    record = make_record()
    expected = (
        '{"action":"noop","changed":false,"content_hash":null,"error":null,'
        '"http_status":null,"input_fingerprint":null,"note":null,'
        '"ntsb_number":"ABC123","output_path":null,"parser_version":null,'
        '"project_id":null,"recorded_at":"2026-03-23T00:00:00Z",'
        '"run_id":"run_test","source":"phase_1_test"}'
    )

    assert serialize_manifest_record(record) == expected


def test_append_jsonl_record_preserves_append_only_order(tmp_path: Path) -> None:
    path = tmp_path / "manifest.jsonl"
    first = (
        '{"action":"noop","changed":false,"content_hash":null,"error":null,'
        '"http_status":null,"input_fingerprint":null,"note":null,'
        '"ntsb_number":"ABC123","output_path":null,"parser_version":null,'
        '"project_id":null,"recorded_at":"2026-03-23T00:00:00Z",'
        '"run_id":"run_one","source":"phase_1_test"}'
    )
    second = (
        '{"action":"noop","changed":false,"content_hash":null,"error":null,'
        '"http_status":null,"input_fingerprint":null,"note":null,'
        '"ntsb_number":"ABC123","output_path":null,"parser_version":null,'
        '"project_id":null,"recorded_at":"2026-03-23T00:00:00Z",'
        '"run_id":"run_two","source":"phase_1_test"}'
    )

    append_jsonl_record(path, make_record(run_id="run_one"))
    append_jsonl_record(path, make_record(run_id="run_two"))

    assert path.read_text(encoding="utf-8").splitlines() == [first, second]


def test_manifest_models_require_run_id() -> None:
    with pytest.raises(ValidationError):
        AcquisitionManifestRecord(
            source="phase_1_test",
            ntsb_number="ABC123",
            action="noop",
            retrieved_at="2026-03-23T00:00:00Z",
            changed=False,
        )
