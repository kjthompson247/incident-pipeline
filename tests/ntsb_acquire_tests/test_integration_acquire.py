from __future__ import annotations

import json
from pathlib import Path

import httpx
from typer.testing import CliRunner

from incident_pipeline.acquisition.ntsb.cli import app
from incident_pipeline.acquisition.ntsb.http import HttpClient

runner = CliRunner()
FIXTURE_ROOT = Path(__file__).resolve().parent / "fixtures"


def acquisition_layout(storage_root: Path) -> tuple[Path, Path, Path]:
    data_root = (storage_root / "ntsb").resolve()
    return (
        data_root,
        data_root / "acquisition" / "state" / "acquisition.db",
        data_root / "raw",
    )


def acquisition_env(storage_root: Path) -> dict[str, str]:
    return {"INCIDENT_PIPELINE_DATA_ROOT": str(storage_root)}


def load_json_fixture(relative_path: str) -> object:
    return json.loads((FIXTURE_ROOT / relative_path).read_text(encoding="utf-8"))


def load_text_fixture(relative_path: str) -> str:
    return (FIXTURE_ROOT / relative_path).read_text(encoding="utf-8")


def parse_structured_output(stdout: str) -> dict[str, object]:
    return json.loads(stdout.strip())


def test_run_command_executes_bounded_acquisition_flow(tmp_path: Path, monkeypatch) -> None:
    carol_payload = {"results": [load_json_fixture("carol/investigations.json")["results"][0]]}
    docket_html = load_text_fixture("dockets/happy_path.html")
    pdf_payloads = {
        "https://example.test/docs/operations.pdf": b"%PDF-1.7 operations",
        "https://example.test/docs/systems.pdf": b"%PDF-1.7 systems",
    }

    def handler(request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        if url.startswith("https://example.test/carol"):
            return httpx.Response(200, json=carol_payload, request=request)
        if url == "https://example.test/dockets/DCA24FM001" or url.startswith(
            "https://example.test/dockets?ProjectID="
        ):
            return httpx.Response(200, text=docket_html, request=request)
        if url in pdf_payloads:
            return httpx.Response(
                200,
                content=pdf_payloads[url],
                headers={"content-type": "application/pdf"},
                request=request,
            )
        raise AssertionError(f"unexpected url: {url}")

    def make_http_client(config):
        client = httpx.Client(transport=httpx.MockTransport(handler))
        return HttpClient(
            client,
            rate_limit_per_second=config.http_rate_limit_per_second,
            max_retries=0,
            backoff_seconds=0.0,
            sleeper=lambda _: None,
            monotonic=lambda: 0.0,
        )

    monkeypatch.setattr("incident_pipeline.acquisition.ntsb.cli.build_http_client", make_http_client)

    storage_root = tmp_path / "storage"
    data_root, sqlite_path, downstream_raw_root = acquisition_layout(storage_root)
    result = runner.invoke(
        app,
        [
            "--carol-base-url",
            "https://example.test/carol",
            "--docket-base-url",
            "https://example.test/dockets",
            "run",
        ],
        env=acquisition_env(storage_root),
    )

    assert result.exit_code == 0
    payload = parse_structured_output(result.stdout)
    promoted_files = sorted((downstream_raw_root / "DCA24FM001").glob("*.pdf"))

    assert payload["command"] == "run"
    assert payload["status"] == "ok"
    assert payload["investigations_discovered"] == 1
    assert payload["dockets_enumerated"] == 1
    assert payload["selection_selected"] == 2
    assert payload["downloads_created"] == 2
    assert payload["promotions_created"] == 2
    assert len(promoted_files) == 2
    assert (data_root / "acquisition" / "manifests" / "investigation_sync.jsonl").exists()
    assert (data_root / "acquisition" / "manifests" / "docket_enumeration.jsonl").exists()
    assert (data_root / "acquisition" / "manifests" / "selection_decisions.jsonl").exists()
    assert (data_root / "acquisition" / "manifests" / "downloads.jsonl").exists()
    assert (data_root / "acquisition" / "manifests" / "promotions.jsonl").exists()
