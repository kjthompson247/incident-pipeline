from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urljoin, urlparse

from bs4 import BeautifulSoup  # type: ignore[import-untyped]

from incident_pipeline.acquisition.ntsb.db import upsert_docket_search_result_current
from incident_pipeline.acquisition.ntsb.http import HttpClient
from incident_pipeline.acquisition.ntsb.identifiers import normalize_ntsb_number, source_fingerprint
from incident_pipeline.acquisition.ntsb.manifests import append_jsonl_record
from incident_pipeline.acquisition.ntsb.models import AcquisitionManifestRecord, DocketSearchResult
from incident_pipeline.acquisition.ntsb.normalize import collapse_whitespace
from incident_pipeline.acquisition.ntsb.runtime import RunContext

PUBLIC_DOCKET_SEARCH_URL = "https://data.ntsb.gov/Docket/Forms/searchdocket"
DOCKET_SEARCH_SOURCE = "public_docket_search"
DOCKET_SEARCH_PARSER_VERSION = "docket_search_v1"


@dataclass(frozen=True)
class _SearchForm:
    action_url: str
    method: str
    fields: dict[str, str]
    pipeline_field_name: str
    pipeline_value: str


@dataclass(frozen=True)
class DocketSearchSyncResult:
    discovered: int
    changed: int
    unchanged: int
    raw_html_path: Path


def _text(node: object) -> str:
    if node is None:
        return ""
    text = getattr(node, "get_text", lambda *args, **kwargs: str(node))(" ", strip=True)
    return collapse_whitespace(str(text))


def _normalized_header(value: str) -> str:
    return collapse_whitespace(value).lower().replace("/", " ")


def _header_key(value: str) -> str | None:
    header = _normalized_header(value)
    if "description" in header:
        return "accident_description"
    if (
        "accident id" in header
        or "accident number" in header
        or "ntsb number" in header
    ):
        return "ntsb_number"
    if "date" in header:
        return "event_date"
    if header == "city":
        return "city"
    if "state" in header or "region" in header:
        return "state_or_region"
    if "docket" in header and "detail" in header:
        return "docket_details"
    return None


def _project_id_from_url(url: str) -> str | None:
    parsed = urlparse(url)
    query = parse_qs(parsed.query)
    for key, values in query.items():
        if key.lower() != "projectid" or not values:
            continue
        project_id = collapse_whitespace(values[0])
        if project_id:
            return project_id
    return None


def _control_name(node: Any) -> str:
    return collapse_whitespace(str(node.get("name") or ""))


def _successful_form_fields(form: Any) -> dict[str, str]:
    fields: dict[str, str] = {}
    for control in form.find_all(["input", "select", "textarea"]):
        name = _control_name(control)
        if not name or control.has_attr("disabled"):
            continue
        tag_name = str(getattr(control, "name", "")).lower()
        if tag_name == "input":
            input_type = str(control.get("type") or "text").lower()
            if input_type in {"button", "file", "image", "reset", "submit"}:
                continue
            if input_type in {"checkbox", "radio"} and not control.has_attr("checked"):
                continue
            value = str(
                control.get("value")
                or ("on" if input_type in {"checkbox", "radio"} else "")
            )
            fields[name] = value
            continue
        if tag_name == "select":
            options = list(control.find_all("option"))
            selected_options = [option for option in options if option.has_attr("selected")]
            current_option = (selected_options or options[:1])[:1]
            if not current_option:
                continue
            option = current_option[0]
            fields[name] = str(option.get("value") or _text(option))
            continue
        if tag_name == "textarea":
            fields[name] = _text(control)
    return fields


def _search_form(html: str, *, public_url: str) -> _SearchForm:
    soup = BeautifulSoup(html, "lxml")
    for form in soup.find_all("form"):
        for select in form.find_all("select"):
            field_name = collapse_whitespace(str(select.get("name") or select.get("id") or ""))
            if not field_name:
                continue
            for option in select.find_all("option"):
                if _text(option).lower() != "pipeline":
                    continue
                return _SearchForm(
                    action_url=urljoin(public_url, str(form.get("action") or public_url)),
                    method=str(form.get("method") or "get").upper(),
                    fields=_successful_form_fields(form),
                    pipeline_field_name=field_name,
                    pipeline_value=str(option.get("value") or _text(option)),
                )
    raise ValueError("public docket search form with Pipeline option was not found")


def parse_docket_search_results(
    *,
    html: str,
    public_url: str,
) -> list[DocketSearchResult]:
    soup = BeautifulSoup(html, "lxml")
    table = soup.find("table", id="resultTable")
    if table is None:
        return []

    rows = list(table.find_all("tr"))
    if not rows:
        return []

    header_cells = rows[0].find_all(["th", "td"])
    header_keys = [_header_key(_text(cell)) for cell in header_cells]
    records: list[DocketSearchResult] = []
    for row in rows[1:]:
        cells = list(row.find_all("td"))
        if not cells:
            continue
        values: dict[str, str] = {}
        for index, cell in enumerate(cells):
            if index >= len(header_keys):
                continue
            header_key = header_keys[index]
            if header_key is None:
                continue
            values[header_key] = _text(cell)

        link_cell = next(
            (
                cell
                for index, cell in enumerate(cells)
                if index < len(header_keys) and header_keys[index] == "ntsb_number"
            ),
            None,
        )
        if link_cell is None:
            continue
        link = link_cell.find("a", href=True)
        if link is None:
            continue
        result_url = urljoin(public_url, str(link["href"]))
        project_id = _project_id_from_url(result_url)
        if project_id is None:
            continue

        ntsb_number = normalize_ntsb_number(_text(link))
        payload = {
            "ntsb_number": ntsb_number,
            "project_id": project_id,
            "result_url": result_url,
            "event_date": values.get("event_date"),
            "city": values.get("city"),
            "state_or_region": values.get("state_or_region"),
            "docket_details": values.get("docket_details"),
            "accident_description": values.get("accident_description"),
        }
        records.append(
            DocketSearchResult(
                ntsb_number=ntsb_number,
                project_id=project_id,
                result_url=result_url,
                event_date=values.get("event_date"),
                city=values.get("city"),
                state_or_region=values.get("state_or_region"),
                docket_details=values.get("docket_details"),
                accident_description=values.get("accident_description"),
                source_fingerprint=source_fingerprint(payload),
            )
        )
    return records


class DocketSearchClient:
    def __init__(self, *, search_url: str, http_client: HttpClient) -> None:
        self._search_url = search_url
        self._http_client = http_client

    def fetch_search_page(self) -> str:
        return self._http_client.get_text(self._search_url)

    def fetch_pipeline_results(self) -> tuple[str, str]:
        search_page_html = self.fetch_search_page()
        form = _search_form(search_page_html, public_url=self._search_url)
        payload = dict(form.fields)
        payload[form.pipeline_field_name] = form.pipeline_value
        response = self._http_client.request(
            form.method,
            form.action_url,
            params=payload if form.method == "GET" else None,
            data=payload if form.method != "GET" else None,
        )
        response.raise_for_status()
        return response.text, str(response.url)


def discover_dockets(
    connection: sqlite3.Connection,
    client: DocketSearchClient,
    *,
    run_context: RunContext,
    manifest_path: Path,
) -> DocketSearchSyncResult:
    observed_at = run_context.started_at
    html, public_url = client.fetch_pipeline_results()
    raw_html_path = (
        run_context.paths.acquisition_raw_root / "docket_search" / "pipeline_results.html"
    )
    raw_html_path.parent.mkdir(parents=True, exist_ok=True)
    raw_html_path.write_text(html, encoding="utf-8")

    records = sorted(
        parse_docket_search_results(html=html, public_url=public_url),
        key=lambda record: record.ntsb_number,
    )
    changed_count = 0
    unchanged_count = 0
    for record in records:
        changed = upsert_docket_search_result_current(
            connection,
            record,
            observed_at=observed_at,
            run_id=run_context.run_id,
        )
        append_jsonl_record(
            manifest_path,
            AcquisitionManifestRecord(
                run_id=run_context.run_id,
                source=DOCKET_SEARCH_SOURCE,
                ntsb_number=record.ntsb_number,
                project_id=record.project_id,
                action="discover_docket",
                input_fingerprint=record.source_fingerprint,
                output_path=str(raw_html_path),
                parser_version=DOCKET_SEARCH_PARSER_VERSION,
                retrieved_at=observed_at,
                changed=changed,
                note=f"result_url={record.result_url}",
            ),
        )
        if changed:
            changed_count += 1
        else:
            unchanged_count += 1

    connection.commit()
    return DocketSearchSyncResult(
        discovered=len(records),
        changed=changed_count,
        unchanged=unchanged_count,
        raw_html_path=raw_html_path,
    )
