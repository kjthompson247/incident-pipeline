from __future__ import annotations

import re
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import urljoin

from bs4 import BeautifulSoup  # type: ignore[import-untyped]

from incident_pipeline.acquisition.ntsb.db import (
    deactivate_missing_docket_items,
    fetch_one,
    upsert_docket_current,
    upsert_docket_item_current,
)
from incident_pipeline.acquisition.ntsb.http import HttpClient
from incident_pipeline.acquisition.ntsb.identifiers import (
    build_docket_id,
    build_docket_item_id,
    docket_fingerprint,
    docket_item_fingerprint,
    normalize_ntsb_number,
)
from incident_pipeline.acquisition.ntsb.manifests import append_jsonl_record
from incident_pipeline.acquisition.ntsb.models import AcquisitionManifestRecord, Docket, DocketItem
from incident_pipeline.acquisition.ntsb.normalize import collapse_whitespace, slugify
from incident_pipeline.acquisition.ntsb.runtime import RunContext

DOCKET_SOURCE = "docket"
DOCKET_PARSER_VERSION = "docket_v1"
INTEGER_RE = re.compile(r"^\d+$")
PAGE_COUNT_RE = re.compile(r"(\d+)\s+pages?", re.IGNORECASE)
PHOTO_COUNT_RE = re.compile(r"(\d+)\s+photos?", re.IGNORECASE)


def _text(node: Any) -> str:
    if node is None:
        return ""
    if hasattr(node, "get_text"):
        return collapse_whitespace(node.get_text(" ", strip=True))
    return collapse_whitespace(str(node))


def _parse_metadata(soup: Any) -> dict[str, str]:
    metadata: dict[str, str] = {}
    for dl in soup.find_all("dl"):
        labels = dl.find_all("dt")
        values = dl.find_all("dd")
        for label_node, value_node in zip(labels, values, strict=False):
            label = _text(label_node).lower()
            if not label:
                continue
            metadata[label] = _text(value_node)
    return metadata


def _parse_counts(text: str) -> tuple[int | None, int | None]:
    page_match = PAGE_COUNT_RE.search(text)
    photo_match = PHOTO_COUNT_RE.search(text)
    page_count = int(page_match.group(1)) if page_match else None
    photo_count = int(photo_match.group(1)) if photo_match else None
    return page_count, photo_count


def _candidate_rows(soup: Any) -> list[Any]:
    tables = list(soup.find_all("table"))
    for table in tables:
        rows = list(table.find_all("tr"))
        if not rows:
            continue
        header_text = " ".join(_text(cell).lower() for cell in rows[0].find_all(["th", "td"]))
        if "title" in header_text and any(row.find("a", href=True) for row in rows[1:]):
            return rows

    if not tables:
        return []
    return list(tables[0].find_all("tr"))


def _cell_integer(cell: Any) -> int | None:
    text = _text(cell)
    if INTEGER_RE.fullmatch(text):
        return int(text)
    return None


def _is_pdf_href(href: str) -> bool:
    normalized_href = href.lower()
    return (
        normalized_href.endswith(".pdf")
        or ".pdf?" in normalized_href
        or "fileextension=pdf" in normalized_href
    )


@dataclass(frozen=True)
class _ParsedCandidateItem:
    row_index: int
    explicit_ordinal: int | None
    title: str
    page_count: int | None
    photo_count: int | None
    display_type: str | None
    view_url: str | None
    download_url: str | None
    normalized_title_slug: str


@dataclass(frozen=True)
class ParsedDocketResult:
    docket: Docket
    items: list[DocketItem]
    ordinal_is_stable: bool
    ordinal_issues: tuple[str, ...]


def _parse_candidate_items(*, soup: Any, public_url: str) -> list[_ParsedCandidateItem]:
    candidates: list[_ParsedCandidateItem] = []
    for row_index, row in enumerate(_candidate_rows(soup), start=1):
        cells = list(row.find_all("td"))
        if not cells:
            continue

        row_text = _text(row)
        links = row.find_all("a", href=True)
        title = _text(cells[1] if len(cells) > 1 else cells[0])
        if not title:
            continue
        if "photo" in title.lower() and not any(
            _is_pdf_href(link["href"]) for link in links
        ):
            continue

        first_cell_text = _text(cells[0])
        explicit_ordinal = (
            int(first_cell_text) if INTEGER_RE.fullmatch(first_cell_text) else None
        )

        pdf_link = next(
            (link["href"] for link in links if _is_pdf_href(link["href"])),
            None,
        )
        href = pdf_link or (links[0]["href"] if links else None)
        if len(cells) >= 6:
            page_count = _cell_integer(cells[2])
            photo_count = _cell_integer(cells[3])
            display_type_text = _text(cells[4])
        else:
            page_count, photo_count = _parse_counts(row_text)
            display_type_text = _text(cells[2]) if len(cells) > 2 else ""
        display_type = display_type_text or None

        if href is None:
            continue

        download_url = urljoin(public_url, href) if href else None
        normalized_title_slug = slugify(title)
        candidates.append(
            _ParsedCandidateItem(
                row_index=row_index,
                explicit_ordinal=explicit_ordinal,
                title=title,
                page_count=page_count,
                photo_count=photo_count,
                display_type=display_type,
                view_url=download_url,
                download_url=download_url,
                normalized_title_slug=normalized_title_slug,
            )
        )
    return candidates


def _materialize_items(
    *,
    ntsb_number: str,
    candidates: list[_ParsedCandidateItem],
) -> tuple[list[DocketItem], bool, tuple[str, ...], list[str]]:
    ordinal_issues: list[str] = []
    seen_ordinals: set[int] = set()
    ordinal_is_stable = True

    for candidate in candidates:
        ordinal = candidate.explicit_ordinal
        if ordinal is None:
            ordinal_is_stable = False
            ordinal_issues.append(f"missing ordinal at row {candidate.row_index}")
            continue
        if ordinal <= 0:
            ordinal_is_stable = False
            ordinal_issues.append(f"non-positive ordinal {ordinal} at row {candidate.row_index}")
            continue
        if ordinal in seen_ordinals:
            ordinal_is_stable = False
            ordinal_issues.append(f"duplicate ordinal {ordinal}")
            continue
        seen_ordinals.add(ordinal)

    if ordinal_is_stable:
        ordered_candidates = sorted(
            candidates,
            key=lambda candidate: (
                candidate.explicit_ordinal if candidate.explicit_ordinal is not None else 0
            ),
        )
        effective_ordinals = []
        for candidate in ordered_candidates:
            assert candidate.explicit_ordinal is not None
            effective_ordinals.append(candidate.explicit_ordinal)
    else:
        ordered_candidates = list(candidates)
        effective_ordinals = list(range(1, len(ordered_candidates) + 1))

    items: list[DocketItem] = []
    ordered_item_fingerprints: list[str] = []
    for ordinal, candidate in zip(effective_ordinals, ordered_candidates, strict=True):
        docket_item = DocketItem(
            docket_item_id=build_docket_item_id(
                ntsb_number=ntsb_number,
                ordinal=ordinal,
                title=candidate.title,
            ),
            ntsb_number=ntsb_number,
            ordinal=ordinal,
            normalized_title_slug=candidate.normalized_title_slug,
            title=candidate.title,
            page_count=candidate.page_count,
            photo_count=candidate.photo_count,
            display_type=candidate.display_type,
            view_url=candidate.view_url,
            download_url=candidate.download_url,
            source_fingerprint=docket_item_fingerprint(
                ntsb_number=ntsb_number,
                ordinal=ordinal,
                normalized_title_slug=candidate.normalized_title_slug,
                title=candidate.title,
                page_count=candidate.page_count,
                photo_count=candidate.photo_count,
                display_type=candidate.display_type,
                view_url=candidate.view_url,
                download_url=candidate.download_url,
            ),
            is_active=True,
        )
        items.append(docket_item)
        ordered_item_fingerprints.append(docket_item.source_fingerprint or "")

    return items, ordinal_is_stable, tuple(ordinal_issues), ordered_item_fingerprints


def parse_docket_html_result(*, ntsb_number: str, html: str, public_url: str) -> ParsedDocketResult:
    normalized_ntsb_number = normalize_ntsb_number(ntsb_number)
    soup = BeautifulSoup(html, "lxml")
    metadata = _parse_metadata(soup)
    candidates = _parse_candidate_items(soup=soup, public_url=public_url)
    items, ordinal_is_stable, ordinal_issues, ordered_item_fingerprints = _materialize_items(
        ntsb_number=normalized_ntsb_number,
        candidates=candidates,
    )
    docket_id = build_docket_id(normalized_ntsb_number)
    docket = Docket(
        docket_id=docket_id,
        ntsb_number=normalized_ntsb_number,
        public_url=public_url,
        item_count=len(items),
        creation_date=metadata.get("created"),
        last_modified=metadata.get("last modified"),
        public_release_at=metadata.get("public release"),
        source_fingerprint=docket_fingerprint(
            ntsb_number=normalized_ntsb_number,
            docket_id=docket_id,
            public_url=public_url,
            item_count=len(items),
            creation_date=metadata.get("created"),
            last_modified=metadata.get("last modified"),
            public_release_at=metadata.get("public release"),
            ordered_item_fingerprints=ordered_item_fingerprints,
        ),
    )
    return ParsedDocketResult(
        docket=docket,
        items=items,
        ordinal_is_stable=ordinal_is_stable,
        ordinal_issues=ordinal_issues,
    )


def parse_docket_html(
    *,
    ntsb_number: str,
    html: str,
    public_url: str,
) -> tuple[Docket, list[DocketItem]]:
    result = parse_docket_html_result(
        ntsb_number=ntsb_number,
        html=html,
        public_url=public_url,
    )
    return result.docket, result.items


class DocketClient:
    def __init__(self, *, base_url: str, http_client: HttpClient) -> None:
        self._base_url = base_url.rstrip("/")
        self._http_client = http_client

    def docket_url(self, project_id: str) -> str:
        return f"{self._base_url}?ProjectID={project_id}"

    def fetch_docket_html(self, project_id: str) -> tuple[str, str]:
        url = self.docket_url(project_id)
        return self._http_client.get_text(url), url


@dataclass(frozen=True)
class DocketEnumerationResult:
    ntsb_number: str
    item_count: int
    changed: bool
    ordinal_is_stable: bool = True
    mutation_suppressed: bool = False


def enumerate_docket(
    connection: sqlite3.Connection,
    client: DocketClient,
    *,
    ntsb_number: str,
    run_context: RunContext,
    manifest_path: Path,
) -> DocketEnumerationResult:
    observed_at = run_context.started_at
    normalized_ntsb_number = normalize_ntsb_number(ntsb_number)
    investigation = fetch_one(
        connection,
        "SELECT project_id FROM investigations_current WHERE ntsb_number = ?",
        (normalized_ntsb_number,),
    )
    project_id = investigation["project_id"] if investigation is not None else None
    if not isinstance(project_id, str) or not project_id.strip():
        search_result = fetch_one(
            connection,
            "SELECT project_id FROM docket_search_results_current WHERE ntsb_number = ?",
            (normalized_ntsb_number,),
        )
        project_id = search_result["project_id"] if search_result is not None else None
    if not isinstance(project_id, str) or not project_id.strip():
        raise ValueError(
            f"project_id is required for docket enumeration for {normalized_ntsb_number}"
        )
    html, public_url = client.fetch_docket_html(project_id.strip())
    raw_html_path = (
        run_context.paths.acquisition_raw_root / "dockets" / f"{normalized_ntsb_number}.html"
    )
    raw_html_path.parent.mkdir(parents=True, exist_ok=True)
    raw_html_path.write_text(html, encoding="utf-8")

    parsed = parse_docket_html_result(
        ntsb_number=normalized_ntsb_number,
        html=html,
        public_url=public_url,
    )
    docket = parsed.docket.model_copy(
        update={
            "raw_html_path": str(raw_html_path),
            "retrieved_at": observed_at,
        }
    )

    existing = fetch_one(
        connection,
        "SELECT source_fingerprint FROM dockets_current WHERE ntsb_number = ?",
        (normalized_ntsb_number,),
    )
    changed = existing is None or existing["source_fingerprint"] != docket.source_fingerprint

    upsert_docket_current(
        connection,
        docket,
        observed_at=observed_at,
        run_id=run_context.run_id,
    )
    note_parts = [f"item_count={len(parsed.items)}"]
    mutation_suppressed = not parsed.ordinal_is_stable
    if parsed.ordinal_is_stable:
        for item in parsed.items:
            upsert_docket_item_current(
                connection,
                item,
                observed_at=observed_at,
                run_id=run_context.run_id,
            )
        deactivate_missing_docket_items(
            connection,
            ntsb_number=normalized_ntsb_number,
            active_docket_item_ids=[item.docket_item_id for item in parsed.items],
            run_id=run_context.run_id,
        )
        note_parts.append("ordinal_stability=stable")
    else:
        note_parts.append("ordinal_stability=unstable")
        note_parts.append("current_state_mutation=suppressed")
        if parsed.ordinal_issues:
            note_parts.append(f"ordinal_issues={'|'.join(parsed.ordinal_issues)}")
    append_jsonl_record(
        manifest_path,
        AcquisitionManifestRecord(
            run_id=run_context.run_id,
            source=DOCKET_SOURCE,
            ntsb_number=normalized_ntsb_number,
            project_id=project_id,
            action="enumerate_docket",
            input_fingerprint=docket.source_fingerprint,
            output_path=str(raw_html_path),
            parser_version=DOCKET_PARSER_VERSION,
            retrieved_at=observed_at,
            changed=changed,
            note="; ".join(note_parts),
        ),
    )
    connection.commit()

    return DocketEnumerationResult(
        ntsb_number=normalized_ntsb_number,
        item_count=len(parsed.items),
        changed=changed,
        ordinal_is_stable=parsed.ordinal_is_stable,
        mutation_suppressed=mutation_suppressed,
    )
