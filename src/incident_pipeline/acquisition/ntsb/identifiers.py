from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path

from incident_pipeline.acquisition.ntsb.hashing import sha256_canonical_json
from incident_pipeline.acquisition.ntsb.normalize import collapse_whitespace, slugify


def normalize_ntsb_number(value: str) -> str:
    return collapse_whitespace(value).upper().replace(" ", "")


def build_investigation_id(ntsb_number: str) -> str:
    normalized = normalize_ntsb_number(ntsb_number)
    return f"ntsb:investigation:{normalized}"


def build_docket_id(ntsb_number: str) -> str:
    normalized = normalize_ntsb_number(ntsb_number)
    return f"ntsb:docket:{normalized}"


def build_docket_item_id(*, ntsb_number: str, ordinal: int, title: str) -> str:
    normalized_number = normalize_ntsb_number(ntsb_number)
    normalized_title_slug = slugify(title)
    return f"ntsb:docket_item:{normalized_number}:{ordinal}:{normalized_title_slug}"


def source_fingerprint(value: object) -> str:
    return sha256_canonical_json(value)


def docket_item_fingerprint(
    *,
    ntsb_number: str,
    ordinal: int,
    normalized_title_slug: str,
    title: str,
    page_count: int | None,
    photo_count: int | None,
    display_type: str | None,
    view_url: str | None,
    download_url: str | None,
) -> str:
    payload = {
        "ntsb_number": normalize_ntsb_number(ntsb_number),
        "ordinal": ordinal,
        "normalized_title_slug": normalized_title_slug,
        "title": title,
        "page_count": page_count,
        "photo_count": photo_count,
        "display_type": display_type,
        "view_url": view_url,
        "download_url": download_url,
    }
    return sha256_canonical_json(payload)


def docket_fingerprint(
    *,
    ntsb_number: str,
    docket_id: str,
    public_url: str | None,
    item_count: int,
    creation_date: str | None,
    last_modified: str | None,
    public_release_at: str | None,
    ordered_item_fingerprints: list[str],
) -> str:
    payload = {
        "ntsb_number": normalize_ntsb_number(ntsb_number),
        "docket_id": docket_id,
        "public_url": public_url,
        "item_count": item_count,
        "creation_date": creation_date,
        "last_modified": last_modified,
        "public_release_at": public_release_at,
        "ordered_item_fingerprints": ordered_item_fingerprints,
    }
    return sha256_canonical_json(payload)


def investigation_fingerprint(
    *,
    ntsb_number: str,
    investigation_id: str,
    project_id: str | None,
    mode: str | None,
    event_date: str | None,
    title: str | None,
    case_type: str | None,
    location: Mapping[str, object],
) -> str:
    payload = {
        "ntsb_number": normalize_ntsb_number(ntsb_number),
        "investigation_id": investigation_id,
        "project_id": project_id,
        "mode": mode,
        "event_date": event_date,
        "title": title,
        "case_type": case_type,
        "location": dict(location),
    }
    return sha256_canonical_json(payload)


def blob_relative_path(blob_sha256: str) -> Path:
    return Path("sha256") / blob_sha256[:2] / blob_sha256[2:4] / blob_sha256
