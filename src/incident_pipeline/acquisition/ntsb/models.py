from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", populate_by_name=True)


class SelectionDecision(StrictModel):
    selected: bool
    rule_version: str
    matched_rules: list[str] = Field(default_factory=list)
    rationale: str


class Investigation(StrictModel):
    investigation_id: str
    ntsb_number: str
    project_id: str | None = None
    mode: str | None = None
    event_date: str | None = None
    location: dict[str, Any] = Field(default_factory=dict)
    title: str | None = None
    case_type: str | None = None
    source_fingerprint: str | None = None
    carol_metadata: dict[str, Any] = Field(default_factory=dict)
    docket_summary: dict[str, Any] = Field(default_factory=dict)
    acquisition_status: dict[str, Any] = Field(default_factory=dict)


class Docket(StrictModel):
    docket_id: str
    ntsb_number: str
    project_id: str | None = None
    mode: str | None = None
    public_url: str | None = None
    item_count: int = 0
    creation_date: str | None = None
    last_modified: str | None = None
    public_release_at: str | None = None
    raw_html_path: str | None = None
    retrieved_at: str | None = None
    source_fingerprint: str | None = None


class DocketSearchResult(StrictModel):
    ntsb_number: str
    project_id: str
    result_url: str
    event_date: str | None = None
    city: str | None = None
    state_or_region: str | None = None
    docket_details: str | None = None
    accident_description: str | None = None
    source_fingerprint: str | None = None


class DocketItem(StrictModel):
    docket_item_id: str
    ntsb_number: str
    ordinal: int
    normalized_title_slug: str | None = None
    title: str
    page_count: int | None = None
    photo_count: int | None = None
    display_type: str | None = None
    view_url: str | None = None
    download_url: str | None = None
    source_fingerprint: str | None = None
    is_active: bool = True
    selection: SelectionDecision | None = None
    download: dict[str, Any] = Field(default_factory=dict)


class BlobRecord(StrictModel):
    blob_sha256: str
    blob_path: str
    media_type: str | None = None
    size_bytes: int


class DownloadRecord(StrictModel):
    docket_item_id: str
    ntsb_number: str
    source_url: str
    source_fingerprint: str | None = None
    blob_sha256: str
    blob_path: str
    media_type: str | None = None
    size_bytes: int
    http_status: int | None = None
    downloaded_at: str


class PromotionRecord(StrictModel):
    docket_item_id: str
    ntsb_number: str
    blob_sha256: str
    promoted_path: str
    rule_version: str
    matched_rules: list[str] = Field(default_factory=list)
    rationale: str
    promoted_at: str


class AcquisitionManifestRecord(StrictModel):
    run_id: str
    source: str
    ntsb_number: str
    project_id: str | None = None
    action: str
    input_fingerprint: str | None = None
    output_path: str | None = None
    content_hash: str | None = None
    http_status: int | None = None
    parser_version: str | None = None
    retrieved_at: str
    changed: bool
    note: str | None = None
    error: str | None = None


class PromotionManifestRecord(StrictModel):
    run_id: str
    ntsb_number: str
    docket_item_id: str
    blob_sha256: str
    source_blob_path: str
    promoted_path: str
    rule_version: str
    matched_rules: list[str] = Field(default_factory=list)
    rationale: str
    promoted_at: str
    downstream_registration_status: str


class SelectionManifestRecord(StrictModel):
    run_id: str
    source: str
    action: str
    ntsb_number: str
    docket_item_id: str
    selected: bool
    rule_version: str
    matched_rules: list[str] = Field(default_factory=list)
    rationale: str
    decided_at: str
