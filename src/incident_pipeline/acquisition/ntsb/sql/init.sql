PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS schema_meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

INSERT INTO schema_meta(key, value)
VALUES ('schema_version', '1')
ON CONFLICT(key) DO UPDATE SET value = excluded.value;

CREATE TABLE IF NOT EXISTS investigations_current (
    ntsb_number TEXT PRIMARY KEY,
    investigation_id TEXT NOT NULL,
    project_id TEXT,
    mode TEXT,
    event_date TEXT,
    title TEXT,
    case_type TEXT,
    location_json TEXT NOT NULL,
    carol_metadata_json TEXT NOT NULL,
    source_fingerprint TEXT,
    first_seen_at TEXT NOT NULL,
    last_seen_at TEXT NOT NULL,
    last_run_id TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS dockets_current (
    ntsb_number TEXT PRIMARY KEY,
    docket_id TEXT NOT NULL,
    project_id TEXT,
    mode TEXT,
    public_url TEXT,
    item_count INTEGER NOT NULL,
    creation_date TEXT,
    last_modified TEXT,
    public_release_at TEXT,
    raw_html_path TEXT,
    retrieved_at TEXT,
    source_fingerprint TEXT,
    first_seen_at TEXT NOT NULL,
    last_seen_at TEXT NOT NULL,
    last_run_id TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS docket_search_results_current (
    ntsb_number TEXT PRIMARY KEY,
    project_id TEXT NOT NULL,
    result_url TEXT NOT NULL,
    event_date TEXT,
    city TEXT,
    state_or_region TEXT,
    docket_details TEXT,
    accident_description TEXT,
    source_fingerprint TEXT,
    first_seen_at TEXT NOT NULL,
    last_seen_at TEXT NOT NULL,
    last_run_id TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_docket_search_results_current_project_id
ON docket_search_results_current(project_id);

CREATE TABLE IF NOT EXISTS docket_items_current (
    docket_item_id TEXT PRIMARY KEY,
    ntsb_number TEXT NOT NULL,
    ordinal INTEGER NOT NULL,
    normalized_title_slug TEXT NOT NULL,
    title TEXT NOT NULL,
    page_count INTEGER,
    photo_count INTEGER,
    display_type TEXT,
    view_url TEXT,
    download_url TEXT,
    source_fingerprint TEXT,
    selection_selected INTEGER,
    selection_rule_version TEXT,
    selection_matched_rules_json TEXT,
    selection_rationale TEXT,
    is_active INTEGER NOT NULL DEFAULT 1 CHECK (is_active IN (0, 1)),
    first_seen_at TEXT NOT NULL,
    last_seen_at TEXT NOT NULL,
    last_run_id TEXT NOT NULL,
    UNIQUE (ntsb_number, ordinal)
);

CREATE INDEX IF NOT EXISTS idx_docket_items_current_ntsb_number
ON docket_items_current(ntsb_number);

CREATE TABLE IF NOT EXISTS blobs_current (
    blob_sha256 TEXT PRIMARY KEY,
    blob_path TEXT NOT NULL,
    media_type TEXT,
    size_bytes INTEGER NOT NULL,
    first_downloaded_at TEXT NOT NULL,
    first_run_id TEXT NOT NULL,
    last_verified_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS downloads_current (
    docket_item_id TEXT PRIMARY KEY,
    ntsb_number TEXT NOT NULL,
    source_url TEXT NOT NULL,
    source_fingerprint TEXT,
    blob_sha256 TEXT NOT NULL,
    blob_path TEXT NOT NULL,
    media_type TEXT,
    size_bytes INTEGER NOT NULL,
    http_status INTEGER,
    downloaded_at TEXT NOT NULL,
    last_run_id TEXT NOT NULL,
    FOREIGN KEY (docket_item_id) REFERENCES docket_items_current(docket_item_id) ON UPDATE CASCADE ON DELETE CASCADE,
    FOREIGN KEY (blob_sha256) REFERENCES blobs_current(blob_sha256)
);

CREATE INDEX IF NOT EXISTS idx_downloads_current_ntsb_number
ON downloads_current(ntsb_number);

CREATE TABLE IF NOT EXISTS promotions_current (
    docket_item_id TEXT PRIMARY KEY,
    ntsb_number TEXT NOT NULL,
    blob_sha256 TEXT NOT NULL,
    promoted_path TEXT NOT NULL,
    rule_version TEXT NOT NULL,
    matched_rules_json TEXT NOT NULL,
    rationale TEXT NOT NULL,
    promoted_at TEXT NOT NULL,
    last_run_id TEXT NOT NULL,
    FOREIGN KEY (docket_item_id) REFERENCES docket_items_current(docket_item_id) ON UPDATE CASCADE ON DELETE CASCADE,
    FOREIGN KEY (blob_sha256) REFERENCES blobs_current(blob_sha256)
);

CREATE INDEX IF NOT EXISTS idx_promotions_current_ntsb_number
ON promotions_current(ntsb_number);
