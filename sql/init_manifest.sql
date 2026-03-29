
CREATE TABLE IF NOT EXISTS documents (
    doc_id TEXT PRIMARY KEY,
    jurisdiction TEXT,
    case_number TEXT,
    filing_date TEXT,
    doc_type TEXT,
    source_url TEXT,
    acquisition_run_id TEXT,
    acquisition_manifest_path TEXT,
    project_id TEXT,
    ntsb_number TEXT,
    docket_item_id TEXT,
    view_url TEXT,
    blob_sha256 TEXT,

    raw_path TEXT UNIQUE,
    sha256 TEXT UNIQUE,
    file_size INTEGER,
    extracted_text_path TEXT,
    structured_json_path TEXT,
    structured_debug_path TEXT,

    status TEXT,
    stage TEXT,

    ingested_at TEXT,
    last_processed_at TEXT,
    error_message TEXT
);

CREATE INDEX IF NOT EXISTS idx_documents_sha256 ON documents(sha256);
CREATE INDEX IF NOT EXISTS idx_documents_status ON documents(status);
CREATE INDEX IF NOT EXISTS idx_documents_stage ON documents(stage);

CREATE TABLE IF NOT EXISTS ocr_runs (
    ocr_run_id INTEGER PRIMARY KEY AUTOINCREMENT,
    doc_id TEXT,
    started_at TEXT,
    completed_at TEXT,
    engine TEXT,
    success INTEGER,
    confidence REAL,
    output_path TEXT,
    error_message TEXT
);

CREATE TABLE IF NOT EXISTS parsed_documents (
    parse_id INTEGER PRIMARY KEY AUTOINCREMENT,
    doc_id TEXT,
    parser_name TEXT,
    output_json_path TEXT,
    full_text_path TEXT,
    page_count INTEGER,
    success INTEGER,
    created_at TEXT
);

CREATE TABLE IF NOT EXISTS chunks (
    chunk_id TEXT PRIMARY KEY,
    doc_id TEXT,
    page_start INTEGER,
    page_end INTEGER,
    chunk_index INTEGER,
    chunk_text_path TEXT,
    token_count INTEGER,
    embedded INTEGER DEFAULT 0
);
