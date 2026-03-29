from __future__ import annotations

import sqlite3
from pathlib import Path

from incident_pipeline.common.paths import REPO_ROOT
from incident_pipeline.common.settings import load_settings

CONFIG_PATH = REPO_ROOT / "configs" / "settings.yaml"
SQL_FILE = REPO_ROOT / "sql" / "init_manifest.sql"


def load_config():
    config_path = None if CONFIG_PATH == REPO_ROOT / "configs" / "settings.yaml" else CONFIG_PATH
    return load_settings(config_path)


def main():
    cfg = load_config()

    db_path = Path(cfg["database"]["manifest_path"])
    db_path.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(db_path)

    sql = SQL_FILE.read_text()
    conn.executescript(sql)

    conn.commit()
    conn.close()

    print(f"Manifest database initialized: {db_path}")


if __name__ == "__main__":
    main()
