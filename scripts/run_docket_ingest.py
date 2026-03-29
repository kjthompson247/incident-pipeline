from __future__ import annotations

from incident_pipeline.ingestion.docket_ingest import run_docket_ingest_batch


def main() -> None:
    try:
        summary = run_docket_ingest_batch()
    except (FileNotFoundError, KeyError, ValueError) as exc:
        print(str(exc))
        raise SystemExit(1) from exc

    for key in ("selected", "completed", "reused_existing", "failed"):
        print(f"{key}={summary[key]}")


if __name__ == "__main__":
    main()
