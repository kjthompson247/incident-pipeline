from __future__ import annotations

from incident_pipeline.extract.pdf_extract import run_extraction_batch


def main() -> None:
    try:
        summary = run_extraction_batch()
    except FileNotFoundError as exc:
        print(str(exc))
        raise SystemExit(1) from exc

    for key in ("selected", "completed", "queued_for_ocr", "reused_existing", "failed"):
        print(f"{key}={summary[key]}")


if __name__ == "__main__":
    main()
