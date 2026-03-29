from __future__ import annotations

from incident_pipeline.extract.structure_extract import run_structure_batch


def main() -> None:
    try:
        summary = run_structure_batch()
    except FileNotFoundError as exc:
        print(str(exc))
        raise SystemExit(1) from exc

    for key in ("selected", "completed", "reused_existing", "failed"):
        print(f"{key}={summary[key]}")


if __name__ == "__main__":
    main()
