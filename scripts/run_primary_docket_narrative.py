from __future__ import annotations

from incident_pipeline.narrative.primary_docket_narrative import run_primary_docket_narrative_batch


def main() -> None:
    try:
        summary = run_primary_docket_narrative_batch()
    except (FileNotFoundError, KeyError, ValueError) as exc:
        print(str(exc))
        raise SystemExit(1) from exc

    for key in ("dockets", "selected", "reused_existing", "failed"):
        print(f"{key}={summary[key]}")


if __name__ == "__main__":
    main()
