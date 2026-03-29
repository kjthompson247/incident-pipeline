from __future__ import annotations

import html
import json
import os
import subprocess
import sys
from pathlib import Path

from rich import box
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from incident_pipeline.common.paths import DEFAULT_NTSB_SOURCE_ROOT, resolve_repo_path

REPORTS_ROOT_ENV_VAR = "INCIDENT_PIPELINE_ACQUISITION_REPORTS_ROOT"


def _default_reports_root() -> Path:
    configured = os.environ.get(REPORTS_ROOT_ENV_VAR)
    if configured:
        return resolve_repo_path(configured)
    return DEFAULT_NTSB_SOURCE_ROOT / "acquisition" / "reports"


REPORTS_ROOT = _default_reports_root()
_FILE_LINK_FIELDS = {"output_dir", "output_path", "report_path"}

_LABELS = {
    "command": "Command",
    "run_id": "Run ID",
    "timestamp": "Timestamp",
    "ntsb_number": "NTSB Number",
    "output_dir": "Output Directory",
    "output_path": "Output Path",
    "report_path": "HTML Report",
}


def _label(key: str) -> str:
    return _LABELS.get(key, key.replace("_", " ").title())


def _value_text(value: object) -> str:
    if value is None:
        return "-"
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, bool):
        return json.dumps(value)
    if isinstance(value, str | int | float):
        return str(value)
    return json.dumps(value, sort_keys=True, ensure_ascii=True)


def _report_filename(*, command: str, run_id: str, timestamp: str) -> str:
    safe_timestamp = (
        timestamp.replace("-", "").replace(":", "").replace("T", "_").replace("Z", "Z")
    )
    return f"{safe_timestamp}_{command}_{run_id}.html"


def _summary_rows(
    *,
    command: str,
    run_id: str,
    timestamp: str,
    fields: dict[str, object],
) -> list[tuple[str, str]]:
    rows = [
        ("command", command),
        ("run_id", run_id),
        ("timestamp", timestamp),
    ]
    rows.extend((key, _value_text(value)) for key, value in fields.items())
    return [(_label(key), value) for key, value in rows]


def _html_table_value(key: str, value: str) -> str:
    escaped_value = html.escape(value)
    if key not in _FILE_LINK_FIELDS or value == "-":
        return escaped_value
    return f'<a href="{html.escape(Path(value).as_uri())}">{escaped_value}</a>'


def _int_value(value: object) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, str) and value.isdigit():
        return int(value)
    return None


def build_narrative(command: str, fields: dict[str, object]) -> list[str]:
    if command == "materialize-case":
        ntsb_number = _value_text(fields.get("ntsb_number"))
        materialized = _int_value(fields.get("materialized"))
        reused = _int_value(fields.get("reused"))
        if materialized is None or reused is None:
            return []
        if materialized > 0 and reused == 0:
            return [
                (
                    f"This run created a new materialized case view for {ntsb_number} "
                    f"and wrote {materialized} files into the output directory."
                )
            ]
        if materialized > 0 and reused > 0:
            return [
                (
                    f"This run added {materialized} new files to the materialized case view "
                    f"for {ntsb_number} and reused {reused} existing files."
                )
            ]
        if materialized == 0 and reused > 0:
            return [
                (
                    f"This run found the materialized case view for {ntsb_number} "
                    "already complete. "
                    f"No new files were created, and {reused} existing files were reused."
                ),
                "Why counts look this way",
                "Materialized 0 means no new files needed to be created.",
                (
                    f"Reused {reused} means the case view already contained "
                    f"{reused} valid files."
                ),
            ]
        return [
            (
                f"This run completed materialize-case for {ntsb_number} with "
                f"{materialized} newly created files and {reused} reused files."
            )
        ]

    if command == "sync-investigations":
        discovered = _int_value(fields.get("discovered"))
        changed = _int_value(fields.get("changed"))
        unchanged = _int_value(fields.get("unchanged"))
        if discovered is None or changed is None or unchanged is None:
            return []
        narrative = [
            (
                "This run processed CAROL investigations: "
                f"{discovered} newly discovered, {changed} updated, and "
                f"{unchanged} re-read with no changes."
            )
        ]
        if changed > 0:
            narrative.append(
                "Updated records mean the latest CAROL response changed "
                "the indexed investigation state."
            )
        elif unchanged > 0:
            narrative.append(
                "Re-read and confirmed records were fetched again and "
                "matched the existing indexed state."
            )
        return narrative

    if command == "enumerate-dockets":
        enumerated = _int_value(fields.get("enumerated"))
        changed = _int_value(fields.get("changed"))
        total_items = _int_value(fields.get("total_items"))
        if enumerated is None or changed is None or total_items is None:
            return []
        unchanged = max(enumerated - changed, 0)
        narrative = [
            (
                f"This run enumerated {enumerated} docket pages and indexed "
                f"{total_items} docket items."
            )
        ]
        if changed > 0 and unchanged > 0:
            narrative.append(
                f"It detected docket changes for {changed} cases and "
                f"re-read and confirmed {unchanged} unchanged cases."
            )
        elif changed > 0:
            narrative.append(
                f"It detected docket changes for {changed} enumerated cases."
            )
        elif enumerated > 0:
            narrative.append(
                "It re-read the docket pages and confirmed the indexed "
                "docket inventory was unchanged."
            )
        return narrative

    if command == "download-selected":
        evaluated = _int_value(fields.get("evaluated"))
        selected = _int_value(fields.get("selected"))
        rejected = _int_value(fields.get("rejected"))
        downloaded = _int_value(fields.get("downloaded"))
        reused = _int_value(fields.get("reused"))
        if (
            evaluated is None
            or selected is None
            or rejected is None
            or downloaded is None
            or reused is None
        ):
            return []
        narrative = [
            (
                f"This run evaluated {evaluated} docket items for download selection: "
                f"{selected} selected and {rejected} rejected."
            )
        ]
        if downloaded > 0 and reused > 0:
            narrative.append(
                f"It downloaded {downloaded} new files and reused "
                f"{reused} previously acquired files."
            )
        elif downloaded > 0:
            narrative.append(f"It downloaded {downloaded} new files.")
        elif reused > 0:
            narrative.append(
                f"No new downloads were needed. The run reused {reused} "
                "previously acquired files."
            )
        else:
            narrative.append("No new downloads were performed during this pass.")
        return narrative

    return []


def emit_terminal_summary(
    *,
    command: str,
    run_id: str,
    timestamp: str,
    **fields: object,
) -> None:
    table = Table(show_header=False, box=box.SIMPLE_HEAVY, expand=True)
    table.add_column("Field", style="bold cyan", no_wrap=True)
    table.add_column("Value")
    for label, value in _summary_rows(
        command=command,
        run_id=run_id,
        timestamp=timestamp,
        fields=fields,
    ):
        table.add_row(label, value)
    console = Console(file=sys.stderr)
    console.print(Panel.fit(table, title="Acquisition Summary", border_style="green"))


def render_html_report(
    *,
    command: str,
    run_id: str,
    timestamp: str,
    **fields: object,
) -> str:
    narrative_lines = build_narrative(command, fields)
    narrative = ""
    if narrative_lines:
        paragraphs = "".join(f"<p>{html.escape(line)}</p>" for line in narrative_lines)
        narrative = f"""
    <section class="explanation">
      <h2>What happened</h2>
      {paragraphs}
    </section>
"""
    summary_fields = [
        ("command", command),
        ("run_id", run_id),
        ("timestamp", timestamp),
        *[(key, _value_text(value)) for key, value in fields.items()],
    ]
    rows = "".join(
        (
            "<tr>"
            f"<th>{html.escape(_label(key))}</th>"
            f"<td>{_html_table_value(key, value)}</td>"
            "</tr>"
        )
        for key, value in summary_fields
    )
    title = html.escape(f"ntsb-acquire {command} {run_id}")
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>{title}</title>
  <style>
    :root {{
      color-scheme: light;
      font-family: Menlo, Monaco, "DejaVu Sans Mono", monospace;
    }}
    body {{
      margin: 2rem;
      background: #f7f4ee;
      color: #1f2933;
    }}
    main {{
      max-width: 960px;
      margin: 0 auto;
      background: #ffffff;
      border: 1px solid #d5dce5;
      border-radius: 12px;
      padding: 1.5rem;
      box-shadow: 0 10px 30px rgba(15, 23, 42, 0.08);
    }}
    h1 {{
      margin-top: 0;
      color: #0f3d5e;
      font-size: 1.6rem;
    }}
    h2 {{
      color: #0f3d5e;
      font-size: 1.1rem;
      margin-top: 1.5rem;
    }}
    table {{
      width: 100%;
      border-collapse: collapse;
      margin-top: 1rem;
    }}
    th,
    td {{
      text-align: left;
      padding: 0.75rem;
      border-bottom: 1px solid #e5e7eb;
      vertical-align: top;
    }}
    th {{
      width: 18rem;
      color: #0f3d5e;
    }}
    .explanation {{
      margin-top: 1.5rem;
      padding: 1rem 1.25rem;
      background: #f8fafc;
      border: 1px solid #d5dce5;
      border-radius: 10px;
    }}
    .explanation p {{
      margin: 0.5rem 0 0;
    }}
    .explanation p:first-of-type {{
      margin-top: 0;
    }}
  </style>
</head>
<body>
  <main>
    <h1>Acquisition Summary</h1>
    {narrative}
    <table>
      <tbody>
        {rows}
      </tbody>
    </table>
  </main>
</body>
</html>
"""


def write_html_report(
    *,
    command: str,
    run_id: str,
    timestamp: str,
    **fields: object,
) -> Path:
    REPORTS_ROOT.mkdir(parents=True, exist_ok=True)
    report_path = REPORTS_ROOT / _report_filename(
        command=command,
        run_id=run_id,
        timestamp=timestamp,
    )
    report_path.write_text(
        render_html_report(
            command=command,
            run_id=run_id,
            timestamp=timestamp,
            **fields,
        ),
        encoding="utf-8",
    )
    return report_path


def open_html_report(report_path: Path) -> None:
    if sys.platform != "darwin":
        return
    subprocess.run(
        ["open", str(report_path)],
        check=False,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
