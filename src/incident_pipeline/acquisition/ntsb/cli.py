from __future__ import annotations

import io
import json
import platform
import sqlite3
import sys
import tempfile
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, TextIO

import typer
from rich import box
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from incident_pipeline.acquisition.ntsb.blobs import download_selected as download_selected_artifacts
from incident_pipeline.acquisition.ntsb.carol import CarolClient
from incident_pipeline.acquisition.ntsb.carol import sync_investigations as sync_carol_investigations
from incident_pipeline.acquisition.ntsb.case_views import materialize_case
from incident_pipeline.acquisition.ntsb.config import AppConfig, load_config
from incident_pipeline.acquisition.ntsb.db import connect_sqlite, fetch_all
from incident_pipeline.acquisition.ntsb.db import init_db as initialize_db
from incident_pipeline.acquisition.ntsb.docket_search import (
    PUBLIC_DOCKET_SEARCH_URL,
    DocketSearchClient,
    discover_dockets,
)
from incident_pipeline.acquisition.ntsb.dockets import DocketClient, enumerate_docket
from incident_pipeline.acquisition.ntsb.feedback import emit_terminal_summary, open_html_report, write_html_report
from incident_pipeline.acquisition.ntsb.http import HttpClient, build_http_client
from incident_pipeline.acquisition.ntsb.log import emit_console_event
from incident_pipeline.acquisition.ntsb.promotion import promote_selected as promote_selected_artifacts
from incident_pipeline.acquisition.ntsb.runtime import RunContext, create_run_context
from incident_pipeline.acquisition.ntsb.selection import apply_selection

app = typer.Typer(
    add_completion=False,
    no_args_is_help=True,
    help=(
        "Deterministic acquisition layer for NTSB investigation discovery, "
        "docket enumeration, download, and promotion."
    ),
)

CONFIG_HELP = "Override config for this invocation."
ENV_FILE_HELP = "Path to a .env-style file. Defaults to the repo-local .env when present."
NTSB_NUMBER_HELP = "Restrict the command to a single NTSB number."
SUPPORTED_PYTHON_VERSION = (3, 12, 13)
SUPPORTED_PYTHON_VERSION_TEXT = ".".join(str(part) for part in SUPPORTED_PYTHON_VERSION)


def _display_value(value: object) -> str:
    if value is None:
        return "-"
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


def _format_elapsed(seconds: float) -> str:
    total_seconds = max(0, int(seconds))
    hours, remainder = divmod(total_seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    return f"{hours:02d}:{minutes:02d}:{seconds:02d}"


def _format_collect_heartbeat(*, phase: str, elapsed_seconds: float, detail: str) -> str:
    return (
        f"Collect | Phase: {phase} | Elapsed: {_format_elapsed(elapsed_seconds)} | {detail}"
    )


def _timestamped_collect_line(*, timestamp: str, line: str) -> str:
    return f"{timestamp} | {line}"


def _summary_panel(*, title: str, **fields: object) -> Panel:
    table = Table(show_header=False, box=box.SIMPLE_HEAVY, expand=True)
    table.add_column("Field", style="bold cyan", no_wrap=True)
    table.add_column("Value")
    for key, value in fields.items():
        table.add_row(key.replace("_", " ").title(), _display_value(value))
    return Panel.fit(table, title=title, border_style="green")


@dataclass
class _CollectReporter:
    log_handle: TextIO
    started_at_monotonic: float
    stdout_console: Console
    last_heartbeat_at: float
    phase: str = ""
    detail: str = ""
    heartbeat_interval_seconds: float = 60.0
    _output_lock: threading.Lock = field(
        default_factory=threading.Lock,
        init=False,
        repr=False,
    )
    _status_lock: threading.Lock = field(
        default_factory=threading.Lock,
        init=False,
        repr=False,
    )
    _stop_event: threading.Event = field(
        default_factory=threading.Event,
        init=False,
        repr=False,
    )
    _heartbeat_thread: threading.Thread | None = field(
        default=None,
        init=False,
        repr=False,
    )

    def _write_log_line(self, line: str) -> None:
        timestamp = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
        self.log_handle.write(_timestamped_collect_line(timestamp=timestamp, line=line))
        self.log_handle.write("\n")
        self.log_handle.flush()

    def line(self, message: str) -> None:
        with self._output_lock:
            typer.echo(message)
            self._write_log_line(message)

    def summary(self, *, title: str, **fields: object) -> None:
        panel = _summary_panel(title=title, **fields)
        capture_console = Console(
            record=True,
            force_terminal=False,
            file=io.StringIO(),
            width=100,
        )
        capture_console.print(panel)
        rendered_lines = capture_console.export_text(styles=False).rstrip().splitlines()
        with self._output_lock:
            self.stdout_console.print(panel)
            for line in rendered_lines:
                self._write_log_line(line)

    def set_status(self, *, phase: str, detail: str) -> None:
        with self._status_lock:
            self.phase = phase
            self.detail = detail

    def start_heartbeat(self) -> None:
        if self._heartbeat_thread is not None:
            return
        self._heartbeat_thread = threading.Thread(
            target=self._heartbeat_loop,
            name="collect-heartbeat",
            daemon=True,
        )
        self._heartbeat_thread.start()

    def stop_heartbeat(self) -> None:
        self._stop_event.set()
        if self._heartbeat_thread is not None:
            self._heartbeat_thread.join(timeout=2.0)
            self._heartbeat_thread = None
        self.flush()

    def flush(self) -> None:
        self.log_handle.flush()

    def _heartbeat_loop(self) -> None:
        while not self._stop_event.wait(timeout=1.0):
            now = time.monotonic()
            with self._status_lock:
                if not self.phase:
                    continue
                if now - self.last_heartbeat_at < self.heartbeat_interval_seconds:
                    continue
                phase = self.phase
                detail = self.detail
                elapsed_seconds = now - self.started_at_monotonic
                self.last_heartbeat_at = now
            self.line(
                _format_collect_heartbeat(
                    phase=phase,
                    elapsed_seconds=elapsed_seconds,
                    detail=detail,
                )
            )


def _config_overrides(
    *,
    log_level: str | None,
    http_user_agent: str | None,
    http_rate_limit_per_second: float | None,
    http_max_retries: int | None,
    http_backoff_seconds: float | None,
    carol_base_url: str | None,
    docket_base_url: str | None,
) -> dict[str, object | None]:
    return {
        "log_level": log_level,
        "http_user_agent": http_user_agent,
        "http_rate_limit_per_second": http_rate_limit_per_second,
        "http_max_retries": http_max_retries,
        "http_backoff_seconds": http_backoff_seconds,
        "carol_base_url": carol_base_url,
        "docket_base_url": docket_base_url,
    }


@app.callback()
def main(
    ctx: typer.Context,
    log_level: str | None = typer.Option(None, help=CONFIG_HELP),
    http_user_agent: str | None = typer.Option(None, help=CONFIG_HELP),
    http_rate_limit_per_second: float | None = typer.Option(None),
    http_max_retries: int | None = typer.Option(None),
    http_backoff_seconds: float | None = typer.Option(None),
    carol_base_url: str | None = typer.Option(None, help=CONFIG_HELP),
    docket_base_url: str | None = typer.Option(None, help=CONFIG_HELP),
    env_file: Path | None = typer.Option(None, help=ENV_FILE_HELP),
) -> None:
    ctx.obj = {
        "carol_base_url": carol_base_url,
        "docket_base_url": docket_base_url,
        "env_file": env_file,
        "http_backoff_seconds": http_backoff_seconds,
        "http_max_retries": http_max_retries,
        "http_rate_limit_per_second": http_rate_limit_per_second,
        "http_user_agent": http_user_agent,
        "log_level": log_level,
    }


def _load_runtime_config(ctx: typer.Context) -> AppConfig:
    state = ctx.obj if isinstance(ctx.obj, dict) else {}
    return load_config(
        cli_overrides=_config_overrides(
            log_level=state.get("log_level"),
            http_user_agent=state.get("http_user_agent"),
            http_rate_limit_per_second=state.get("http_rate_limit_per_second"),
            http_max_retries=state.get("http_max_retries"),
            http_backoff_seconds=state.get("http_backoff_seconds"),
            carol_base_url=state.get("carol_base_url"),
            docket_base_url=state.get("docket_base_url"),
        ),
        env_file=state.get("env_file"),
    )


def _emit_command_result(
    *,
    command: str,
    status: str,
    state_changed: bool,
    **fields: Any,
) -> None:
    emit_console_event(
        level="info",
        event="command_result",
        command=command,
        phase="acquisition",
        state_changed=state_changed,
        status=status,
        **fields,
    )


def _emit_command_failure(
    *,
    command: str,
    error_message: str,
    error_type: str = "runtime_error",
) -> None:
    emit_console_event(
        level="error",
        event="command_result",
        command=command,
        error_message=error_message,
        error_type=error_type,
        phase="acquisition",
        state_changed=False,
        status="error",
    )


def _manifest_path(run_context: RunContext, filename: str) -> Path:
    path = run_context.paths.acquisition_manifests_root / filename
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


def _emit_command_feedback(
    *,
    command: str,
    run_context: RunContext,
    html_feedback: bool,
    **fields: Any,
) -> None:
    feedback_fields = {key: value for key, value in fields.items() if value is not None}
    report_path: Path | None = None
    if html_feedback:
        try:
            report_path = write_html_report(
                command=command,
                run_id=run_context.run_id,
                timestamp=run_context.started_at,
                **feedback_fields,
            )
            open_html_report(report_path)
        except Exception as error:
            typer.echo(f"HTML feedback unavailable: {error}", err=True)

    summary_fields = dict(feedback_fields)
    if report_path is not None:
        summary_fields["report_path"] = str(report_path)

    emit_terminal_summary(
        command=command,
        run_id=run_context.run_id,
        timestamp=run_context.started_at,
        **summary_fields,
    )


def _ensure_runtime_dirs(config: AppConfig) -> None:
    paths = config.paths
    paths.acquisition_state_root.mkdir(parents=True, exist_ok=True)
    paths.acquisition_raw_root.mkdir(parents=True, exist_ok=True)
    paths.acquisition_blobs_root.mkdir(parents=True, exist_ok=True)
    paths.acquisition_manifests_root.mkdir(parents=True, exist_ok=True)
    paths.downstream_raw_root.mkdir(parents=True, exist_ok=True)


def _require_base_url(value: str | None, *, field_name: str) -> str:
    if value:
        return value
    raise ValueError(f"{field_name} is required for this command")


def _with_config(command: str, ctx: typer.Context) -> tuple[AppConfig, RunContext]:
    try:
        config = _load_runtime_config(ctx)
        _ensure_runtime_dirs(config)
        initialize_db(config.paths.sqlite_path)
        return config, create_run_context(config)
    except Exception as error:
        _emit_command_failure(
            command=command,
            error_message=str(error),
            error_type=type(error).__name__,
        )
        raise typer.Exit(code=1) from error


def _query_ntsb_numbers(
    connection: sqlite3.Connection,
    query: str,
    parameters: tuple[object, ...] = (),
) -> list[str]:
    rows = fetch_all(connection, query, parameters)
    return [str(row["ntsb_number"]) for row in rows]


def _write_text_atomic(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        "w",
        encoding="utf-8",
        dir=path.parent,
        prefix=f".{path.name}.",
        suffix=".tmp",
        delete=False,
    ) as handle:
        handle.write(content)
        temp_path = Path(handle.name)
    temp_path.replace(path)


def _export_ingestion_manifest_rows(connection: sqlite3.Connection) -> list[sqlite3.Row]:
    return fetch_all(
        connection,
        """
        SELECT
            docket_search_results_current.project_id,
            downloads_current.ntsb_number,
            downloads_current.docket_item_id,
            docket_items_current.ordinal,
            docket_items_current.title,
            docket_items_current.view_url,
            downloads_current.blob_sha256,
            downloads_current.blob_path,
            downloads_current.media_type,
            downloads_current.source_url
        FROM downloads_current
        JOIN docket_items_current
          ON docket_items_current.docket_item_id = downloads_current.docket_item_id
        LEFT JOIN docket_search_results_current
          ON docket_search_results_current.ntsb_number = downloads_current.ntsb_number
        ORDER BY
            downloads_current.ntsb_number,
            docket_items_current.ordinal,
            downloads_current.docket_item_id
        """,
    )


def _download_phase(
    *,
    connection: sqlite3.Connection,
    config: AppConfig,
    run_context: RunContext,
    ntsb_numbers: list[str],
    http_client: HttpClient | None = None,
    progress_callback: Callable[[dict[str, int]], None] | None = None,
) -> tuple[int, int, int, int, int]:
    phase_http_client = http_client or build_http_client(config)
    owns_http_client = http_client is None
    try:
        evaluated = 0
        selected = 0
        rejected = 0
        downloaded = 0
        reused = 0
        for ntsb_number in ntsb_numbers:
            selection_result = apply_selection(
                connection,
                ntsb_number=ntsb_number,
                run_context=run_context,
                manifest_path=_manifest_path(run_context, "selection_decisions.jsonl"),
            )
            download_result = download_selected_artifacts(
                connection,
                phase_http_client,
                ntsb_number=ntsb_number,
                run_context=run_context,
                manifest_path=_manifest_path(run_context, "downloads.jsonl"),
            )
            evaluated += selection_result.evaluated
            selected += selection_result.selected
            rejected += selection_result.rejected
            downloaded += download_result.downloaded
            reused += download_result.reused
            if progress_callback is not None:
                progress_callback(
                    {
                        "evaluated": evaluated,
                        "selected": selected,
                        "rejected": rejected,
                        "downloaded": downloaded,
                        "reused": reused,
                    }
                )
        return evaluated, selected, rejected, downloaded, reused
    finally:
        if owns_http_client:
            phase_http_client.close()


def _table_count(connection: sqlite3.Connection, *, table: str) -> int:
    row = connection.execute(f"SELECT COUNT(*) AS count FROM {table}").fetchone()
    return int(row["count"]) if row is not None else 0


def _summarize_case_counts(connection: sqlite3.Connection) -> list[dict[str, object]]:
    case_rows = fetch_all(
        connection,
        """
        SELECT ntsb_number FROM investigations_current
        UNION
        SELECT ntsb_number FROM dockets_current
        UNION
        SELECT ntsb_number FROM docket_items_current
        UNION
        SELECT ntsb_number FROM downloads_current
        ORDER BY ntsb_number
        """,
    )
    case_counts = {
        str(row["ntsb_number"]): {
            "ntsb_number": str(row["ntsb_number"]),
            "docket_items_count": 0,
            "downloads_count": 0,
        }
        for row in case_rows
    }
    for row in fetch_all(
        connection,
        """
        SELECT ntsb_number, COUNT(*) AS count
        FROM docket_items_current
        GROUP BY ntsb_number
        ORDER BY ntsb_number
        """,
    ):
        case_counts[str(row["ntsb_number"])]["docket_items_count"] = int(row["count"])
    for row in fetch_all(
        connection,
        """
        SELECT ntsb_number, COUNT(*) AS count
        FROM downloads_current
        GROUP BY ntsb_number
        ORDER BY ntsb_number
        """,
    ):
        case_counts[str(row["ntsb_number"])]["downloads_count"] = int(row["count"])
    return [case_counts[ntsb_number] for ntsb_number in sorted(case_counts)]


def _doctor_checks(config: AppConfig) -> list[dict[str, object]]:
    python_version = platform.python_version()
    paths = config.paths
    return [
        {
            "detail": python_version,
            "name": "python_version",
            "ok": python_version == SUPPORTED_PYTHON_VERSION_TEXT
            and sys.version_info[:3] == SUPPORTED_PYTHON_VERSION,
        },
        {"detail": "loaded", "name": "config", "ok": True},
        {"detail": str(paths.data_root), "name": "data_root", "ok": paths.data_root.is_absolute()},
        {
            "detail": str(paths.sqlite_path),
            "name": "computed_sqlite_path",
            "ok": paths.sqlite_path.is_absolute(),
        },
        {
            "detail": str(paths.downstream_raw_root),
            "name": "computed_downstream_raw_root",
            "ok": paths.downstream_raw_root.is_absolute(),
        },
    ]


@app.command("init-db")
def init_db_command(ctx: typer.Context) -> None:
    config, run_context = _with_config("init-db", ctx)
    _emit_command_result(
        command="init-db",
        status="ok",
        state_changed=True,
        run_id=run_context.run_id,
        sqlite_path=str(config.paths.sqlite_path),
    )


@app.command("sync-investigations")
def sync_investigations_command(
    ctx: typer.Context,
    limit: int | None = typer.Option(
        None,
        help="Optional bound on investigations fetched from CAROL.",
    ),
    html_feedback: bool = typer.Option(
        False,
        help="Generate and open an HTML feedback report after successful completion.",
    ),
) -> None:
    config, run_context = _with_config("sync-investigations", ctx)
    try:
        http_client = build_http_client(config)
        try:
            client = CarolClient(
                base_url=_require_base_url(config.carol_base_url, field_name="carol_base_url"),
                http_client=http_client,
            )
            with connect_sqlite(config.paths.sqlite_path) as connection:
                result = sync_carol_investigations(
                    connection,
                    client,
                    run_context=run_context,
                    manifest_path=_manifest_path(run_context, "investigation_sync.jsonl"),
                    limit=limit,
                )
        finally:
            http_client.close()
    except Exception as error:
        _emit_command_failure(
            command="sync-investigations",
            error_message=str(error),
            error_type=type(error).__name__,
        )
        raise typer.Exit(code=1) from error

    _emit_command_result(
        command="sync-investigations",
        status="ok",
        state_changed=result.changed > 0 or result.discovered > 0,
        run_id=run_context.run_id,
        discovered=result.discovered,
        changed=result.changed,
        unchanged=result.unchanged,
    )
    _emit_command_feedback(
        command="sync-investigations",
        run_context=run_context,
        html_feedback=html_feedback,
        discovered=result.discovered,
        changed=result.changed,
        unchanged=result.unchanged,
    )


@app.command("discover-dockets")
def discover_dockets_command(ctx: typer.Context) -> None:
    config, run_context = _with_config("discover-dockets", ctx)
    try:
        http_client = build_http_client(config)
        try:
            client = DocketSearchClient(
                search_url=PUBLIC_DOCKET_SEARCH_URL,
                http_client=http_client,
            )
            with connect_sqlite(config.paths.sqlite_path) as connection:
                result = discover_dockets(
                    connection,
                    client,
                    run_context=run_context,
                    manifest_path=_manifest_path(run_context, "docket_search_discovery.jsonl"),
                )
        finally:
            http_client.close()
    except Exception as error:
        _emit_command_failure(
            command="discover-dockets",
            error_message=str(error),
            error_type=type(error).__name__,
        )
        raise typer.Exit(code=1) from error

    _emit_command_result(
        command="discover-dockets",
        status="ok",
        state_changed=result.changed > 0 or result.discovered > 0,
        run_id=run_context.run_id,
        discovered=result.discovered,
        changed=result.changed,
        unchanged=result.unchanged,
        raw_html_path=str(result.raw_html_path),
    )


@app.command("enumerate-dockets")
def enumerate_dockets_command(
    ctx: typer.Context,
    ntsb_number: str | None = typer.Option(None, help=NTSB_NUMBER_HELP),
    html_feedback: bool = typer.Option(
        False,
        help="Generate and open an HTML feedback report after successful completion.",
    ),
) -> None:
    config, run_context = _with_config("enumerate-dockets", ctx)
    try:
        http_client = build_http_client(config)
        try:
            client = DocketClient(
                base_url=_require_base_url(config.docket_base_url, field_name="docket_base_url"),
                http_client=http_client,
            )
            with connect_sqlite(config.paths.sqlite_path) as connection:
                ntsb_numbers = (
                    [ntsb_number]
                    if ntsb_number
                    else _query_ntsb_numbers(
                        connection,
                        """
                        SELECT ntsb_number FROM investigations_current
                        UNION
                        SELECT ntsb_number FROM docket_search_results_current
                        ORDER BY ntsb_number
                        """,
                    )
                )
                changed = 0
                enumerated = 0
                total_items = 0
                for current_ntsb_number in ntsb_numbers:
                    result = enumerate_docket(
                        connection,
                        client,
                        ntsb_number=current_ntsb_number,
                        run_context=run_context,
                        manifest_path=_manifest_path(run_context, "docket_enumeration.jsonl"),
                    )
                    enumerated += 1
                    total_items += result.item_count
                    if result.changed:
                        changed += 1
        finally:
            http_client.close()
    except Exception as error:
        _emit_command_failure(
            command="enumerate-dockets",
            error_message=str(error),
            error_type=type(error).__name__,
        )
        raise typer.Exit(code=1) from error

    _emit_command_result(
        command="enumerate-dockets",
        status="ok",
        state_changed=changed > 0 or enumerated > 0,
        run_id=run_context.run_id,
        enumerated=enumerated,
        changed=changed,
        total_items=total_items,
    )
    _emit_command_feedback(
        command="enumerate-dockets",
        run_context=run_context,
        html_feedback=html_feedback,
        ntsb_number=ntsb_number,
        enumerated=enumerated,
        changed=changed,
        total_items=total_items,
    )


@app.command("download-selected")
def download_selected_command(
    ctx: typer.Context,
    ntsb_number: str | None = typer.Option(None, help=NTSB_NUMBER_HELP),
    html_feedback: bool = typer.Option(
        False,
        help="Generate and open an HTML feedback report after successful completion.",
    ),
) -> None:
    config, run_context = _with_config("download-selected", ctx)
    try:
        with connect_sqlite(config.paths.sqlite_path) as connection:
            ntsb_numbers = (
                [ntsb_number]
                if ntsb_number
                else _query_ntsb_numbers(
                    connection,
                    """
                    SELECT DISTINCT ntsb_number
                    FROM docket_items_current
                    WHERE is_active = 1
                    ORDER BY ntsb_number
                    """,
                )
            )
            evaluated, selected, rejected, downloaded, reused = _download_phase(
                connection=connection,
                config=config,
                run_context=run_context,
                ntsb_numbers=ntsb_numbers,
            )
    except Exception as error:
        _emit_command_failure(
            command="download-selected",
            error_message=str(error),
            error_type=type(error).__name__,
        )
        raise typer.Exit(code=1) from error

    _emit_command_result(
        command="download-selected",
        status="ok",
        state_changed=downloaded > 0 or selected > 0 or rejected > 0,
        run_id=run_context.run_id,
        evaluated=evaluated,
        selected=selected,
        rejected=rejected,
        downloaded=downloaded,
        reused=reused,
    )
    _emit_command_feedback(
        command="download-selected",
        run_context=run_context,
        html_feedback=html_feedback,
        ntsb_number=ntsb_number,
        evaluated=evaluated,
        selected=selected,
        rejected=rejected,
        downloaded=downloaded,
        reused=reused,
    )


@app.command("collect")
def collect_command(ctx: typer.Context) -> None:
    config, run_context = _with_config("collect", ctx)
    started_at_monotonic = time.monotonic()
    log_path = (
        config.paths.data_root / "acquisition" / "logs" / f"collect_{run_context.run_id}.log"
    )
    log_path.parent.mkdir(parents=True, exist_ok=True)

    with log_path.open("a", encoding="utf-8") as log_handle:
        reporter = _CollectReporter(
            log_handle=log_handle,
            started_at_monotonic=started_at_monotonic,
            stdout_console=Console(file=sys.stdout),
            last_heartbeat_at=started_at_monotonic,
        )
        reporter.start_heartbeat()
        try:
            http_client = build_http_client(config)
            try:
                discover_client = DocketSearchClient(
                    search_url=PUBLIC_DOCKET_SEARCH_URL,
                    http_client=http_client,
                )
                docket_client = DocketClient(
                    base_url=_require_base_url(
                        config.docket_base_url,
                        field_name="docket_base_url",
                    ),
                    http_client=http_client,
                )
                with connect_sqlite(config.paths.sqlite_path) as connection:
                    reporter.line("--- Starting: discover-dockets ---")
                    reporter.set_status(
                        phase="Discovering dockets",
                        detail="Waiting for public docket search results",
                    )
                    discover_result = discover_dockets(
                        connection,
                        discover_client,
                        run_context=run_context,
                        manifest_path=_manifest_path(run_context, "docket_search_discovery.jsonl"),
                    )
                    reporter.summary(
                        title="discover-dockets complete",
                        run_id=run_context.run_id,
                        discovered=discover_result.discovered,
                        changed=discover_result.changed,
                        unchanged=discover_result.unchanged,
                        raw_html_path=discover_result.raw_html_path,
                    )

                    reporter.line("--- Starting: enumerate-dockets ---")
                    reporter.set_status(
                        phase="Enumerating dockets",
                        detail="0 processed, 0 updated, 0 items total",
                    )
                    ntsb_numbers = _query_ntsb_numbers(
                        connection,
                        """
                        SELECT ntsb_number FROM investigations_current
                        UNION
                        SELECT ntsb_number FROM docket_search_results_current
                        ORDER BY ntsb_number
                        """,
                    )
                    enumerated = 0
                    changed = 0
                    total_items = 0
                    for current_ntsb_number in ntsb_numbers:
                        result = enumerate_docket(
                            connection,
                            docket_client,
                            ntsb_number=current_ntsb_number,
                            run_context=run_context,
                            manifest_path=_manifest_path(run_context, "docket_enumeration.jsonl"),
                        )
                        enumerated += 1
                        total_items += result.item_count
                        if result.changed:
                            changed += 1
                        reporter.set_status(
                            phase="Enumerating dockets",
                            detail=(
                                f"{enumerated} processed, {changed} updated, "
                                f"{total_items:,} items total"
                            ),
                        )
                    reporter.summary(
                        title="enumerate-dockets complete",
                        run_id=run_context.run_id,
                        enumerated=enumerated,
                        changed=changed,
                        total_items=total_items,
                    )

                    reporter.line("--- Starting: download-selected ---")
                    reporter.set_status(
                        phase="Downloading documents",
                        detail="0 downloaded, 0 reused, 0 rejected",
                    )
                    download_ntsb_numbers = _query_ntsb_numbers(
                        connection,
                        """
                        SELECT DISTINCT ntsb_number
                        FROM docket_items_current
                        WHERE is_active = 1
                        ORDER BY ntsb_number
                        """,
                    )

                    def download_progress(counts: dict[str, int]) -> None:
                        reporter.set_status(
                            phase="Downloading documents",
                            detail=(
                                f"{counts['downloaded']} downloaded, "
                                f"{counts['reused']} reused, "
                                f"{counts['rejected']} rejected"
                            ),
                        )

                    evaluated, selected, rejected, downloaded, reused = _download_phase(
                        connection=connection,
                        config=config,
                        run_context=run_context,
                        ntsb_numbers=download_ntsb_numbers,
                        http_client=http_client,
                        progress_callback=download_progress,
                    )
                    reporter.summary(
                        title="download-selected complete",
                        run_id=run_context.run_id,
                        evaluated=evaluated,
                        selected=selected,
                        rejected=rejected,
                        downloaded=downloaded,
                        reused=reused,
                    )
            finally:
                http_client.close()
        except Exception as error:
            reporter.stop_heartbeat()
            reporter.line(f"Collect failed | {error}")
            reporter.flush()
            _emit_command_failure(
                command="collect",
                error_message=str(error),
                error_type=type(error).__name__,
            )
            raise typer.Exit(code=1) from error

        reporter.stop_heartbeat()
        elapsed = _format_elapsed(time.monotonic() - reporter.started_at_monotonic)
        overall_changed = discover_result.changed + changed
        reporter.summary(
            title="collect complete",
            run_id=run_context.run_id,
            discovered=discover_result.discovered,
            enumerated=enumerated,
            changed=overall_changed,
            total_items=total_items,
            selected=selected,
            downloaded=downloaded,
            reused=reused,
            rejected=rejected,
            total_elapsed_time=elapsed,
            log_path=log_path,
        )
        reporter.flush()
        _emit_command_result(
            command="collect",
            status="ok",
            state_changed=(
                discover_result.discovered > 0
                or discover_result.changed > 0
                or changed > 0
                or selected > 0
                or rejected > 0
                or downloaded > 0
            ),
            run_id=run_context.run_id,
            discovered=discover_result.discovered,
            enumerated=enumerated,
            changed=overall_changed,
            total_items=total_items,
            selected=selected,
            downloaded=downloaded,
            reused=reused,
            rejected=rejected,
            elapsed_duration=elapsed,
        )
        reporter.flush()


@app.command("promote")
def promote_command(
    ctx: typer.Context,
    ntsb_number: str | None = typer.Option(None, help=NTSB_NUMBER_HELP),
) -> None:
    config, run_context = _with_config("promote", ctx)
    try:
        with connect_sqlite(config.paths.sqlite_path) as connection:
            ntsb_numbers = (
                [ntsb_number]
                if ntsb_number
                else _query_ntsb_numbers(
                    connection,
                    """
                    SELECT DISTINCT ntsb_number
                    FROM downloads_current
                    ORDER BY ntsb_number
                    """,
                )
            )
            evaluated = 0
            promoted = 0
            reused = 0
            for current_ntsb_number in ntsb_numbers:
                result = promote_selected_artifacts(
                    connection,
                    ntsb_number=current_ntsb_number,
                    run_context=run_context,
                    manifest_path=_manifest_path(run_context, "promotions.jsonl"),
                )
                evaluated += result.evaluated
                promoted += result.promoted
                reused += result.reused
    except Exception as error:
        _emit_command_failure(
            command="promote",
            error_message=str(error),
            error_type=type(error).__name__,
        )
        raise typer.Exit(code=1) from error

    _emit_command_result(
        command="promote",
        status="ok",
        state_changed=promoted > 0,
        run_id=run_context.run_id,
        evaluated=evaluated,
        promoted=promoted,
        reused=reused,
    )


@app.command("materialize-case")
def materialize_case_command(
    ctx: typer.Context,
    ntsb_number: str = typer.Option(..., help=NTSB_NUMBER_HELP),
    html_feedback: bool = typer.Option(
        False,
        help="Generate and open an HTML feedback report after successful completion.",
    ),
) -> None:
    config, run_context = _with_config("materialize-case", ctx)
    try:
        with connect_sqlite(config.paths.sqlite_path) as connection:
            result = materialize_case(
                connection,
                ntsb_number=ntsb_number,
                output_root=config.paths.data_root / "case_views",
            )
    except Exception as error:
        _emit_command_failure(
            command="materialize-case",
            error_message=str(error),
            error_type=type(error).__name__,
        )
        raise typer.Exit(code=1) from error

    _emit_command_result(
        command="materialize-case",
        status="ok",
        state_changed=result.materialized > 0,
        run_id=run_context.run_id,
        ntsb_number=result.ntsb_number,
        materialized=result.materialized,
        reused=result.reused,
        output_dir=str(result.output_dir),
    )
    _emit_command_feedback(
        command="materialize-case",
        run_context=run_context,
        html_feedback=html_feedback,
        ntsb_number=result.ntsb_number,
        materialized=result.materialized,
        reused=result.reused,
        output_dir=str(result.output_dir),
    )


@app.command("export-ingestion-manifest")
def export_ingestion_manifest_command(
    ctx: typer.Context,
    quiet: bool = typer.Option(False, help="Suppress NDJSON row output to stdout."),
) -> None:
    config, run_context = _with_config("export-ingestion-manifest", ctx)
    try:
        with connect_sqlite(config.paths.sqlite_path) as connection:
            rows = _export_ingestion_manifest_rows(connection)
    except Exception as error:
        _emit_command_failure(
            command="export-ingestion-manifest",
            error_message=str(error),
            error_type=type(error).__name__,
        )
        raise typer.Exit(code=1) from error

    lines = [
        json.dumps(
            {
                "project_id": row["project_id"],
                "ntsb_number": row["ntsb_number"],
                "docket_item_id": row["docket_item_id"],
                # Each row carries the acquisition run identity so downstream
                # registration can preserve authoritative upstream lineage.
                "acquisition_run_id": run_context.run_id,
                "ordinal": row["ordinal"],
                "title": row["title"],
                "view_url": row["view_url"],
                "blob_sha256": row["blob_sha256"],
                "blob_path": row["blob_path"],
                "media_type": row["media_type"],
                "source_url": row["source_url"],
            },
            ensure_ascii=True,
            separators=(",", ":"),
        )
        for row in rows
    ]
    content = "".join(f"{line}\n" for line in lines)
    exports_root = config.paths.acquisition_root / "exports"
    snapshot_path = exports_root / f"ingestion_manifest_{run_context.run_id}.jsonl"
    # The snapshot is the authoritative handoff artifact for reproducible reruns.
    latest_path = exports_root / "ingestion_manifest_latest.jsonl"
    # The mutable latest alias is a convenience pointer for day-to-day operators.
    _write_text_atomic(snapshot_path, content)
    _write_text_atomic(latest_path, content)

    if not quiet:
        for line in lines:
            typer.echo(line)

    typer.echo(
        (
            "export-ingestion-manifest complete"
            f" | run_id={run_context.run_id}"
            f" | records_exported={len(lines)}"
            f" | snapshot_path={snapshot_path}"
            f" | latest_path={latest_path}"
        ),
        err=True,
    )


@app.command("summarize")
def summarize_command(ctx: typer.Context) -> None:
    config, run_context = _with_config("summarize", ctx)
    try:
        with connect_sqlite(config.paths.sqlite_path) as connection:
            _emit_command_result(
                command="summarize",
                status="ok",
                state_changed=False,
                run_id=run_context.run_id,
                investigations_current_count=_table_count(
                    connection,
                    table="investigations_current",
                ),
                dockets_current_count=_table_count(connection, table="dockets_current"),
                docket_items_current_count=_table_count(
                    connection,
                    table="docket_items_current",
                ),
                downloads_current_count=_table_count(connection, table="downloads_current"),
                blobs_current_count=_table_count(connection, table="blobs_current"),
                promotions_current_count=_table_count(connection, table="promotions_current"),
                per_case_counts=_summarize_case_counts(connection),
            )
    except Exception as error:
        _emit_command_failure(
            command="summarize",
            error_message=str(error),
            error_type=type(error).__name__,
        )
        raise typer.Exit(code=1) from error


@app.command("run")
def run_command(
    ctx: typer.Context,
    limit: int | None = typer.Option(None, help="Optional bound on CAROL discovery."),
    ntsb_number: str | None = typer.Option(None, help=NTSB_NUMBER_HELP),
) -> None:
    config, run_context = _with_config("run", ctx)
    try:
        http_client = build_http_client(config)
        try:
            carol_client = CarolClient(
                base_url=_require_base_url(config.carol_base_url, field_name="carol_base_url"),
                http_client=http_client,
            )
            docket_client = DocketClient(
                base_url=_require_base_url(config.docket_base_url, field_name="docket_base_url"),
                http_client=http_client,
            )
            with connect_sqlite(config.paths.sqlite_path) as connection:
                sync_result = sync_carol_investigations(
                    connection,
                    carol_client,
                    run_context=run_context,
                    manifest_path=_manifest_path(run_context, "investigation_sync.jsonl"),
                    limit=limit,
                )
                ntsb_numbers = (
                    [ntsb_number]
                    if ntsb_number
                    else _query_ntsb_numbers(
                        connection,
                        "SELECT ntsb_number FROM investigations_current ORDER BY ntsb_number",
                    )
                )
                enumerated = 0
                total_items = 0
                docket_changed = 0
                for current_ntsb_number in ntsb_numbers:
                    docket_result = enumerate_docket(
                        connection,
                        docket_client,
                        ntsb_number=current_ntsb_number,
                        run_context=run_context,
                        manifest_path=_manifest_path(run_context, "docket_enumeration.jsonl"),
                    )
                    enumerated += 1
                    total_items += docket_result.item_count
                    if docket_result.changed:
                        docket_changed += 1
                evaluated, selected, rejected, downloaded, download_reused = _download_phase(
                    connection=connection,
                    config=config,
                    run_context=run_context,
                    ntsb_numbers=ntsb_numbers,
                    http_client=http_client,
                )
                promoted = 0
                promotion_reused = 0
                promotion_evaluated = 0
                for current_ntsb_number in ntsb_numbers:
                    promotion_result = promote_selected_artifacts(
                        connection,
                        ntsb_number=current_ntsb_number,
                        run_context=run_context,
                        manifest_path=_manifest_path(run_context, "promotions.jsonl"),
                    )
                    promotion_evaluated += promotion_result.evaluated
                    promoted += promotion_result.promoted
                    promotion_reused += promotion_result.reused
        finally:
            http_client.close()
    except Exception as error:
        _emit_command_failure(
            command="run",
            error_message=str(error),
            error_type=type(error).__name__,
        )
        raise typer.Exit(code=1) from error

    _emit_command_result(
        command="run",
        status="ok",
        state_changed=(
            sync_result.changed > 0
            or docket_changed > 0
            or downloaded > 0
            or promoted > 0
        ),
        run_id=run_context.run_id,
        investigations_discovered=sync_result.discovered,
        investigations_changed=sync_result.changed,
        dockets_enumerated=enumerated,
        docket_items_total=total_items,
        selection_evaluated=evaluated,
        selection_selected=selected,
        selection_rejected=rejected,
        downloads_created=downloaded,
        downloads_reused=download_reused,
        promotions_evaluated=promotion_evaluated,
        promotions_created=promoted,
        promotions_reused=promotion_reused,
    )


@app.command("doctor")
def doctor(ctx: typer.Context) -> None:
    try:
        config = _load_runtime_config(ctx)
    except Exception as error:
        _emit_command_failure(
            command="doctor",
            error_message=str(error),
            error_type=type(error).__name__,
        )
        raise typer.Exit(code=1) from error

    checks = _doctor_checks(config)
    overall_ok = all(bool(check["ok"]) for check in checks)
    emit_console_event(
        level="info" if overall_ok else "error",
        event="command_result",
        checks=checks,
        command="doctor",
        phase="acquisition",
        state_changed=False,
        status="ok" if overall_ok else "error",
    )
    if not overall_ok:
        raise typer.Exit(code=1)
