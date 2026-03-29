from __future__ import annotations

import importlib
import json
from pathlib import Path
from typing import Any

import typer

from incident_pipeline.extract.atomic_extract import (
    SentenceSpanTransformer,
    load_config as load_atomic_config,
    resolve_output_root as resolve_atomic_output_root,
    run_atomic_extraction_batch,
)
from incident_pipeline.extract.sentence_spans import (
    load_config as load_sentence_span_config,
    resolve_output_root as resolve_sentence_span_output_root,
    run_sentence_span_batch,
)


sentence_spans_app = typer.Typer(
    add_completion=False,
    help="Run governed sentence span generation.",
)
atomic_extract_app = typer.Typer(
    add_completion=False,
    help="Run governed atomic extraction with an explicit transformer adapter.",
)


def load_transformer(transformer_spec: str) -> SentenceSpanTransformer:
    if ":" not in transformer_spec:
        raise ValueError(
            "Transformer import path must use the form 'package.module:callable_name'"
        )
    module_name, callable_name = transformer_spec.split(":", 1)
    if not module_name or not callable_name:
        raise ValueError(
            "Transformer import path must use the form 'package.module:callable_name'"
        )

    module = importlib.import_module(module_name)
    try:
        transformer = getattr(module, callable_name)
    except AttributeError as exc:
        raise ValueError(
            f"Transformer callable not found: {transformer_spec}"
        ) from exc

    if not callable(transformer):
        raise ValueError(f"Transformer target is not callable: {transformer_spec}")

    return transformer


def _snapshot_runs(output_root: Path) -> dict[str, Path]:
    runs_root = output_root / "runs"
    if not runs_root.exists():
        return {}
    return {
        path.name: path
        for path in runs_root.iterdir()
        if path.is_dir()
    }


def _find_created_run_dir(output_root: Path, before: dict[str, Path]) -> Path:
    after = _snapshot_runs(output_root)
    new_runs = [path for name, path in after.items() if name not in before]
    if new_runs:
        return max(new_runs, key=lambda path: path.stat().st_mtime_ns)
    if after:
        return max(after.values(), key=lambda path: path.stat().st_mtime_ns)
    raise RuntimeError(f"No run directory was created under {output_root / 'runs'}")


def _read_run_summary(run_dir: Path, filename: str) -> dict[str, Any]:
    summary_path = run_dir / filename
    with summary_path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    if not isinstance(payload, dict):
        raise ValueError(f"Run summary must be a JSON object: {summary_path}")
    return payload


def _print_compact_summary(run_dir: Path, summary: dict[str, Any]) -> None:
    typer.echo(f"stage={summary['stage_name']}")
    typer.echo(f"run_id={summary['run_id']}")
    typer.echo(
        "run_status="
        f"{summary['run_status']} "
        f"validation={summary['validation_status']} "
        f"certification={summary['certification_status']}"
    )
    typer.echo(
        "records_seen="
        f"{summary['records_seen']} "
        f"records_failed={summary['records_failed']} "
        f"primary_output_count={summary['primary_output_count']}"
    )
    typer.echo(f"run_dir={run_dir}")
    blocking_issues = summary.get("blocking_issues") or []
    if blocking_issues:
        typer.echo(f"blocking_issues={'; '.join(str(item) for item in blocking_issues)}")


def _exit_for_failed_run(summary: dict[str, Any]) -> None:
    if (
        summary.get("run_status") != "completed"
        or summary.get("validation_status") != "passed"
        or summary.get("certification_status") != "certified"
    ):
        raise typer.Exit(code=1)


@sentence_spans_app.callback(invoke_without_command=True)
def sentence_spans_command(
    ctx: typer.Context,
    config: Path | None = typer.Option(
        None,
        "--config",
        "-c",
        help="Optional settings YAML path.",
    ),
) -> None:
    if ctx.invoked_subcommand is not None:
        return

    try:
        cfg = load_sentence_span_config(config)
        output_root = resolve_sentence_span_output_root(cfg)
        before = _snapshot_runs(output_root)
        run_sentence_span_batch(config)
        run_dir = _find_created_run_dir(output_root, before)
        summary = _read_run_summary(run_dir, "sentence_span_run_summary.json")
        _print_compact_summary(run_dir, summary)
        _exit_for_failed_run(summary)
    except Exception as exc:
        typer.echo(str(exc), err=True)
        raise typer.Exit(code=1) from exc


@atomic_extract_app.callback(invoke_without_command=True)
def atomic_extract_command(
    ctx: typer.Context,
    config: Path | None = typer.Option(
        None,
        "--config",
        "-c",
        help="Optional settings YAML path.",
    ),
    transformer: str = typer.Option(
        ...,
        "--transformer",
        help="Explicit transformer import path in the form package.module:callable_name",
    ),
) -> None:
    if ctx.invoked_subcommand is not None:
        return

    try:
        transformer_callable = load_transformer(transformer)
        cfg = load_atomic_config(config)
        output_root = resolve_atomic_output_root(cfg)
        before = _snapshot_runs(output_root)
        run_atomic_extraction_batch(config, transform_span=transformer_callable)
        run_dir = _find_created_run_dir(output_root, before)
        summary = _read_run_summary(run_dir, "atomic_run_summary.json")
        _print_compact_summary(run_dir, summary)
        _exit_for_failed_run(summary)
    except Exception as exc:
        typer.echo(str(exc), err=True)
        raise typer.Exit(code=1) from exc
