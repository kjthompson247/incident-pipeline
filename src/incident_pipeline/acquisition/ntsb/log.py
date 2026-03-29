from __future__ import annotations

import json
import logging
import sys
from typing import Any

import typer


def render_log_event(*, level: str, event: str, **fields: Any) -> str:
    payload: dict[str, Any] = {"event": event, "level": level.lower(), **fields}
    return json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def get_logger(name: str = "incident_pipeline.acquisition.ntsb") -> logging.Logger:
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(logging.Formatter("%(message)s"))
        logger.addHandler(handler)
    logger.propagate = False
    return logger


def configure_logger(level: str, *, name: str = "incident_pipeline.acquisition.ntsb") -> logging.Logger:
    logger = get_logger(name)
    logger.setLevel(level.upper())
    return logger


def emit_console_event(*, level: str, event: str, **fields: Any) -> None:
    typer.echo(render_log_event(level=level, event=event, **fields))


def log_event(logger: logging.Logger, *, level: str, event: str, **fields: Any) -> None:
    message = render_log_event(level=level, event=event, **fields)
    getattr(logger, level.lower())(message)
