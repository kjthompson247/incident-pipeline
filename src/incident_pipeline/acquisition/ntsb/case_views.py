from __future__ import annotations

import mimetypes
import shutil
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import parse_qs, urlparse

from incident_pipeline.acquisition.ntsb.identifiers import normalize_ntsb_number
from incident_pipeline.acquisition.ntsb.normalize import slugify


@dataclass(frozen=True)
class CaseMaterializationResult:
    ntsb_number: str
    output_dir: Path
    materialized: int
    reused: int


def _download_rows(connection: sqlite3.Connection, *, ntsb_number: str) -> list[sqlite3.Row]:
    rows = connection.execute(
        """
        SELECT
            docket_items_current.docket_item_id,
            docket_items_current.ordinal,
            docket_items_current.title,
            docket_items_current.normalized_title_slug,
            downloads_current.source_url,
            downloads_current.blob_path,
            downloads_current.media_type
        FROM docket_items_current
        JOIN downloads_current
          ON downloads_current.docket_item_id = docket_items_current.docket_item_id
        WHERE docket_items_current.ntsb_number = ?
          AND docket_items_current.is_active = 1
        ORDER BY docket_items_current.ordinal, docket_items_current.title
        """,
        (ntsb_number,),
    )
    return list(rows.fetchall())


def _source_extension(*, source_url: str | None) -> str | None:
    if not source_url:
        return None
    parsed = urlparse(source_url)
    suffix = Path(parsed.path).suffix
    if suffix:
        return suffix.lower()

    query = parse_qs(parsed.query)
    for key in ("FileExtension", "fileExtension"):
        values = query.get(key)
        if not values:
            continue
        extension = values[0].strip().lower()
        if extension:
            return extension if extension.startswith(".") else f".{extension}"

    for key in ("FileName", "fileName", "filename"):
        values = query.get(key)
        if not values:
            continue
        suffix = Path(values[0]).suffix
        if suffix:
            return suffix.lower()
    return None


def _media_type_extension(*, media_type: str | None) -> str | None:
    if not media_type:
        return None
    normalized_type = media_type.partition(";")[0].strip().lower()
    if not normalized_type:
        return None
    guessed = mimetypes.guess_extension(normalized_type)
    return guessed.lower() if guessed else None


def _materialized_extension(
    *,
    source_url: str | None,
    media_type: str | None,
    blob_path: str,
) -> str:
    for candidate in (
        _source_extension(source_url=source_url),
        _media_type_extension(media_type=media_type),
        Path(blob_path).suffix.lower() or None,
    ):
        if candidate:
            return candidate
    return ".bin"


def _destination_path(
    *,
    output_dir: Path,
    ordinal: int,
    title: str,
    normalized_title_slug: str | None,
    source_url: str | None,
    media_type: str | None,
    blob_path: str,
) -> Path:
    title_slug = (normalized_title_slug or "").strip() or slugify(title) or "item"
    extension = _materialized_extension(
        source_url=source_url,
        media_type=media_type,
        blob_path=blob_path,
    )
    return output_dir / f"{ordinal:03d}_{title_slug}{extension}"


def _is_reusable(destination: Path, *, source: Path) -> bool:
    if destination.is_symlink():
        try:
            return destination.resolve() == source.resolve()
        except FileNotFoundError:
            return False
    if not destination.exists():
        return False
    return destination.stat().st_size == source.stat().st_size


def _materialize_path(source: Path, destination: Path) -> bool:
    if _is_reusable(destination, source=source):
        return False

    if destination.exists() or destination.is_symlink():
        destination.unlink()

    try:
        destination.symlink_to(source)
    except OSError:
        shutil.copyfile(source, destination)
    return True


def materialize_case(
    connection: sqlite3.Connection,
    *,
    ntsb_number: str,
    output_root: Path,
) -> CaseMaterializationResult:
    normalized_ntsb_number = normalize_ntsb_number(ntsb_number)
    output_dir = output_root / normalized_ntsb_number
    output_dir.mkdir(parents=True, exist_ok=True)

    materialized = 0
    reused = 0
    for row in _download_rows(connection, ntsb_number=normalized_ntsb_number):
        source_blob_path = Path(str(row["blob_path"]))
        if not source_blob_path.exists():
            raise FileNotFoundError(
                f"blob_path does not exist for {row['docket_item_id']}: {source_blob_path}"
            )

        destination = _destination_path(
            output_dir=output_dir,
            ordinal=int(row["ordinal"]),
            title=str(row["title"]),
            normalized_title_slug=row["normalized_title_slug"],
            source_url=row["source_url"],
            media_type=row["media_type"],
            blob_path=str(row["blob_path"]),
        )
        changed = _materialize_path(source_blob_path, destination)
        if changed:
            materialized += 1
        else:
            reused += 1

    return CaseMaterializationResult(
        ntsb_number=normalized_ntsb_number,
        output_dir=output_dir,
        materialized=materialized,
        reused=reused,
    )
