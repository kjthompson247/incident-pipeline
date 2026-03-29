from __future__ import annotations

from contextlib import redirect_stderr
from dataclasses import dataclass
import io
from pathlib import Path


@dataclass(frozen=True)
class PDFExtractionResult:
    text: str
    warnings: tuple[str, ...] = ()


class PDFExtractionError(RuntimeError):
    def __init__(self, message: str, *, warnings: tuple[str, ...] = ()) -> None:
        super().__init__(message)
        self.warnings = warnings


def _normalize_warning_lines(raw_text: str) -> list[str]:
    lines: list[str] = []
    for raw_line in raw_text.splitlines():
        line = raw_line.strip()
        if line:
            lines.append(line)
    return lines


def extract_pdf_text_with_warnings(pdf_path: Path) -> PDFExtractionResult:
    try:
        from pypdf import PdfReader
    except ImportError as exc:
        raise RuntimeError("pypdf is required for PDF extraction") from exc

    stderr_buffer = io.StringIO()
    page_text: list[str] = []
    warnings: list[str] = []
    successful_pages = 0
    page_count = 0
    open_error: Exception | None = None

    with redirect_stderr(stderr_buffer):
        try:
            reader = PdfReader(str(pdf_path))
            page_count = len(reader.pages)
        except Exception as exc:
            open_error = exc
        else:
            for page_index in range(page_count):
                try:
                    page = reader.pages[page_index]
                    text = page.extract_text() or ""
                except Exception as exc:
                    warnings.append(f"page {page_index + 1}: {exc}")
                    continue

                successful_pages += 1
                page_text.append(text)

    warnings.extend(_normalize_warning_lines(stderr_buffer.getvalue()))

    if open_error is not None:
        raise PDFExtractionError(
            f"Unable to open PDF for extraction: {pdf_path}",
            warnings=tuple(warnings),
        ) from open_error

    if page_count > 0 and successful_pages == 0:
        raise PDFExtractionError(
            f"No pages could be extracted from PDF: {pdf_path}",
            warnings=tuple(warnings),
        )

    return PDFExtractionResult(
        text="\n\n".join(page_text).strip(),
        warnings=tuple(warnings),
    )


def extract_pdf_text(pdf_path: Path) -> str:
    return extract_pdf_text_with_warnings(pdf_path).text
