from __future__ import annotations

from pathlib import Path
import sys
import types

import pytest

from incident_pipeline.ingestion.pdf_extract import PDFExtractionError, extract_pdf_text, extract_pdf_text_with_warnings


class FakePage:
    def __init__(self, *, text: str | None = None, error: Exception | None = None) -> None:
        self.text = text
        self.error = error

    def extract_text(self) -> str | None:
        if self.error is not None:
            raise self.error
        return self.text


def test_extract_pdf_text_skips_failed_pages_and_keeps_recovered_text(monkeypatch) -> None:
    class FakePdfReader:
        def __init__(self, path: str) -> None:
            self.path = path
            self.pages = [
                FakePage(text="page one"),
                FakePage(error=UnicodeError("utf-16-be decode failed")),
                FakePage(text="page three"),
            ]

    fake_module = types.ModuleType("pypdf")
    fake_module.PdfReader = FakePdfReader
    monkeypatch.setitem(sys.modules, "pypdf", fake_module)

    text = extract_pdf_text(Path("/tmp/sample.pdf"))

    assert text == "page one\n\npage three"


def test_extract_pdf_text_with_warnings_captures_stderr_and_page_failures(monkeypatch) -> None:
    class FakePdfReader:
        def __init__(self, path: str) -> None:
            self.path = path
            print("Ignoring wrong pointing object 12 0", file=sys.stderr)
            self.pages = [
                FakePage(text="page one"),
                FakePage(error=UnicodeError("utf-16-be decode failed")),
                FakePage(text="page three"),
            ]

    fake_module = types.ModuleType("pypdf")
    fake_module.PdfReader = FakePdfReader
    monkeypatch.setitem(sys.modules, "pypdf", fake_module)

    result = extract_pdf_text_with_warnings(Path("/tmp/sample.pdf"))

    assert result.text == "page one\n\npage three"
    assert result.warnings == (
        "page 2: utf-16-be decode failed",
        "Ignoring wrong pointing object 12 0",
    )


def test_extract_pdf_text_raises_when_no_pages_can_be_recovered(monkeypatch) -> None:
    class FakePdfReader:
        def __init__(self, path: str) -> None:
            self.path = path
            self.pages = [
                FakePage(error=ValueError("EOF marker not found")),
                FakePage(error=UnicodeError("utf-16-be decode failed")),
            ]

    fake_module = types.ModuleType("pypdf")
    fake_module.PdfReader = FakePdfReader
    monkeypatch.setitem(sys.modules, "pypdf", fake_module)

    with pytest.raises(PDFExtractionError):
        extract_pdf_text(Path("/tmp/broken.pdf"))


def test_extract_pdf_text_with_warnings_attaches_stderr_to_failures(monkeypatch) -> None:
    class FakePdfReader:
        def __init__(self, path: str) -> None:
            self.path = path
            print("Object 5 0 not defined.", file=sys.stderr)
            self.pages = [
                FakePage(error=ValueError("EOF marker not found")),
                FakePage(error=UnicodeError("utf-16-be decode failed")),
            ]

    fake_module = types.ModuleType("pypdf")
    fake_module.PdfReader = FakePdfReader
    monkeypatch.setitem(sys.modules, "pypdf", fake_module)

    with pytest.raises(PDFExtractionError) as exc_info:
        extract_pdf_text_with_warnings(Path("/tmp/broken.pdf"))

    assert exc_info.value.warnings == (
        "page 1: EOF marker not found",
        "page 2: utf-16-be decode failed",
        "Object 5 0 not defined.",
    )
