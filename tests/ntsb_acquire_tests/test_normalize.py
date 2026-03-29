from __future__ import annotations

from incident_pipeline.acquisition.ntsb.normalize import SLUG_MAX_LENGTH, normalize_text, normalize_unicode, slugify


def test_normalize_unicode_applies_nfkc() -> None:
    assert normalize_unicode("ＡＢＣ１２３") == "ABC123"


def test_normalize_text_lowercases_and_collapses_whitespace() -> None:
    assert normalize_text("  Final\tREPORT  ") == "final report"


def test_slugify_strips_punctuation_and_collapses_underscores() -> None:
    assert slugify(" Final Report: Preliminary / Update ") == "final_report_preliminary_update"


def test_slugify_enforces_max_length() -> None:
    slug = slugify("word " * 40)

    assert len(slug) <= SLUG_MAX_LENGTH
    assert not slug.startswith("_")
    assert not slug.endswith("_")
