from __future__ import annotations

import re
import unicodedata

SLUG_MAX_LENGTH = 80
WHITESPACE_RE = re.compile(r"\s+")
UNDERSCORE_RE = re.compile(r"_+")


def normalize_unicode(value: str) -> str:
    return unicodedata.normalize("NFKC", value)


def lowercase_text(value: str) -> str:
    return normalize_unicode(value).lower()


def collapse_whitespace(value: str) -> str:
    return WHITESPACE_RE.sub(" ", normalize_unicode(value)).strip()


def strip_punctuation(value: str) -> str:
    normalized = normalize_unicode(value)
    return "".join(
        character for character in normalized if not unicodedata.category(character).startswith("P")
    )


def normalize_text(value: str) -> str:
    return collapse_whitespace(lowercase_text(value))


def slugify(value: str, *, max_length: int = SLUG_MAX_LENGTH) -> str:
    lowered = normalize_text(value)
    without_punctuation = strip_punctuation(lowered)
    underscored = WHITESPACE_RE.sub("_", without_punctuation)
    collapsed = UNDERSCORE_RE.sub("_", underscored).strip("_")
    trimmed = collapsed[:max_length].strip("_")
    return trimmed or "item"
