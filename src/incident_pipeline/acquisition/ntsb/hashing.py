from __future__ import annotations

import hashlib
import json
from typing import Any


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def sha256_text(value: str, *, encoding: str = "utf-8") -> str:
    return sha256_bytes(value.encode(encoding))


def canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def sha256_canonical_json(value: Any) -> str:
    return sha256_text(canonical_json(value))
