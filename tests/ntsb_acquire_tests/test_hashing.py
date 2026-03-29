from __future__ import annotations

from incident_pipeline.acquisition.ntsb.hashing import canonical_json, sha256_bytes, sha256_canonical_json, sha256_text


def test_sha256_helpers_are_stable() -> None:
    expected = "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"

    assert sha256_bytes(b"abc") == expected
    assert sha256_text("abc") == expected


def test_canonical_json_sorts_keys_and_omits_extra_whitespace() -> None:
    assert canonical_json({"b": 1, "a": 2}) == '{"a":2,"b":1}'


def test_sha256_canonical_json_is_independent_of_dict_key_order() -> None:
    first = {"a": 1, "b": {"c": 2, "d": 3}}
    second = {"b": {"d": 3, "c": 2}, "a": 1}

    assert sha256_canonical_json(first) == sha256_canonical_json(second)
