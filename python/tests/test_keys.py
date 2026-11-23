"""Tests for key normalization and object identity."""

import pytest
from strands.storage.storage import _normalize_key, _normalize_prefix
from strands.types.exceptions import StorageError

from strands_weaviate._keys import key_uuid, normalize_key, normalize_prefix, scope_prefix

KEYS = ["a", "a/b.md", "/a//b/", "a/.hidden", "a/b..c", "a b/c"]
BAD_KEYS = ["", "/", "a\\b", "../a", "a/../b", "a/.."]
PREFIXES = ["", "a", "a/", "/a//b/", "a_", "a/b..c/"]
BAD_PREFIXES = ["a\\b", "../a", "a/../b"]


@pytest.mark.parametrize("key", KEYS)
def test_normalize_key_matches_sdk(key):
    assert normalize_key(key) == _normalize_key(key)


@pytest.mark.parametrize("key", BAD_KEYS)
def test_normalize_key_rejects_like_sdk(key):
    with pytest.raises(StorageError) as sdk_error:
        _normalize_key(key)
    with pytest.raises(StorageError) as error:
        normalize_key(key)
    assert str(error.value) == str(sdk_error.value)


@pytest.mark.parametrize("prefix", PREFIXES)
def test_normalize_prefix_matches_sdk(prefix):
    assert normalize_prefix(prefix) == _normalize_prefix(prefix)


@pytest.mark.parametrize("prefix", BAD_PREFIXES)
def test_normalize_prefix_rejects_like_sdk(prefix):
    with pytest.raises(StorageError) as sdk_error:
        _normalize_prefix(prefix)
    with pytest.raises(StorageError) as error:
        normalize_prefix(prefix)
    assert str(error.value) == str(sdk_error.value)


@pytest.mark.parametrize(
    ("prefix", "expected"),
    [("", ""), ("/", ""), ("a", "a/"), ("a/", "a/"), ("/a//b//", "a/b/")],
)
def test_scope_prefix(prefix, expected):
    assert scope_prefix(prefix) == expected


@pytest.mark.parametrize(
    ("full_key", "expected"),
    [
        ("memory/notes.md", "d41749b0-5ae6-53f2-8578-4760b3834bde"),
        ("agents/session/abc/snapshot_latest.json", "4babca55-bc07-54f9-a61d-2c7b19296aa4"),
    ],
)
def test_key_uuid_is_stable_across_sdks(full_key, expected):
    """These vectors are shared with the TypeScript package's tests."""
    assert str(key_uuid(full_key)) == expected
