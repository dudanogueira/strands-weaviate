"""Key normalization and object identity shared by every storage operation."""

import re
import uuid

from strands.types.exceptions import StorageError

# Shared with the TypeScript package so both derive the same object id for a key.
_KEY_NAMESPACE = uuid.UUID("13a087ca-fcce-5b2e-8a07-8379f81bd956")


def normalize_key(key: str) -> str:
    """Validate and normalize a storage key the way the SDK's built-in backends do.

    Collapses runs of '/', strips leading and trailing '/', and rejects empty keys,
    backslashes, and '..' segments.

    Args:
        key: The raw key to normalize.

    Returns:
        The normalized key.

    Raises:
        StorageError: If the key is empty, contains backslashes, or contains a '..' segment.
    """
    if "\\" in key:
        raise StorageError(f"Invalid storage key '{key}': backslashes are not allowed")
    normalized = re.sub(r"/+", "/", key).strip("/")
    if not normalized:
        raise StorageError("Storage key must not be empty")
    if ".." in normalized.split("/"):
        raise StorageError(f"Invalid storage key '{key}': '..' path segments are not allowed")
    return normalized


def normalize_prefix(prefix: str) -> str:
    """Normalize a key prefix the way the SDK's built-in backends do.

    Collapses slash runs and strips leading slashes. An empty prefix is valid and a
    trailing slash is preserved, because it is significant for prefix matching.

    Args:
        prefix: The raw prefix to normalize.

    Returns:
        The normalized prefix.

    Raises:
        StorageError: If the prefix contains backslashes or a '..' segment.
    """
    if "\\" in prefix:
        raise StorageError(f"Invalid storage prefix '{prefix}': backslashes are not allowed")
    normalized = re.sub(r"/+", "/", prefix).lstrip("/")
    if ".." in normalized.split("/"):
        raise StorageError(f"Invalid storage prefix '{prefix}': '..' path segments are not allowed")
    return normalized


def scope_prefix(prefix: str) -> str:
    """Turn a user-supplied prefix into a storage scope: empty, or ending in exactly one '/'.

    Args:
        prefix: The raw prefix.

    Returns:
        The scope prefix.

    Raises:
        StorageError: If the prefix contains backslashes or a '..' segment.
    """
    normalized = normalize_prefix(prefix).rstrip("/")
    return f"{normalized}/" if normalized else ""


def key_uuid(full_key: str) -> uuid.UUID:
    """Derive the deterministic Weaviate object id for a fully prefixed key.

    Args:
        full_key: The storage prefix plus the normalized key.

    Returns:
        The object id.
    """
    return uuid.uuid5(_KEY_NAMESPACE, full_key)
