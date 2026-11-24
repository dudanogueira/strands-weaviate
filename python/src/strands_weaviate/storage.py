"""Weaviate storage implementation."""

from __future__ import annotations

import asyncio
import base64
import builtins
import copy
import logging
import re
import threading
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Literal

import weaviate
from strands.storage import Storage
from strands.storage.search import StorageSearchResult
from strands.types.exceptions import StorageError
from weaviate.classes.config import Configure, DataType, Property, Tokenization, Vectorizers
from weaviate.classes.data import DataObject
from weaviate.classes.query import Filter, MetadataQuery

from strands_weaviate._keys import key_uuid, normalize_key, normalize_prefix, scope_prefix
from strands_weaviate._telemetry import register_integration

if TYPE_CHECKING:
    from strands.storage.search import Embedder

try:
    from strands.storage.storage import _NAMESPACED
except ImportError:  # pragma: no cover - depends on the installed SDK version
    _NAMESPACED = None

logger = logging.getLogger(__name__)

SearchMode = Literal["hybrid", "vector", "bm25"]

_SEARCH_MODES: tuple[SearchMode, ...] = ("hybrid", "vector", "bm25")

# A filtered fetch is only complete within a single page, so list() asks for one page of this
# size and falls back to a full cursor scan when the page comes back full.
_LIST_FILTER_LIMIT = 1000

# The tenant names Weaviate accepts.
_TENANT_NAME = re.compile(r"[A-Za-z0-9_-]{1,64}")


def _validate_tenant(tenant: str) -> str:
    """Return the tenant name, or raise if Weaviate would reject it."""
    if not _TENANT_NAME.fullmatch(tenant):
        raise ValueError(f"tenant must be 1 to 64 characters of letters, digits, '_' or '-', got '{tenant}'")
    return tenant


class WeaviateStorage(Storage):
    """Persists bytes as objects in a Weaviate collection, with built-in search.

    Each key is one object. The raw value is stored as a blob, and text values are also
    stored in a searchable property, so ``search()`` runs a Weaviate hybrid (BM25 plus
    vector), vector, or BM25 query instead of a keyword scan. Because one object holds a
    key's value, text, and vector, an overwrite or delete can never leave a stale index entry.

    Vectors come from one of two places: pass an ``embedder`` to compute them in the SDK, or
    create the collection yourself with a Weaviate vectorizer (see
    :meth:`collection_properties`). With neither, search is BM25 only.

    Pass ``tenant`` (or call :meth:`with_tenant`) to keep each tenant's objects in its own
    shard of a multi-tenant collection. A tenant is created by the first value written to it,
    never by a read, list, search or delete.

    Example:
        ```python
        import weaviate
        from strands_weaviate import WeaviateStorage

        client = weaviate.connect_to_local()
        storage = WeaviateStorage(client, prefix="agents/")
        await storage.write("memory/notes.md", b"prefers concise answers")
        results = await storage.search("how should answers be written")
        ```
    """

    def __init__(
        self,
        client: weaviate.WeaviateClient,
        *,
        collection_name: str = "StrandsStorage",
        prefix: str = "",
        embedder: Embedder | None = None,
        search_mode: SearchMode | None = None,
        alpha: float = 0.5,
        max_results: int = 10,
        index_prefixes: builtins.list[str] | None = None,
        max_indexed_chars: int = 8000,
        create_collection: bool = True,
        vector_name: str | None = None,
        tenant: str | None = None,
    ) -> None:
        """Initialize Weaviate storage.

        Args:
            client: A connected synchronous Weaviate client. The caller owns it: the storage
                never connects or closes it.
            collection_name: Collection that holds the objects.
            prefix: Key prefix prepended to every key (a namespace within the collection).
            embedder: Computes vectors for text values on write and for queries on search. Omit
                it when the collection has a Weaviate vectorizer, or for BM25-only search.
            search_mode: ``"hybrid"``, ``"vector"`` or ``"bm25"``. When omitted, hybrid is used
                if an ``embedder`` is set or the collection has a vectorizer, and BM25 otherwise.
            alpha: Hybrid weighting between BM25 (0) and vector (1) search.
            max_results: Maximum number of results ``search()`` returns.
            index_prefixes: Key prefixes, relative to ``prefix``, whose text values are made
                searchable. Omit to index every text value. Values outside these prefixes are
                stored and readable but never embedded or returned by ``search()``.
            max_indexed_chars: Longest text made searchable per value; longer values are
                truncated in the search index only.
            create_collection: Create the collection when it does not exist. When ``False``, a
                missing collection raises on first use.
            vector_name: Named vector to write to and search, for a collection that defines
                more than one or names its vector something other than ``default``.
            tenant: Tenant whose shard this storage reads and writes, in a multi-tenant
                collection. When the storage creates the collection for a tenant, it enables
                multi-tenancy; an existing single-tenant collection cannot be converted.

        Raises:
            StorageError: If ``client`` is an asynchronous client, or ``prefix`` is invalid.
            ValueError: If ``search_mode``, ``alpha``, ``max_results``, ``max_indexed_chars``
                or ``tenant`` is out of range.
        """
        if isinstance(client, weaviate.WeaviateAsyncClient):
            raise StorageError(
                "WeaviateStorage requires a synchronous weaviate.WeaviateClient: "
                "the SDK runs storage calls on more than one event loop, which an async client cannot span"
            )
        if search_mode is not None and search_mode not in _SEARCH_MODES:
            raise ValueError(f"search_mode must be one of {_SEARCH_MODES}, got '{search_mode}'")
        if not 0 <= alpha <= 1:
            raise ValueError(f"alpha must be between 0 and 1, got {alpha}")
        if max_results < 1:
            raise ValueError(f"max_results must be at least 1, got {max_results}")
        if max_indexed_chars < 1:
            raise ValueError(f"max_indexed_chars must be at least 1, got {max_indexed_chars}")

        self._client = client
        register_integration(client)
        self._collection_name = collection_name
        self._prefix = scope_prefix(prefix)
        self._embedder = embedder
        self._search_mode = search_mode
        self._alpha = alpha
        self._max_results = max_results
        self._index_prefixes = (
            None if index_prefixes is None else [normalize_prefix(index_prefix) for index_prefix in index_prefixes]
        )
        self._max_indexed_chars = max_indexed_chars
        self._create_collection = create_collection
        self._vector_name = vector_name
        self._tenant = None if tenant is None else _validate_tenant(tenant)
        # Set on instances returned by namespace() and with_tenant(), so they share the root's
        # collection state.
        self._root: WeaviateStorage | None = None
        self._lock = threading.Lock()
        self._collection: Any = None
        self._has_vectorizer = False
        self._multi_tenant = False

    @property
    def prefix(self) -> str:
        """The normalized key prefix (ending in ``/``), or ``""`` when none was configured."""
        return self._prefix

    @property
    def tenant(self) -> str | None:
        """The tenant this storage is bound to, or ``None`` for a single-tenant collection."""
        return self._tenant

    @staticmethod
    def collection_properties() -> builtins.list[Property]:
        """Return the property definitions a collection must have.

        Use this to create the collection yourself, for example to attach a Weaviate vectorizer.
        Vectorize only the ``text`` property.

        Example:
            ```python
            client.collections.create(
                "StrandsStorage",
                properties=WeaviateStorage.collection_properties(),
                vector_config=Configure.Vectors.text2vec_weaviate(
                    source_properties=["text"], vectorize_collection_name=False
                ),
            )
            ```

        Returns:
            The property definitions.
        """
        return [
            Property(
                name="key",
                data_type=DataType.TEXT,
                tokenization=Tokenization.FIELD,
                index_filterable=True,
                index_searchable=False,
                skip_vectorization=True,
            ),
            Property(name="text", data_type=DataType.TEXT, tokenization=Tokenization.WORD, index_searchable=True),
            Property(name="data", data_type=DataType.BLOB),
            Property(name="is_text", data_type=DataType.BOOL, index_filterable=True),
            Property(name="size", data_type=DataType.INT),
            Property(name="updated_at", data_type=DataType.DATE),
        ]

    async def write(self, key: str, data: bytes) -> None:
        """Store data as a Weaviate object, replacing any existing value, text and vector.

        Args:
            key: '/'-separated key identifying the value.
            data: Raw bytes to persist.

        Raises:
            StorageError: If the key is invalid, embedding the value fails (the previous value is
                kept in that case), or the write fails.
        """
        full_key = f"{self._prefix}{normalize_key(key)}"
        text = self._indexable_text(full_key, data)
        if text is None:
            logger.debug(
                "key=<%s>, bytes=<%d> | value is not indexable text | stored without search index", key, len(data)
            )

        vector: builtins.list[float] | None = None
        if text is not None and self._embedder is not None and self._search_mode != "bm25":
            try:
                vector = await self._embedder.embed(text)
            except Exception as error:
                raise StorageError(f"Failed to embed '{key}' for Weaviate") from error

        try:
            await asyncio.to_thread(self._write_sync, full_key, data, text, vector)
        except Exception as error:
            if self._is_missing_tenant(error):
                raise StorageError(
                    f"Failed to write '{key}' to Weaviate: tenant '{self._tenant}' does not exist and the "
                    "collection does not create tenants automatically; enable automatic tenant creation on "
                    "the collection or create the tenant first"
                ) from error
            if isinstance(error, StorageError):
                raise
            raise StorageError(f"Failed to write '{key}' to Weaviate ({len(data)} bytes)") from error

    async def read(self, key: str) -> bytes | None:
        """Read the bytes stored under key.

        Args:
            key: The key to read.

        Returns:
            The stored bytes, or None if the key does not exist.

        Raises:
            StorageError: If the key is invalid or the read fails.
        """
        full_key = f"{self._prefix}{normalize_key(key)}"
        try:
            return await asyncio.to_thread(self._read_sync, full_key)
        except StorageError:
            raise
        except Exception as error:
            if self._is_missing_tenant(error):
                return None
            raise StorageError(f"Failed to read '{key}' from Weaviate") from error

    async def delete(self, key: str) -> None:
        """Delete the object stored under key. A no-op if the key does not exist.

        Args:
            key: The key to delete.

        Raises:
            StorageError: If the key is invalid or the delete fails.
        """
        full_key = f"{self._prefix}{normalize_key(key)}"
        try:
            await asyncio.to_thread(self._delete_sync, full_key)
        except StorageError:
            raise
        except Exception as error:
            if self._is_missing_tenant(error):
                return
            raise StorageError(f"Failed to delete '{key}' from Weaviate") from error

    async def list(self, query: str = "") -> builtins.list[str]:
        """List keys matching the given prefix.

        Args:
            query: A prefix string to filter keys. Empty string matches all.

        Returns:
            Matching keys sorted ascending, with the storage-level prefix stripped.

        Raises:
            StorageError: If the prefix is invalid or the listing fails.
        """
        full_prefix = f"{self._prefix}{normalize_prefix(query)}"
        try:
            keys = await asyncio.to_thread(self._list_sync, full_prefix)
        except StorageError:
            raise
        except Exception as error:
            if self._is_missing_tenant(error):
                return []
            raise StorageError(f"Failed to list keys with prefix '{query}' from Weaviate") from error
        logger.debug("prefix=<%s>, count=<%d> | listed keys", full_prefix, len(keys))
        return keys

    async def search(self, query: str) -> builtins.list[StorageSearchResult]:
        """Search indexed text values under this storage's prefix.

        Args:
            query: Natural-language search query.

        Returns:
            Up to ``max_results`` matches ranked best-first, each carrying its stored bytes.
            Scores are higher-is-better and are not comparable across search modes.

        Raises:
            StorageError: If embedding the query or the search fails.
        """
        if not query.strip():
            return []
        try:
            mode = await asyncio.to_thread(self._resolve_mode)
            vector: builtins.list[float] | None = None
            if mode != "bm25" and self._embedder is not None:
                vector = await self._embedder.embed(query)
            return await asyncio.to_thread(self._search_sync, query, mode, vector)
        except StorageError:
            raise
        except Exception as error:
            if self._is_missing_tenant(error):
                return []
            raise StorageError(f"Failed to search Weaviate collection '{self._collection_name}'") from error

    def namespace(self, prefix: str) -> WeaviateStorage:
        """Return a ``WeaviateStorage`` scoped to ``prefix`` within this storage's prefix.

        The scoped instance shares this instance's client and collection, and its ``search()``
        filters by the prefix inside Weaviate rather than after ranking. A prefix scopes keys;
        it is not an access-control boundary.

        Args:
            prefix: Prefix to prepend to all keys.

        Returns:
            A ``WeaviateStorage`` scoped to the prefix.

        Raises:
            StorageError: If the prefix contains backslashes or a '..' segment.
        """
        scoped = copy.copy(self)
        scoped._prefix = f"{self._prefix}{scope_prefix(prefix)}"
        scoped._root = self._root or self
        if _NAMESPACED is not None:
            # Tells SDK constructs this view is already scoped, as the built-in backends do.
            scoped._namespaced = _NAMESPACED  # type: ignore[attr-defined]
        return scoped

    def with_tenant(self, tenant: str) -> WeaviateStorage:
        """Return a ``WeaviateStorage`` bound to ``tenant``, sharing this instance's client.

        The view keeps this storage's prefix and configuration. Calling this makes no request
        to Weaviate: the tenant comes into existence when the first value is written to it.

        Args:
            tenant: The tenant whose shard the returned storage reads and writes.

        Returns:
            A ``WeaviateStorage`` bound to the tenant.

        Raises:
            ValueError: If the tenant name is not one Weaviate accepts.
        """
        scoped = copy.copy(self)
        scoped._tenant = _validate_tenant(tenant)
        scoped._root = self._root or self
        return scoped

    def _is_missing_tenant(self, error: Exception) -> bool:
        """Whether an error means this storage's tenant has not been created yet."""
        return self._tenant is not None and "tenant not found" in str(error)

    def _indexable_text(self, full_key: str, data: bytes) -> str | None:
        """Return the text to make searchable for a value, or None if it is not indexed."""
        root = self._root or self
        relative_key = full_key[len(root._prefix) :]
        if self._index_prefixes is not None and not any(
            relative_key.startswith(index_prefix) for index_prefix in self._index_prefixes
        ):
            return None
        # A NUL byte marks framed binary payloads that happen to decode as UTF-8.
        if not data or b"\x00" in data:
            return None
        try:
            text = data.decode("utf-8")
        except UnicodeDecodeError:
            return None
        return text[: self._max_indexed_chars]

    def _get_collection(self) -> Any:
        """Return the validated collection handle for this storage's tenant (called via to_thread).

        The collection is created on first use. A tenant is not: the handle only names it.
        """
        root = self._root or self
        if root._collection is None:
            with root._lock:
                if root._collection is None:
                    root._collection = root._open_collection(multi_tenant=self._tenant is not None)

        name = self._collection_name
        if self._tenant is None:
            if root._multi_tenant:
                raise StorageError(
                    f"Weaviate collection '{name}' is multi-tenant; pass tenant=... or use with_tenant()"
                )
            return root._collection
        if not root._multi_tenant:
            raise StorageError(
                f"Weaviate collection '{name}' is not multi-tenant, so tenant '{self._tenant}' cannot be used; "
                "multi-tenancy can only be enabled when a collection is created, so use a new collection_name"
            )
        return root._collection.with_tenant(self._tenant)

    def _open_collection(self, multi_tenant: bool) -> Any:
        """Create the collection if needed and check that its schema fits this backend."""
        name = self._collection_name
        collections = self._client.collections
        if not collections.exists(name):
            if not self._create_collection:
                raise StorageError(f"Weaviate collection '{name}' does not exist")
            try:
                collections.create(
                    name,
                    properties=self.collection_properties(),
                    vector_config=Configure.Vectors.self_provided(name=self._vector_name or "default"),
                    # Tenants are created by the first object inserted into them, never up front.
                    multi_tenancy_config=Configure.multi_tenancy(
                        enabled=multi_tenant, auto_tenant_creation=multi_tenant, auto_tenant_activation=multi_tenant
                    ),
                )
                logger.debug("collection=<%s> | created collection", name)
            except Exception:
                # Another process may have created it between the check and the create.
                if not collections.exists(name):
                    raise

        collection = collections.get(name)
        config = collection.config.get()
        properties = {schema_property.name: schema_property for schema_property in config.properties}
        key_property = properties.get("key")
        data_property = properties.get("data")
        if key_property is None or key_property.tokenization != Tokenization.FIELD:
            raise StorageError(
                f"Weaviate collection '{name}' needs a 'key' text property with field tokenization; "
                "create it with WeaviateStorage.collection_properties()"
            )
        if data_property is None or data_property.data_type != DataType.BLOB:
            raise StorageError(
                f"Weaviate collection '{name}' needs a 'data' blob property; "
                "create it with WeaviateStorage.collection_properties()"
            )

        named_vectors = config.vector_config or {}
        self._has_vectorizer = any(
            vector.vectorizer.vectorizer != Vectorizers.NONE for vector in named_vectors.values()
        ) or config.vectorizer not in (None, Vectorizers.NONE)
        self._multi_tenant = bool(config.multi_tenancy_config.enabled)
        return collection

    def _resolve_mode(self) -> SearchMode:
        """Pick the search mode from the configuration and the collection (called via to_thread)."""
        if self._search_mode is not None:
            return self._search_mode
        self._get_collection()
        root = self._root or self
        mode: SearchMode = "hybrid" if self._embedder is not None or root._has_vectorizer else "bm25"
        return mode

    def _write_sync(self, full_key: str, data: bytes, text: str | None, vector: builtins.list[float] | None) -> None:
        """Upsert one object (called via to_thread)."""
        properties: dict[str, Any] = {
            "key": full_key,
            "is_text": text is not None,
            "size": len(data),
            "updated_at": datetime.now(timezone.utc),
        }
        # Weaviate rejects an empty blob, so an empty value is the object without one.
        if data:
            properties["data"] = base64.b64encode(data).decode("ascii")
        if text is not None:
            properties["text"] = text

        named_vector: Any = vector
        if vector is not None and self._vector_name is not None:
            named_vector = {self._vector_name: vector}

        # A single-object batch replaces the whole object by id; data.insert rejects existing ids.
        result = self._get_collection().data.insert_many(
            [DataObject(properties=properties, uuid=key_uuid(full_key), vector=named_vector)]
        )
        if result.has_errors:
            messages = "; ".join(error.message for error in result.errors.values())
            raise StorageError(f"Failed to write '{full_key}' to Weaviate ({len(data)} bytes): {messages}")

    def _read_sync(self, full_key: str) -> bytes | None:
        """Fetch one object's bytes by id (called via to_thread)."""
        stored = self._get_collection().query.fetch_object_by_id(key_uuid(full_key), return_properties=["data"])
        if stored is None:
            return None
        encoded = stored.properties.get("data")
        return base64.b64decode(encoded) if encoded else b""

    def _delete_sync(self, full_key: str) -> None:
        """Delete one object by id (called via to_thread)."""
        self._get_collection().data.delete_by_id(key_uuid(full_key))

    def _list_sync(self, full_prefix: str) -> builtins.list[str]:
        """Collect keys under a fully prefixed prefix (called via to_thread)."""
        collection = self._get_collection()
        keys: builtins.list[str] | None = None
        if full_prefix:
            page = collection.query.fetch_objects(
                filters=Filter.by_property("key").like(f"{full_prefix}*"),
                limit=_LIST_FILTER_LIMIT,
                return_properties=["key"],
            ).objects
            if len(page) < _LIST_FILTER_LIMIT:
                keys = [stored.properties["key"] for stored in page]
        if keys is None:
            keys = [stored.properties["key"] for stored in collection.iterator(return_properties=["key"])]

        # Like treats '*' and '?' in the prefix as wildcards, so the server can over-match.
        return sorted(key[len(self._prefix) :] for key in keys if key.startswith(full_prefix))

    def _search_sync(
        self, query: str, mode: SearchMode, vector: builtins.list[float] | None
    ) -> builtins.list[StorageSearchResult]:
        """Run the query in the given mode (called via to_thread)."""
        filters = Filter.by_property("is_text").equal(True)
        if self._prefix:
            filters = filters & Filter.by_property("key").like(f"{self._prefix}*")
        common: dict[str, Any] = {
            "filters": filters,
            "limit": self._max_results,
            "return_properties": ["key", "data"],
        }
        queries = self._get_collection().query

        if mode == "bm25":
            response = queries.bm25(
                query=query, query_properties=["text"], return_metadata=MetadataQuery(score=True), **common
            )
        elif mode == "hybrid":
            response = queries.hybrid(
                query=query,
                vector=vector,
                alpha=self._alpha,
                query_properties=["text"],
                target_vector=self._vector_name,
                return_metadata=MetadataQuery(score=True),
                **common,
            )
        elif vector is not None:
            response = queries.near_vector(
                near_vector=vector,
                target_vector=self._vector_name,
                return_metadata=MetadataQuery(distance=True),
                **common,
            )
        else:
            response = queries.near_text(
                query=query,
                target_vector=self._vector_name,
                return_metadata=MetadataQuery(distance=True),
                **common,
            )

        results: builtins.list[StorageSearchResult] = []
        for stored in response.objects:
            full_key = stored.properties["key"]
            if not full_key.startswith(self._prefix):
                continue
            encoded = stored.properties.get("data")
            results.append(
                StorageSearchResult(
                    key=full_key[len(self._prefix) :],
                    score=_score(mode, stored.metadata),
                    data=base64.b64decode(encoded) if encoded else b"",
                )
            )
        return results


def _score(mode: SearchMode, metadata: Any) -> float:
    """Map Weaviate result metadata to a higher-is-better score."""
    if mode == "vector":
        distance = metadata.distance
        return 1 / (1 + distance) if distance is not None else 0.0
    score = metadata.score
    return float(score) if score is not None else 0.0
