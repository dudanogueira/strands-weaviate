"""Shared fixtures: an in-memory fake Weaviate client and a real one for integration runs.

Behavioural tests take the ``client`` fixture, which runs each test against the fake and,
when selected with ``-m integ``, against a real Weaviate instance. Keeping one suite for both
is what stops the fake from drifting away from real server behaviour.
"""

from __future__ import annotations

import copy
import math
import os
import re
import uuid
import zlib
from collections.abc import AsyncGenerator, Iterator
from types import SimpleNamespace
from typing import Any

import pytest
import weaviate
from strands.models import Model
from strands.storage.search import Embedder
from weaviate.classes.config import DataType, Tokenization, Vectorizers

EMBEDDING_DIMENSIONS = 32


def _tokens(text: str) -> list[str]:
    return re.findall(r"[a-z0-9]+", text.lower())


class FakeEmbedder(Embedder):
    """Deterministic bag-of-words embedder, so texts sharing words get nearby vectors."""

    def __init__(self) -> None:
        self.calls: list[str] = []
        self.error: Exception | None = None

    async def embed(self, text: str) -> list[float]:
        self.calls.append(text)
        if self.error is not None:
            raise self.error
        vector = [0.0] * EMBEDDING_DIMENSIONS
        for token in _tokens(text):
            vector[zlib.crc32(token.encode()) % EMBEDDING_DIMENSIONS] += 1.0
        norm = math.sqrt(sum(value * value for value in vector)) or 1.0
        return [value / norm for value in vector]


class ScriptedModel(Model):
    """Model that replies with fixed text, for driving an Agent without a provider."""

    def __init__(self, reply: str = "ok") -> None:
        self.reply = reply

    def update_config(self, **model_config: Any) -> None:
        pass

    def get_config(self) -> Any:
        return {}

    async def structured_output(self, output_model: Any, prompt: Any, system_prompt: Any = None, **kwargs: Any) -> Any:
        raise NotImplementedError

    async def stream(self, messages: Any, *args: Any, **kwargs: Any) -> AsyncGenerator[Any, None]:
        yield {"messageStart": {"role": "assistant"}}
        yield {"contentBlockStart": {"start": {}}}
        yield {"contentBlockDelta": {"delta": {"text": self.reply}}}
        yield {"contentBlockStop": {}}
        yield {"messageStop": {"stopReason": "end_turn"}}


def _matches(filters: Any, properties: dict[str, Any]) -> bool:
    """Evaluate the subset of Weaviate filters the backend builds."""
    if filters is None:
        return True
    if hasattr(filters, "filters"):
        return all(_matches(part, properties) for part in filters.filters)
    value = properties.get(filters.target)
    operator = filters.operator.value
    if operator == "Equal":
        return bool(value == filters.value)
    if operator == "Like":
        pattern = "".join(".*" if char == "*" else "." if char == "?" else re.escape(char) for char in filters.value)
        return value is not None and re.fullmatch(pattern, value, flags=re.DOTALL) is not None
    raise NotImplementedError(f"fake does not support operator {operator}")


def _project(stored: dict[str, Any], return_properties: list[str] | None, metadata: Any = None) -> Any:
    properties = stored["properties"]
    # Like the server, blobs are only returned when asked for by name.
    names = return_properties if return_properties is not None else [name for name in properties if name != "data"]
    return SimpleNamespace(
        properties={name: properties.get(name) for name in names},
        metadata=metadata or SimpleNamespace(score=None, distance=None),
        vector=stored["vector"],
    )


class FakeCollection:
    """In-memory stand-in for the collection calls WeaviateStorage makes."""

    def __init__(
        self,
        name: str,
        properties: list[Any],
        has_vectorizer: bool,
        multi_tenant: bool = False,
        auto_tenant_creation: bool = False,
    ) -> None:
        self.name = name
        self.schema = properties
        self.has_vectorizer = has_vectorizer
        self.multi_tenant = multi_tenant
        self.auto_tenant_creation = auto_tenant_creation
        # Objects per tenant; a single-tenant collection keeps them under "".
        self.shards: dict[str, dict[str, dict[str, Any]]] = {}
        self.tenant: str | None = None
        self.calls: list[tuple[str, dict[str, Any]]] = []
        self.config = SimpleNamespace(get=self._config)
        self.tenants = SimpleNamespace(get=lambda: {tenant: None for tenant in self.shards if tenant})
        self._bind()

    def _bind(self) -> None:
        self.data = SimpleNamespace(insert_many=self._insert_many, delete_by_id=self._delete_by_id)
        self.query = SimpleNamespace(
            fetch_object_by_id=self._fetch_object_by_id,
            fetch_objects=self._fetch_objects,
            bm25=self._bm25,
            hybrid=self._hybrid,
            near_vector=self._near_vector,
            near_text=self._near_text,
        )

    def with_tenant(self, tenant: str) -> FakeCollection:
        view = copy.copy(self)
        view.tenant = tenant
        view._bind()
        return view

    def _shard(self, create: bool = False) -> dict[str, dict[str, Any]]:
        """The current tenant's objects, failing the way Weaviate does for a bad tenant."""
        if not self.multi_tenant:
            if self.tenant is not None:
                raise RuntimeError(f"class {self.name} has multi-tenancy disabled, but request was with tenant")
            return self.shards.setdefault("", {})
        if self.tenant is None:
            raise RuntimeError(f"class {self.name} has multi-tenancy enabled, but request was without tenant")
        if self.tenant not in self.shards:
            if not (create and self.auto_tenant_creation):
                raise RuntimeError(f'tenant not found: "{self.tenant}"')
            self.shards[self.tenant] = {}
        return self.shards[self.tenant]

    @property
    def objects(self) -> dict[str, dict[str, Any]]:
        return self._shard()

    def _config(self) -> Any:
        vectorizer = Vectorizers.TEXT2VEC_WEAVIATE if self.has_vectorizer else Vectorizers.NONE
        return SimpleNamespace(
            properties=[
                SimpleNamespace(name=prop.name, data_type=prop.dataType, tokenization=prop.tokenization)
                for prop in self.schema
            ],
            vector_config={"default": SimpleNamespace(vectorizer=SimpleNamespace(vectorizer=vectorizer))},
            vectorizer=None,
            multi_tenancy_config=SimpleNamespace(enabled=self.multi_tenant),
        )

    def _insert_many(self, objects: list[Any]) -> Any:
        # Like the server, the tenant is created before the objects are validated.
        shard = self._shard(create=True)
        errors: dict[int, Any] = {}
        for index, item in enumerate(objects):
            if item.properties.get("data") == "":
                errors[index] = SimpleNamespace(message="invalid blob property 'data': not a valid blob base64 string")
                continue
            shard[str(item.uuid)] = {"properties": dict(item.properties), "vector": item.vector}
        return SimpleNamespace(has_errors=bool(errors), errors=errors)

    def _delete_by_id(self, object_id: uuid.UUID) -> bool:
        self.objects.pop(str(object_id), None)
        return True

    def _fetch_object_by_id(self, object_id: uuid.UUID, return_properties: list[str] | None = None) -> Any:
        stored = self.objects.get(str(object_id))
        return None if stored is None else _project(stored, return_properties)

    def _fetch_objects(self, filters: Any = None, limit: int = 10, return_properties: list[str] | None = None) -> Any:
        self.calls.append(("fetch_objects", {"filters": filters, "limit": limit}))
        matched = [stored for stored in self.objects.values() if _matches(filters, stored["properties"])]
        return SimpleNamespace(objects=[_project(stored, return_properties) for stored in matched[:limit]])

    def iterator(self, return_properties: list[str] | None = None) -> Iterator[Any]:
        self.calls.append(("iterator", {}))
        return iter([_project(stored, return_properties) for stored in list(self.objects.values())])

    def _keyword_score(self, query: str, stored: dict[str, Any]) -> float:
        text_tokens = _tokens(stored["properties"].get("text") or "")
        return float(sum(text_tokens.count(token) for token in set(_tokens(query))))

    def _ranked(self, scored: list[tuple[float, dict[str, Any]]], kwargs: dict[str, Any], distance: bool) -> Any:
        scored.sort(key=lambda pair: pair[0], reverse=not distance)
        results = []
        for value, stored in scored[: kwargs["limit"]]:
            metadata = SimpleNamespace(score=None if distance else value, distance=value if distance else None)
            results.append(_project(stored, kwargs.get("return_properties"), metadata))
        return SimpleNamespace(objects=results)

    def _candidates(self, kwargs: dict[str, Any]) -> list[dict[str, Any]]:
        return [stored for stored in self.objects.values() if _matches(kwargs.get("filters"), stored["properties"])]

    def _bm25(self, query: str, **kwargs: Any) -> Any:
        self.calls.append(("bm25", {"query": query, **kwargs}))
        scored = [(self._keyword_score(query, stored), stored) for stored in self._candidates(kwargs)]
        return self._ranked([pair for pair in scored if pair[0] > 0], kwargs, distance=False)

    def _hybrid(self, query: str, vector: list[float] | None = None, **kwargs: Any) -> Any:
        self.calls.append(("hybrid", {"query": query, "vector": vector, **kwargs}))
        if vector is None and not self.has_vectorizer:
            raise RuntimeError("VectorFromInput was called without vectorizer")
        alpha = kwargs["alpha"]
        scored = []
        for stored in self._candidates(kwargs):
            keyword = min(self._keyword_score(query, stored), 1.0)
            similarity = _cosine(vector, _first_vector(stored)) if vector is not None else 0.0
            score = (1 - alpha) * keyword + alpha * similarity
            if score > 0:
                scored.append((score, stored))
        return self._ranked(scored, kwargs, distance=False)

    def _near_vector(self, near_vector: list[float], **kwargs: Any) -> Any:
        self.calls.append(("near_vector", {"near_vector": near_vector, **kwargs}))
        scored = [
            (1 - _cosine(near_vector, _first_vector(stored)), stored)
            for stored in self._candidates(kwargs)
            if _first_vector(stored) is not None
        ]
        return self._ranked(scored, kwargs, distance=True)

    def _near_text(self, query: str, **kwargs: Any) -> Any:
        self.calls.append(("near_text", {"query": query, **kwargs}))
        if not self.has_vectorizer:
            raise RuntimeError("near_text requires a vectorizer")
        scored = [(1.0 / (1.0 + self._keyword_score(query, stored)), stored) for stored in self._candidates(kwargs)]
        return self._ranked(scored, kwargs, distance=True)


def _first_vector(stored: dict[str, Any]) -> list[float] | None:
    vector = stored["vector"]
    if isinstance(vector, dict):
        return next(iter(vector.values()), None)
    return vector  # type: ignore[no-any-return]


def _cosine(left: list[float] | None, right: list[float] | None) -> float:
    if left is None or right is None:
        return 0.0
    dot = sum(a * b for a, b in zip(left, right, strict=True))
    norms = math.sqrt(sum(a * a for a in left)) * math.sqrt(sum(b * b for b in right))
    return dot / norms if norms else 0.0


class FakeClient:
    """In-memory stand-in for ``weaviate.WeaviateClient``."""

    def __init__(self) -> None:
        self.store: dict[str, FakeCollection] = {}
        self.collections = SimpleNamespace(
            exists=lambda name: name in self.store,
            create=self._create,
            get=lambda name: self.store[name],
            delete=lambda name: self.store.pop(name, None),
        )

    def _create(
        self, name: str, properties: list[Any], vector_config: Any = None, multi_tenancy_config: Any = None
    ) -> FakeCollection:
        # A test asks for a server-side vectorizer by passing the string "vectorizer".
        self.store[name] = FakeCollection(
            name,
            properties,
            has_vectorizer=vector_config == "vectorizer",
            multi_tenant=bool(getattr(multi_tenancy_config, "enabled", False)),
            auto_tenant_creation=bool(getattr(multi_tenancy_config, "autoTenantCreation", False)),
        )
        return self.store[name]


def _connect_real() -> weaviate.WeaviateClient:
    host = os.environ.get("WEAVIATE_HOST", "localhost")
    http_port = int(os.environ.get("WEAVIATE_HTTP_PORT", "8080"))
    grpc_port = int(os.environ.get("WEAVIATE_GRPC_PORT", "50051"))
    try:
        return weaviate.connect_to_local(host=host, port=http_port, grpc_port=grpc_port)
    except Exception as error:
        pytest.skip(f"no Weaviate at {host}:{http_port} ({type(error).__name__})")


@pytest.fixture(params=["fake", pytest.param("weaviate", marks=pytest.mark.integ)])
def client(request: pytest.FixtureRequest) -> Iterator[Any]:
    """A Weaviate client: the in-memory fake, or a real connection under ``-m integ``."""
    if request.param == "fake":
        yield FakeClient()
        return
    real = _connect_real()
    try:
        yield real
    finally:
        real.close()


@pytest.fixture
def collection_name(client: Any) -> Iterator[str]:
    """A unique collection name, deleted after the test."""
    name = f"StrandsTest_{uuid.uuid4().hex}"
    yield name
    client.collections.delete(name)


@pytest.fixture
def embedder() -> FakeEmbedder:
    return FakeEmbedder()


def stored_object(client: Any, collection_name: str, object_id: uuid.UUID) -> Any:
    """Fetch one raw object with its vector, on either kind of client."""
    collection = client.collections.get(collection_name)
    if isinstance(client, FakeClient):
        raw = collection.objects.get(str(object_id))
        return None if raw is None else _project(raw, ["key", "text", "data", "is_text", "size"])
    return collection.query.fetch_object_by_id(
        object_id, include_vector=True, return_properties=["key", "text", "data", "is_text", "size"]
    )


def create_misconfigured_collection(client: Any, name: str) -> None:
    """Create a collection whose ``key`` property uses the default word tokenization."""
    from weaviate.classes.config import Configure, Property

    properties = [
        Property(name="key", data_type=DataType.TEXT, tokenization=Tokenization.WORD),
        Property(name="data", data_type=DataType.BLOB),
    ]
    if isinstance(client, FakeClient):
        client.collections.create(name, properties=properties)
    else:
        client.collections.create(name, properties=properties, vector_config=Configure.Vectors.self_provided())


def create_manual_tenant_collection(client: Any, name: str) -> None:
    """Create a multi-tenant collection that does not create tenants automatically."""
    from weaviate.classes.config import Configure

    from strands_weaviate import WeaviateStorage

    client.collections.create(
        name,
        properties=WeaviateStorage.collection_properties(),
        vector_config=Configure.Vectors.self_provided(),
        multi_tenancy_config=Configure.multi_tenancy(enabled=True),
    )


def tenant_names(client: Any, collection_name: str) -> list[str]:
    """The tenants that exist in a collection, on either kind of client."""
    return sorted(client.collections.get(collection_name).tenants.get())
