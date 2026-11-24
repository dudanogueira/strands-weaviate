"""Tests for WeaviateStorage.

Tests that take the ``client`` fixture run against the in-memory fake and, under ``-m integ``,
against a real Weaviate instance.
"""

import importlib.metadata
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
import weaviate
from conftest import (
    FakeClient,
    ScriptedModel,
    create_manual_tenant_collection,
    create_misconfigured_collection,
    stored_object,
    tenant_names,
)
from strands import Agent
from strands.memory import MemoryManager
from strands.session import SnapshotSessionManager
from strands.storage import Storage
from strands.storage.storage import _NAMESPACED, _NamespacedStorage
from strands.types.exceptions import StorageError
from strands.vended_memory_stores import FileMemoryStore

import strands_weaviate.storage as storage_module
from strands_weaviate import WeaviateStorage
from strands_weaviate._keys import key_uuid
from strands_weaviate._telemetry import integration_version


@pytest.fixture
def storage(client, collection_name):
    return WeaviateStorage(client, collection_name=collection_name)


class TestReadWriteDelete:
    @pytest.mark.parametrize(
        "data",
        [b"hello world", b"\x00\xff\xfe binary", b"", "café ☃".encode()],
        ids=["text", "binary", "empty", "unicode"],
    )
    async def test_round_trips_bytes(self, storage, data):
        await storage.write("a/value", data)

        assert await storage.read("a/value") == data

    async def test_read_missing_returns_none(self, storage):
        assert await storage.read("missing") is None

    async def test_write_overwrites(self, storage):
        await storage.write("key", b"first")
        await storage.write("key", b"second")

        assert await storage.read("key") == b"second"
        assert await storage.list() == ["key"]

    async def test_delete_removes_value(self, storage):
        await storage.write("key", b"value")
        await storage.delete("key")

        assert await storage.read("key") is None
        assert await storage.list() == []

    async def test_delete_missing_is_noop(self, storage):
        await storage.delete("missing")

    async def test_normalizes_keys(self, storage):
        await storage.write("/a//b/", b"value")

        assert await storage.read("a/b") == b"value"
        assert await storage.list() == ["a/b"]

    @pytest.mark.parametrize("key", ["", "a\\b", "../a", "a/../b"])
    async def test_rejects_invalid_keys(self, storage, key):
        for operation in (storage.read(key), storage.delete(key), storage.write(key, b"value")):
            with pytest.raises(StorageError):
                await operation

    async def test_stores_multi_megabyte_value(self, storage):
        data = b"snapshot " * 400_000

        await storage.write("session/large.json", data)

        assert await storage.read("session/large.json") == data


class TestList:
    async def test_lists_all_sorted(self, storage):
        for key in ("b/2", "a/1", "c"):
            await storage.write(key, b"value")

        assert await storage.list() == ["a/1", "b/2", "c"]

    async def test_lists_by_prefix_recursively(self, storage):
        for key in ("a/1", "a/sub/2", "ab/3", "b/4"):
            await storage.write(key, b"value")

        assert await storage.list("a/") == ["a/1", "a/sub/2"]

    async def test_prefix_need_not_be_a_directory(self, storage):
        for key in ("block_0", "block_1", "blocked_0"):
            await storage.write(key, b"value")

        assert await storage.list("block_") == ["block_0", "block_1"]

    async def test_prefix_is_case_sensitive(self, storage):
        for key in ("a/1", "A/1"):
            await storage.write(key, b"value")

        assert await storage.list("a/") == ["a/1"]

    async def test_wildcard_characters_in_prefix_match_literally(self, storage):
        for key in ("a*/1", "ab/2", "a?/3", "ac/4"):
            await storage.write(key, b"value")

        assert await storage.list("a*/") == ["a*/1"]
        assert await storage.list("a?/") == ["a?/3"]

    async def test_falls_back_to_scan_when_filtered_page_is_full(self, storage, monkeypatch):
        monkeypatch.setattr(storage_module, "_LIST_FILTER_LIMIT", 5)
        keys = sorted(f"p/{index:03d}" for index in range(23))
        for key in [*keys, "q/other"]:
            await storage.write(key, b"value")

        assert await storage.list("p/") == keys

    async def test_rejects_invalid_prefix(self, storage):
        with pytest.raises(StorageError):
            await storage.list("../a")


class TestPrefixAndNamespace:
    async def test_prefix_scopes_keys(self, client, collection_name):
        root = WeaviateStorage(client, collection_name=collection_name)
        scoped = WeaviateStorage(client, collection_name=collection_name, prefix="/agents//one/")

        await scoped.write("key", b"value")

        assert scoped.prefix == "agents/one/"
        assert await scoped.list() == ["key"]
        assert await root.list() == ["agents/one/key"]
        assert await root.read("agents/one/key") == b"value"

    async def test_namespace_scopes_and_composes(self, storage):
        tenant = storage.namespace("tenant")
        nested = tenant.namespace("memory/")

        await nested.write("notes.md", b"value")
        await storage.write("other", b"value")

        assert nested.prefix == "tenant/memory/"
        assert await nested.list() == ["notes.md"]
        assert await tenant.list() == ["memory/notes.md"]
        assert await storage.list() == ["other", "tenant/memory/notes.md"]

    def test_namespace_is_marked_and_root_is_not(self, storage):
        scoped = storage.namespace("tenant")

        assert getattr(scoped, "_namespaced", None) is _NAMESPACED
        assert getattr(storage, "_namespaced", None) is None
        assert not hasattr(scoped, "_ephemeral")

    def test_empty_namespace_keeps_prefix(self, storage):
        assert storage.namespace("").prefix == ""

    def test_namespace_rejects_traversal(self, storage):
        with pytest.raises(StorageError):
            storage.namespace("../other")

    async def test_works_under_sdk_namespace_wrapper(self, storage):
        wrapped = _NamespacedStorage(storage, "session")

        await wrapped.write("abc/snapshot.json", b"{}")

        assert await wrapped.list("abc/") == ["abc/snapshot.json"]
        assert await wrapped.read("abc/snapshot.json") == b"{}"
        assert await storage.list() == ["session/abc/snapshot.json"]


class TestSearch:
    async def test_bm25_when_no_vector_source(self, storage):
        await storage.write("memory/coffee.md", b"the user drinks oat milk coffee")
        await storage.write("memory/editor.md", b"the user writes code in vim")

        results = await storage.search("coffee")

        assert [result.key for result in results] == ["memory/coffee.md"]
        assert results[0].data == b"the user drinks oat milk coffee"
        assert results[0].score > 0

    async def test_hybrid_with_embedder(self, client, collection_name, embedder):
        storage = WeaviateStorage(client, collection_name=collection_name, embedder=embedder)
        await storage.write("memory/coffee.md", b"the user drinks oat milk coffee")
        await storage.write("memory/editor.md", b"the user writes code in vim")

        results = await storage.search("which coffee does the user drink")

        assert results[0].key == "memory/coffee.md"
        assert embedder.calls[-1] == "which coffee does the user drink"
        vector = stored_object(client, collection_name, key_uuid("memory/coffee.md")).vector
        assert vector

    async def test_vector_mode_scores_are_similarities(self, client, collection_name, embedder):
        storage = WeaviateStorage(client, collection_name=collection_name, embedder=embedder, search_mode="vector")
        await storage.write("coffee.md", b"oat milk coffee")
        await storage.write("editor.md", b"vim editor")

        results = await storage.search("oat milk coffee")

        assert results[0].key == "coffee.md"
        assert results[0].score == pytest.approx(1.0, abs=1e-3)
        assert all(0 < result.score <= 1.0001 for result in results)
        assert [result.score for result in results] == sorted((result.score for result in results), reverse=True)

    async def test_search_is_scoped_to_namespace(self, storage):
        await storage.namespace("one").write("notes.md", b"shared keyword alpha")
        await storage.namespace("two").write("notes.md", b"shared keyword beta")

        results = await storage.namespace("one").search("shared keyword")

        assert [(result.key, result.data) for result in results] == [("notes.md", b"shared keyword alpha")]

    async def test_max_results_applies_within_namespace(self, client, collection_name):
        storage = WeaviateStorage(client, collection_name=collection_name, max_results=2)
        for index in range(6):
            await storage.namespace("noise").write(f"{index}.md", b"topic topic topic")
        await storage.namespace("mine").write("only.md", b"topic")

        results = await storage.namespace("mine").search("topic")

        assert [result.key for result in results] == ["only.md"]
        assert len(await storage.search("topic")) == 2

    @pytest.mark.parametrize("query", ["", "   "])
    async def test_empty_query_returns_nothing(self, storage, query):
        await storage.write("notes.md", b"some text")

        assert await storage.search(query) == []

    async def test_search_after_delete_and_overwrite(self, storage):
        await storage.write("gone.md", b"ephemeral marker")
        await storage.write("changed.md", b"ephemeral marker")
        await storage.delete("gone.md")
        await storage.write("changed.md", b"something else entirely")

        assert await storage.search("ephemeral marker") == []


class TestIndexing:
    async def test_binary_values_are_not_searchable(self, client, collection_name, embedder):
        storage = WeaviateStorage(client, collection_name=collection_name, embedder=embedder)
        offloader_frame = b"\x00\x0atext/plain" + b"needle in a frame"

        await storage.write("offloader/frame", offloader_frame)
        await storage.write("offloader/image", b"\xff\xd8\xff needle")

        assert await storage.search("needle") == []
        assert embedder.calls == ["needle"]
        assert await storage.read("offloader/frame") == offloader_frame

    async def test_text_overwritten_by_binary_drops_text_and_vector(self, client, collection_name, embedder):
        storage = WeaviateStorage(client, collection_name=collection_name, embedder=embedder)
        await storage.write("value", b"needle")
        await storage.write("value", b"\x00needle")

        stored = stored_object(client, collection_name, key_uuid("value"))

        assert stored.properties["text"] is None
        assert stored.properties["is_text"] is False
        assert not stored.vector
        assert await storage.search("needle") == []

    async def test_index_prefixes_gate_is_relative_to_storage_prefix(self, client, collection_name, embedder):
        storage = WeaviateStorage(
            client, collection_name=collection_name, prefix="agents", embedder=embedder, index_prefixes=["memory/"]
        )
        await storage.write("session/abc/snapshot.json", b'{"messages": "needle"}')
        await storage.namespace("memory/user").write("notes.md", b"needle")

        results = await storage.search("needle")

        assert [result.key for result in results] == ["memory/user/notes.md"]
        assert embedder.calls == ["needle", "needle"]
        assert await storage.read("session/abc/snapshot.json") == b'{"messages": "needle"}'

    async def test_truncates_indexed_text_but_not_value(self, client, collection_name):
        storage = WeaviateStorage(client, collection_name=collection_name, max_indexed_chars=10)
        data = b"headword and then a tailword"

        await storage.write("long.md", data)

        assert [result.key for result in await storage.search("headword")] == ["long.md"]
        assert await storage.search("tailword") == []
        assert await storage.read("long.md") == data


class TestCollection:
    async def test_creates_collection_on_first_use(self, client, collection_name):
        storage = WeaviateStorage(client, collection_name=collection_name)
        assert not client.collections.exists(collection_name)

        await storage.write("key", b"value")

        assert client.collections.exists(collection_name)

    async def test_missing_collection_raises_when_creation_disabled(self, client, collection_name):
        storage = WeaviateStorage(client, collection_name=collection_name, create_collection=False)

        with pytest.raises(StorageError, match="does not exist"):
            await storage.read("key")

    async def test_rejects_collection_with_wrong_key_tokenization(self, client, collection_name):
        create_misconfigured_collection(client, collection_name)
        storage = WeaviateStorage(client, collection_name=collection_name)

        with pytest.raises(StorageError, match="field tokenization"):
            await storage.list()

    async def test_namespaces_share_one_collection_setup(self, client, collection_name):
        storage = WeaviateStorage(client, collection_name=collection_name)
        scoped = storage.namespace("a")

        await scoped.write("key", b"value")

        assert storage._collection is not None
        assert scoped._get_collection() is storage._collection


class TestSdkConsumers:
    def test_satisfies_storage_protocol(self, storage):
        assert isinstance(storage, Storage)

    async def test_file_memory_store_recalls_without_extra_reads(self, client, collection_name, embedder, monkeypatch):
        storage = WeaviateStorage(client, collection_name=collection_name, embedder=embedder)
        store = FileMemoryStore(name="user", storage=storage)
        manager = MemoryManager(stores=[store])
        await store.add("# Drinks\nthe user drinks oat milk coffee")
        await store.add("# Drinks\nthe user dislikes green tea")
        await store.add("# Editor\nthe user writes code in vim")
        reads = MagicMock(wraps=WeaviateStorage._read_sync)
        monkeypatch.setattr(WeaviateStorage, "_read_sync", reads)

        entries = await manager.search("what coffee does the user drink")

        assert entries[0].content == "# Drinks\nthe user drinks oat milk coffee\nthe user dislikes green tea"
        assert entries[0].metadata["path"] == "drinks.md"
        assert reads.call_count == 0
        assert await storage.list() == ["memory/user/drinks.md", "memory/user/editor.md"]

    def test_session_survives_sync_invocations_on_separate_event_loops(self, client, collection_name):
        storage = WeaviateStorage(client, collection_name=collection_name, index_prefixes=["memory/"])

        first = Agent(
            model=ScriptedModel("noted"),
            session_manager=SnapshotSessionManager(session_id="chat", storage=storage),
            callback_handler=None,
        )
        first("my favourite colour is teal")
        first("and my favourite number is seven")

        second = Agent(
            model=ScriptedModel("teal"),
            session_manager=SnapshotSessionManager(session_id="chat", storage=storage),
            callback_handler=None,
        )

        texts = [block["text"] for message in second.messages for block in message["content"] if "text" in block]
        assert texts == ["my favourite colour is teal", "noted", "and my favourite number is seven", "noted"]


class TestMultiTenancy:
    @pytest.fixture
    def acme(self, client, collection_name):
        return WeaviateStorage(client, collection_name=collection_name, tenant="acme")

    async def test_tenants_are_isolated(self, acme):
        globex = acme.with_tenant("globex")
        await acme.write("memory/notes.md", b"acme prefers oat milk")
        await globex.write("memory/notes.md", b"globex prefers green tea")
        await globex.write("memory/extra.md", b"globex only")

        assert await acme.read("memory/notes.md") == b"acme prefers oat milk"
        assert await globex.read("memory/notes.md") == b"globex prefers green tea"
        assert await acme.list() == ["memory/notes.md"]
        assert await globex.list() == ["memory/extra.md", "memory/notes.md"]
        assert [result.data for result in await acme.search("prefers")] == [b"acme prefers oat milk"]
        assert await acme.search("globex") == []

    async def test_delete_only_affects_own_tenant(self, acme):
        globex = acme.with_tenant("globex")
        await acme.write("key", b"acme")
        await globex.write("key", b"globex")

        await acme.delete("key")

        assert await acme.read("key") is None
        assert await globex.read("key") == b"globex"

    async def test_with_tenant_and_namespace_compose(self, acme):
        scoped = acme.namespace("memory").with_tenant("globex").namespace("user")

        await scoped.write("notes.md", b"value")

        assert (acme.tenant, scoped.tenant, scoped.prefix) == ("acme", "globex", "memory/user/")
        assert await acme.with_tenant("globex").list() == ["memory/user/notes.md"]
        assert await acme.list() == []
        assert scoped._root is acme

    async def test_no_tenant_is_created_without_data(self, client, collection_name, embedder):
        storage = WeaviateStorage(client, collection_name=collection_name, tenant="acme", embedder=embedder)
        ghost = storage.with_tenant("ghost").namespace("memory")
        assert not client.collections.exists(collection_name)

        assert await ghost.read("notes.md") is None
        assert await ghost.list() == []
        assert await ghost.search("anything") == []
        await ghost.delete("notes.md")
        assert await storage.read("notes.md") is None
        embedder.error = RuntimeError("embedder down")
        with pytest.raises(StorageError, match="Failed to embed"):
            await ghost.write("notes.md", b"never stored")
        with pytest.raises(StorageError):
            await ghost.write("../escape", b"never stored")

        assert client.collections.exists(collection_name)
        assert tenant_names(client, collection_name) == []

    async def test_first_write_creates_only_that_tenant(self, acme, client, collection_name):
        acme.with_tenant("globex")
        await acme.list()
        assert tenant_names(client, collection_name) == []

        await acme.write("key", b"value")

        assert tenant_names(client, collection_name) == ["acme"]

    async def test_empty_value_is_data(self, acme, client, collection_name):
        await acme.write("empty", b"")

        assert await acme.read("empty") == b""
        assert tenant_names(client, collection_name) == ["acme"]

    async def test_tenant_on_single_tenant_collection_raises(self, client, collection_name):
        await WeaviateStorage(client, collection_name=collection_name).write("key", b"value")
        tenant = WeaviateStorage(client, collection_name=collection_name, tenant="acme")

        for operation in (tenant.read("key"), tenant.write("key", b"value"), tenant.list(), tenant.search("value")):
            with pytest.raises(StorageError, match="is not multi-tenant"):
                await operation

    async def test_missing_tenant_on_multi_tenant_collection_raises(self, acme, client, collection_name):
        await acme.write("key", b"value")
        untenanted = WeaviateStorage(client, collection_name=collection_name)

        for operation in (untenanted.read("key"), untenanted.write("key", b"value"), untenanted.list()):
            with pytest.raises(StorageError, match="is multi-tenant; pass tenant"):
                await operation

    async def test_tenantless_root_serves_tenants_through_with_tenant(self, client, collection_name):
        root = WeaviateStorage(client, collection_name=collection_name)

        await root.with_tenant("acme").write("key", b"value")

        assert await root.with_tenant("acme").read("key") == b"value"
        assert tenant_names(client, collection_name) == ["acme"]

    async def test_write_fails_clearly_without_automatic_tenant_creation(self, client, collection_name):
        create_manual_tenant_collection(client, collection_name)
        storage = WeaviateStorage(client, collection_name=collection_name, tenant="acme")

        with pytest.raises(StorageError, match="does not create tenants automatically"):
            await storage.write("key", b"value")

        assert await storage.read("key") is None
        assert tenant_names(client, collection_name) == []

    async def test_file_memory_stores_do_not_share_memories_across_tenants(self, acme):
        acme_store = FileMemoryStore(name="user", storage=acme)
        globex_store = FileMemoryStore(name="user", storage=acme.with_tenant("globex"))
        await acme_store.add("# Drinks\nthe user drinks oat milk coffee")

        assert [entry.metadata["path"] for entry in await acme_store.search("coffee")] == ["drinks.md"]
        assert await globex_store.search("coffee") == []

    @pytest.mark.parametrize("tenant", ["", "has space", "a/b", "café", "x" * 65])
    def test_rejects_invalid_tenant_names(self, tenant):
        with pytest.raises(ValueError, match="tenant must be"):
            WeaviateStorage(FakeClient(), tenant=tenant)
        with pytest.raises(ValueError, match="tenant must be"):
            WeaviateStorage(FakeClient()).with_tenant(tenant)

    @pytest.mark.parametrize("tenant", ["a", "ok_Name-1", "x" * 64])
    async def test_accepts_valid_tenant_names(self, client, collection_name, tenant):
        storage = WeaviateStorage(client, collection_name=collection_name, tenant=tenant)

        await storage.write("key", b"value")

        assert tenant_names(client, collection_name) == [tenant]


class TestIntegrationHeader:
    @pytest.fixture
    def unconnected_client(self):
        """A real client that is never connected, so its headers can be inspected offline."""
        return weaviate.WeaviateClient(
            connection_params=weaviate.connect.ConnectionParams.from_params(
                http_host="localhost",
                http_port=8080,
                http_secure=False,
                grpc_host="localhost",
                grpc_port=50051,
                grpc_secure=False,
            ),
            skip_init_checks=True,
        )

    def test_tags_http_and_grpc_requests(self, unconnected_client):
        WeaviateStorage(unconnected_client)

        expected = f"strands-python/{integration_version()}"
        connection = unconnected_client._connection
        assert connection._headers["X-Weaviate-Client-Integration"] == expected
        assert ("x-weaviate-client-integration", expected) in connection.grpc_headers()

    def test_falls_back_to_unknown_version(self, unconnected_client, monkeypatch):
        def missing(name):
            raise importlib.metadata.PackageNotFoundError(name)

        monkeypatch.setattr(importlib.metadata, "version", missing)

        WeaviateStorage(unconnected_client)

        assert unconnected_client._connection._headers["X-Weaviate-Client-Integration"] == "strands-python/unknown"

    def test_client_without_integrations_api_is_left_alone(self):
        WeaviateStorage(FakeClient())


class TestConfiguration:
    def test_rejects_async_client(self):
        async_client = weaviate.use_async_with_local()

        with pytest.raises(StorageError, match="synchronous"):
            WeaviateStorage(async_client)

    @pytest.mark.parametrize(
        "config",
        [{"search_mode": "fuzzy"}, {"alpha": 1.5}, {"alpha": -0.1}, {"max_results": 0}, {"max_indexed_chars": 0}],
    )
    def test_rejects_out_of_range_config(self, config):
        with pytest.raises(ValueError):
            WeaviateStorage(FakeClient(), **config)

    def test_rejects_invalid_prefix(self):
        with pytest.raises(StorageError):
            WeaviateStorage(FakeClient(), prefix="../escape")


class TestSearchModeResolution:
    """Fake-only: asserts which Weaviate query each configuration issues."""

    def _calls(self, client, name):
        return [call for call, _ in client.collections.get(name).calls if call != "fetch_objects"]

    async def test_no_vector_source_uses_bm25(self):
        client = FakeClient()
        storage = WeaviateStorage(client, collection_name="C")
        await storage.write("a", b"text")

        await storage.search("text")

        assert self._calls(client, "C") == ["bm25"]

    async def test_embedder_uses_hybrid_with_query_vector_and_alpha(self, embedder):
        client = FakeClient()
        storage = WeaviateStorage(client, collection_name="C", embedder=embedder, alpha=0.25, vector_name="content")
        await storage.write("a", b"text")

        await storage.search("text")

        name, kwargs = client.collections.get("C").calls[-1]
        assert name == "hybrid"
        assert kwargs["vector"] == await embedder.embed("text")
        assert kwargs["alpha"] == 0.25
        assert kwargs["target_vector"] == "content"
        assert kwargs["query_properties"] == ["text"]
        assert client.collections.get("C").objects[str(key_uuid("a"))]["vector"] == {"content": kwargs["vector"]}

    async def test_collection_vectorizer_uses_hybrid_without_query_vector(self):
        client = FakeClient()
        client.collections.create("C", properties=WeaviateStorage.collection_properties(), vector_config="vectorizer")
        storage = WeaviateStorage(client, collection_name="C")
        await storage.write("a", b"text")

        results = await storage.search("text")

        name, kwargs = client.collections.get("C").calls[-1]
        assert (name, kwargs["vector"]) == ("hybrid", None)
        assert [result.key for result in results] == ["a"]

    async def test_explicit_bm25_skips_embedder(self, embedder):
        client = FakeClient()
        storage = WeaviateStorage(client, collection_name="C", embedder=embedder, search_mode="bm25")
        await storage.write("a", b"text")

        await storage.search("text")

        assert embedder.calls == []
        assert self._calls(client, "C") == ["bm25"]

    async def test_vector_mode_without_embedder_uses_near_text(self):
        client = FakeClient()
        client.collections.create("C", properties=WeaviateStorage.collection_properties(), vector_config="vectorizer")
        storage = WeaviateStorage(client, collection_name="C", search_mode="vector")
        await storage.write("a", b"text")

        results = await storage.search("text")

        assert self._calls(client, "C") == ["near_text"]
        distance = 0.5
        assert results[0].score == pytest.approx(1 / (1 + distance))

    async def test_list_uses_one_filtered_fetch_when_page_is_not_full(self):
        client = FakeClient()
        storage = WeaviateStorage(client, collection_name="C")
        await storage.write("a/1", b"text")

        await storage.list("a/")
        await storage.list("")

        assert [call for call, _ in client.collections.get("C").calls] == ["fetch_objects", "iterator"]


class TestErrors:
    """Fake-only: failures from the client and the embedder surface as StorageError."""

    @pytest.fixture
    def failing(self):
        client = MagicMock()
        client.collections.exists.side_effect = RuntimeError("connection refused")
        return WeaviateStorage(client)

    @pytest.mark.parametrize(
        ("call", "message"),
        [
            (lambda storage: storage.write("key", b"value"), r"Failed to write 'key' to Weaviate \(5 bytes\)"),
            (lambda storage: storage.read("key"), "Failed to read 'key' from Weaviate"),
            (lambda storage: storage.delete("key"), "Failed to delete 'key' from Weaviate"),
            (lambda storage: storage.list("pre"), "Failed to list keys with prefix 'pre' from Weaviate"),
            (lambda storage: storage.search("query"), "Failed to search Weaviate collection 'StrandsStorage'"),
        ],
    )
    async def test_wraps_client_errors(self, failing, call, message):
        with pytest.raises(StorageError, match=message) as error:
            await call(failing)

        assert isinstance(error.value.__cause__, RuntimeError)

    async def test_embedding_failure_keeps_previous_value(self, embedder):
        client = FakeClient()
        storage = WeaviateStorage(client, collection_name="C", embedder=embedder)
        await storage.write("key", b"first")
        embedder.error = RuntimeError("embedder down")

        with pytest.raises(StorageError, match="Failed to embed 'key'") as error:
            await storage.write("key", b"second")

        assert error.value.__cause__ is embedder.error
        assert await storage.read("key") == b"first"

    async def test_query_embedding_failure_is_wrapped(self, embedder):
        storage = WeaviateStorage(FakeClient(), collection_name="C", embedder=embedder)
        embedder.error = RuntimeError("embedder down")

        with pytest.raises(StorageError, match="Failed to search") as error:
            await storage.search("query")

        assert error.value.__cause__ is embedder.error

    async def test_rejected_object_raises(self):
        client = FakeClient()
        storage = WeaviateStorage(client, collection_name="C")
        await storage.write("key", b"first")
        rejected = SimpleNamespace(has_errors=True, errors={0: SimpleNamespace(message="object too large")})
        client.collections.get("C").data.insert_many = lambda objects: rejected

        with pytest.raises(StorageError, match="object too large"):
            await storage.write("key", b"second")
