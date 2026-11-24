# strands-weaviate (Python)

A [Weaviate](https://weaviate.io) `Storage` backend for
[Strands Agents](https://strandsagents.com). One collection holds session snapshots, offloaded
context and agent memory, and `search()` runs a Weaviate hybrid (BM25 + vector) query instead
of a keyword scan.

Each key is one Weaviate object carrying the raw bytes, the searchable text and the vector, so
an overwrite or delete can never leave a stale search entry behind.

## Install

The package is not on PyPI yet. Install it from source, straight from GitHub:

```bash
pip install "strands-weaviate @ git+https://github.com/dudanogueira/strands-weaviate.git#subdirectory=python"
```

Add `@<branch-or-tag>` after `.git` to pin a version, for example
`...strands-weaviate.git@python-v0.1.0#subdirectory=python`.

Or from a local clone, as an editable install if you want to change the code:

```bash
git clone https://github.com/dudanogueira/strands-weaviate.git
pip install -e strands-weaviate/python
```

In a `requirements.txt`, use the same `strands-weaviate @ git+https://...` line. In a
`pyproject.toml`, add it to `dependencies` the same way.

Requires Python 3.10+, `strands-agents>=1.64.0` and a Weaviate server reachable through the
synchronous `weaviate-client` v4. Both are installed as dependencies.

## Quickstart

```python
import weaviate
from strands_weaviate import WeaviateStorage

client = weaviate.connect_to_local()  # or weaviate.connect_to_weaviate_cloud(...)
storage = WeaviateStorage(client)

await storage.write("memory/notes.md", b"The user prefers concise answers.")
await storage.read("memory/notes.md")        # b"The user prefers concise answers."
await storage.list("memory/")                # ["memory/notes.md"]
await storage.search("concise answers")      # [StorageSearchResult(key="memory/notes.md", ...)]
```

You own the client: `WeaviateStorage` never connects or closes it. The collection
(`StrandsStorage` by default) is created on first use.

### Agent memory

```python
from strands import Agent
from strands.memory import MemoryManager
from strands.vended_memory_stores import FileMemoryStore

memory_store = FileMemoryStore(name="user", storage=storage)
agent = Agent(memory_manager=MemoryManager(stores=[memory_store]))
```

Recall goes through `storage.search()`, and results carry their content, so the store does not
re-read each hit.

### Sessions

```python
from strands.session import SnapshotSessionManager

agent = Agent(session_manager=SnapshotSessionManager(session_id="chat-42", storage=storage))
```

## Search modes

| Vectors come from | How to set it up | Default search |
|---|---|---|
| Nothing | `WeaviateStorage(client)` | BM25 |
| An SDK embedder | `WeaviateStorage(client, embedder=...)` | Hybrid |
| A Weaviate vectorizer | Create the collection yourself (below) | Hybrid |

Set `search_mode` to `"hybrid"`, `"vector"` or `"bm25"` to override, and `alpha` (0 = BM25
only, 1 = vector only) to weight hybrid search. Scores are higher-is-better and are not
comparable across modes.

With an SDK embedder:

```python
from strands.storage.embeddings.bedrock import BedrockEmbedder

storage = WeaviateStorage(client, embedder=BedrockEmbedder())
```

With a Weaviate vectorizer, create the collection from the backend's property definitions and
vectorize only `text`:

```python
from weaviate.classes.config import Configure

client.collections.create(
    "StrandsStorage",
    properties=WeaviateStorage.collection_properties(),
    vector_config=Configure.Vectors.text2vec_weaviate(
        source_properties=["text"], vectorize_collection_name=False
    ),
)
storage = WeaviateStorage(client)
```

## What gets indexed

A value is searchable when it is non-empty UTF-8 text without NUL bytes. Everything else
(images, framed context-offloader payloads) is stored and readable but never embedded.

Session snapshots are JSON text, so a storage shared by sessions and memory would embed every
snapshot on every turn. Restrict indexing to the keys you search:

```python
storage = WeaviateStorage(client, embedder=embedder, index_prefixes=["memory/"])
```

## Configuration

| Argument | Default | Purpose |
|---|---|---|
| `client` | required | Connected synchronous `weaviate.WeaviateClient` |
| `collection_name` | `"StrandsStorage"` | Collection that holds the objects |
| `prefix` | `""` | Key prefix for everything this storage reads and writes |
| `embedder` | `None` | SDK `Embedder` used for values and queries |
| `search_mode` | `None` (automatic) | `"hybrid"`, `"vector"` or `"bm25"` |
| `alpha` | `0.5` | Hybrid weighting |
| `max_results` | `10` | Results returned by `search()` |
| `index_prefixes` | `None` (all text) | Key prefixes, relative to `prefix`, to make searchable |
| `max_indexed_chars` | `8000` | Longest text indexed per value |
| `create_collection` | `True` | Create the collection when missing |
| `vector_name` | `None` | Named vector to write and search |
| `tenant` | `None` | Tenant to read and write, in a multi-tenant collection |

`storage.namespace("memory")` returns a view scoped to a key prefix that shares the client
and collection. Its searches are filtered by prefix inside Weaviate, so `max_results` applies
within the namespace.

## Multi-tenancy

Pass `tenant` to keep each tenant's keys, text and vectors in its own shard of a
[multi-tenant collection](https://docs.weaviate.io/weaviate/manage-collections/multi-tenancy):

```python
storage = WeaviateStorage(client, collection_name="AgentData", tenant="acme")

await storage.write("memory/notes.md", b"Acme prefers concise answers.")
await storage.with_tenant("globex").read("memory/notes.md")   # None: a different shard
```

`with_tenant(name)` returns a view for another tenant that shares the client, so one storage
can serve every user: `FileMemoryStore(name="user", storage=storage.with_tenant(user_id))`.
`namespace()` still scopes by key prefix inside a tenant.

- **Tenants are created by data, not by access.** The first value written to a tenant creates
  it. `read`, `list`, `search` and `delete` on a tenant that does not exist return `None`,
  `[]`, `[]` and nothing, without creating it; so do `with_tenant()` and writes that fail
  before reaching Weaviate (invalid key, embedder error). The one exception: if Weaviate
  itself rejects the first object written to a new tenant, the tenant is left behind empty.
- **Tenants are never deleted.** Removing the last key leaves an empty tenant. Deactivating,
  offloading and deleting tenants is done with the Weaviate client.
- **Multi-tenancy is fixed when the collection is created.** The storage enables it when it
  creates the collection for a storage that has a `tenant`. A single-tenant collection cannot
  be converted: use a new `collection_name`. Mixing the two raises `StorageError`.
- **Tenant names** are 1 to 64 letters, digits, `_` or `-`.

To attach a Weaviate vectorizer, create the multi-tenant collection yourself with automatic
tenant creation enabled:

```python
client.collections.create(
    "AgentData",
    properties=WeaviateStorage.collection_properties(),
    vector_config=Configure.Vectors.text2vec_weaviate(
        source_properties=["text"], vectorize_collection_name=False
    ),
    multi_tenancy_config=Configure.multi_tenancy(
        enabled=True, auto_tenant_creation=True, auto_tenant_activation=True
    ),
)
```

## Telemetry

`WeaviateStorage` adds an `X-Weaviate-Client-Integration: strands-python/<version>` header to
the client's requests, so Weaviate's usage telemetry can count this integration. It carries only
the integration name and the installed `strands-weaviate` version. Weaviate servers report it
only when their telemetry is enabled; set `DISABLE_TELEMETRY=true` on the server to turn that off.

## Limits

- **Synchronous client only.** The SDK's synchronous entry points run storage calls on a new
  event loop per call, which an async client cannot span. Passing a `WeaviateAsyncClient`
  raises `StorageError`.
- **Value size.** A value travels base64-encoded in a single gRPC message. Against a default
  Weaviate 1.40 server, 64 MB values stored and read back and a 90 MB value was rejected.
  Larger writes raise `StorageError`. Use an object store for media-heavy sessions.
- **One vector per key.** Values are not chunked. Text beyond `max_indexed_chars` is readable
  but not searchable.
- **BM25 is lexical.** Without a vector source, `drink` does not match `drinks`.
- **Listing.** `list(prefix)` is one filtered query when fewer than 1000 keys match, and a
  scan of the whole collection (or of the tenant's shard) otherwise.
- **Namespaces are not an isolation boundary.** They share one index. Use `tenant` to give
  each tenant its own shard; every view still shares one credential.
- **Replicated clusters.** The SDK expects read-after-write; this backend uses the client's
  default consistency level.
- **Concurrent memory writers.** `FileMemoryStore.add` merges in-process only, so two
  processes appending to the same entry can lose an update (true of every backend).

## Development

```bash
pip install -e ".[dev]"
hatch run prepare      # format, lint, type-check, unit tests
```

The unit tests run against an in-memory fake. The same tests run against a real Weaviate with
the `integ` marker:

```bash
docker compose -f ../docker-compose.yml up -d
hatch run test-integ
python examples/memory.py
```

Set `WEAVIATE_HTTP_PORT` and `WEAVIATE_GRPC_PORT` when Weaviate is not on 8080/50051.

## Publishing

Not published to PyPI yet. When it is: create a GitHub release tagged `python-v<version>` (for
example `python-v0.1.0`). The `publish-python.yml` workflow runs the checks, builds, and
publishes through PyPI trusted publishing (environment `pypi`), which must be configured on
PyPI first.

## License

BSD 3-Clause. See [LICENSE](../LICENSE).
