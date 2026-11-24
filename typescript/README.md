# strands-weaviate (TypeScript)

A [Weaviate](https://weaviate.io) `Storage` backend for
[Strands Agents](https://strandsagents.com). One collection holds session snapshots, offloaded
context and agent memory, and `search()` runs a Weaviate hybrid (BM25 + vector) query instead
of a keyword scan.

Each key is one Weaviate object carrying the raw bytes, the searchable text and the vector, so
an overwrite or delete can never leave a stale search entry behind.

## Install

```bash
npm install strands-weaviate @strands-agents/sdk weaviate-client
```

Requires Node.js 22+, `@strands-agents/sdk` 1.25+ and `weaviate-client` 3.15.3+ (both peer
dependencies).

## Quickstart

```ts
import weaviate from 'weaviate-client'
import { WeaviateStorage } from 'strands-weaviate'

const client = await weaviate.connectToLocal() // or weaviate.connectToWeaviateCloud(...)
const storage = new WeaviateStorage(client)

await storage.write('memory/notes.md', new TextEncoder().encode('The user prefers concise answers.'))
await storage.read('memory/notes.md') // Uint8Array
await storage.list('memory/') // ['memory/notes.md']
await storage.search('concise answers') // [{ key: 'memory/notes.md', score, data }]
```

You own the client: `WeaviateStorage` never connects or closes it. The collection
(`StrandsStorage` by default) is created on first use.

### Agent memory

```ts
import { Agent, MemoryManager } from '@strands-agents/sdk'
import { FileMemoryStore } from '@strands-agents/sdk/vended-memory-stores/file-memory-store'

const memoryStore = new FileMemoryStore({ name: 'user', storage })
const agent = new Agent({ memoryManager: new MemoryManager({ stores: [memoryStore] }) })
```

Recall goes through `storage.search()`, and results carry their content, so the store does not
re-read each hit.

### Sessions

```ts
import { Agent, SessionManager } from '@strands-agents/sdk'

const agent = new Agent({ sessionManager: new SessionManager({ sessionId: 'chat-42', storage }) })
```

## Search modes

| Vectors come from | How to set it up | Default search |
|---|---|---|
| Nothing | `new WeaviateStorage(client)` | BM25 |
| An SDK embedder | `new WeaviateStorage(client, { embedder })` | Hybrid |
| A Weaviate vectorizer | Create the collection yourself (below) | Hybrid |

Set `searchMode` to `'hybrid'`, `'vector'` or `'bm25'` to override, and `alpha` (0 = BM25 only,
1 = vector only) to weight hybrid search. Scores are higher-is-better and are not comparable
across modes.

With an SDK embedder:

```ts
import { BedrockEmbedder } from '@strands-agents/sdk/storage/embeddings/bedrock'

const storage = new WeaviateStorage(client, { embedder: new BedrockEmbedder() })
```

With a Weaviate vectorizer, create the collection from the backend's property definitions and
vectorize only `text`:

```ts
import { configure } from 'weaviate-client'

await client.collections.create({
  name: 'StrandsStorage',
  properties: WeaviateStorage.collectionProperties(),
  vectorizers: configure.vectors.text2VecWeaviate({
    sourceProperties: ['text'],
    vectorizeCollectionName: false,
  }),
})
const storage = new WeaviateStorage(client)
```

## What gets indexed

A value is searchable when it is non-empty UTF-8 text without NUL bytes. Everything else
(images, framed context-offloader payloads) is stored and readable but never embedded.

Session snapshots are JSON text, so a storage shared by sessions and memory would embed every
snapshot on every turn. Restrict indexing to the keys you search:

```ts
const storage = new WeaviateStorage(client, { embedder, indexPrefixes: ['memory/'] })
```

## Configuration

`new WeaviateStorage(client, config?)`

| Option | Default | Purpose |
|---|---|---|
| `collectionName` | `'StrandsStorage'` | Collection that holds the objects |
| `prefix` | `''` | Key prefix for everything this storage reads and writes |
| `embedder` | none | SDK `Embedder` used for values and queries |
| `searchMode` | automatic | `'hybrid'`, `'vector'` or `'bm25'` |
| `alpha` | `0.5` | Hybrid weighting |
| `maxResults` | `10` | Results returned by `search()` |
| `indexPrefixes` | all text | Key prefixes, relative to `prefix`, to make searchable |
| `maxIndexedChars` | `8000` | Longest text indexed per value |
| `createCollection` | `true` | Create the collection when missing |
| `vectorName` | none | Named vector to write and search |
| `tenant` | none | Tenant to read and write, in a multi-tenant collection |

`storage.namespace('memory')` returns a view scoped to a key prefix that shares the client
and collection. Its searches are filtered by prefix inside Weaviate, so `maxResults` applies
within the namespace.

## Multi-tenancy

Pass `tenant` to keep each tenant's keys, text and vectors in its own shard of a
[multi-tenant collection](https://docs.weaviate.io/weaviate/manage-collections/multi-tenancy):

```ts
const storage = new WeaviateStorage(client, { collectionName: 'AgentData', tenant: 'acme' })

await storage.write('memory/notes.md', new TextEncoder().encode('Acme prefers concise answers.'))
await storage.withTenant('globex').read('memory/notes.md') // null: a different shard
```

`withTenant(name)` returns a view for another tenant that shares the client, so one storage can
serve every user: `new FileMemoryStore({ name: 'user', storage: storage.withTenant(userId) })`.
`namespace()` still scopes by key prefix inside a tenant.

- **Tenants are created by data, not by access.** The first value written to a tenant creates
  it. `read`, `list`, `search` and `delete` on a tenant that does not exist return `null`,
  `[]`, `[]` and nothing, without creating it; so do `withTenant()` and writes that fail before
  reaching Weaviate (invalid key, embedder error). The one exception: if Weaviate itself
  rejects the first object written to a new tenant, the tenant is left behind empty.
- **Tenants are never deleted.** Removing the last key leaves an empty tenant. Deactivating,
  offloading and deleting tenants is done with the Weaviate client.
- **Multi-tenancy is fixed when the collection is created.** The storage enables it when it
  creates the collection for a storage that has a `tenant`. A single-tenant collection cannot
  be converted: use a new `collectionName`. Mixing the two throws `StorageError`.
- **Tenant names** are 1 to 64 letters, digits, `_` or `-`.

To attach a Weaviate vectorizer, create the multi-tenant collection yourself with automatic
tenant creation enabled:

```ts
await client.collections.create({
  name: 'AgentData',
  properties: WeaviateStorage.collectionProperties(),
  vectorizers: configure.vectors.text2VecWeaviate({
    sourceProperties: ['text'],
    vectorizeCollectionName: false,
  }),
  multiTenancy: configure.multiTenancy({
    enabled: true,
    autoTenantCreation: true,
    autoTenantActivation: true,
  }),
})
```

## Python parity

The [Python package](../python/README.md) uses the same collection schema and derives the same
object id for a key, so both SDKs can read and write one collection. Option names are the
snake_case forms of the ones above (`collection_name`, `search_mode`, `index_prefixes`, ...).

## Telemetry

`WeaviateStorage` adds an `X-Weaviate-Client-Integration: strands-typescript/<version>` header
to the client's requests, so Weaviate's usage telemetry can count this integration. It carries
only the integration name and the installed `strands-weaviate` version. Weaviate servers report
it only when their telemetry is enabled; set `DISABLE_TELEMETRY=true` on the server to turn
that off.

## Limits

- **Node.js only.** The Weaviate client uses gRPC and does not run in the browser.
- **Value size.** A value travels base64-encoded in a single gRPC message, so very large
  values are rejected by Weaviate (measured with the Python package against a default
  Weaviate 1.40 server: 64 MB stored, 90 MB rejected). Use an object store for media-heavy
  sessions.
- **One vector per key.** Values are not chunked. Text beyond `maxIndexedChars` is readable
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
npm install
npm run check        # format, lint, type-check, unit tests
```

The unit tests run against an in-memory fake. The same tests also run against a real Weaviate:

```bash
docker compose -f ../docker-compose.yml up -d
npm run test:integ
```

Set `WEAVIATE_HTTP_PORT` and `WEAVIATE_GRPC_PORT` when Weaviate is not on 8080/50051.

## Publishing

Bump `version` in `package.json`, then create a GitHub release tagged `typescript-v<version>`.
The `publish-typescript.yml` workflow runs the checks, builds, and publishes with the
`NPM_TOKEN` secret of the `npm` environment, which must be configured first.

## License

Apache-2.0. See [LICENSE](LICENSE).
