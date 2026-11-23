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

`storage.namespace('tenant-a')` returns a view scoped to a key prefix that shares the client
and collection. Its searches are filtered by prefix inside Weaviate, so `maxResults` applies
within the namespace.

## Python parity

The [Python package](../python/README.md) uses the same collection schema and derives the same
object id for a key, so both SDKs can read and write one collection. Option names are the
snake_case forms of the ones above (`collection_name`, `search_mode`, `index_prefixes`, ...).

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
  scan of the whole collection otherwise.
- **Namespaces are not a security boundary.** Every view shares one collection and credential.
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
