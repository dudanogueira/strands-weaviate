# Knowledge agent example

A support agent that answers from a small handbook stored in Weaviate, running entirely on
your machine with no API keys.

- **Weaviate** stores the handbook, the agent's chat sessions, and does the search.
- **Ollama** provides the embedding model (called by Weaviate) and the chat model (called by
  the agent).
- **`strands-weaviate`** connects the two to a [Strands](https://strandsagents.com) agent.

```
content/*.md ──ingest.py──▶ Weaviate ◀──search── MemoryManager ──▶ Agent ──▶ Ollama chat model
                              │  ▲
                 embeds via Ollama  └── SnapshotSessionManager saves the conversation
```

## Run it

You need Docker and Python 3.10+.

**1. Start Weaviate and Ollama.** The first start downloads the two models (about 2.3 GB).

```bash
cd examples/knowledge-agent
docker compose up -d
docker compose logs -f ollama-pull   # wait until it exits
```

**2. Install the example.**

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
```

**3. Ingest the handbook.** Each `##` section of the files in `content/` becomes one entry.

```bash
python ingest.py
```

**4. Search it**, to see what the agent will retrieve:

```bash
python search.py "can I send a bag back?"
```

```
1.00  orders--returns.md
      # Orders and returns: Returns
0.68  orders--damaged-parcels.md
      # Orders and returns: Damaged parcels
```

**5. Chat.**

```bash
python chat.py "How long does cold brew concentrate keep?"
python chat.py          # interactive
```

The conversation is saved in Weaviate, so the next run continues it. Set `SESSION_ID` to start
a separate conversation.

To ingest your own content, point `ingest.py` at a folder of markdown files:
`python ingest.py path/to/docs`.

## If Ollama already runs on your machine

Start only Weaviate, and tell it where to find your Ollama:

```bash
docker compose up -d weaviate
export WEAVIATE_OLLAMA_URL=http://host.docker.internal:11434
ollama pull nomic-embed-text && ollama pull llama3.2
```

`host.docker.internal` works on Docker Desktop and Rancher Desktop. On Linux, use the Ollama
service from the compose file instead, or add
`extra_hosts: ["host.docker.internal:host-gateway"]` to the `weaviate` service.

`WEAVIATE_OLLAMA_URL` is stored in the collection when it is created. To change it later,
recreate the stack with `docker compose down -v`.

## Tenants

The collection is multi-tenant. Each tenant has its own handbook and its own chat sessions in
a separate shard:

```bash
TENANT=acme python ingest.py path/to/acme-docs
TENANT=acme python chat.py
```

A tenant is created by its first ingest. Searching or chatting as a tenant with no data
returns nothing and does not create it.

## Settings

All optional, read from the environment:

| Variable | Default | Purpose |
|---|---|---|
| `TENANT` | `default` | Tenant to ingest into and chat as |
| `SESSION_ID` | `demo` | Conversation to continue in `chat.py` |
| `CHAT_MODEL` | `llama3.2` | Ollama model the agent uses |
| `EMBED_MODEL` | `nomic-embed-text` | Ollama model Weaviate embeds with (fixed at collection creation) |
| `OLLAMA_URL` | `http://localhost:11434` | Ollama as the scripts reach it |
| `WEAVIATE_OLLAMA_URL` | `http://ollama:11434` | Ollama as the Weaviate container reaches it |
| `WEAVIATE_HTTP_PORT`, `WEAVIATE_GRPC_PORT` | `8080`, `50051` | Weaviate ports, for compose and the scripts |
| `OLLAMA_PORT` | `11434` | Host port of the compose Ollama |
| `WEAVIATE_COLLECTION` | `KnowledgeAgent` | Collection name |

## How it uses the integration

- [`common.py`](common.py) creates the collection from `WeaviateStorage.collection_properties()`
  with Weaviate's Ollama vectorizer and multi-tenancy, and opens a `WeaviateStorage` for the
  tenant. `index_prefixes=["memory/"]` keeps session snapshots out of the search index.
- [`ingest.py`](ingest.py) writes each section with `storage.write()` under `memory/handbook/`.
- [`chat.py`](chat.py) gives that same storage to a `FileMemoryStore` (knowledge) and a
  `SnapshotSessionManager` (conversation). The `MemoryManager` injects the best-matching
  sections into each prompt.

## Clean up

```bash
docker compose down -v    # removes Weaviate's data and the downloaded models
```
