# Knowledge agent example

A support agent that answers from a small handbook stored in Weaviate, running entirely on
your machine with no API keys.

- **Weaviate** stores the handbook, the agent's chat sessions, and does the search.
- **Ollama** provides the embedding model (called by Weaviate) and the chat model (called by
  the agent).
- **`strands-weaviate`** connects the two to a [Strands](https://strandsagents.com) agent.

```
content/*.md ──ingest.py──▶ Weaviate ◀── search_handbook tool ──┐
                              │  ▲                              Agent ──▶ Ollama chat model
                 embeds via Ollama  └── saves the conversation ──┘   └── brew_ratio tool
```

The agent has two tools: `search_handbook`, a hybrid search over the ingested content in
Weaviate, and `brew_ratio`, an ordinary function that works out how much coffee to use for an
amount of water.

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
python chat.py "How much coffee do I need for 600 grams of water with pour-over?"
python chat.py          # interactive
```

The first question is answered from the handbook, the second with the calculator. Each tool
call is shown above the answer:

```
  -> brew_ratio(method="pour-over", water_grams=600)
For a pour-over, use 36 grams of coffee for 600 grams of water.
```

The conversation is saved in Weaviate, so the next run continues it. Set `SESSION_ID` to start
a separate conversation.

**6. See what happened.** Add `--debug` to print, for each turn, the setup, every tool call
with its input and full result, the answer, why the model stopped, token usage, and what the
session holds in Weaviate:

```bash
python chat.py --debug "And how long should it steep?"
```

```
[debug] weaviate: KnowledgeAgent, session=demo
[debug] tools=['search_handbook', 'brew_ratio']
[debug] session restored with 4 earlier messages
[debug] this turn added 4 messages:
[debug]   user      text: And how long should it steep?
[debug]   assistant tool call: search_handbook({"query": "French press steeping time"})
[debug]   user      tool result [success]: # Brewing guide: French press Use 30 grams of ...
[debug]   assistant text: The recommended steeping time for a French press is 4 minutes. ...
[debug] stop_reason=end_turn tokens in=1184 out=65 total=1249
[debug] session now holds 8 messages, saved under 1 keys:
```

Debug lines go to stderr, so `python chat.py --debug "..." 2>debug.log` keeps the answer clean.

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

By default everything goes into one plain collection, `KnowledgeAgent`. Set `TENANT` to give
each tenant its own handbook and chat sessions, in a separate shard:

```bash
TENANT=acme python ingest.py path/to/acme-docs
TENANT=acme python chat.py
```

Tenants live in a separate multi-tenant collection, `KnowledgeAgentTenants`, because Weaviate
fixes multi-tenancy when a collection is created. A tenant is created by its first ingest.
Searching or chatting as a tenant with no data returns nothing and does not create it.

If a collection exists in the other mode (for example `KnowledgeAgent` from an earlier
version of this example, which was always multi-tenant), the scripts say so and stop. Set
`WEAVIATE_COLLECTION` to a new name, or run `docker compose down -v` to start over.

## Settings

All optional, read from the environment:

| Variable | Default | Purpose |
|---|---|---|
| `TENANT` | unset | Tenant to ingest into and chat as; unset uses a single-tenant collection |
| `SESSION_ID` | `demo` | Conversation to continue in `chat.py` |
| `CHAT_MODEL` | `llama3.2` | Ollama model the agent uses |
| `EMBED_MODEL` | `nomic-embed-text` | Ollama model Weaviate embeds with (fixed at collection creation) |
| `OLLAMA_URL` | `http://localhost:11434` | Ollama as the scripts reach it |
| `WEAVIATE_OLLAMA_URL` | `http://ollama:11434` | Ollama as the Weaviate container reaches it |
| `WEAVIATE_HTTP_PORT`, `WEAVIATE_GRPC_PORT` | `8080`, `50051` | Weaviate ports, for compose and the scripts |
| `OLLAMA_PORT` | `11434` | Host port of the compose Ollama |
| `WEAVIATE_COLLECTION` | `KnowledgeAgent`, or `KnowledgeAgentTenants` with `TENANT` | Collection name |
| `DISABLE_TELEMETRY` | `false` | Set to `true` to stop the compose Weaviate sending usage telemetry |

## How it uses the integration

- [`common.py`](common.py) creates the collection from `WeaviateStorage.collection_properties()`
  with Weaviate's Ollama vectorizer (and multi-tenancy when `TENANT` is set), and opens a
  `WeaviateStorage`. `index_prefixes=["memory/"]` keeps session snapshots out of the search
  index.
- [`ingest.py`](ingest.py) writes each section with `storage.write()` under `memory/handbook/`.
- [`chat.py`](chat.py) wraps `storage.search()` in the `search_handbook` tool and gives the
  same storage to a `SnapshotSessionManager` for the conversation.
- [`tools.py`](tools.py) holds `brew_ratio`, a plain `@tool` function with no Weaviate in it.

## About small local models

The default chat model, `llama3.2`, is a 3B model chosen because it is a small download. Two
things follow from that:

- Local models call a tool on almost every turn once any tool is available. That is why the
  handbook is a tool here instead of text injected into the prompt by a `MemoryManager`: with
  injection plus a calculator tool, the model called the calculator on unrelated questions
  and then answered without the handbook.
- Answers can still be imprecise or padded. `--debug` shows whether the right section was
  retrieved, which is usually the case; a larger model (`CHAT_MODEL=llama3.1`, or any other
  Strands model provider in `chat.py`) improves the wording.

## Clean up

```bash
docker compose down -v    # removes Weaviate's data and the downloaded models
```
