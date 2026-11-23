"""Store and recall agent memories in Weaviate.

Start Weaviate first (``docker compose up -d`` from the repository root), then run::

    python examples/memory.py

No model or API key is needed: the memories are added directly, and search is BM25. Pass an
``embedder`` to ``WeaviateStorage`` to get hybrid (BM25 plus vector) search instead.
"""

import asyncio
import os

import weaviate
from strands.memory import MemoryManager
from strands.vended_memory_stores import FileMemoryStore

from strands_weaviate import WeaviateStorage


async def main() -> None:
    client = weaviate.connect_to_local(
        port=int(os.environ.get("WEAVIATE_HTTP_PORT", "8080")),
        grpc_port=int(os.environ.get("WEAVIATE_GRPC_PORT", "50051")),
    )
    try:
        storage = WeaviateStorage(client, collection_name="StrandsStorageExample")
        store = FileMemoryStore(name="user", storage=storage)
        memory = MemoryManager(stores=[store])

        await store.add("# Drinks\nThe user drinks oat milk flat whites.")
        await store.add("# Editor\nThe user writes Python in Neovim.")

        for entry in await memory.search("which drinks does the user like"):
            print(f"{entry.metadata['path']} (score {entry.metadata['score']:.2f})\n{entry.content}\n")

        print("keys in Weaviate:", await storage.list())
    finally:
        client.collections.delete("StrandsStorageExample")
        client.close()


if __name__ == "__main__":
    asyncio.run(main())
