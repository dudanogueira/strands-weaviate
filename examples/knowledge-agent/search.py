"""Search the ingested content directly, without a chat model.

    python search.py "can I send a bag back?"

Shows what the agent's memory retrieves: a Weaviate hybrid (keyword + vector) search.
"""

import asyncio
import sys

from common import KNOWLEDGE_BASE, connect, open_storage, where


async def main(query: str) -> None:
    client = connect()
    try:
        knowledge = open_storage(client).namespace(f"memory/{KNOWLEDGE_BASE}")
        results = await knowledge.search(query)
        if not results:
            print(f"No results in {where()}. Run ingest.py first.")
        for result in results:
            first_line = (result.data or b"").decode("utf-8").splitlines()[0]
            print(f"{result.score:.2f}  {result.key}\n      {first_line}")
    finally:
        client.close()


if __name__ == "__main__":
    asyncio.run(main(" ".join(sys.argv[1:]) or "how do I make cold brew?"))
