"""Chat with an agent that answers from the ingested content.

    python chat.py                          # interactive
    python chat.py "how long does cold brew keep?"

Weaviate backs two things here, both in the tenant's shard:

- the knowledge: before each model call, the memory manager searches the ingested sections
  and injects the best matches into the prompt;
- the conversation: the session manager saves it after every turn, so running the script
  again with the same SESSION_ID continues where you left off.
"""

import os
import sys

from strands import Agent
from strands.memory import MemoryManager
from strands.models.ollama import OllamaModel
from strands.session import SnapshotSessionManager
from strands.vended_memory_stores import FileMemoryStore

from common import CHAT_MODEL, KNOWLEDGE_BASE, OLLAMA_URL, TENANT, connect, open_storage

SYSTEM_PROMPT = (
    "You are the support assistant for Tidewater Coffee Roasters. Answer from the handbook "
    "entries provided to you. If they do not contain the answer, say you do not know."
)


def build_agent(client) -> Agent:
    storage = open_storage(client)
    handbook = FileMemoryStore(
        name=KNOWLEDGE_BASE,
        description="Tidewater Coffee Roasters handbook",
        storage=storage,
        writable=False,
    )
    return Agent(
        model=OllamaModel(OLLAMA_URL, model_id=CHAT_MODEL),
        system_prompt=SYSTEM_PROMPT,
        # Injection puts the retrieved entries in the prompt on every turn, which does not
        # depend on a small local model deciding to call a search tool.
        memory_manager=MemoryManager(stores=[handbook], search_tool_config=False),
        session_manager=SnapshotSessionManager(session_id=os.environ.get("SESSION_ID", "demo"), storage=storage),
    )


def main() -> None:
    client = connect()
    try:
        agent = build_agent(client)
        if len(sys.argv) > 1:
            agent(" ".join(sys.argv[1:]))
            print()
            return
        print(f"Tenant '{TENANT}'. Ask about brewing, orders or subscriptions; an empty line quits.")
        while question := input("\n> ").strip():
            agent(question)
            print()
    except (EOFError, KeyboardInterrupt):
        print()
    finally:
        client.close()


if __name__ == "__main__":
    main()
