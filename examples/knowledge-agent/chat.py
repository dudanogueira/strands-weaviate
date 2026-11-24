"""Chat with an agent that answers from the ingested content.

    python chat.py                          # interactive
    python chat.py "how long does cold brew keep?"
    python chat.py --debug "how much coffee for 600 grams of water, pour-over?"

The agent has two tools:

- ``search_handbook`` searches the ingested sections in Weaviate (a hybrid keyword + vector
  search through ``WeaviateStorage.search``);
- ``brew_ratio`` is an ordinary function (see tools.py) that does not use Weaviate.

Weaviate also stores the conversation: the session manager saves it after every turn, so
running the script again with the same SESSION_ID continues where you left off.

``--debug`` prints what happens around each turn: the setup, every message the turn added
(tool calls, their inputs and their full results, and the answer), why the model stopped,
token usage, and what the session holds afterwards.
"""

import argparse
import asyncio
import json
import logging
import os
import sys
import warnings

from strands import Agent, tool
from strands.models.ollama import OllamaModel
from strands.session import SnapshotSessionManager

from common import CHAT_MODEL, COLLECTION, KNOWLEDGE_BASE, OLLAMA_URL, TENANT, connect, open_storage
from tools import brew_ratio

# Importing weaviate turns every warning category on, and the SDK's Ollama provider opens an
# HTTP client per model call without closing it, so Python would report each of those sockets
# as unclosed when the script exits. The OS closes them at exit; hide the noise.
warnings.simplefilter("ignore", ResourceWarning)

SESSION_ID = os.environ.get("SESSION_ID", "demo")

SYSTEM_PROMPT = (
    "You are the support assistant for Tidewater Coffee Roasters. You have two tools. "
    "Use search_handbook to look up anything about brewing, orders, shipping, returns or "
    "subscriptions before you answer. Use brew_ratio only when the customer asks how many "
    "grams of coffee to use for a specific amount of water. After a tool returns, answer the "
    "customer in plain sentences using only what the tool returned. If the handbook does not "
    "contain the answer, say you do not know."
)


def dim(text: str) -> str:
    """Grey out secondary output when writing to a terminal."""
    return f"\033[2m{text}\033[0m" if sys.stdout.isatty() else text


def show_progress(**event) -> None:
    """Stream the answer as it is generated, and show each tool call with its arguments."""
    if text := event.get("data"):
        print(text, end="", flush=True)
    message = event.get("message")
    if message and message["role"] == "assistant":
        if any("text" in block for block in message["content"]):
            print()  # ends the line of text streamed above
        for block in message["content"]:
            if "toolUse" in block:
                tool_use = block["toolUse"]
                arguments = ", ".join(f"{name}={json.dumps(value)}" for name, value in tool_use["input"].items())
                print(dim(f"  -> {tool_use['name']}({arguments})"))


def debug(text: str = "") -> None:
    """Print a debug line to stderr, so it never mixes into piped answers."""
    print(f"\033[2m[debug] {text}\033[0m" if sys.stderr.isatty() else f"[debug] {text}", file=sys.stderr)


def describe_block(block: dict) -> str:
    """Render one content block of a message on a single line."""
    if "text" in block:
        return f"text: {block['text'].strip()}"
    if "toolUse" in block:
        tool_use = block["toolUse"]
        return f"tool call: {tool_use['name']}({json.dumps(tool_use['input'])})"
    if "toolResult" in block:
        result = block["toolResult"]
        content = " ".join(item.get("text") or json.dumps(item, default=str) for item in result["content"])
        return f"tool result [{result['status']}]: {' '.join(content.split())}"
    return f"{next(iter(block))}: {json.dumps(block, default=str)}"


class Chat:
    """The agent plus the pieces the debug output reports on."""

    def __init__(self, client, show_debug: bool) -> None:
        self.show_debug = show_debug
        self.storage = open_storage(client)
        knowledge = self.storage.namespace(f"memory/{KNOWLEDGE_BASE}")

        @tool
        async def search_handbook(query: str) -> str:
            """Search the Tidewater Coffee Roasters handbook.

            Covers brewing instructions, storing beans, shipping, returns, damaged parcels and
            subscriptions. Returns the most relevant handbook sections.

            Args:
                query: The customer's question, or the topic to look up.
            """
            # The two best sections are enough context, and keep a small model on topic.
            results = (await knowledge.search(query))[:2]
            if not results:
                return "The handbook has no entry about this."
            return "\n\n---\n\n".join((result.data or b"").decode("utf-8").strip() for result in results)

        # The handbook is a tool rather than text injected into the prompt. Small local models
        # call a tool on almost every turn once any tool exists, so each kind of question needs
        # a right tool to call, and a tool result stays in the conversation for the model call
        # that writes the answer.
        self.agent = Agent(
            model=OllamaModel(OLLAMA_URL, model_id=CHAT_MODEL),
            system_prompt=SYSTEM_PROMPT,
            tools=[search_handbook, brew_ratio],
            callback_handler=show_progress,
            session_manager=SnapshotSessionManager(session_id=SESSION_ID, storage=self.storage),
        )
        self.usage = {"inputTokens": 0, "outputTokens": 0, "totalTokens": 0}
        if show_debug:
            debug(f"weaviate collection={COLLECTION} tenant={TENANT} session={SESSION_ID}")
            debug(f"model={CHAT_MODEL} at {OLLAMA_URL}")
            debug(f"tools={self.agent.tool_names}")
            debug(f"system prompt: {SYSTEM_PROMPT}")
            debug(f"session restored with {len(self.agent.messages)} earlier messages")

    def ask(self, question: str) -> None:
        before = len(self.agent.messages)
        result = self.agent(question)
        if self.show_debug:
            self._debug_turn(result, before)

    def _debug_turn(self, result, before: int) -> None:
        """Show what the turn added to the conversation and what it cost."""
        new_messages = self.agent.messages[before:]
        debug(f"this turn added {len(new_messages)} messages:")
        for message in new_messages:
            for block in message["content"]:
                debug(f"  {message['role']:<9} {describe_block(block)}")

        usage = result.metrics.accumulated_usage
        spent = {name: usage.get(name, 0) - self.usage[name] for name in self.usage}
        self.usage = {name: usage.get(name, 0) for name in self.usage}
        debug(
            f"stop_reason={result.stop_reason} tokens in={spent['inputTokens']} "
            f"out={spent['outputTokens']} total={spent['totalTokens']}"
        )

        keys = asyncio.run(self.storage.list(f"session/{SESSION_ID}/"))
        debug(f"session now holds {len(self.agent.messages)} messages, saved under {len(keys)} keys:")
        for key in keys:
            debug(f"  {key}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Chat with the handbook agent.")
    parser.add_argument("question", nargs="*", help="ask one question and exit; omit for an interactive chat")
    parser.add_argument("--debug", action="store_true", help="print messages, tool calls, results and usage")
    args = parser.parse_args()
    if args.debug:
        # The storage backend's own debug lines (collection setup, listings, unindexed values).
        logging.basicConfig(format="[debug] %(name)s | %(message)s", stream=sys.stderr)
        logging.getLogger("strands_weaviate").setLevel(logging.DEBUG)

    client = connect()
    try:
        chat = Chat(client, args.debug)
        if args.question:
            chat.ask(" ".join(args.question))
            return
        print(f"Tenant '{TENANT}'. Ask about brewing, orders or subscriptions; an empty line quits.")
        while question := input("\n> ").strip():
            chat.ask(question)
    except (EOFError, KeyboardInterrupt):
        print()
    finally:
        client.close()


if __name__ == "__main__":
    main()
