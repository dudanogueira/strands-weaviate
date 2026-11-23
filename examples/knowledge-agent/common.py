"""Settings and Weaviate setup shared by the example scripts."""

import os

import weaviate
from weaviate.classes.config import Configure

from strands_weaviate import WeaviateStorage

COLLECTION = os.environ.get("WEAVIATE_COLLECTION", "KnowledgeAgent")
# Every tenant gets its own shard: its own handbook, memories and chat sessions.
TENANT = os.environ.get("TENANT", "default")
# The name of the knowledge base; FileMemoryStore keeps it under the "memory/<name>/" prefix.
KNOWLEDGE_BASE = "handbook"

EMBED_MODEL = os.environ.get("EMBED_MODEL", "nomic-embed-text")
CHAT_MODEL = os.environ.get("CHAT_MODEL", "llama3.2")
# Ollama as the scripts reach it, and as the Weaviate container reaches it.
OLLAMA_URL = os.environ.get("OLLAMA_URL", "http://localhost:11434")
WEAVIATE_OLLAMA_URL = os.environ.get("WEAVIATE_OLLAMA_URL", "http://ollama:11434")


def connect() -> weaviate.WeaviateClient:
    """Connect to the Weaviate started by docker compose."""
    return weaviate.connect_to_local(
        port=int(os.environ.get("WEAVIATE_HTTP_PORT", "8080")),
        grpc_port=int(os.environ.get("WEAVIATE_GRPC_PORT", "50051")),
    )


def ensure_collection(client: weaviate.WeaviateClient) -> None:
    """Create the collection once: multi-tenant, with Weaviate embedding the text via Ollama."""
    if client.collections.exists(COLLECTION):
        return
    client.collections.create(
        COLLECTION,
        properties=WeaviateStorage.collection_properties(),
        vector_config=Configure.Vectors.text2vec_ollama(
            api_endpoint=WEAVIATE_OLLAMA_URL,
            model=EMBED_MODEL,
            source_properties=["text"],
            vectorize_collection_name=False,
        ),
        multi_tenancy_config=Configure.multi_tenancy(
            enabled=True, auto_tenant_creation=True, auto_tenant_activation=True
        ),
    )


def open_storage(client: weaviate.WeaviateClient, tenant: str = TENANT) -> WeaviateStorage:
    """Return the tenant's storage.

    Only keys under ``memory/`` are embedded and searchable, so chat session snapshots stored
    in the same collection are not sent to the embedding model.
    """
    ensure_collection(client)
    return WeaviateStorage(
        client,
        collection_name=COLLECTION,
        tenant=tenant,
        index_prefixes=["memory/"],
        max_results=4,
    )
