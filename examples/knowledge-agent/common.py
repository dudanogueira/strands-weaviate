"""Settings and Weaviate setup shared by the example scripts."""

import os

import weaviate
from weaviate.classes.config import Configure

from strands_weaviate import WeaviateStorage

# Unset by default: one plain collection. Set TENANT to give each tenant its own shard, with
# its own handbook and chat sessions.
TENANT = os.environ.get("TENANT") or None
# Multi-tenancy is fixed when a collection is created, so tenants live in a separate collection.
COLLECTION = os.environ.get("WEAVIATE_COLLECTION") or ("KnowledgeAgentTenants" if TENANT else "KnowledgeAgent")
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
    """Create the collection once, with Weaviate embedding the text via Ollama.

    It is multi-tenant only when TENANT is set. Tenants are not created here: each one is
    created by its first ingest.
    """
    multi_tenant = TENANT is not None
    if client.collections.exists(COLLECTION):
        existing = client.collections.get(COLLECTION).config.get().multi_tenancy_config.enabled
        if existing != multi_tenant:
            kind = "multi-tenant" if existing else "single-tenant"
            usage = f"with TENANT={TENANT}" if TENANT else "without a TENANT"
            raise SystemExit(
                f"Collection {COLLECTION} already exists as {kind}, so it cannot be used {usage}. "
                "Set WEAVIATE_COLLECTION to another name, or remove the example's data with "
                "`docker compose down -v`."
            )
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
            enabled=multi_tenant, auto_tenant_creation=multi_tenant, auto_tenant_activation=multi_tenant
        ),
    )


def where() -> str:
    """Describe where the scripts read and write, for their output."""
    return f"tenant '{TENANT}' of {COLLECTION}" if TENANT else COLLECTION


def open_storage(client: weaviate.WeaviateClient, tenant: str | None = TENANT) -> WeaviateStorage:
    """Return the storage, bound to the tenant when one is set.

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
