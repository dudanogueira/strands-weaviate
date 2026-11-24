"""Tags Weaviate connections so server telemetry can count usage of this integration."""

import importlib.metadata
import logging
from typing import Any

logger = logging.getLogger(__name__)

INTEGRATION_HEADER = "X-Weaviate-Client-Integration"
INTEGRATION_NAME = "strands-python"


def integration_version() -> str:
    """Return the installed strands-weaviate version, or ``unknown`` if it cannot be read."""
    try:
        return importlib.metadata.version("strands-weaviate")
    except Exception:
        return "unknown"


def register_integration(client: Any) -> None:
    """Add the integration header to every HTTP and gRPC request the client makes.

    Best effort: a client without the integrations API (older ``weaviate-client``
    releases, or a stand-in in tests) is left unchanged, and nothing here raises.

    Args:
        client: The Weaviate client to tag.
    """
    try:
        from pydantic import Field
        from weaviate.connect.integrations import _IntegrationConfig

        value = f"{INTEGRATION_NAME}/{integration_version()}"

        class _StrandsIntegration(_IntegrationConfig):
            integration: str = Field(default=value, serialization_alias=INTEGRATION_HEADER)

        client.integrations.configure(_StrandsIntegration())
    except Exception:
        logger.debug("header=<%s> | could not register the integration header", INTEGRATION_HEADER, exc_info=True)
