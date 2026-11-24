/**
 * Tags Weaviate connections so server telemetry can count usage of this integration.
 */

import { createRequire } from 'node:module'
import type { WeaviateClient } from 'weaviate-client'

export const INTEGRATION_HEADER = 'X-Weaviate-Client-Integration'
export const INTEGRATION_NAME = 'strands-typescript'

/**
 * Returns the installed strands-weaviate version, or `unknown` if it cannot be read.
 *
 * @returns The package version
 */
export function integrationVersion(): string {
  try {
    // package.json sits one level above both src/ and the built dist/.
    const { version } = createRequire(import.meta.url)('../package.json') as { version?: unknown }
    return typeof version === 'string' && version ? version : 'unknown'
  } catch {
    return 'unknown'
  }
}

/**
 * Adds the integration header to every HTTP and gRPC request the client makes.
 *
 * The TypeScript client has no integrations API, but `getConnectionDetails()` returns the
 * headers object the client spreads into each request, so adding a key to it tags all later
 * requests. Best effort: a client of another shape is left unchanged, and this never throws.
 *
 * @param client - The Weaviate client to tag
 */
export async function registerIntegration(client: WeaviateClient): Promise<void> {
  try {
    const { headers } = await client.getConnectionDetails()
    // Only the object form of the headers is spread into requests.
    if (headers && typeof headers === 'object' && !Array.isArray(headers)) {
      ;(headers as Record<string, string>)[INTEGRATION_HEADER] = `${INTEGRATION_NAME}/${integrationVersion()}`
    }
  } catch {
    // Telemetry must never break storage.
  }
}
