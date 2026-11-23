/**
 * Key normalization and object identity shared by every storage operation.
 */

import { Buffer } from 'node:buffer'
import { createHash } from 'node:crypto'
import { StorageError } from '@strands-agents/sdk'

// Shared with the Python package so both derive the same object id for a key.
const KEY_NAMESPACE = '13a087ca-fcce-5b2e-8a07-8379f81bd956'

/**
 * Validates and normalizes a storage key the way the SDK's built-in backends do.
 *
 * Collapses runs of '/', strips leading and trailing '/', and rejects empty keys,
 * backslashes, and '..' segments.
 *
 * @param key - The raw key to normalize
 * @returns The normalized key
 * @throws StorageError if the key is empty, contains backslashes, or contains a '..' segment
 */
export function normalizeKey(key: string): string {
  if (key.includes('\\')) {
    throw new StorageError(`Invalid storage key '${key}': backslashes are not allowed`)
  }
  const normalized = key.replace(/\/+/g, '/').replace(/^\/|\/$/g, '')
  if (!normalized) {
    throw new StorageError('Storage key must not be empty')
  }
  if (normalized.split('/').includes('..')) {
    throw new StorageError(`Invalid storage key '${key}': '..' path segments are not allowed`)
  }
  return normalized
}

/**
 * Normalizes a key prefix the way the SDK's built-in backends do.
 *
 * Collapses slash runs and strips leading slashes. An empty prefix is valid and a trailing
 * slash is preserved, because it is significant for prefix matching.
 *
 * @param prefix - The raw prefix to normalize
 * @returns The normalized prefix
 * @throws StorageError if the prefix contains backslashes or a '..' segment
 */
export function normalizePrefix(prefix: string): string {
  if (prefix.includes('\\')) {
    throw new StorageError(`Invalid storage prefix '${prefix}': backslashes are not allowed`)
  }
  const normalized = prefix.replace(/\/+/g, '/').replace(/^\//, '')
  if (normalized.split('/').includes('..')) {
    throw new StorageError(`Invalid storage prefix '${prefix}': '..' path segments are not allowed`)
  }
  return normalized
}

/**
 * Turns a user-supplied prefix into a storage scope: empty, or ending in exactly one '/'.
 *
 * @param prefix - The raw prefix
 * @returns The scope prefix
 * @throws StorageError if the prefix contains backslashes or a '..' segment
 */
export function scopePrefix(prefix: string): string {
  const normalized = normalizePrefix(prefix).replace(/\/$/, '')
  return normalized ? `${normalized}/` : ''
}

/**
 * Derives the deterministic Weaviate object id (a version 5 UUID) for a fully prefixed key.
 *
 * @param fullKey - The storage prefix plus the normalized key
 * @returns The object id
 */
export function keyUuid(fullKey: string): string {
  const hash = createHash('sha1')
    .update(Buffer.from(KEY_NAMESPACE.replaceAll('-', ''), 'hex'))
    .update(fullKey, 'utf8')
    .digest()
  hash[6] = (hash[6]! & 0x0f) | 0x50
  hash[8] = (hash[8]! & 0x3f) | 0x80
  const hex = hash.subarray(0, 16).toString('hex')
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`
}
