/**
 * Weaviate storage implementation.
 *
 * Persists bytes as objects in a Weaviate collection, with built-in search. Each key is one
 * object: the raw value is stored as a blob, and text values are also stored in a searchable
 * property, so `search()` runs a Weaviate hybrid (BM25 plus vector), vector, or BM25 query
 * instead of a keyword scan. Because one object holds a key's value, text, and vector, an
 * overwrite or delete can never leave a stale index entry.
 *
 * @example
 * ```ts
 * import weaviate from 'weaviate-client'
 * import { WeaviateStorage } from 'strands-weaviate'
 *
 * const client = await weaviate.connectToLocal()
 * const storage = new WeaviateStorage(client, { prefix: 'agents/' })
 * await storage.write('memory/notes.md', new TextEncoder().encode('prefers concise answers'))
 * const results = await storage.search('how should answers be written')
 * ```
 */

import { Buffer } from 'node:buffer'
import { TextDecoder } from 'node:util'
import { StorageError, type Storage } from '@strands-agents/sdk'
import type { Embedder, StorageSearchResult } from '@strands-agents/sdk/storage/search'
import { Filters, configure, type Collection, type FilterValue, type WeaviateClient } from 'weaviate-client'
import { keyUuid, normalizeKey, normalizePrefix, scopePrefix } from './keys.js'
import { registerIntegration } from './telemetry.js'

/** How `search()` queries Weaviate. */
export type WeaviateSearchMode = 'hybrid' | 'vector' | 'bm25'

const SEARCH_MODES: readonly WeaviateSearchMode[] = ['hybrid', 'vector', 'bm25']

// Tells SDK constructs a view is already scoped, as the built-in backends' namespace() does.
const NAMESPACED = Symbol.for('strands.storage.namespaced')

/**
 * Configuration for {@link WeaviateStorage}.
 */
export interface WeaviateStorageConfig {
  /** Collection that holds the objects. Defaults to `StrandsStorage`. */
  collectionName?: string
  /** Key prefix prepended to every key (a namespace within the collection). */
  prefix?: string
  /**
   * Computes vectors for text values on write and for queries on search. Omit it when the
   * collection has a Weaviate vectorizer, or for BM25-only search.
   */
  embedder?: Embedder
  /**
   * Search mode. When omitted, hybrid is used if an `embedder` is set or the collection has a
   * vectorizer, and BM25 otherwise.
   */
  searchMode?: WeaviateSearchMode
  /** Hybrid weighting between BM25 (0) and vector (1) search. Defaults to 0.5. */
  alpha?: number
  /** Maximum number of results `search()` returns. Defaults to 10. */
  maxResults?: number
  /**
   * Key prefixes, relative to `prefix`, whose text values are made searchable. Omit to index
   * every text value. Values outside these prefixes are stored and readable but never
   * embedded or returned by `search()`.
   */
  indexPrefixes?: string[]
  /**
   * Longest text made searchable per value; longer values are truncated in the search index
   * only. Defaults to 8000.
   */
  maxIndexedChars?: number
  /**
   * Create the collection when it does not exist. When `false`, a missing collection throws
   * on first use. Defaults to `true`.
   */
  createCollection?: boolean
  /**
   * Named vector to write to and search, for a collection that defines more than one or names
   * its vector something other than `default`.
   */
  vectorName?: string
  /**
   * Tenant whose shard this storage reads and writes, in a multi-tenant collection. When the
   * storage creates the collection for a tenant, it enables multi-tenancy; an existing
   * single-tenant collection cannot be converted.
   */
  tenant?: string
}

/** Collection state shared by a storage and every view from `namespace()` or `withTenant()`. */
interface SharedState {
  collection?: Promise<Collection>
  hasVectorizer: boolean
  multiTenant: boolean
  /** The root storage's prefix, which `indexPrefixes` are relative to. */
  rootPrefix: string
}

/**
 * A Strands `Storage` backend that persists to a Weaviate collection.
 *
 * Vectors come from one of two places: pass an `embedder` to compute them in the SDK, or
 * create the collection yourself with a Weaviate vectorizer (see
 * {@link WeaviateStorage.collectionProperties}). With neither, search is BM25 only.
 *
 * Pass `tenant` (or call {@link WeaviateStorage.withTenant}) to keep each tenant's objects in
 * its own shard of a multi-tenant collection. A tenant is created by the first value written
 * to it, never by a read, list, search or delete.
 */
export class WeaviateStorage implements Storage {
  private readonly _client: WeaviateClient
  private readonly _collectionName: string
  private _prefix: string
  private readonly _embedder: Embedder | undefined
  private readonly _searchMode: WeaviateSearchMode | undefined
  private readonly _alpha: number
  private readonly _maxResults: number
  private readonly _indexPrefixes: string[] | undefined
  private readonly _maxIndexedChars: number
  private readonly _createCollection: boolean
  private readonly _vectorName: string | undefined
  private _tenant: string | undefined
  private readonly _shared: SharedState
  // A filtered fetch is only complete within a single page, so list() asks for one page of
  // this size and falls back to a full cursor scan when the page comes back full.
  private _listFilterLimit = 1000

  /**
   * Creates Weaviate storage.
   *
   * @param client - A connected Weaviate client. The caller owns it: the storage never
   *   connects or closes it.
   * @param config - Storage configuration
   * @throws StorageError if `prefix` is invalid
   * @throws RangeError if `searchMode`, `alpha`, `maxResults`, `maxIndexedChars` or `tenant` is
   *   out of range
   */
  constructor(client: WeaviateClient, config: WeaviateStorageConfig = {}) {
    const alpha = config.alpha ?? 0.5
    const maxResults = config.maxResults ?? 10
    const maxIndexedChars = config.maxIndexedChars ?? 8000
    if (config.searchMode !== undefined && !SEARCH_MODES.includes(config.searchMode)) {
      throw new RangeError(`searchMode must be one of ${SEARCH_MODES.join(', ')}, got '${config.searchMode}'`)
    }
    if (!(alpha >= 0 && alpha <= 1)) {
      throw new RangeError(`alpha must be between 0 and 1, got ${alpha}`)
    }
    if (!(maxResults >= 1)) {
      throw new RangeError(`maxResults must be at least 1, got ${maxResults}`)
    }
    if (!(maxIndexedChars >= 1)) {
      throw new RangeError(`maxIndexedChars must be at least 1, got ${maxIndexedChars}`)
    }

    this._client = client
    this._collectionName = config.collectionName ?? 'StrandsStorage'
    this._prefix = scopePrefix(config.prefix ?? '')
    this._embedder = config.embedder
    this._searchMode = config.searchMode
    this._alpha = alpha
    this._maxResults = maxResults
    this._indexPrefixes = config.indexPrefixes?.map(normalizePrefix)
    this._maxIndexedChars = maxIndexedChars
    this._createCollection = config.createCollection ?? true
    this._vectorName = config.vectorName
    this._tenant = config.tenant === undefined ? undefined : validateTenant(config.tenant)
    this._shared = { hasVectorizer: false, multiTenant: false, rootPrefix: this._prefix }
  }

  /** The tenant this storage is bound to, or `undefined` for a single-tenant collection. */
  get tenant(): string | undefined {
    return this._tenant
  }

  /** The normalized key prefix (ending in `/`), or `''` when none was configured. */
  get prefix(): string {
    return this._prefix
  }

  /**
   * Returns the property definitions a collection must have.
   *
   * Use this to create the collection yourself, for example to attach a Weaviate vectorizer.
   * Vectorize only the `text` property.
   *
   * @example
   * ```ts
   * await client.collections.create({
   *   name: 'StrandsStorage',
   *   properties: WeaviateStorage.collectionProperties(),
   *   vectorizers: configure.vectors.text2VecWeaviate({
   *     sourceProperties: ['text'],
   *     vectorizeCollectionName: false,
   *   }),
   * })
   * ```
   *
   * @returns The property definitions
   */
  static collectionProperties() {
    return [
      {
        name: 'key',
        dataType: 'text' as const,
        tokenization: 'field' as const,
        indexFilterable: true,
        indexSearchable: false,
        skipVectorization: true,
      },
      { name: 'text', dataType: 'text' as const, tokenization: 'word' as const, indexSearchable: true },
      { name: 'data', dataType: 'blob' as const },
      { name: 'is_text', dataType: 'boolean' as const, indexFilterable: true },
      { name: 'size', dataType: 'int' as const },
      { name: 'updated_at', dataType: 'date' as const },
    ]
  }

  /**
   * Stores data as a Weaviate object, replacing any existing value, text and vector.
   *
   * @param key - '/'-separated key identifying the value
   * @param data - Raw bytes to persist
   * @throws StorageError if the key is invalid, embedding the value fails (the previous value
   *   is kept in that case), or the write fails
   */
  async write(key: string, data: Uint8Array): Promise<void> {
    const fullKey = this._prefix + normalizeKey(key)
    const text = this._indexableText(fullKey, data)

    let vector: number[] | undefined
    if (text !== undefined && this._embedder && this._searchMode !== 'bm25') {
      try {
        vector = await this._embedder.embed(text)
      } catch (error) {
        throw new StorageError(`Failed to embed '${key}' for Weaviate`, { cause: error })
      }
    }

    const properties: Record<string, string | number | boolean | Date> = {
      key: fullKey,
      is_text: text !== undefined,
      size: data.byteLength,
      updated_at: new Date(),
    }
    // Weaviate rejects an empty blob, so an empty value is the object without one.
    if (data.byteLength > 0) properties.data = Buffer.from(data).toString('base64')
    if (text !== undefined) properties.text = text

    const insert = async (collection: Collection): Promise<void> => {
      // A single-object batch replaces the whole object by id; data.insert rejects existing ids.
      const result = await collection.data.insertMany([
        {
          id: keyUuid(fullKey),
          properties,
          ...(vector && { vectors: this._vectorName ? { [this._vectorName]: vector } : vector }),
        },
      ])
      if (result.hasErrors) {
        const messages = Object.values(result.errors)
          .map((error) => error.message)
          .join('; ')
        throw new StorageError(`Failed to write '${key}' to Weaviate (${data.byteLength} bytes): ${messages}`)
      }
    }
    await this._run(`Failed to write '${key}' to Weaviate (${data.byteLength} bytes)`, insert, (error) => {
      throw new StorageError(
        `Failed to write '${key}' to Weaviate: tenant '${this._tenant}' does not exist and the collection ` +
          'does not create tenants automatically; enable automatic tenant creation on the collection or ' +
          'create the tenant first',
        { cause: error }
      )
    })
  }

  /**
   * Reads the bytes stored under key.
   *
   * @param key - The key to read
   * @returns The stored bytes, or null if the key does not exist
   * @throws StorageError if the key is invalid or the read fails
   */
  async read(key: string): Promise<Uint8Array | null> {
    const fullKey = this._prefix + normalizeKey(key)
    const fetch = async (collection: Collection): Promise<Uint8Array | null> => {
      const stored = await collection.query.fetchObjectById(keyUuid(fullKey), { returnProperties: ['data'] })
      return stored ? decodeBlob(stored.properties.data) : null
    }
    return this._run(`Failed to read '${key}' from Weaviate`, fetch, () => null)
  }

  /**
   * Deletes the object stored under key. A no-op if the key does not exist.
   *
   * @param key - The key to delete
   * @throws StorageError if the key is invalid or the delete fails
   */
  async delete(key: string): Promise<void> {
    const fullKey = this._prefix + normalizeKey(key)
    const remove = async (collection: Collection): Promise<void> => {
      await collection.data.deleteById(keyUuid(fullKey))
    }
    await this._run(`Failed to delete '${key}' from Weaviate`, remove, () => undefined)
  }

  /**
   * Lists keys matching the given prefix.
   *
   * @param query - A prefix string to filter keys. Empty string matches all.
   * @returns Matching keys sorted ascending, with the storage-level prefix stripped
   * @throws StorageError if the prefix is invalid or the listing fails
   */
  async list(query: string): Promise<string[]> {
    const fullPrefix = this._prefix + normalizePrefix(query)
    const collect = async (collection: Collection): Promise<string[]> => {
      if (fullPrefix) {
        const page = await collection.query.fetchObjects({
          filters: collection.filter.byProperty('key').like(`${fullPrefix}*`),
          limit: this._listFilterLimit,
          returnProperties: ['key'],
        })
        if (page.objects.length < this._listFilterLimit) {
          return page.objects.map((stored) => stored.properties.key as string)
        }
      }
      const scanned: string[] = []
      for await (const stored of collection.iterator({ returnProperties: ['key'] })) {
        scanned.push(stored.properties.key as string)
      }
      return scanned
    }
    const keys = await this._run(`Failed to list keys with prefix '${query}' from Weaviate`, collect, () => [])

    // Like treats '*' and '?' in the prefix as wildcards, so the server can over-match.
    return keys
      .filter((key) => key.startsWith(fullPrefix))
      .map((key) => key.slice(this._prefix.length))
      .sort()
  }

  /**
   * Searches indexed text values under this storage's prefix.
   *
   * @param query - Natural-language search query
   * @returns Up to `maxResults` matches ranked best-first, each carrying its stored bytes.
   *   Scores are higher-is-better and are not comparable across search modes.
   * @throws StorageError if embedding the query or the search fails
   */
  async search(query: string): Promise<StorageSearchResult[]> {
    if (!query.trim()) return []

    const find = async (collection: Collection): Promise<StorageSearchResult[]> => {
      const mode = this._searchMode ?? (this._embedder || this._shared.hasVectorizer ? 'hybrid' : 'bm25')
      const vector = mode !== 'bm25' && this._embedder ? await this._embedder.embed(query) : undefined

      const isText = collection.filter.byProperty('is_text').equal(true)
      const filters: FilterValue = this._prefix
        ? Filters.and(isText, collection.filter.byProperty('key').like(`${this._prefix}*`))
        : isText
      const common = { filters, limit: this._maxResults, returnProperties: ['key', 'data'] }
      const target = this._vectorName ? { targetVector: this._vectorName } : {}

      let response
      if (mode === 'bm25') {
        response = await collection.query.bm25(query, {
          ...common,
          queryProperties: ['text'],
          returnMetadata: ['score'],
        })
      } else if (mode === 'hybrid') {
        response = await collection.query.hybrid(query, {
          ...common,
          ...target,
          ...(vector && { vector }),
          alpha: this._alpha,
          queryProperties: ['text'],
          returnMetadata: ['score'],
        })
      } else if (vector) {
        response = await collection.query.nearVector(vector, { ...common, ...target, returnMetadata: ['distance'] })
      } else {
        response = await collection.query.nearText(query, { ...common, ...target, returnMetadata: ['distance'] })
      }

      const results: StorageSearchResult[] = []
      for (const stored of response.objects) {
        const fullKey = stored.properties.key as string
        if (!fullKey.startsWith(this._prefix)) continue
        results.push({
          key: fullKey.slice(this._prefix.length),
          score: toScore(mode, stored.metadata),
          data: decodeBlob(stored.properties.data),
        })
      }
      return results
    }
    return this._run(`Failed to search Weaviate collection '${this._collectionName}'`, find, () => [])
  }

  /**
   * Returns a `WeaviateStorage` scoped to `prefix` within this storage's prefix.
   *
   * The scoped instance shares this instance's client and collection, and its `search()`
   * filters by the prefix inside Weaviate rather than after ranking. A prefix scopes keys; it
   * is not an access-control boundary.
   *
   * @param prefix - Prefix to prepend to all keys
   * @returns A `WeaviateStorage` scoped to the prefix
   * @throws StorageError if the prefix contains backslashes or a '..' segment
   */
  namespace(prefix: string): WeaviateStorage {
    const scoped = this._clone()
    scoped._prefix = this._prefix + scopePrefix(prefix)
    Object.defineProperty(scoped, NAMESPACED, { value: true })
    return scoped
  }

  /**
   * Returns a `WeaviateStorage` bound to `tenant`, sharing this instance's client.
   *
   * The view keeps this storage's prefix and configuration. Calling this makes no request to
   * Weaviate: the tenant comes into existence when the first value is written to it.
   *
   * @param tenant - The tenant whose shard the returned storage reads and writes
   * @returns A `WeaviateStorage` bound to the tenant
   * @throws RangeError if the tenant name is not one Weaviate accepts
   */
  withTenant(tenant: string): WeaviateStorage {
    const scoped = this._clone()
    scoped._tenant = validateTenant(tenant)
    return scoped
  }

  /** Copies this storage, keeping the shared collection state and the namespaced marker. */
  private _clone(): WeaviateStorage {
    const copy: WeaviateStorage = Object.create(Object.getPrototypeOf(this))
    Object.assign(copy, this)
    if (NAMESPACED in this) Object.defineProperty(copy, NAMESPACED, { value: true })
    return copy
  }

  /** Whether an error means this storage's tenant has not been created yet. */
  private _isMissingTenant(error: unknown): boolean {
    const message = error instanceof Error ? error.message : String(error)
    return this._tenant !== undefined && message.includes('tenant not found')
  }

  /** Returns the text to make searchable for a value, or undefined if it is not indexed. */
  private _indexableText(fullKey: string, data: Uint8Array): string | undefined {
    const relativeKey = fullKey.slice(this._shared.rootPrefix.length)
    if (this._indexPrefixes && !this._indexPrefixes.some((prefix) => relativeKey.startsWith(prefix))) {
      return undefined
    }
    // A NUL byte marks framed binary payloads that happen to decode as UTF-8.
    if (data.byteLength === 0 || data.includes(0)) return undefined
    let text: string
    try {
      text = new TextDecoder('utf-8', { fatal: true }).decode(data)
    } catch {
      return undefined
    }
    if (text.length <= this._maxIndexedChars) return text
    // Count code points, as the Python package does, so both index the same text. A code point
    // is at most two UTF-16 units, so twice the limit always holds enough of them.
    return Array.from(text.slice(0, this._maxIndexedChars * 2))
      .slice(0, this._maxIndexedChars)
      .join('')
  }

  /**
   * Runs an operation against the collection, wrapping any failure in a StorageError.
   *
   * @param onMissingTenant - Supplies the result when this storage's tenant does not exist yet
   */
  private async _run<T>(
    message: string,
    operation: (collection: Collection) => Promise<T>,
    onMissingTenant: (error: unknown) => T
  ): Promise<T> {
    try {
      return await operation(await this._getCollection())
    } catch (error) {
      if (this._isMissingTenant(error)) return onMissingTenant(error)
      if (error instanceof StorageError) throw error
      throw new StorageError(message, { cause: error })
    }
  }

  /**
   * Returns the validated collection handle for this storage's tenant.
   *
   * The collection is created on first use. A tenant is not: the handle only names it.
   */
  private async _getCollection(): Promise<Collection> {
    if (!this._shared.collection) {
      const opening = this._openCollection(this._tenant !== undefined)
      this._shared.collection = opening
      // A failed open is not cached, so the next call retries.
      opening.catch(() => {
        if (this._shared.collection === opening) delete this._shared.collection
      })
    }
    const collection = await this._shared.collection

    const name = this._collectionName
    if (this._tenant === undefined) {
      if (this._shared.multiTenant) {
        throw new StorageError(`Weaviate collection '${name}' is multi-tenant; pass tenant or use withTenant()`)
      }
      return collection
    }
    if (!this._shared.multiTenant) {
      throw new StorageError(
        `Weaviate collection '${name}' is not multi-tenant, so tenant '${this._tenant}' cannot be used; ` +
          'multi-tenancy can only be enabled when a collection is created, so use a new collectionName'
      )
    }
    return collection.withTenant(this._tenant)
  }

  /** Creates the collection if needed and checks that its schema fits this backend. */
  private async _openCollection(multiTenant: boolean): Promise<Collection> {
    // Before the first request, so every request this storage makes carries the header.
    await registerIntegration(this._client)
    const name = this._collectionName
    const collections = this._client.collections
    if (!(await collections.exists(name))) {
      if (!this._createCollection) {
        throw new StorageError(`Weaviate collection '${name}' does not exist`)
      }
      try {
        await collections.create({
          name,
          properties: WeaviateStorage.collectionProperties(),
          vectorizers: configure.vectors.selfProvided({ name: this._vectorName ?? 'default' }),
          // Tenants are created by the first object inserted into them, never up front.
          multiTenancy: configure.multiTenancy({
            enabled: multiTenant,
            autoTenantCreation: multiTenant,
            autoTenantActivation: multiTenant,
          }),
        })
      } catch (error) {
        // Another process may have created it between the check and the create.
        if (!(await collections.exists(name))) throw error
      }
    }

    const collection = collections.use(name)
    const config = await collection.config.get()
    const keyProperty = config.properties.find((property) => property.name === 'key')
    const dataProperty = config.properties.find((property) => property.name === 'data')
    if (keyProperty?.tokenization !== 'field') {
      throw new StorageError(
        `Weaviate collection '${name}' needs a 'key' text property with field tokenization; ` +
          'create it with WeaviateStorage.collectionProperties()'
      )
    }
    if (dataProperty?.dataType !== 'blob') {
      throw new StorageError(
        `Weaviate collection '${name}' needs a 'data' blob property; ` +
          'create it with WeaviateStorage.collectionProperties()'
      )
    }
    this._shared.hasVectorizer = Object.values(config.vectorizers).some((vector) => vector.vectorizer.name !== 'none')
    this._shared.multiTenant = config.multiTenancy.enabled
    return collection
  }
}

/** Returns the tenant name, or throws if Weaviate would reject it. */
function validateTenant(tenant: string): string {
  if (!/^[A-Za-z0-9_-]{1,64}$/.test(tenant)) {
    throw new RangeError(`tenant must be 1 to 64 characters of letters, digits, '_' or '-', got '${tenant}'`)
  }
  return tenant
}

/** Decodes a blob property into a fresh buffer; an absent blob is an empty value. */
function decodeBlob(encoded: unknown): Uint8Array {
  return typeof encoded === 'string' && encoded ? new Uint8Array(Buffer.from(encoded, 'base64')) : new Uint8Array(0)
}

/** Maps Weaviate result metadata to a higher-is-better score. */
function toScore(mode: WeaviateSearchMode, metadata: { score?: number; distance?: number } | undefined): number {
  if (mode === 'vector') {
    return metadata?.distance === undefined ? 0 : 1 / (1 + metadata.distance)
  }
  return metadata?.score ?? 0
}
