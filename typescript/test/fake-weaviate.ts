/**
 * An in-memory stand-in for the Weaviate client calls WeaviateStorage makes.
 *
 * The behavioural tests run against this fake and, when WEAVIATE_INTEG is set, against a real
 * Weaviate instance. Keeping one suite for both stops the fake drifting from real behaviour.
 */

import { Embedder } from '@strands-agents/sdk/storage/search'
import type { WeaviateClient } from 'weaviate-client'

export const EMBEDDING_DIMENSIONS = 32

function tokens(text: string): string[] {
  return text.toLowerCase().match(/[a-z0-9]+/g) ?? []
}

function hash(token: string): number {
  let value = 0
  for (const char of token) value = (value * 31 + char.charCodeAt(0)) >>> 0
  return value
}

/** Deterministic bag-of-words embedder, so texts sharing words get nearby vectors. */
export class FakeEmbedder extends Embedder {
  readonly calls: string[] = []
  error: Error | undefined

  async embed(text: string): Promise<number[]> {
    this.calls.push(text)
    if (this.error) throw this.error
    const vector = new Array<number>(EMBEDDING_DIMENSIONS).fill(0)
    for (const token of tokens(text)) vector[hash(token) % EMBEDDING_DIMENSIONS]! += 1
    const norm = Math.sqrt(vector.reduce((sum, value) => sum + value * value, 0)) || 1
    return vector.map((value) => value / norm)
  }
}

interface Filter {
  operator: string
  filters?: Filter[]
  target?: { property: string }
  value: unknown
}

interface StoredObject {
  properties: Record<string, unknown>
  vectors: number[] | Record<string, number[]> | undefined
}

interface QueryOptions {
  filters?: Filter
  limit?: number
  returnProperties?: string[]
  vector?: number[]
  alpha?: number
  targetVector?: string
  queryProperties?: string[]
}

function matches(filter: Filter | undefined, properties: Record<string, unknown>): boolean {
  if (!filter) return true
  if (filter.operator === 'And') return filter.filters!.every((part) => matches(part, properties))
  const value = properties[filter.target!.property]
  if (filter.operator === 'Equal') return value === filter.value
  if (filter.operator === 'Like') {
    const pattern = Array.from(filter.value as string, (char) =>
      char === '*' ? '[\\s\\S]*' : char === '?' ? '[\\s\\S]' : char.replace(/[.+^${}()|[\]\\/-]/g, '\\$&')
    ).join('')
    return typeof value === 'string' && new RegExp(`^${pattern}$`).test(value)
  }
  throw new Error(`fake does not support operator ${filter.operator}`)
}

function firstVector(stored: StoredObject): number[] | undefined {
  if (!stored.vectors) return undefined
  return Array.isArray(stored.vectors) ? stored.vectors : Object.values(stored.vectors)[0]
}

function cosine(left: number[] | undefined, right: number[] | undefined): number {
  if (!left || !right) return 0
  const dot = left.reduce((sum, value, index) => sum + value * right[index]!, 0)
  const norms = Math.hypot(...left) * Math.hypot(...right)
  return norms ? dot / norms : 0
}

function project(stored: StoredObject, returnProperties: string[] | undefined, metadata = {}) {
  const names = returnProperties ?? Object.keys(stored.properties)
  return {
    properties: Object.fromEntries(
      names.filter((name) => name in stored.properties).map((name) => [name, stored.properties[name]])
    ),
    metadata,
    vectors: stored.vectors,
  }
}

export class FakeCollection {
  readonly objects = new Map<string, StoredObject>()
  readonly calls: [string, Record<string, unknown>][] = []

  constructor(
    readonly schema: { name: string; dataType: string; tokenization?: string }[],
    readonly hasVectorizer: boolean
  ) {}

  readonly filter = {
    byProperty: (property: string) => ({
      equal: (value: unknown): Filter => ({ operator: 'Equal', target: { property }, value }),
      like: (value: string): Filter => ({ operator: 'Like', target: { property }, value }),
    }),
  }

  readonly config = {
    get: async () => ({
      properties: this.schema.map((property) => ({ tokenization: 'none', ...property })),
      vectorizers: { default: { vectorizer: { name: this.hasVectorizer ? 'text2vec-weaviate' : 'none' } } },
    }),
  }

  readonly data = {
    insertMany: async (
      objects: { id: string; properties: Record<string, unknown>; vectors?: StoredObject['vectors'] }[]
    ) => {
      const errors: Record<number, { message: string }> = {}
      objects.forEach((item, index) => {
        if (item.properties.data === '') {
          errors[index] = { message: "invalid blob property 'data': not a valid blob base64 string" }
          return
        }
        this.objects.set(item.id, { properties: { ...item.properties }, vectors: item.vectors })
      })
      return { hasErrors: Object.keys(errors).length > 0, errors }
    },
    deleteById: async (id: string) => {
      this.objects.delete(id)
      return true
    },
  }

  readonly query = {
    fetchObjectById: async (id: string, options: QueryOptions = {}) => {
      const stored = this.objects.get(id)
      return stored ? project(stored, options.returnProperties) : null
    },
    fetchObjects: async (options: QueryOptions = {}) => {
      this.calls.push(['fetchObjects', { ...options }])
      const matched = this._candidates(options).slice(0, options.limit ?? 10)
      return { objects: matched.map((stored) => project(stored, options.returnProperties)) }
    },
    bm25: async (query: string, options: QueryOptions = {}) => {
      this.calls.push(['bm25', { query, ...options }])
      const scored = this._candidates(options).map((stored) => [this._keywordScore(query, stored), stored] as const)
      return this._ranked(
        scored.filter(([score]) => score > 0),
        options,
        false
      )
    },
    hybrid: async (query: string, options: QueryOptions = {}) => {
      this.calls.push(['hybrid', { query, ...options }])
      if (!options.vector && !this.hasVectorizer) throw new Error('VectorFromInput was called without vectorizer')
      const alpha = options.alpha ?? 0.5
      const scored = this._candidates(options).map((stored) => {
        const keyword = Math.min(this._keywordScore(query, stored), 1)
        const similarity = options.vector ? cosine(options.vector, firstVector(stored)) : 0
        return [(1 - alpha) * keyword + alpha * similarity, stored] as const
      })
      return this._ranked(
        scored.filter(([score]) => score > 0),
        options,
        false
      )
    },
    nearVector: async (vector: number[], options: QueryOptions = {}) => {
      this.calls.push(['nearVector', { vector, ...options }])
      const scored = this._candidates(options)
        .filter((stored) => firstVector(stored))
        .map((stored) => [1 - cosine(vector, firstVector(stored)), stored] as const)
      return this._ranked(scored, options, true)
    },
    nearText: async (query: string, options: QueryOptions = {}) => {
      this.calls.push(['nearText', { query, ...options }])
      if (!this.hasVectorizer) throw new Error('nearText requires a vectorizer')
      const scored = this._candidates(options).map(
        (stored) => [1 / (1 + this._keywordScore(query, stored)), stored] as const
      )
      return this._ranked(scored, options, true)
    },
  }

  async *iterator(options: QueryOptions = {}) {
    this.calls.push(['iterator', {}])
    for (const stored of [...this.objects.values()]) yield project(stored, options.returnProperties)
  }

  private _candidates(options: QueryOptions): StoredObject[] {
    return [...this.objects.values()].filter((stored) => matches(options.filters, stored.properties))
  }

  private _keywordScore(query: string, stored: StoredObject): number {
    const textTokens = tokens((stored.properties.text as string | undefined) ?? '')
    return [...new Set(tokens(query))].reduce(
      (sum, token) => sum + textTokens.filter((textToken) => textToken === token).length,
      0
    )
  }

  private _ranked(scored: (readonly [number, StoredObject])[], options: QueryOptions, distance: boolean) {
    const sorted = [...scored].sort((left, right) => (distance ? left[0] - right[0] : right[0] - left[0]))
    return {
      objects: sorted
        .slice(0, options.limit ?? 10)
        .map(([value, stored]) =>
          project(stored, options.returnProperties, distance ? { distance: value } : { score: value })
        ),
    }
  }
}

export class FakeClient {
  readonly store = new Map<string, FakeCollection>()

  readonly collections = {
    exists: async (name: string) => this.store.has(name),
    create: async (config: { name: string; properties: FakeCollection['schema']; vectorizers?: unknown }) => {
      // A test asks for a server-side vectorizer by passing the string "vectorizer".
      const collection = new FakeCollection(config.properties, config.vectorizers === 'vectorizer')
      this.store.set(config.name, collection)
      return collection
    },
    use: (name: string) => this.store.get(name)!,
    delete: async (name: string) => {
      this.store.delete(name)
    },
  }

  /** The fake typed as the client WeaviateStorage expects. */
  asClient(): WeaviateClient {
    return this as unknown as WeaviateClient
  }
}
