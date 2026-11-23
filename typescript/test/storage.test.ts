/**
 * Tests for WeaviateStorage.
 *
 * The behavioural suite runs against the in-memory fake and, when WEAVIATE_INTEG is set,
 * against a real Weaviate instance (WEAVIATE_HTTP_PORT / WEAVIATE_GRPC_PORT, default 8080/50051).
 */

import { Buffer } from 'node:buffer'
import { randomUUID } from 'node:crypto'
import process from 'node:process'
import { TextDecoder, TextEncoder } from 'node:util'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { SessionManager, StorageError, type Storage } from '@strands-agents/sdk'
import { FileMemoryStore } from '@strands-agents/sdk/vended-memory-stores/file-memory-store'
import weaviate, { configure, type WeaviateClient } from 'weaviate-client'
import { WeaviateStorage } from '../src/storage.js'
import { keyUuid } from '../src/keys.js'
import { FakeClient, FakeEmbedder } from './fake-weaviate.js'

const NAMESPACED = Symbol.for('strands.storage.namespaced')
const EPHEMERAL = Symbol.for('strands.storage.ephemeral')

const encode = (text: string): Uint8Array => new TextEncoder().encode(text)
const decode = (data: Uint8Array | null | undefined): string => new TextDecoder().decode(data ?? new Uint8Array())
const keysOf = (results: { key: string }[]): string[] => results.map((result) => result.key)

interface Backend {
  name: string
  connect(): Promise<WeaviateClient>
}

const backends: Backend[] = [{ name: 'fake', connect: async () => new FakeClient().asClient() }]
if (process.env.WEAVIATE_INTEG) {
  backends.push({
    name: 'weaviate',
    connect: () =>
      weaviate.connectToLocal({
        port: Number(process.env.WEAVIATE_HTTP_PORT ?? 8080),
        grpcPort: Number(process.env.WEAVIATE_GRPC_PORT ?? 50051),
      }),
  })
}

/** Reads one raw object, with its vector, from either kind of client. */
async function storedObject(client: WeaviateClient, collectionName: string, id: string) {
  const stored = await client.collections
    .use(collectionName)
    .query.fetchObjectById(id, { includeVector: true, returnProperties: ['key', 'text', 'is_text'] })
  const vectors = stored?.vectors as number[] | Record<string, number[]> | undefined
  return {
    text: stored?.properties.text ?? null,
    isText: stored?.properties.is_text,
    hasVector: Array.isArray(vectors) ? vectors.length > 0 : Object.keys(vectors ?? {}).length > 0,
  }
}

describe.each(backends)('WeaviateStorage on $name', ({ connect }) => {
  let client: WeaviateClient
  let collectionName: string
  let storage: WeaviateStorage
  let embedder: FakeEmbedder

  beforeEach(async () => {
    client = await connect()
    collectionName = `StrandsTest_${randomUUID().replaceAll('-', '')}`
    storage = new WeaviateStorage(client, { collectionName })
    embedder = new FakeEmbedder()
  })

  afterEach(async () => {
    vi.restoreAllMocks()
    await client.collections.delete(collectionName)
    if (!(client instanceof FakeClient)) await client.close()
  })

  describe('read, write and delete', () => {
    it.each([
      ['text', encode('hello world')],
      ['binary', new Uint8Array([0, 255, 254, 32, 98])],
      ['empty', new Uint8Array(0)],
      ['unicode', encode('café ☃')],
    ])('round-trips %s bytes', async (_label, data) => {
      await storage.write('a/value', data)

      expect(await storage.read('a/value')).toStrictEqual(data)
    })

    it('returns a fresh buffer on each read', async () => {
      await storage.write('key', encode('value'))

      const first = await storage.read('key')
      first!.fill(0)

      expect(decode(await storage.read('key'))).toBe('value')
    })

    it('returns null for a missing key', async () => {
      expect(await storage.read('missing')).toBeNull()
    })

    it('overwrites an existing value', async () => {
      await storage.write('key', encode('first'))
      await storage.write('key', encode('second'))

      expect(decode(await storage.read('key'))).toBe('second')
      expect(await storage.list('')).toStrictEqual(['key'])
    })

    it('deletes a value', async () => {
      await storage.write('key', encode('value'))
      await storage.delete('key')

      expect(await storage.read('key')).toBeNull()
      expect(await storage.list('')).toStrictEqual([])
    })

    it('treats deleting a missing key as a no-op', async () => {
      await expect(storage.delete('missing')).resolves.toBeUndefined()
    })

    it('normalizes keys', async () => {
      await storage.write('/a//b/', encode('value'))

      expect(decode(await storage.read('a/b'))).toBe('value')
      expect(await storage.list('')).toStrictEqual(['a/b'])
    })

    it.each(['', 'a\\b', '../a', 'a/../b'])('rejects the invalid key %j', async (key) => {
      await expect(storage.read(key)).rejects.toThrow(StorageError)
      await expect(storage.delete(key)).rejects.toThrow(StorageError)
      await expect(storage.write(key, encode('value'))).rejects.toThrow(StorageError)
    })

    it('stores a multi-megabyte value', async () => {
      const data = encode('snapshot '.repeat(400_000))

      await storage.write('session/large.json', data)

      const stored = await storage.read('session/large.json')
      expect(Buffer.from(stored!).equals(data)).toBe(true)
    })
  })

  describe('list', () => {
    const writeAll = async (keys: string[]) => {
      for (const key of keys) await storage.write(key, encode('value'))
    }

    it('lists all keys sorted', async () => {
      await writeAll(['b/2', 'a/1', 'c'])

      expect(await storage.list('')).toStrictEqual(['a/1', 'b/2', 'c'])
    })

    it('lists by prefix recursively', async () => {
      await writeAll(['a/1', 'a/sub/2', 'ab/3', 'b/4'])

      expect(await storage.list('a/')).toStrictEqual(['a/1', 'a/sub/2'])
    })

    it('accepts a prefix that is not a directory', async () => {
      await writeAll(['block_0', 'block_1', 'blocked_0'])

      expect(await storage.list('block_')).toStrictEqual(['block_0', 'block_1'])
    })

    it('matches prefixes case-sensitively', async () => {
      await writeAll(['a/1', 'A/1'])

      expect(await storage.list('a/')).toStrictEqual(['a/1'])
    })

    it('matches wildcard characters in a prefix literally', async () => {
      await writeAll(['a*/1', 'ab/2', 'a?/3', 'ac/4'])

      expect(await storage.list('a*/')).toStrictEqual(['a*/1'])
      expect(await storage.list('a?/')).toStrictEqual(['a?/3'])
    })

    it('falls back to a scan when the filtered page is full', async () => {
      Object.assign(storage, { _listFilterLimit: 5 })
      const keys = Array.from({ length: 23 }, (_, index) => `p/${String(index).padStart(3, '0')}`)
      await writeAll([...keys, 'q/other'])

      expect(await storage.list('p/')).toStrictEqual(keys)
    })

    it('rejects an invalid prefix', async () => {
      await expect(storage.list('../a')).rejects.toThrow(StorageError)
    })
  })

  describe('prefix and namespace', () => {
    it('scopes keys under the configured prefix', async () => {
      const scoped = new WeaviateStorage(client, { collectionName, prefix: '/agents//one/' })

      await scoped.write('key', encode('value'))

      expect(scoped.prefix).toBe('agents/one/')
      expect(await scoped.list('')).toStrictEqual(['key'])
      expect(await storage.list('')).toStrictEqual(['agents/one/key'])
      expect(decode(await storage.read('agents/one/key'))).toBe('value')
    })

    it('scopes and composes namespaces', async () => {
      const tenant = storage.namespace('tenant')
      const nested = tenant.namespace('memory/')

      await nested.write('notes.md', encode('value'))
      await storage.write('other', encode('value'))

      expect(nested.prefix).toBe('tenant/memory/')
      expect(await nested.list('')).toStrictEqual(['notes.md'])
      expect(await tenant.list('')).toStrictEqual(['memory/notes.md'])
      expect(await storage.list('')).toStrictEqual(['other', 'tenant/memory/notes.md'])
    })

    it('marks namespaced views but not the root', () => {
      const scoped = storage.namespace('tenant')

      expect(NAMESPACED in scoped).toBe(true)
      expect(NAMESPACED in storage).toBe(false)
      expect(EPHEMERAL in scoped).toBe(false)
      expect(scoped).toBeInstanceOf(WeaviateStorage)
    })

    it('keeps the prefix for an empty namespace', () => {
      expect(storage.namespace('').prefix).toBe('')
    })

    it('rejects a namespace that traverses upward', () => {
      expect(() => storage.namespace('../other')).toThrow(StorageError)
    })
  })

  describe('search', () => {
    it('uses BM25 when there is no vector source', async () => {
      await storage.write('memory/coffee.md', encode('the user drinks oat milk coffee'))
      await storage.write('memory/editor.md', encode('the user writes code in vim'))

      const results = await storage.search('coffee')

      expect(keysOf(results)).toStrictEqual(['memory/coffee.md'])
      expect(decode(results[0]!.data)).toBe('the user drinks oat milk coffee')
      expect(results[0]!.score).toBeGreaterThan(0)
    })

    it('uses hybrid search with an embedder', async () => {
      const hybrid = new WeaviateStorage(client, { collectionName, embedder })
      await hybrid.write('memory/coffee.md', encode('the user drinks oat milk coffee'))
      await hybrid.write('memory/editor.md', encode('the user writes code in vim'))

      const results = await hybrid.search('which coffee does the user drink')

      expect(results[0]!.key).toBe('memory/coffee.md')
      expect(embedder.calls.at(-1)).toBe('which coffee does the user drink')
      expect((await storedObject(client, collectionName, keyUuid('memory/coffee.md'))).hasVector).toBe(true)
    })

    it('returns similarities in vector mode', async () => {
      const vectors = new WeaviateStorage(client, { collectionName, embedder, searchMode: 'vector' })
      await vectors.write('coffee.md', encode('oat milk coffee'))
      await vectors.write('editor.md', encode('vim editor'))

      const results = await vectors.search('oat milk coffee')
      const scores = results.map((result) => result.score)

      expect(results[0]!.key).toBe('coffee.md')
      expect(scores[0]).toBeCloseTo(1, 3)
      expect(scores.every((score) => score > 0 && score <= 1.0001)).toBe(true)
      expect(scores).toStrictEqual([...scores].sort((left, right) => right - left))
    })

    it('scopes search to the namespace', async () => {
      await storage.namespace('one').write('notes.md', encode('shared keyword alpha'))
      await storage.namespace('two').write('notes.md', encode('shared keyword beta'))

      const results = await storage.namespace('one').search('shared keyword')

      expect(results.map((result) => [result.key, decode(result.data)])).toStrictEqual([
        ['notes.md', 'shared keyword alpha'],
      ])
    })

    it('applies maxResults within the namespace', async () => {
      const limited = new WeaviateStorage(client, { collectionName, maxResults: 2 })
      for (let index = 0; index < 6; index++) {
        await limited.namespace('noise').write(`${index}.md`, encode('topic topic topic'))
      }
      await limited.namespace('mine').write('only.md', encode('topic'))

      expect(keysOf(await limited.namespace('mine').search('topic'))).toStrictEqual(['only.md'])
      expect(await limited.search('topic')).toHaveLength(2)
    })

    it.each(['', '   '])('returns nothing for the empty query %j', async (query) => {
      await storage.write('notes.md', encode('some text'))

      expect(await storage.search(query)).toStrictEqual([])
    })

    it('drops deleted and overwritten text from search', async () => {
      await storage.write('gone.md', encode('ephemeral marker'))
      await storage.write('changed.md', encode('ephemeral marker'))
      await storage.delete('gone.md')
      await storage.write('changed.md', encode('something else entirely'))

      expect(await storage.search('ephemeral marker')).toStrictEqual([])
    })
  })

  describe('indexing', () => {
    it('does not make binary values searchable', async () => {
      const indexed = new WeaviateStorage(client, { collectionName, embedder })
      const frame = new Uint8Array([0, 10, ...encode('text/plainneedle in a frame')])

      await indexed.write('offloader/frame', frame)
      await indexed.write('offloader/image', new Uint8Array([255, 216, 255, ...encode(' needle')]))

      expect(await indexed.search('needle')).toStrictEqual([])
      expect(embedder.calls).toStrictEqual(['needle'])
      expect(await indexed.read('offloader/frame')).toStrictEqual(frame)
    })

    it('drops text and vector when text is overwritten by binary', async () => {
      const indexed = new WeaviateStorage(client, { collectionName, embedder })
      await indexed.write('value', encode('needle'))
      await indexed.write('value', new Uint8Array([0, ...encode('needle')]))

      expect(await storedObject(client, collectionName, keyUuid('value'))).toStrictEqual({
        text: null,
        isText: false,
        hasVector: false,
      })
      expect(await indexed.search('needle')).toStrictEqual([])
    })

    it('applies indexPrefixes relative to the storage prefix', async () => {
      const gated = new WeaviateStorage(client, {
        collectionName,
        prefix: 'agents',
        embedder,
        indexPrefixes: ['memory/'],
      })
      await gated.write('session/abc/snapshot.json', encode('{"messages": "needle"}'))
      await gated.namespace('memory/user').write('notes.md', encode('needle'))

      expect(keysOf(await gated.search('needle'))).toStrictEqual(['memory/user/notes.md'])
      expect(embedder.calls).toStrictEqual(['needle', 'needle'])
      expect(decode(await gated.read('session/abc/snapshot.json'))).toBe('{"messages": "needle"}')
    })

    it('truncates the indexed text but not the value', async () => {
      const truncating = new WeaviateStorage(client, { collectionName, maxIndexedChars: 10 })

      await truncating.write('long.md', encode('headword and then a tailword'))

      expect(keysOf(await truncating.search('headword'))).toStrictEqual(['long.md'])
      expect(await truncating.search('tailword')).toStrictEqual([])
      expect(decode(await truncating.read('long.md'))).toBe('headword and then a tailword')
    })
  })

  describe('collection', () => {
    it('creates the collection on first use', async () => {
      expect(await client.collections.exists(collectionName)).toBe(false)

      await storage.write('key', encode('value'))

      expect(await client.collections.exists(collectionName)).toBe(true)
    })

    it('throws for a missing collection when creation is disabled', async () => {
      const strict = new WeaviateStorage(client, { collectionName, createCollection: false })

      await expect(strict.read('key')).rejects.toThrow(/does not exist/)
    })

    it('rejects a collection whose key property is not field-tokenized', async () => {
      await client.collections.create({
        name: collectionName,
        properties: [
          { name: 'key', dataType: 'text', tokenization: 'word' },
          { name: 'data', dataType: 'blob' },
        ],
        vectorizers: configure.vectors.selfProvided(),
      })

      await expect(storage.list('')).rejects.toThrow(/field tokenization/)
    })
  })

  describe('SDK consumers', () => {
    it('satisfies the Storage interface and backs a SessionManager', () => {
      const typed: Storage = storage

      expect(new SessionManager({ sessionId: 'test-session', storage: typed })).toBeInstanceOf(SessionManager)
    })

    it('lets FileMemoryStore recall without extra reads', async () => {
      const store = new FileMemoryStore({
        name: 'user',
        storage: new WeaviateStorage(client, { collectionName, embedder }),
      })
      await store.add('# Drinks\nthe user drinks oat milk coffee')
      await store.add('# Drinks\nthe user dislikes green tea')
      await store.add('# Editor\nthe user writes code in vim')
      const reads = vi.spyOn(WeaviateStorage.prototype, 'read')

      const entries = await store.search('what coffee does the user drink')

      expect(entries[0]!.content).toBe('# Drinks\nthe user drinks oat milk coffee\nthe user dislikes green tea')
      expect(entries[0]!.metadata?.path).toBe('drinks.md')
      expect(reads).not.toHaveBeenCalled()
      expect(await storage.list('')).toStrictEqual(['memory/user/drinks.md', 'memory/user/editor.md'])
    })
  })
})

describe('WeaviateStorage configuration', () => {
  it.each([{ alpha: 1.5 }, { alpha: -0.1 }, { maxResults: 0 }, { maxIndexedChars: 0 }, { alpha: Number.NaN }])(
    'rejects the out-of-range config %j',
    (config) => {
      expect(() => new WeaviateStorage(new FakeClient().asClient(), config)).toThrow(RangeError)
    }
  )

  it('rejects an unknown search mode', () => {
    // @ts-expect-error -- searchMode only accepts the documented modes
    expect(() => new WeaviateStorage(new FakeClient().asClient(), { searchMode: 'fuzzy' })).toThrow(RangeError)
  })

  it('rejects an invalid prefix', () => {
    expect(() => new WeaviateStorage(new FakeClient().asClient(), { prefix: '../escape' })).toThrow(StorageError)
  })
})

describe('WeaviateStorage search mode resolution', () => {
  const setup = async (config: ConstructorParameters<typeof WeaviateStorage>[1] = {}, vectorizer = false) => {
    const fake = new FakeClient()
    if (vectorizer) {
      await fake.collections.create({
        name: 'C',
        properties: WeaviateStorage.collectionProperties(),
        vectorizers: 'vectorizer',
      })
    }
    const storage = new WeaviateStorage(fake.asClient(), { collectionName: 'C', ...config })
    await storage.write('a', encode('text'))
    return { storage, calls: () => fake.store.get('C')!.calls, collection: () => fake.store.get('C')! }
  }
  const queryCalls = (calls: [string, Record<string, unknown>][]) =>
    calls.map(([name]) => name).filter((name) => name !== 'fetchObjects')

  it('uses BM25 with no vector source', async () => {
    const { storage, calls } = await setup()

    await storage.search('text')

    expect(queryCalls(calls())).toStrictEqual(['bm25'])
  })

  it('uses hybrid with the query vector, alpha and named vector when an embedder is set', async () => {
    const embedder = new FakeEmbedder()
    const { storage, calls, collection } = await setup({ embedder, alpha: 0.25, vectorName: 'content' })

    await storage.search('text')

    const vector = await new FakeEmbedder().embed('text')
    expect(calls().at(-1)).toStrictEqual([
      'hybrid',
      expect.objectContaining({ vector, alpha: 0.25, targetVector: 'content', queryProperties: ['text'] }),
    ])
    expect(collection().objects.get(keyUuid('a'))!.vectors).toStrictEqual({ content: vector })
  })

  it('uses hybrid without a query vector when the collection has a vectorizer', async () => {
    const { storage, calls } = await setup({}, true)

    const results = await storage.search('text')

    const [name, options] = calls().at(-1)!
    expect(name).toBe('hybrid')
    expect('vector' in options).toBe(false)
    expect(keysOf(results)).toStrictEqual(['a'])
  })

  it('skips the embedder in explicit BM25 mode', async () => {
    const embedder = new FakeEmbedder()
    const { storage, calls } = await setup({ embedder, searchMode: 'bm25' })

    await storage.search('text')

    expect(embedder.calls).toStrictEqual([])
    expect(queryCalls(calls())).toStrictEqual(['bm25'])
  })

  it('uses nearText in vector mode without an embedder', async () => {
    const { storage, calls } = await setup({ searchMode: 'vector' }, true)

    const results = await storage.search('text')

    const distance = 0.5
    expect(queryCalls(calls())).toStrictEqual(['nearText'])
    expect(results[0]!.score).toBeCloseTo(1 / (1 + distance))
  })

  it('lists with one filtered fetch when the page is not full', async () => {
    const { storage, calls } = await setup()

    await storage.list('a')
    await storage.list('')

    expect(calls().map(([name]) => name)).toStrictEqual(['fetchObjects', 'iterator'])
  })
})

describe('WeaviateStorage errors', () => {
  const failing = () => {
    const cause = new Error('connection refused')
    const client = { collections: { exists: vi.fn().mockRejectedValue(cause) } } as unknown as WeaviateClient
    return { storage: new WeaviateStorage(client), cause }
  }

  it.each([
    [
      'write',
      (storage: WeaviateStorage) => storage.write('key', encode('value')),
      "Failed to write 'key' to Weaviate (5 bytes)",
    ],
    ['read', (storage: WeaviateStorage) => storage.read('key'), "Failed to read 'key' from Weaviate"],
    ['delete', (storage: WeaviateStorage) => storage.delete('key'), "Failed to delete 'key' from Weaviate"],
    ['list', (storage: WeaviateStorage) => storage.list('pre'), "Failed to list keys with prefix 'pre' from Weaviate"],
    [
      'search',
      (storage: WeaviateStorage) => storage.search('query'),
      "Failed to search Weaviate collection 'StrandsStorage'",
    ],
  ])('wraps client errors from %s', async (_name, call, message) => {
    const { storage, cause } = failing()

    await expect(call(storage)).rejects.toThrow(new StorageError(message, { cause }))
    await expect(call(storage)).rejects.toHaveProperty('cause', cause)
  })

  it('keeps the previous value when embedding fails', async () => {
    const embedder = new FakeEmbedder()
    const storage = new WeaviateStorage(new FakeClient().asClient(), { embedder })
    await storage.write('key', encode('first'))
    embedder.error = new Error('embedder down')

    await expect(storage.write('key', encode('second'))).rejects.toThrow(
      new StorageError("Failed to embed 'key' for Weaviate")
    )
    expect(decode(await storage.read('key'))).toBe('first')
  })

  it('wraps a query embedding failure', async () => {
    const embedder = new FakeEmbedder()
    const storage = new WeaviateStorage(new FakeClient().asClient(), { embedder })
    embedder.error = new Error('embedder down')

    await expect(storage.search('query')).rejects.toHaveProperty('cause', embedder.error)
  })

  it('throws when Weaviate rejects the object', async () => {
    const fake = new FakeClient()
    const storage = new WeaviateStorage(fake.asClient(), { collectionName: 'C' })
    await storage.write('key', encode('first'))
    fake.store.get('C')!.data.insertMany = async () => ({
      hasErrors: true,
      errors: { 0: { message: 'object too large' } },
    })

    await expect(storage.write('key', encode('second'))).rejects.toThrow(/object too large/)
  })

  it('retries opening the collection after a failure', async () => {
    const fake = new FakeClient()
    const exists = vi.spyOn(fake.collections, 'exists').mockRejectedValueOnce(new Error('connection refused'))
    const storage = new WeaviateStorage(fake.asClient())

    await expect(storage.read('key')).rejects.toThrow(StorageError)

    expect(await storage.read('key')).toBeNull()
    expect(exists).toHaveBeenCalledTimes(2)
  })
})
