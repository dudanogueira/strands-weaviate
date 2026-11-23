import { describe, it, expect } from 'vitest'
import { StorageError } from '@strands-agents/sdk'
import { keyUuid, normalizeKey, normalizePrefix, scopePrefix } from '../src/keys.js'

describe('normalizeKey', () => {
  it.each([
    ['a', 'a'],
    ['a/b.md', 'a/b.md'],
    ['/a//b/', 'a/b'],
    ['a/.hidden', 'a/.hidden'],
    ['a/b..c', 'a/b..c'],
    ['a b/c', 'a b/c'],
  ])('normalizes %j to %j', (key, expected) => {
    expect(normalizeKey(key)).toBe(expected)
  })

  it.each([
    ['', 'Storage key must not be empty'],
    ['/', 'Storage key must not be empty'],
    ['a\\b', "Invalid storage key 'a\\b': backslashes are not allowed"],
    ['../a', "Invalid storage key '../a': '..' path segments are not allowed"],
    ['a/../b', "Invalid storage key 'a/../b': '..' path segments are not allowed"],
    ['a/..', "Invalid storage key 'a/..': '..' path segments are not allowed"],
  ])('rejects %j', (key, message) => {
    expect(() => normalizeKey(key)).toThrow(new StorageError(message))
  })
})

describe('normalizePrefix', () => {
  it.each([
    ['', ''],
    ['a', 'a'],
    ['a/', 'a/'],
    ['/a//b/', 'a/b/'],
    ['a_', 'a_'],
    ['a/b..c/', 'a/b..c/'],
  ])('normalizes %j to %j', (prefix, expected) => {
    expect(normalizePrefix(prefix)).toBe(expected)
  })

  it.each([
    ['a\\b', "Invalid storage prefix 'a\\b': backslashes are not allowed"],
    ['../a', "Invalid storage prefix '../a': '..' path segments are not allowed"],
    ['a/../b', "Invalid storage prefix 'a/../b': '..' path segments are not allowed"],
  ])('rejects %j', (prefix, message) => {
    expect(() => normalizePrefix(prefix)).toThrow(new StorageError(message))
  })
})

describe('scopePrefix', () => {
  it.each([
    ['', ''],
    ['/', ''],
    ['a', 'a/'],
    ['a/', 'a/'],
    ['/a//b//', 'a/b/'],
  ])('scopes %j to %j', (prefix, expected) => {
    expect(scopePrefix(prefix)).toBe(expected)
  })
})

describe('keyUuid', () => {
  // These vectors are shared with the Python package's tests.
  it.each([
    ['memory/notes.md', 'd41749b0-5ae6-53f2-8578-4760b3834bde'],
    ['agents/session/abc/snapshot_latest.json', '4babca55-bc07-54f9-a61d-2c7b19296aa4'],
  ])('derives the same id as the Python package for %j', (fullKey, expected) => {
    expect(keyUuid(fullKey)).toBe(expected)
  })
})
