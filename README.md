# strands-weaviate

A [Weaviate](https://weaviate.io) `Storage` backend for
[Strands Agents](https://strandsagents.com): sessions, offloaded context and agent memory in
one Weaviate collection, with built-in hybrid (BM25 + vector) search.

| Language | Package | Status | Docs |
|---|---|---|---|
| Python | `strands-weaviate` (PyPI) | Implemented, not yet published | [python/](python/README.md) |
| TypeScript | `strands-weaviate` (npm) | Implemented, not yet published | [typescript/](typescript/README.md) |

Both packages use the same collection schema and derive the same object id for a key, so one
collection can be shared across the two SDKs.

## Local Weaviate

```bash
docker compose up -d
```

This is a community-maintained package, created from the
[Strands extension template](https://github.com/strands-agents/extension-template). It is not
an official Weaviate or Strands Agents product.

## License

BSD 3-Clause. See [LICENSE](LICENSE).
