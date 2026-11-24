# strands-weaviate

A [Weaviate](https://weaviate.io) `Storage` backend for
[Strands Agents](https://strandsagents.com): sessions, offloaded context and agent memory in
one Weaviate collection, with built-in hybrid (BM25 + vector) search.

| Language | Package | Status | Docs |
|---|---|---|---|
| Python | `strands-weaviate` | Install from source; not on PyPI yet | [python/](python/README.md) |
| TypeScript | `strands-weaviate` | Install from source; not on npm yet | [typescript/](typescript/README.md) |

Both packages use the same collection schema and derive the same object id for a key, so one
collection can be shared across the two SDKs.

## Install from source

Python, straight from GitHub:

```bash
pip install "strands-weaviate @ git+https://github.com/dudanogueira/strands-weaviate.git#subdirectory=python"
```

TypeScript, from a clone:

```bash
git clone https://github.com/dudanogueira/strands-weaviate.git
cd strands-weaviate/typescript && npm ci && npm pack
# then, in your project:
npm install /path/to/strands-weaviate/typescript/strands-weaviate-0.0.0.tgz @strands-agents/sdk weaviate-client
```

See each package's README for details.

## Local Weaviate

```bash
docker compose up -d
```

This is a community-maintained package, created from the
[Strands extension template](https://github.com/strands-agents/extension-template). It is not
an official Weaviate or Strands Agents product.

## License

BSD 3-Clause. See [LICENSE](LICENSE).
