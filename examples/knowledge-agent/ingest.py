"""Ingest the markdown files in a folder into Weaviate, one entry per section.

    python ingest.py              # ingests ./content
    python ingest.py path/to/docs
    TENANT=acme python ingest.py  # into another tenant

Running it again overwrites the same keys, so it is safe to re-run after editing the files.
"""

import asyncio
import re
import sys
from pathlib import Path

from common import KNOWLEDGE_BASE, TENANT, connect, open_storage


def sections(path: Path) -> list[tuple[str, str]]:
    """Split a markdown file on its ``##`` headings into (key, text) entries."""
    text = path.read_text(encoding="utf-8")
    title_match = re.search(r"^# (.+)$", text, flags=re.MULTILINE)
    title = title_match.group(1).strip() if title_match else path.stem

    entries = []
    for block in re.split(r"^## ", text, flags=re.MULTILINE)[1:]:
        heading, _, body = block.partition("\n")
        slug = re.sub(r"[^a-z0-9]+", "-", heading.lower()).strip("-")
        # The title goes into the text so each entry stands on its own when retrieved.
        entries.append((f"{path.stem}--{slug}.md", f"# {title}: {heading.strip()}\n\n{body.strip()}\n"))
    return entries


async def main(folder: Path) -> None:
    client = connect()
    try:
        knowledge = open_storage(client).namespace(f"memory/{KNOWLEDGE_BASE}")
        count = 0
        for path in sorted(folder.glob("*.md")):
            for key, text in sections(path):
                await knowledge.write(key, text.encode("utf-8"))
                count += 1
                print(f"  stored {key}")
        print(f"\nIngested {count} sections from {folder} into tenant '{TENANT}'.")
    finally:
        client.close()


if __name__ == "__main__":
    asyncio.run(main(Path(sys.argv[1]) if len(sys.argv) > 1 else Path(__file__).parent / "content"))
