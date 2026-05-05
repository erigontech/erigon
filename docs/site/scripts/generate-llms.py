#!/usr/bin/env python3
"""
Generate llms.txt and llms-full.txt for docs.erigon.tech.

llms.txt  — page index with titles and descriptions (LLM routing)
llms-full.txt — full concatenated clean markdown (long-context LLMs)

Run from repo root:
  python3 docs/site/scripts/generate-llms.py

Outputs to docs/site/static/ (served at site root by Docusaurus).
"""

import os
import re
import json
from pathlib import Path

# ── Config ────────────────────────────────────────────────────────────────────

REPO_ROOT   = Path(__file__).parent.parent.parent.parent  # erigon repo root
SITE_ROOT   = REPO_ROOT / "docs" / "site"
DOCS_DIR    = SITE_ROOT / "docs"
HELP_DIR    = SITE_ROOT / "help-center"
OUT_DIR     = SITE_ROOT / "static"
BASE_URL    = "https://docs.erigon.tech"

# Sections in display order (label → routeBasePath prefix)
SECTIONS = [
    ("docs",        DOCS_DIR,  ""),
    ("help-center", HELP_DIR,  "help-center"),
]

# ── Helpers ───────────────────────────────────────────────────────────────────

def parse_frontmatter(text):
    """Return (meta_dict, body_without_frontmatter)."""
    meta = {}
    if not text.startswith("---"):
        return meta, text
    end = text.find("\n---", 3)
    if end == -1:
        return meta, text
    block = text[3:end].strip()
    for line in block.splitlines():
        if ":" in line:
            k, _, v = line.partition(":")
            meta[k.strip()] = v.strip().strip('"').strip("'")
    return meta, text[end + 4:]


def strip_mdx(text):
    """Remove MDX-specific syntax, leaving clean prose markdown."""
    # Remove import/export statements
    text = re.sub(r"^(import|export)\s+.+$", "", text, flags=re.MULTILINE)
    # Strip all HTML/JSX tags (both PascalCase components and lowercase html)
    text = re.sub(r"<[A-Za-z][^>]*/?>", "", text)
    text = re.sub(r"</[A-Za-z][^>]*>", "", text)
    # Remove {/* comments */}
    text = re.sub(r"\{/\*.*?\*/\}", "", text, flags=re.DOTALL)
    # Remove leftover JSX expressions like {foo.bar}
    text = re.sub(r"\{[^}]{1,120}\}", "", text)
    # Collapse 3+ blank lines to 2
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def file_to_url(filepath, route_prefix):
    """Convert a source file path to its deployed URL."""
    # Make path relative to the instance base dir (docs/ or help-center/)
    base = SITE_ROOT / (route_prefix if route_prefix else "docs")
    rel = filepath.relative_to(base)
    parts = list(rel.parts)

    # Drop filename extension and handle index files
    stem = re.sub(r"\.(md|mdx)$", "", parts[-1])
    if stem == "index":
        parts = parts[:-1]
    else:
        parts = parts[:-1] + [stem]

    if route_prefix:
        path = "/" + route_prefix + ("/" + "/".join(parts) if parts else "")
    else:
        path = ("/" + "/".join(parts)) if parts else "/"

    return BASE_URL + path


def first_description(body):
    """Extract first non-empty prose paragraph (not a heading, code, or table)."""
    in_code = False
    for line in body.splitlines():
        stripped = line.strip()
        if stripped.startswith("```"):
            in_code = not in_code
            continue
        if in_code:
            continue
        if not stripped:
            continue
        if stripped.startswith(("#", "|", "-", "*", ">", "!", "[")):
            continue
        # Skip lines that look like leftover code/JSX artifacts
        if re.search(r"(const|let|var|function|=>|\{|\}|<[a-z])", stripped):
            continue
        if len(stripped) > 40:
            # Strip markdown links before truncating to avoid broken [text](/url…
            plain = re.sub(r'\[([^\]]+)\]\([^)]+\)', r'\1', stripped)
            plain = re.sub(r'\[([^\]]+)\]\([^)]*$', r'\1', plain)  # unclosed links
            return plain[:200] + ("…" if len(plain) > 200 else "")
    return ""


def get_category_label(dirpath):
    """Read _category_.json label if present."""
    cat = dirpath / "_category_.json"
    if cat.exists():
        try:
            data = json.loads(cat.read_text())
            return data.get("label", "")
        except Exception:
            pass
    return ""


def get_category_position(dirpath):
    cat = dirpath / "_category_.json"
    if cat.exists():
        try:
            data = json.loads(cat.read_text())
            return int(data.get("position", 99))
        except Exception:
            pass
    return 99


def collect_pages(base_dir, route_prefix):
    """
    Walk base_dir, return list of page dicts sorted by sidebar_position.
    Groups by top-level section directory.
    """
    # Gather all files with metadata
    files = []
    for fpath in sorted(base_dir.rglob("*.md")) :
        files.append(fpath)
    for fpath in sorted(base_dir.rglob("*.mdx")):
        files.append(fpath)

    pages = []
    for fpath in sorted(set(files)):
        text = fpath.read_text(encoding="utf-8")
        meta, body = parse_frontmatter(text)
        clean_body = strip_mdx(body)
        title = meta.get("title", "")
        if not title:
            # Fall back to first H1
            m = re.search(r"^#\s+(.+)$", clean_body, re.MULTILINE)
            title = m.group(1).strip() if m else fpath.stem.replace("-", " ").title()

        description = meta.get("description", "") or first_description(clean_body)
        url = file_to_url(fpath, route_prefix)
        position = int(meta.get("sidebar_position", 50))

        # Determine top-level section
        rel = fpath.relative_to(base_dir)
        section_dir = base_dir / rel.parts[0] if len(rel.parts) > 1 else base_dir
        section_label = get_category_label(section_dir) if len(rel.parts) > 1 else ""
        section_pos = get_category_position(section_dir) if len(rel.parts) > 1 else 0

        pages.append({
            "title":       title,
            "description": description,
            "url":         url,
            "position":    position,
            "section":     section_label,
            "section_pos": section_pos,
            "body":        clean_body,
            "fpath":       str(fpath),
        })

    return pages


# ── Build ─────────────────────────────────────────────────────────────────────

def build():
    all_pages = []
    for label, base_dir, route_prefix in SECTIONS:
        pages = collect_pages(base_dir, route_prefix)
        for p in pages:
            p["instance"] = label
        all_pages.extend(pages)

    # Sort: docs instance first, then section_pos, then sidebar_position within section.
    # Index files (position=1 or stem=="index") always lead their section.
    def sort_key(p):
        is_help = p["instance"] != "docs"
        is_index = p["position"] <= 1 and p["section"] == ""
        return (is_help, p["section_pos"], is_index, p["position"])

    all_pages.sort(key=sort_key)

    # ── llms.txt ──────────────────────────────────────────────────────────────
    lines = []
    lines.append("# Erigon")
    lines.append("")
    lines.append("> Erigon is a high-performance Ethereum execution client known for its efficiency,")
    lines.append("> modularity, and minimal disk footprint. It features an integrated consensus layer")
    lines.append("> (Caplin), BitTorrent-based historical data distribution, and fast node synchronization.")
    lines.append("")

    current_section = None
    for p in all_pages:
        section = p["section"] or (
            "Erigon Docs" if p["instance"] == "docs" else "Help Center"
        )
        if section != current_section:
            if current_section is not None:
                lines.append("")
            lines.append(f"## {section}")
            lines.append("")
            current_section = section

        # Strip markdown links from description so it reads as plain text
        desc_plain = re.sub(r'\[([^\]]+)\]\([^)]+\)', r'\1', p["description"])
        desc = f": {desc_plain}" if desc_plain else ""
        lines.append(f"- [{p['title']}]({p['url']}){desc}")

    lines.append("")
    lines.append("## Source")
    lines.append("")
    lines.append("- [GitHub Repository](https://github.com/erigontech/erigon): Main source code repository")
    lines.append("- [Releases](https://github.com/erigontech/erigon/releases): Download binaries and release notes")
    lines.append("- [Docker Hub](https://hub.docker.com/r/erigontech/erigon): Official Docker images")

    llms_txt = "\n".join(lines) + "\n"

    # ── llms-full.txt ─────────────────────────────────────────────────────────
    full_parts = []
    for p in all_pages:
        full_parts.append(f"# {p['title']}")
        full_parts.append(f"URL: {p['url']}")
        full_parts.append("")
        full_parts.append(p["body"])
        full_parts.append("")
        full_parts.append("---")
        full_parts.append("")

    llms_full_txt = "\n".join(full_parts)

    # ── Write ─────────────────────────────────────────────────────────────────
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    # Docusaurus static dir (served at site root)
    (OUT_DIR / "llms.txt").write_text(llms_txt, encoding="utf-8")
    (OUT_DIR / "llms-full.txt").write_text(llms_full_txt, encoding="utf-8")

    # Repo root — exact copies, for LLMs/tools that read the GitHub repo directly
    (REPO_ROOT / "llms.txt").write_text(llms_txt, encoding="utf-8")
    (REPO_ROOT / "llms-full.txt").write_text(llms_full_txt, encoding="utf-8")

    WARN_BYTES = 1_500_000  # 1.5 MB
    full_bytes = len(llms_full_txt.encode("utf-8"))
    size_note = f"  ⚠  WARNING: {full_bytes:,} bytes exceeds {WARN_BYTES:,} — LLMs may truncate" if full_bytes > WARN_BYTES else ""

    print(f"llms.txt      {len(llms_txt.encode('utf-8')):>8,} bytes  {len(all_pages)} pages")
    print(f"llms-full.txt {full_bytes:>8,} bytes{size_note}")
    print(f"→ written to {OUT_DIR} and {REPO_ROOT}")
    print()
    print("--- llms.txt preview (first 80 lines) ---")
    for line in llms_txt.splitlines()[:80]:
        print(line)


if __name__ == "__main__":
    build()
