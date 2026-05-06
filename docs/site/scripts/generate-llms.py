#!/usr/bin/env python3
"""
Generate llms.txt and llms-full.txt for docs.erigon.tech.

llms.txt  — page index with titles and descriptions (LLM routing)
llms-full.txt — full concatenated clean markdown (long-context LLMs)

Run from repo root:
  python3 docs/site/scripts/generate-llms.py

Outputs to docs/site/static/ (served at site root by Docusaurus).
"""

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


def _sub_outside_backticks(pattern, repl, line):
    """Apply re.sub only outside backtick-quoted inline code spans."""
    parts = re.split(r"(`[^`]*`)", line)
    return "".join(
        re.sub(pattern, repl, p) if not p.startswith("`") else p
        for p in parts
    )


def _strip_multiline_expr_blocks(text):
    """Strip multi-line inline JSX expression blocks that open with { on their own line.

    Handles constructs like:
        {[
          {stat:'10x', label:'...'},
        ].map(({stat, label}) => (
          <div key={stat}>...</div>
        ))}
    which contain nested braces that defeat the single-line {0,120} pass.
    Tracks brace depth; skips lines inside code fences.
    """
    out = []
    in_fence = False
    skip_depth = 0

    for line in text.splitlines():
        if line.lstrip().startswith("```"):
            if skip_depth == 0:
                in_fence = not in_fence
                out.append(line)
            # else: fence marker is inside a skipped block — discard it
            continue

        if in_fence:
            out.append(line)
            continue

        if skip_depth > 0:
            for ch in line:
                if ch == '{':
                    skip_depth += 1
                elif ch == '}':
                    skip_depth -= 1
            if skip_depth < 0:
                skip_depth = 0
            continue  # discard this line (part of expression block)

        # Detect opening of a multi-line expression block: line starts with {
        if line.lstrip().startswith('{'):
            depth = sum(1 if c == '{' else -1 if c == '}' else 0 for c in line)
            if depth > 0:
                skip_depth = depth
                continue  # discard opening line

        out.append(line)

    return "\n".join(out)


def strip_mdx(text):
    """Remove MDX-specific syntax, leaving clean prose markdown."""
    # Remove single-line import statements
    text = re.sub(r"^import\s+.+$", "", text, flags=re.MULTILINE)
    # Remove multi-line export const/function blocks ending with };
    # The single-line pass below only catches the opening line; this handles the body.
    text = re.sub(r"^export\b.*?^};\s*$\n?", "", text, flags=re.MULTILINE | re.DOTALL)
    # Remove any remaining single-line export statements
    text = re.sub(r"^export\s+.+$", "", text, flags=re.MULTILINE)
    # Remove {/* comments */} — JSX block comments don't appear inside code fences
    text = re.sub(r"\{/\*.*?\*/\}", "", text, flags=re.DOTALL)
    # Pre-pass: strip multi-line JSX component opening/closing tags before
    # line-by-line processing. [^>]* matches newlines (char class, not .), so
    # <Link\n  prop="val"\n> is consumed in one shot without needing DOTALL.
    # Requires lowercase second letter ([A-Z][a-z]) so that ALL_CAPS placeholders
    # like <IP>, <PID>, <DOWNLOADED_FILE_NAME> in code fences are NOT stripped.
    text = re.sub(r"<[A-Z][a-z][a-zA-Z]*[^>]*>", "", text)
    text = re.sub(r"</[A-Z][a-z][a-zA-Z]*>", "", text)
    # Pre-pass: strip multi-line inline JSX expression blocks that start with {
    # on their own line (e.g. {[...].map(...)}). These contain nested braces that
    # defeat the single-line {0,120} pattern used in the loop below. The brace
    # counter runs outside code fences only.
    text = _strip_multiline_expr_blocks(text)
    # Line-by-line pass: apply remaining stripping only outside fenced code blocks.
    out, in_fence = [], False
    for line in text.splitlines():
        if line.lstrip().startswith("```"):
            in_fence = not in_fence
            out.append(line)
            continue
        if in_fence:
            # Preserve lines inside code fences completely unchanged
            out.append(line)
        else:
            # Strip lowercase HTML tags and JSX expressions, backtick-aware so
            # inline code spans like `<YOUR_ADDRESS>` and `{ERIGON_VERSION}` survive.
            line = _sub_outside_backticks(r"<[A-Za-z][^>]*/?>", "", line)
            line = _sub_outside_backticks(r"</[A-Za-z][^>]*>", "", line)
            # Apply {expr} stripping twice: first pass handles inner brace of {{...}},
            # second pass catches the outer {} left behind. {0,120} also strips {}.
            line = _sub_outside_backticks(r"\{[^}]{0,120}\}", "", line)
            line = _sub_outside_backticks(r"\{[^}]{0,120}\}", "", line)
            # Remove }> prefix artifact: remnant when [^>] stops at > inside => arrow
            # functions in JSX attributes (e.g. onClick={e => {...}}>).
            line = re.sub(r"^\s*\}\s*>", "", line)
            out.append(line)
    text = "\n".join(out)
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
            data = json.loads(cat.read_text(encoding="utf-8"))
            return data.get("label", "")
        except Exception:
            pass
    return ""


def get_category_position(dirpath):
    cat = dirpath / "_category_.json"
    if cat.exists():
        try:
            data = json.loads(cat.read_text(encoding="utf-8"))
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
    for fpath in sorted(base_dir.rglob("*.md")):
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
            "depth":       len(rel.parts),  # nesting depth from base_dir
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

    # Sort: docs instance first, then section_pos, then depth (shallow before deep),
    # then index files before siblings, then sidebar_position.
    def sort_key(p):
        is_help = p["instance"] != "docs"
        is_not_index = Path(p["fpath"]).stem != "index"  # 0 = index leads at each depth
        return (is_help, p["section_pos"], p["depth"], is_not_index, p["position"])

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
        # Strip a leading H1 from the body — many docs start with an H1 that
        # matches the title, which would produce a duplicate heading in the export.
        body = re.sub(r"^#\s+[^\n]+\n?", "", p["body"].lstrip("\n"), count=1)
        full_parts.append(body)
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
