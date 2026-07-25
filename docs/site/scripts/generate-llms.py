#!/usr/bin/env python3
"""
Generate llms.txt and llms-full.txt for docs.erigon.tech.

llms.txt  — page index with titles and descriptions (LLM routing)
llms-full.txt — full concatenated clean markdown (long-context LLMs)

Run from repo root:
  python3 docs/site/scripts/generate-llms.py
  python3 docs/site/scripts/generate-llms.py --check   # CI: fail if drifted

Outputs to docs/site/static/ (served at site root by Docusaurus) and to the repo
root (for tools that read raw GitHub).

Requires: Python 3.8+ (uses f-strings, Path.rglob, walrus-free).
"""

import argparse
import json
import re
import sys
from collections import Counter
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
    """Parse a minimal YAML frontmatter subset.

    Supports: simple `key: value` pairs with single- or double-quoted scalars.
    Does NOT support: YAML lists, multi-line strings (>, |), nested objects.
    Indented continuation lines are silently skipped (so a `tags:` list does
    not pollute keys, but the list value itself is lost — acceptable for the
    fields this script consumes: title, description, sidebar_position).
    """
    meta = {}
    if not text.startswith("---"):
        return meta, text
    end = text.find("\n---", 3)
    if end == -1:
        return meta, text
    block = text[3:end].strip()
    for line in block.splitlines():
        # Skip indented continuations (e.g. YAML list items, block-scalar tails).
        if line and line[0] in (" ", "\t"):
            continue
        if ":" in line:
            k, _, v = line.partition(":")
            meta[k.strip()] = v.strip().strip('"').strip("'")
    return meta, text[end + 4:]


# Pure-uppercase identifier: e.g. ERIGON_VERSION, IP, YOUR_ADDRESS, API_KEY.
# Used to preserve text-substitution placeholders that look like JSX expressions
# but are meant to be read as literal tokens by humans.
_IDENT_BRACE_INNER = re.compile(r"^\s*[A-Z][A-Z0-9_]*\s*$")


def _is_placeholder_brace(match):
    """Return True if a `{...}` match wraps a pure-uppercase identifier."""
    inner = match.group(0)[1:-1]
    return bool(_IDENT_BRACE_INNER.match(inner))


def _strip_jsx_expr(match):
    """re.sub callback: drop JSX expressions, preserve placeholder identifiers.

    If the expression contains a nullish-coalescing fallback (`?? 'value'` or
    `?? "value"`), emit that fallback string so table cells stay readable in
    the plain-text LLM artifact instead of going blank.
    """
    if _is_placeholder_brace(match):
        return match.group(0)
    inner = match.group(0)[1:-1]
    fb = re.search(r'\?\?\s*[\'"]([^\'"]+)[\'"]', inner)
    if fb:
        return fb.group(1)
    return ""


def _sub_outside_backticks(pattern, repl, line):
    """Apply re.sub only outside backtick-quoted inline code spans."""
    parts = re.split(r"(`[^`]*`)", line)
    return "".join(
        re.sub(pattern, repl, p) if not p.startswith("`") else p
        for p in parts
    )


def _strip_multiline_expr_blocks(text):
    """Strip multi-line inline JSX expression blocks that open with { on their
    own line.

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


def _strip_mdx_module_syntax(text):
    """Remove MDX import/export syntax only outside fenced code blocks."""
    lines = text.splitlines()
    out = []
    in_fence = False
    in_export_block = False

    import_re = re.compile(r"^\s*import\s+.+$")
    # Narrow: only match MDX-style exports (default / brace / const|function|class).
    # Shell `export VAR=value` inside ```bash fences is preserved by the in_fence
    # check above; this regex would not match those lines anyway, but the fence
    # guard is the load-bearing protection.
    export_single_re = re.compile(
        r"^\s*export\s+(?:default\b.+|\{.+\}\s*;?|(?:const|function|class)\b.+;?)$"
    )
    export_block_start_re = re.compile(r"^\s*export\s+(?:const|function|class)\b")
    export_block_end_re = re.compile(r"^\s*};?\s*$")

    for line in lines:
        if line.lstrip().startswith("```"):
            in_fence = not in_fence
            out.append(line)
            continue

        if in_fence:
            out.append(line)
            continue

        if in_export_block:
            if export_block_end_re.match(line):
                in_export_block = False
            continue

        if import_re.match(line):
            continue

        if export_single_re.match(line):
            continue

        if export_block_start_re.match(line):
            in_export_block = True
            continue

        out.append(line)

    return "\n".join(out)


def _split_fenced(text):
    """Split text into (is_fenced, segment) pairs, preserving newlines."""
    result = []
    buf = []
    in_fence = False
    for line in text.splitlines(keepends=True):
        if line.lstrip().startswith("```"):
            if not in_fence:
                result.append((False, "".join(buf)))
                buf = [line]
                in_fence = True
            else:
                buf.append(line)
                result.append((True, "".join(buf)))
                buf = []
                in_fence = False
        else:
            buf.append(line)
    if buf:
        result.append((in_fence, "".join(buf)))
    return result


def _apply_nonfenced(text, *funcs):
    """Apply each func only to text segments outside fenced code blocks."""
    out = []
    for is_fenced, segment in _split_fenced(text):
        if is_fenced:
            out.append(segment)
        else:
            s = segment
            for f in funcs:
                s = f(s)
            out.append(s)
    return "".join(out)


def strip_mdx(text):
    """Remove MDX-specific syntax, leaving clean prose markdown."""
    # Remove MDX import/export statements only outside fenced code blocks.
    # This preserves shell `export VAR=...` inside ```bash fences.
    text = _strip_mdx_module_syntax(text)
    # Remove JSX block comments and multi-line component tags (fence-aware).
    # [^>]* in the component regexes matches newlines (char class), consuming
    # <Link\n  prop="val"\n> in one shot without DOTALL.
    # Uppercase-then-lowercase guard keeps ALL_CAPS placeholders like <IP>, <PID>.
    text = _apply_nonfenced(
        text,
        lambda t: re.sub(r"\{/\*.*?\*/\}", "", t, flags=re.DOTALL),
        lambda t: re.sub(r"<[A-Z][a-z][a-zA-Z]*[^>]*>", "", t),
        lambda t: re.sub(r"</[A-Z][a-z][a-zA-Z]*>", "", t),
    )
    # Strip multi-line inline JSX expression blocks ({[...].map(...)} etc.) that
    # contain nested braces and defeat the single-line {0,120} pattern below.
    text = _strip_multiline_expr_blocks(text)
    # Line-by-line pass: apply remaining stripping only outside fenced blocks.
    out, in_fence = [], False
    for line in text.splitlines():
        if line.lstrip().startswith("```"):
            in_fence = not in_fence
            out.append(line)
            continue
        if in_fence:
            out.append(line)
        else:
            line = _sub_outside_backticks(r"<[A-Za-z][^>]*/?>", "", line)
            line = _sub_outside_backticks(r"</[A-Za-z][^>]*>", "", line)
            # Two passes catch {{...}} (inner first, then outer). The callback
            # preserves uppercase-identifier braces like {ERIGON_VERSION} —
            # docs authors use these as text-substitution placeholders.
            line = _sub_outside_backticks(r"\{[^}]{0,120}\}", _strip_jsx_expr, line)
            line = _sub_outside_backticks(r"\{[^}]{0,120}\}", _strip_jsx_expr, line)
            # Remove }> prefix artifact (remnant when [^>] stops at > inside =>).
            line = re.sub(r"^\s*\}\s*>", "", line)
            out.append(line)
    text = "\n".join(out)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


# Card-grid landing pages — the docs site uses these for top-level section
# index pages. After MDX stripping, they collapse to a structureless pile of
# title/desc fragments. We detect the lp-card pattern and synthesize a clean
# bullet list instead, keeping link structure intact for LLM consumption.
_LANDING_CARD_RE = re.compile(
    r'<Link\s[^>]*\bto=[\'"]([^\'"]+)[\'"][^>]*>'
    r'(?:.*?)'
    r'<div[^>]*\blp-card-title\b[^>]*>([^<]+)</div>'
    r'(?:.*?)'
    r'<div[^>]*\blp-card-desc\b[^>]*>([^<]+)</div>'
    r'(?:.*?)'
    r'</Link>',
    re.DOTALL,
)
_LANDING_HERO_RE = re.compile(
    r'<div[^>]*\blp-hero\b[^>]*>'
    r'(?:.*?)'
    r'<h1>([^<]+)</h1>'
    r'(?:.*?)'
    r'<p>([^<]+)</p>',
    re.DOTALL,
)


def synthesize_landing(raw_body):
    """If body is a card-grid landing page, emit structured markdown.

    Returns synthesized markdown, or None if no `lp-card` patterns are present.
    Card hrefs that start with `/` are rewritten to absolute BASE_URL form so
    bullets remain useful when the body is read out of context.
    """
    cards = list(_LANDING_CARD_RE.finditer(raw_body))
    if not cards:
        return None

    out = []
    hero = _LANDING_HERO_RE.search(raw_body)
    if hero:
        lead = hero.group(2).strip()
        if lead:
            out.append(lead)
            out.append("")

    out.append("## Sections")
    out.append("")
    for m in cards:
        href = m.group(1).strip()
        if href.startswith("/"):
            href = BASE_URL + href.rstrip("/")
        title = m.group(2).strip()
        desc = m.group(3).strip()
        out.append(f"- [{title}]({href}): {desc}")
    return "\n".join(out)


def file_to_url(filepath, route_prefix):
    """Convert a source file path to its deployed URL."""
    base = SITE_ROOT / (route_prefix if route_prefix else "docs")
    rel = filepath.relative_to(base)
    parts = list(rel.parts)

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
        if stripped.startswith(("#", "|", "-", "*", ">", "!")):
            continue
        # Skip reference-style link definitions: [label]: url
        if re.match(r"\[[^\]]+\]:\s", stripped):
            continue
        # Skip lines that LOOK LIKE JSX leaks (start with `<tag` or `{`, or are
        # an arrow-function expression). Plain prose mentioning these tokens
        # mid-line is allowed.
        if re.match(r"^\s*<[a-z]", stripped) or stripped.startswith("{"):
            continue
        if stripped.endswith("=>") or re.search(r"=>\s*\($", stripped):
            continue
        if len(stripped) > 40:
            plain = re.sub(r'\[([^\]]+)\]\([^)]+\)', r'\1', stripped)
            plain = re.sub(r'\[([^\]]+)\]\([^)]*$', r'\1', plain)  # unclosed links
            return plain[:200] + ("…" if len(plain) > 200 else "")
    return ""


def _read_category(dirpath):
    """Read _category_.json — returns (label, position) or ('', 99) on miss."""
    cat = dirpath / "_category_.json"
    if not cat.exists():
        return ("", 99)
    try:
        data = json.loads(cat.read_text(encoding="utf-8"))
    except Exception:
        return ("", 99)
    label = data.get("label", "")
    try:
        position = int(data.get("position", 99))
    except (TypeError, ValueError):
        position = 99
    return (label, position)


def get_category_label(dirpath):
    return _read_category(dirpath)[0]


def get_category_position(dirpath):
    return _read_category(dirpath)[1]


def ancestor_positions(filepath, base_dir):
    """Tuple of category positions from each ancestor dir, root → leaf.

    Used as a sort tier so that nested sections (depth ≥ 2) honour their own
    `_category_.json` position rather than falling back to sidebar_position
    alone. The first element matches the top-level section position.
    """
    rel = filepath.relative_to(base_dir)
    positions = []
    cur = base_dir
    for part in rel.parts[:-1]:  # exclude filename
        cur = cur / part
        positions.append(get_category_position(cur))
    return tuple(positions)


def _safe_int(value, default):
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def collect_pages(base_dir, route_prefix):
    """Walk base_dir, return list of page dicts."""
    files = sorted(set(list(base_dir.rglob("*.md")) + list(base_dir.rglob("*.mdx"))))

    pages = []
    for fpath in files:
        text = fpath.read_text(encoding="utf-8")
        meta, body = parse_frontmatter(text)

        # Card-grid landing pages: synthesize structured bullets BEFORE strip_mdx
        # so we still have the original JSX to extract titles/hrefs from.
        synth = synthesize_landing(body)
        clean_body = synth if synth is not None else strip_mdx(body)

        title = meta.get("title", "")
        if not title:
            m = re.search(r"^#\s+(.+)$", clean_body, re.MULTILINE)
            title = m.group(1).strip() if m else fpath.stem.replace("-", " ").title()

        description = meta.get("description", "") or first_description(clean_body)
        url = file_to_url(fpath, route_prefix)
        position = _safe_int(meta.get("sidebar_position", 50), 50)

        rel = fpath.relative_to(base_dir)
        section_dir = base_dir / rel.parts[0] if len(rel.parts) > 1 else base_dir
        section_label = get_category_label(section_dir) if len(rel.parts) > 1 else ""
        section_pos = get_category_position(section_dir) if len(rel.parts) > 1 else 0
        anc_pos = ancestor_positions(fpath, base_dir)

        pages.append({
            "title":           title,
            "description":     description,
            "url":             url,
            "position":        position,
            "section":         section_label,
            "section_pos":     section_pos,
            "ancestor_pos":    anc_pos,
            "depth":           len(rel.parts),
            "body":            clean_body,
            "fpath":           str(fpath),
        })

    return pages


# ── Build ─────────────────────────────────────────────────────────────────────

def build():
    """Return (llms_txt, llms_full_txt, n_pages)."""
    all_pages = []
    for label, base_dir, route_prefix in SECTIONS:
        pages = collect_pages(base_dir, route_prefix)
        for p in pages:
            p["instance"] = label
        all_pages.extend(pages)

    # Sort: docs instance first, then by ancestor category positions (deep-aware),
    # then depth (shallow before deep), then index files before siblings, then
    # sidebar_position, then path for stable order.
    def sort_key(p):
        is_help = p["instance"] != "docs"
        is_not_index = Path(p["fpath"]).stem != "index"
        return (
            is_help,
            p["ancestor_pos"],
            p["depth"],
            is_not_index,
            p["position"],
            p["fpath"],
        )

    all_pages.sort(key=sort_key)

    # Resolve display section for each page (real label, or per-instance fallback).
    def display_section(p):
        return p["section"] or (
            "Erigon Docs" if p["instance"] == "docs" else "Help Center"
        )

    section_counts = Counter(display_section(p) for p in all_pages)

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
        section = display_section(p)
        is_singleton = section_counts[section] == 1

        if section != current_section:
            if not is_singleton:
                if current_section is not None:
                    lines.append("")
                lines.append(f"## {section}")
                lines.append("")
            elif current_section is not None:
                # Separate singleton bullet from the previous section visually.
                lines.append("")
            current_section = section

        # Strip markdown links from description so it reads as plain text.
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
        # matches the title, which would produce a duplicate heading.
        body = re.sub(r"^#\s+[^\n]+\n?", "", p["body"].lstrip("\n"), count=1)
        full_parts.append(body)
        full_parts.append("")
        full_parts.append("---")
        full_parts.append("")

    llms_full_txt = "\n".join(full_parts)

    return llms_txt, llms_full_txt, len(all_pages)


def write_outputs(llms_txt, llms_full_txt):
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    (OUT_DIR / "llms.txt").write_text(llms_txt, encoding="utf-8")
    (OUT_DIR / "llms-full.txt").write_text(llms_full_txt, encoding="utf-8")
    (REPO_ROOT / "llms.txt").write_text(llms_txt, encoding="utf-8")
    (REPO_ROOT / "llms-full.txt").write_text(llms_full_txt, encoding="utf-8")


def check_outputs(llms_txt, llms_full_txt):
    """Compare regenerated content to on-disk files. Return list of stale paths."""
    targets = [
        (OUT_DIR / "llms.txt",        llms_txt),
        (OUT_DIR / "llms-full.txt",   llms_full_txt),
        (REPO_ROOT / "llms.txt",      llms_txt),
        (REPO_ROOT / "llms-full.txt", llms_full_txt),
    ]
    stale = []
    for path, expected in targets:
        try:
            actual = path.read_text(encoding="utf-8")
        except FileNotFoundError:
            actual = ""
        if actual != expected:
            stale.append(path)
    return stale


def main():
    parser = argparse.ArgumentParser(description="Generate llms.txt artifacts.")
    parser.add_argument(
        "--check",
        action="store_true",
        help="Exit non-zero if regenerated content differs from committed files. "
             "For CI use; does not write any files.",
    )
    args = parser.parse_args()

    llms_txt, llms_full_txt, n_pages = build()

    if args.check:
        stale = check_outputs(llms_txt, llms_full_txt)
        if stale:
            print("ERROR: regenerated content differs from committed files:", file=sys.stderr)
            for path in stale:
                print(f"  {path}", file=sys.stderr)
            print("Run: python3 docs/site/scripts/generate-llms.py", file=sys.stderr)
            sys.exit(1)
        print(f"OK: 4 llms files match regenerated content ({n_pages} pages)")
        return

    write_outputs(llms_txt, llms_full_txt)

    WARN_BYTES = 1_500_000  # 1.5 MB
    full_bytes = len(llms_full_txt.encode("utf-8"))
    size_note = (
        f"  ⚠  WARNING: {full_bytes:,} bytes exceeds {WARN_BYTES:,} — LLMs may truncate"
        if full_bytes > WARN_BYTES else ""
    )

    print(f"llms.txt      {len(llms_txt.encode('utf-8')):>8,} bytes  {n_pages} pages")
    print(f"llms-full.txt {full_bytes:>8,} bytes{size_note}")
    print(f"→ written to {OUT_DIR} and {REPO_ROOT}")
    print()
    print("--- llms.txt preview (first 80 lines) ---")
    for line in llms_txt.splitlines()[:80]:
        print(line)


if __name__ == "__main__":
    main()
