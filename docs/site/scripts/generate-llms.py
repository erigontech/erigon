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
from html.parser import HTMLParser
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

_FM_END_RE = re.compile(r"\n---[ \t]*(?:\r?\n|\r?\Z)")


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
    # `---` has to be the whole line: `find("\n---")` also matches `\n----` or
    # `\n---x`, cutting the block short at a value that merely starts that way.
    m = _FM_END_RE.search(text, 3)
    if not m:
        return meta, text
    end = m.start()
    block = text[3:end].strip()
    for line in block.splitlines():
        # Skip indented continuations (e.g. YAML list items, block-scalar tails).
        if line and line[0] in (" ", "\t"):
            continue
        if ":" in line:
            k, _, v = line.partition(":")
            meta[k.strip()] = v.strip().strip('"').strip("'")
    return meta, text[m.end():]


# ── Page text from the built site ─────────────────────────────────────────────
#
# Page bodies are read from the HTML Docusaurus produced, not from the MDX
# source. The source is MDX plus arbitrary JSX, and reproducing what MDX renders
# means reimplementing CommonMark block parsing (fences inside containers, code
# spans versus block boundaries, HTML blocks) plus JSX. Docusaurus has already
# done that work by build time: components are expanded, links are resolved to
# absolute site paths, and the page body sits in one <article>.
#
# Requires `npm run build` in docs/site first. The generator says so rather than
# silently emitting an empty corpus.

BUILD_DIR = SITE_ROOT / "build"

# Rendered inside <article> but navigation, not page content. These are the
# stable `theme-*` class names Docusaurus documents, matched as whole class
# tokens — the sibling hashed names (`breadcrumbsContainer_Z_bl`) change between
# builds, and the content itself lives under `theme-doc-markdown`, so a prefix
# match on `theme-doc-` would delete the page.
_CHROME_CLASSES = (
    "theme-doc-breadcrumbs",
    "theme-doc-toc-mobile",
    "theme-doc-toc-desktop",
    "theme-doc-footer",
    "theme-doc-version-banner",
    "theme-doc-version-badge",
    "pagination-nav",
    # The ¶-style anchor Docusaurus injects into every heading.
    "hash-link",
)
# The admonition heading holds either the type as a bare word ("warning"), which
# `:::type` already carries, or a custom title from `:::tip Some title`. It is
# captured rather than skipped, and kept only when it says something the marker
# does not.
_ADM_HEADING_PREFIX = "admonitionHeading_"

_BLOCK_TAGS = {
    "p", "div", "section", "article", "ul", "ol", "table", "thead", "tbody",
    "blockquote", "pre", "figure", "hr", "h1", "h2", "h3", "h4", "h5", "h6",
    "dl", "dt", "dd", "details", "summary", "figcaption",
}
_INLINE_WRAP = {"strong": "**", "b": "**", "em": "*", "i": "*", "summary": "**"}
# Stands in for an inline code span's opening delimiter until its interior is
# known, since a span holding a backtick needs a longer one.
_CODE_MARK = "\x00CODE\x00"
# HTMLParser reports these through handle_starttag with no matching end tag, so
# they must not change nesting depth or open a skipped subtree.
_VOID_TAGS = {
    "area", "base", "br", "col", "embed", "hr", "img", "input", "link", "meta",
    "param", "source", "track", "wbr",
}


class _ArticleText(HTMLParser):
    """Render a Docusaurus <article> subtree as Markdown-ish plain text.

    Chrome subtrees are skipped whole. A link keeps its resolved href, which is
    what makes the corpus usable: the MDX source carries relative targets like
    `../fundamentals/downloader`, meaningless once the page text is pooled into a
    flat file.
    """

    def __init__(self, page_url=""):
        super().__init__(convert_charrefs=True)
        # A bare `#section` target is ambiguous once page text is pooled into one
        # flat file, so it is resolved against the page it came from.
        self._page_url = page_url
        self.out = []
        self._skip_depth = 0      # >0 while inside a chrome subtree
        self._href = None
        self._link_segs = []
        self._link_cur = []
        self._in_pre = 0
        self._pre_start = 0
        self._at_list_marker = False
        self._table_depth = 0
        self._row_cells = 0
        self._need_header_rule = False
        self._table_body_seen = False
        self._in_cell = False
        self._list_stack = []
        # One entry per open <li>: the spaces its own content is indented by.
        self._cont = []
        # Element nesting depth, kept so a container that spans other elements
        # (admonition, blockquote) knows which end tag is its own.
        self._depth = 0
        self._adm_stack = []
        self._bq_stack = []
        # Docusaurus renders a <Tabs> group as a list of labels followed by one
        # panel each, with no id linking them. The labels are held back and
        # attached to their panels in order, or the tables under them read as an
        # unlabelled run and a reader has to guess which chain is which.
        self._adm_head = []
        self._tablist = []
        self._tab_labels = []
        self._li_start = None

    # -- helpers ----------------------------------------------------------
    def _emit(self, s):
        if self._skip_depth:
            return
        (self._link_cur if self._href is not None else self.out).append(s)

    def _break(self, n=2):
        if self._skip_depth:
            return
        if self._in_pre:
            # Docusaurus wraps each code line in a <div class="token-line"> that
            # already ends with <br>. That <br> supplies the newline, so a block
            # break here would double-space every code block.
            return
        if self._href is not None:
            # Checked before the cell case: a card link inside a table cell still
            # has to segment, or its label and blurb fuse into one word.
            seg = "".join(self._link_cur).strip()
            if seg:
                self._link_segs.append(seg)
            self._link_cur = []
            return
        if self._in_cell:
            # A cell is one Markdown line; a <p> or <ul> inside it must not split
            # the row into two.
            if self.out and not self.out[-1].endswith((" ", "|")):
                self.out.append(" ")
            return
        if self._at_list_marker:
            # The item's first block starts on the marker's line, not below it.
            self._at_list_marker = False
            return
        # Every block inside a list item has to start at the item's content
        # column, or CommonMark closes the item and the nested fence, paragraph
        # or sub-list reads as a sibling at top level.
        while self.out and self.out[-1] == "\n":
            self.out.pop()
        self.out.append("\n" * n)

    # -- HTMLParser hooks -------------------------------------------------
    def handle_starttag(self, tag, attrs):
        # HTMLParser gives None for a valueless attribute (`<div class>`), which
        # would fail every string operation below.
        a = {k: _strip_markers(v or "") for k, v in attrs}
        void = tag in _VOID_TAGS
        if not void:
            self._depth += 1
        if self._skip_depth:
            if not void:
                self._skip_depth += 1
            return
        # Whole class tokens: a substring test would skip real content whose
        # class merely contains one of these (`my-breadcrumb-note`).
        classes = a.get("class", "").split()
        if any(c in classes for c in _CHROME_CLASSES):
            if void:
                # Nothing to skip past: dropping the element itself is enough,
                # and entering skip mode here would swallow the text after it.
                return
            self._skip_depth = 1
            return
        adm = next((c[len("theme-admonition-"):] for c in classes
                    if c.startswith("theme-admonition-")), None)
        if adm and self._href is None and not self._in_cell:
            # `:::type` carries both the kind and, with its closer, the extent —
            # neither of which survives as plain prose.
            self._break()
            self._adm_stack.append([self._depth, len(self.out), adm, ""])
            return
        if self._adm_stack and any(c.startswith(_ADM_HEADING_PREFIX) for c in classes):
            self._adm_head.append((self._depth, len(self.out)))
            return
        if tag == "blockquote" and self._href is None:
            # A quote is a distinct block in the rendered page; without the `> `
            # prefix it reads as the page's own prose.
            self._break()
            self._bq_stack.append((self._depth, len(self.out)))
            return
        if tag == "a" and a.get("href"):
            self._href, self._link_segs, self._link_cur = a["href"], [], []
            return
        if tag == "pre":
            if self._href is not None:
                # Inside a link there is nowhere to put a fence; keep the text.
                self._in_pre += 1
                return
            # A fence has to open its own line. _break() would otherwise return
            # early on _at_list_marker, which is there so an item's first
            # paragraph shares the marker's line — right for prose, wrong for a
            # fence, which would fuse into "- Step one```".
            first = self._at_list_marker
            self._at_list_marker = False
            # One newline, not a block break, when the marker line is still
            # empty: an item may begin with at most one blank line, so a blank
            # here would end the item and leave the fence at top level
            # (CommonMark 5.2).
            self._break(1 if first else 2)
            self._in_pre += 1
            self._pre_start = len(self.out)
            # Docusaurus puts the info string on the block as `language-bash`.
            # Dropping it costs a consumer the one hint that says whether a block
            # is shell, JSON or YAML.
            lang = next((c[len("language-"):] for c in classes
                         if c.startswith("language-")), "")
            self.out.append("\x00FENCE\x00" + lang + "\n")
            return
        if tag in ("h1", "h2", "h3", "h4", "h5", "h6"):
            self._break()
            if self._href is None:
                self.out.append("#" * int(tag[1]) + " ")
            return
        if tag == "table":
            self._table_depth += 1
            self._need_header_rule = False
            self._break()
            return
        if tag in ("thead", "tbody", "tfoot"):
            if tag == "thead":
                self._need_header_rule = True
            return
        if tag == "tr" and self._table_depth and self._href is None:
            self._break(1)
            self.out.append("|")
            self._row_cells = 0
            return
        if tag in ("td", "th") and self._table_depth and self._href is None:
            if tag == "th" and self._row_cells == 0 and not self._table_body_seen:
                self._need_header_rule = True
            self.out.append(" ")
            self._row_cells += 1
            self._in_cell = True
            return
        if tag == "ul" and (a.get("role") == "tablist" or "tabs" in classes):
            self._tablist.append(self._depth)
            self._tab_labels = []
            self._break()
            return
        if tag == "li" and self._tablist:
            self._li_start = len(self.out)
            return
        if a.get("role") == "tabpanel" and self._tab_labels:
            self._break()
            self.out.append(f"**{self._tab_labels.pop(0)}**")
            self._break()
            return
        if tag in ("ul", "ol"):
            start = 1
            if tag == "ol" and a.get("start", "").isdigit():
                start = int(a["start"])
            # Remember how deep the continuation stack was, so closing this
            # list restores it even if an <li> never closed. HTMLParser does not
            # imply an end tag, and an unbalanced pop would indent everything
            # after the list by a level that is no longer open.
            self._list_stack.append([tag, start, len(self._cont)])
            self._break()
            return
        if tag == "li":
            # HTMLParser does not imply </li>, so a source that omits it would
            # fuse two items into one line and leave this item's column open.
            # A new item at the same list depth closes the previous one.
            if self._list_stack and len(self._cont) > self._list_stack[-1][2]:
                self._cont.pop()
                self.out.append(_POP)
                # </li> would have cleared this; without it the break below
                # returns early and the two items fuse onto one line.
                self._at_list_marker = False
            self._break(1)
            if self._href is not None:
                # A list inside a card is part of its text, not page structure.
                # Emit a zero-width level anyway so the matching </li> pop does
                # not steal an enclosing item's indentation.
                self.out.append(_PUSH % 0)
                self._cont.append(0)
                return
            if self._list_stack and self._list_stack[-1][0] == "ol":
                n = self._list_stack[-1][1]
                self._list_stack[-1][1] = n + 1
                marker = f"{n}. "
            else:
                marker = "- "
            # The marker sits at the parent item's content column; this item's
            # own content column is that plus the marker's own width, which is
            # why an ordered marker cannot use a fixed two-space step. text()
            # applies the column to every line of the item, including the ones
            # emitted from inside <pre> where _break() never runs.
            self.out.append(_PUSH % len(marker))
            self.out.append(marker)
            self._cont.append(len(marker))
            self._at_list_marker = True
            return
        if tag == "br":
            # A newline inside a table cell would split the Markdown row.
            self._emit(" " if self._table_depth else "\n")
            return
        if tag == "img":
            alt = a.get("alt", "").strip()
            if alt:
                src = a.get("src", "")
                if src.startswith("/"):
                    src = BASE_URL + src
                self._emit(f"![{alt}]({src})" if src else alt)
            return
        if tag == "code" and not self._in_pre:
            # Delimiter length is only known once the interior is in hand.
            self._emit(_CODE_MARK)
            return
        if tag in _INLINE_WRAP and not self._in_pre:
            self._emit(_INLINE_WRAP[tag])
            return
        if tag in _BLOCK_TAGS:
            self._break()

    def handle_endtag(self, tag):
        if tag in _VOID_TAGS:
            # `<br/>` reaches here through handle_startendtag. It closes nothing,
            # so letting it decrement the skip depth would end a chrome subtree
            # early and leak the nav text after it.
            return
        self._depth -= 1
        if self._skip_depth:
            self._skip_depth -= 1
            return
        if self._adm_head and self._adm_head[-1][0] - 1 == self._depth:
            _, hstart = self._adm_head.pop()
            title = " ".join("".join(self.out[hstart:]).split())
            del self.out[hstart:]
            if self._adm_stack:
                self._adm_stack[-1][3] = title
            return
        if self._adm_stack and self._adm_stack[-1][0] - 1 == self._depth:
            _, start, adm, title = self._adm_stack.pop()
            body = "".join(self.out[start:]).strip("\n")
            del self.out[start:]
            if body:
                # A heading that only repeats the type says nothing the marker
                # does not; a custom title does and has to survive.
                head = f":::{adm} {title}" if title.lower() != adm.lower() else f":::{adm}"
                self.out.append(f"{head}\n{body}\n:::")
            self._break()
            return
        if self._bq_stack and self._bq_stack[-1][0] - 1 == self._depth:
            _, start = self._bq_stack.pop()
            seg = "".join(self.out[start:]).strip("\n")
            del self.out[start:]
            if seg:
                # A `>` line is not blank, so the collapse pass in text() cannot
                # reach a run of them; squeeze them here instead.
                seg = _squeeze_blank_runs(seg)
                self.out.append("\n".join(
                    ("> " + ln) if ln.strip() else ">" for ln in seg.split("\n")))
            self._break()
            return
        if tag == "table" and self._table_depth:
            self._table_depth -= 1
            self._table_body_seen = False
            self._break()
            return
        if tag in ("thead", "tbody", "tfoot"):
            return
        if tag in ("td", "th") and self._table_depth and self._href is None:
            self._in_cell = False
            self.out.append(" |")
            return
        if tag == "tr" and self._table_depth and self._href is None:
            # A Markdown table needs the rule under its header row to read as one.
            if self._need_header_rule and self._row_cells:
                self.out.append("\n|" + " --- |" * self._row_cells)
                self._need_header_rule = False
            self._table_body_seen = True
            return
        if tag == "li" and self._tablist and self._li_start is not None:
            label = "".join(self.out[self._li_start:]).strip()
            del self.out[self._li_start:]
            self._li_start = None
            if label:
                self._tab_labels.append(label)
            return
        if tag == "ul" and self._tablist and self._tablist[-1] - 1 == self._depth:
            self._tablist.pop()
            self._break()
            return
        if tag in ("ul", "ol"):
            if self._list_stack:
                entry = self._list_stack.pop()
                if len(entry) > 2:
                    for _ in range(len(self._cont) - entry[2]):
                        self._cont.pop()
                        self.out.append(_POP)
            self._break()
            return
        if tag == "li":
            # Clear the marker flag so the next item gets its own line, without
            # forcing the blank line a full block break would add.
            self._at_list_marker = False
            if self._cont:
                self._cont.pop()
                self.out.append(_POP)
            return
        if tag == "a" and self._href is not None:
            seg = "".join(self._link_cur).strip()
            segs = self._link_segs + ([seg] if seg else [])
            href, self._href = self._href, None
            self._link_segs, self._link_cur = [], []
            if not segs:
                return
            # A link wrapping several blocks is a card: the first block is its
            # label, the rest is the blurb. A card often opens with a decorative
            # icon, and that image is not the label.
            texty = [x for x in segs if not _IMAGE_ONLY_RE.fullmatch(x)]
            segs = texty or segs
            label, rest = segs[0], " ".join(segs[1:]).strip()
            if href.startswith("/"):
                href = BASE_URL + href
            elif href.startswith("#") and self._page_url:
                href = self._page_url + href
            link = f"[{label}]({href})" if href else label
            if rest:
                # A card is a block in the rendered page, so give it its own
                # line rather than running it into the next one.
                self._break(1)
                self.out.append(f"- {link}: {rest}")
                self._break(1)
            else:
                self.out.append(link)
            return
        if tag == "pre":
            self._in_pre = max(0, self._in_pre - 1)
            if self._href is not None:
                return
            while self.out and self.out[-1].endswith("\n"):
                self.out[-1] = self.out[-1].rstrip("\n")
                if self.out[-1] == "":
                    self.out.pop()
                else:
                    break
            # A literal backtick run inside the block needs a longer fence.
            body = "".join(self.out[self._pre_start:])
            longest = max((len(m) for m in re.findall(r"`+", body)), default=0)
            fence = "`" * max(3, longest + 1)
            for i in range(self._pre_start, len(self.out)):
                self.out[i] = self.out[i].replace("\x00FENCE\x00", fence)
            self.out.append("\n" + fence)
            self._break()
            return
        if tag == "code" and not self._in_pre:
            buf = self._link_cur if self._href is not None else self.out
            try:
                i = len(buf) - 1 - buf[::-1].index(_CODE_MARK)
            except ValueError:
                # The buffer was flushed mid-span; a plain span is the best that
                # can be said about the text that survived.
                self._emit("`")
                return
            inner = "".join(buf[i + 1:])
            longest = max((len(m) for m in re.findall(r"`+", inner)), default=0)
            delim = "`" * max(1, longest + 1)
            # A span whose text touches a backtick needs a space inside the
            # delimiters, which CommonMark strips back out (6.1).
            pad = " " if inner.startswith("`") or inner.endswith("`") else ""
            buf[i] = delim + pad
            self._emit(pad + delim)
            return
        if tag in _INLINE_WRAP and not self._in_pre:
            self._emit(_INLINE_WRAP[tag])
            return
        if tag in _BLOCK_TAGS:
            self._break()

    def handle_data(self, data):
        data = _strip_markers(data)
        if self._in_cell:
            # An unescaped pipe reads as a column separator: `QUANTITY|TAG` would
            # silently widen the row.
            data = data.replace("|", "\\|")
        if not self._in_pre:
            data = re.sub(r"\s+", " ", data)
            if not data.strip():
                # Collapse inter-tag whitespace to a single separator. The buffer
                # tested has to be the one being written to, or the space between
                # two inline elements inside a link is dropped and the words fuse.
                buf = self._link_cur if self._href is not None else self.out
                if buf and not buf[-1].endswith((" ", "\n")):
                    self._emit(" ")
                return
        self._emit(data)

    @staticmethod
    def _apply_indent(s):
        """Turn the item markers into real indentation, one line at a time.

        A push or pop found on a line takes effect from the *next* line, so an
        item's marker sits at its parent's column while everything below it sits
        at its own. A line left empty once its markers are removed stays empty:
        indenting it would ship trailing whitespace.
        """
        stack, out = [], []
        for ln in s.split("\n"):
            prefix = " " * sum(stack)
            events = _STACK_RE.findall(ln)
            clean = _STACK_RE.sub("", ln)
            # A line with no content is emitted empty, inside a fence as well as
            # outside it. Keeping its spaces would be more faithful to the code's
            # own bytes, but they are trailing whitespace in a committed file and
            # `git diff --check` — a gate on this repo — rejects them.
            out.append(prefix + clean if clean.strip() else "")
            for width in events:
                if width == "":
                    if stack:
                        stack.pop()
                else:
                    stack.append(int(width))
        return "\n".join(out)

    def text(self):
        s = self._apply_indent("".join(self.out))
        # Collapse runs of whitespace inside each line but keep the leading run,
        # which is list indentation. Fenced blocks are left byte-for-byte: CLI
        # help output and directory trees are column-aligned.
        out, open_fence = [], None
        for ln in s.split("\n"):
            m = _LINE_FENCE_RE.match(ln)
            if m:
                marker = m.group(1)
                if open_fence is None:
                    open_fence = marker
                    out.append(ln)
                    continue
                # Only a closer at least as long as its opener ends the block, so
                # a ``` line inside a ```` block stays code and keeps its columns.
                if marker[0] == open_fence[0] and len(marker) >= len(open_fence):
                    open_fence = None
                    out.append(ln)
                    continue
            if open_fence:
                out.append(ln)
            elif not ln.strip():
                # A blank line inside a list item would otherwise keep the
                # item's continuation prefix and ship trailing whitespace.
                out.append("")
            else:
                out.append(re.match(r"[ \t]*", ln).group(0)
                           + re.sub(r"[ \t]+", " ", ln.strip()))
        s = "\n".join(out)
        # Collapse blank runs outside fenced blocks only; inside one they are
        # part of the code.
        # Lines are marked fenced or not, then blank runs are collapsed in a
        # single pass over the whole text. Collapsing per-chunk and re-joining
        # the chunks would put the separator back, leaving a second blank line
        # wherever a paragraph met a fence.
        fenced, open_fence = [], None
        for ln in s.split("\n"):
            m = _LINE_FENCE_RE.match(ln)
            inside = open_fence is not None
            if m:
                marker = m.group(1)
                if open_fence is None:
                    open_fence = marker
                elif marker[0] == open_fence[0] and len(marker) >= len(open_fence):
                    open_fence = None
                    inside = True          # the closer belongs to its block
            fenced.append(inside or open_fence is not None)

        lines = s.split("\n")
        kept = []
        for i, ln in enumerate(lines):
            if ln.strip() or fenced[i]:
                kept.append(ln)
                continue
            # A blank line outside a fence: keep at most one in a row.
            if kept and not kept[-1].strip() and not (fenced[i - 1] if i else False):
                continue
            kept.append(ln)
        return "\n".join(kept).strip()


_ARTICLE_OPEN_RE = re.compile(r"<article\b[^>]*>", re.IGNORECASE)
_ARTICLE_TAG_RE = re.compile(r"<article\b[^>]*>|</article\s*>", re.IGNORECASE)
_HTML_COMMENT_RE = re.compile(r"<!--.*?-->", re.DOTALL)


def _article_body(html_text):
    """Contents of the outermost <article>, or None.

    Depth-counted rather than matched with a lazy regex, which would stop at the
    first nested `</article>` and silently return a partial page as a success.
    """
    m = _ARTICLE_OPEN_RE.search(html_text)
    if not m:
        return None
    # ReactDOM emits comments, and a tag inside one is text. Counting it would
    # end the body early and return the truncated head of the page as a success.
    comments = [(c.start(), c.end()) for c in _HTML_COMMENT_RE.finditer(html_text)]

    def commented(i):
        return any(a <= i < b for a, b in comments)

    depth, pos = 1, m.end()
    for t in _ARTICLE_TAG_RE.finditer(html_text, m.end()):
        if commented(t.start()):
            continue
        depth += -1 if t.group(0).startswith("</") else 1
        if depth == 0:
            return html_text[pos:t.start()]
    return None  # unbalanced: refuse rather than return a truncated body
# A segment holding nothing but an image is a decorative icon, not a label.
_IMAGE_ONLY_RE = re.compile(r"!\[[^\]]*\]\([^)]*\)")

# List indentation is applied after the whole article is assembled, not while it
# is written: the fence body and closer inside a list item come out of <pre>,
# where _break() returns early, so nothing written there could carry a prefix.
# The parser drops these markers where an item opens and closes; _apply_indent()
# turns them into real columns on every line, code included.
_PUSH = "\x02%d\x02"
_POP = "\x03"
_STACK_RE = re.compile(r"\x02(\d+)\x02|\x03")


def _strip_markers(text):
    """Drop the marker bytes from anything the page supplied.

    They are control characters, so they carry no meaning in rendered text, but
    left in place the page could forge them: a pushed width indents everything
    after it, and a pop steals an enclosing item's column. Text, attribute
    values and spliced diagram sources all pass through here.
    """
    if "\x02" in text or "\x03" in text:
        return text.replace("\x02", "").replace("\x03", "")
    return text

# Docusaurus compiles a mermaid fence into a component that draws client-side, so
# the diagram source appears nowhere in the built HTML. It is real page content —
# a graph described in text — so it is spliced back in from the source. A fence
# with a known info string is unambiguous on its own; no block parsing needed.
# Indentation does not gate a fence here. CommonMark demotes a four-column line
# to an indented code block, but MDX has no indented code blocks at all: a
# four-space ```mermaid at top level renders as a diagram in this site's own
# production build, so rejecting it dropped the graph from the corpus silently.
# The opener's indent is still read, because its content is measured from it.
_FENCE_LINE_RE = re.compile(r"^[ \t]{0,3}(`{3,}|~{3,})[ \t]*([^\s`~]*)")
_ANY_FENCE_LINE_RE = re.compile(r"^([ \t]*)(`{3,}|~{3,})[ \t]*([^\s`~]*)")


def _fence_at(line):
    """Match a fence on `line` at any indent, with the indent it opened at."""
    m = _ANY_FENCE_LINE_RE.match(line)
    if not m:
        return None
    return m.group(2), m.group(3), len(m.group(1).expandtabs(4))


def _dedent_to(line, base):
    """Remove up to `base` columns of indent from a fenced line.

    CommonMark 4.5: when the opening fence is indented N columns, up to N are
    stripped from every content line. Leaving them on would ship the container's
    indentation into the diagram source, where whitespace is significant.
    """
    if base <= 0:
        return line
    # Only the leading run is measured, and the rest of the line is returned
    # byte-for-byte: expanding the whole line would rewrite tabs inside the
    # diagram source, where they are content rather than indentation.
    i, col = 0, 0
    while i < len(line) and col < base and line[i] in " \t":
        col += (4 - col % 4) if line[i] == "\t" else 1
        i += 1
    return line[i:]


_JSX_COMMENT_RE = re.compile(r"\{/\*.*?\*/\}", re.DOTALL)
_ATX_HEADING_RE = re.compile(r"^[ \t]{0,3}#{1,6}[ \t]+(.*?)[ \t]*#*[ \t]*$")
_QUOTE_PREFIX_RE = re.compile(r"^[ \t]{0,3}(?:>[ \t]?)+")


def _peel_quote(line):
    """Strip blockquote markers so a fence inside a quote is seen as a fence."""
    return _QUOTE_PREFIX_RE.sub("", line)


_CODE_SPAN_RE = re.compile(r"`+[^`\n]*`+")


def _mask_jsx_comments(source):
    """Blank `{/* … */}` regions, keeping line structure.

    A delimiter written inside code is prose about JSX, not a comment: the page
    documenting `{/*` and `*/}` sits either side of a live diagram, and a span
    taken between them removes content that renders. Fenced blocks and inline
    code spans are therefore hidden from the search, and only the regions found
    in what remains are blanked.
    """
    blank = lambda text: re.sub(r"[^\n]", " ", text)  # noqa: E731
    shadow, open_fence = [], None
    for line in source.split("\n"):
        m = _LINE_FENCE_RE.match(line)
        if m:
            marker = m.group(1)
            if open_fence is None:
                open_fence = marker
            elif marker[0] == open_fence[0] and len(marker) >= len(open_fence):
                open_fence = None
            shadow.append(blank(line))
            continue
        shadow.append(blank(line) if open_fence is not None
                      else _CODE_SPAN_RE.sub(lambda c: blank(c.group(0)), line))
    shadow = "\n".join(shadow)

    out = list(source)
    for match in _JSX_COMMENT_RE.finditer(shadow):
        for i in range(match.start(), match.end()):
            if out[i] != "\n":
                out[i] = " "
    return "".join(out)


def mermaid_blocks(source):
    """Fenced mermaid diagrams in an MDX source, as fenced Markdown.

    Returns (heading, occurrence, block) triples. `heading` is the text of the
    nearest ATX heading above the fence, or "" at the top of a page, and
    `occurrence` says which sighting of that text it was. The heading is what
    lets the diagram go back where it belongs: prose around it says things like
    "every box above", which is false if the diagram is appended to the page.
    The occurrence is needed because the text alone is ambiguous — three pages
    here repeat a heading within themselves, `debug.md` sixteen times.

    Fences are walked in order so a ```mermaid line inside a longer enclosing
    fence is code being displayed, not a diagram, and a closer shorter than its
    opener does not end the block (CommonMark 4.5). A fence inside a blockquote
    is a diagram too, so container markers are peeled before the fence test.
    """
    # A commented-out diagram renders nothing, so it is not page content. The
    # regions are blanked rather than removed so line structure is unchanged.
    source = _mask_jsx_comments(source)
    blocks, lines, seen = [], source.split("\n"), {}
    i, n, heading, occurrence = 0, len(lines), "", 0
    while i < n:
        h = _ATX_HEADING_RE.match(lines[i])
        opener = _peel_quote(lines[i])
        # Only a fence that was itself quoted has its body peeled: `>` is legal
        # inside a diagram, and stripping it from a top-level fence would edit
        # the diagram's own source.
        see = _peel_quote if opener != lines[i] else (lambda ln: ln)

        m = _fence_at(opener)
        if not m:
            if h:
                heading = h.group(1).strip()
                seen[heading] = seen.get(heading, 0) + 1
                occurrence = seen[heading] - 1
            i += 1
            continue
        marker, info, fence_indent = m[0], m[1].lower(), m[2]
        at, at_occurrence, body, i = heading, occurrence, [], i + 1
        while i < n:
            c = _fence_at(see(lines[i]))
            if c and c[0][0] == marker[0] and len(c[0]) >= len(marker) \
                    and not c[1]:
                break
            body.append(_dedent_to(see(lines[i]), fence_indent))
            i += 1
        i += 1
        if info == "mermaid" and body:
            text = "\n".join(body).strip("\n")
            longest = max((len(t) for t in re.findall(r"`+", text)), default=0)
            fence = "`" * max(3, longest + 1)
            # Spliced in after text() has run, so these bytes would reach the
            # output with no stack effect at all — stripped here for the same
            # reason handle_data strips them.
            # Heading text alone cannot place a diagram: three pages here
            # repeat a heading within themselves. The count must be of heading
            # sightings, not of diagrams — a section with no diagram still
            # advances the occurrence its successors are placed by.
            blocks.append((at, at_occurrence,
                           _strip_markers(f"{fence}mermaid\n{text}\n{fence}")))
    return blocks


def splice_diagram(body, heading, block, occurrence=0):
    """Insert `block` under the `occurrence`-th `heading` in `body`.

    Appends when that heading is gone. Matching the text alone put every
    diagram under the first section sharing a title.
    """
    if not heading:
        return body + "\n\n" + block

    lines = body.split("\n")
    # A `#` line inside a fence is code, not a heading: a bash `# Usage` comment
    # would otherwise match the "Usage" heading and splice the diagram into the
    # middle of the code block.
    idx, fence, hits = None, None, 0
    for i, ln in enumerate(lines):
        m = _FENCE_LINE_RE.match(ln)
        if m:
            marker = m.group(1)
            if fence is None:
                fence = marker
            elif marker[0] == fence[0] and len(marker) >= len(fence):
                fence = None
            continue
        if fence is not None:
            continue
        h = _ATX_HEADING_RE.match(ln)
        if h and h.group(1).strip() == heading:
            if hits == occurrence:
                idx = i
                break
            hits += 1
    if idx is None:
        return body + "\n\n" + block

    # Step over diagrams already spliced under this heading, so a heading
    # carrying several of them keeps them in source order.
    ins, j = idx, idx + 1
    while j < len(lines):
        if not lines[j].strip():
            j += 1
            continue
        m = _FENCE_LINE_RE.match(lines[j])
        if not (m and m.group(2).lower() == "mermaid"):
            break
        marker = m.group(1)
        j += 1
        while j < len(lines):
            c = _FENCE_LINE_RE.match(lines[j])
            j += 1
            if c and c.group(1)[0] == marker[0] and len(c.group(1)) >= len(marker) \
                    and not c.group(2):
                break
        ins = j - 1
    return "\n".join(lines[:ins + 1] + ["", block] + lines[ins + 1:])


def built_path_for(url):
    """Built HTML file for a deployed page URL, or None if absent."""
    path = url[len(BASE_URL):].strip("/")
    candidates = (
        [BUILD_DIR / "index.html"]
        if not path
        else [BUILD_DIR / f"{path}.html", BUILD_DIR / path / "index.html"]
    )
    return next((c for c in candidates if c.is_file()), None)


def page_text_from_build(url):
    """Markdown-ish text of a page's <article>.

    Returns None when the built page cannot be read at all, and "" when it reads
    but renders no text — a client-drawn-only page. The caller reports those
    differently, since "run npm run build" is the wrong advice for the second.
    """
    built = built_path_for(url)
    if built is None:
        return None
    body = _article_body(built.read_text(encoding="utf-8"))
    if body is None:
        return None
    p = _ArticleText(page_url=url)
    p.feed(body)
    p.close()
    return p.text()


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


# A fence keeps its meaning inside a blockquote, where the line already carries
# `> ` markers by the time the whitespace passes run. Without peeling them those
# passes reflow quoted code, which is column-aligned CLI output.
_LINE_FENCE_RE = re.compile(r"[ \t]*(?:>[ \t]?)*[ \t]*(`{3,}|~{3,})")


def _squeeze_blank_runs(text):
    """Collapse runs of blank lines, leaving fenced blocks untouched."""
    out, open_fence = [], None
    for ln in text.split("\n"):
        m = _LINE_FENCE_RE.match(ln)
        fenced = open_fence is not None
        if m:
            marker = m.group(1)
            if open_fence is None:
                open_fence = marker
            elif marker[0] == open_fence[0] and len(marker) >= len(open_fence):
                open_fence = None
                fenced = True
        if ln.strip() or fenced or open_fence is not None or (out and out[-1].strip()):
            out.append(ln)
    return "\n".join(out)


# `\s` spans newlines, so `#\s+` on a body opening with an empty H1 would reach
# past the blank line and delete the first real prose line with it.
_LEADING_H1_RE = re.compile(r"^#[^\S\n]+[^\n]*\n?")


def strip_leading_h1(body):
    """Drop a body's opening H1, which duplicates the page title."""
    return _LEADING_H1_RE.sub("", body.lstrip("\n"), count=1).lstrip("\n")


def first_description(body):
    """Extract first non-empty prose paragraph (not a heading, code, or table)."""
    open_fence = None
    for line in body.splitlines():
        stripped = line.strip()
        f = _FENCE_LINE_RE.match(line)
        if f:
            marker = f.group(1)
            # A shorter run inside a longer fence is displayed code, not a
            # closer, so a 4-backtick block does not desync on its interior.
            if open_fence is None:
                open_fence = marker
            elif marker[0] == open_fence[0] and len(marker) >= len(open_fence):
                open_fence = None
            continue
        if open_fence:
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
    except (OSError, ValueError) as exc:
        # Falling back silently would reorder the corpus with no way to notice.
        print(f"generate-llms: warning: cannot read {cat}: {exc}", file=sys.stderr)
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

    pages, missing, empty = [], [], []
    for fpath in files:
        text = fpath.read_text(encoding="utf-8")
        meta, _ = parse_frontmatter(text)

        # Docusaurus omits a draft page from the build, so it has no page text
        # and belongs in neither artifact.
        if str(meta.get("draft", "")).lower() == "true":
            continue

        url = file_to_url(fpath, route_prefix)
        # The body comes from the built page, so every MDX component and every
        # relative link is already resolved.
        clean_body = page_text_from_build(url)
        if clean_body is None:
            missing.append((fpath, url))
            continue

        # Splice every occurrence. Testing `diagram not in clean_body` deduped by
        # text: two identical diagrams under different headings collapsed to one,
        # and a diagram whose text also appeared in a literal example was skipped
        # entirely. mermaid_blocks() is fence-accurate, so each block it yields is
        # a real diagram that belongs under its own heading.
        for heading, occurrence, diagram in mermaid_blocks(text):
            clean_body = splice_diagram(clean_body, heading, diagram, occurrence)

        # The duplicate H1 comes off when llms-full.txt is assembled, so a page
        # holding nothing but its own heading is empty there while passing a
        # guard applied to the body with the heading still on it.
        if not strip_leading_h1(clean_body).strip():
            empty.append((fpath, url))
            continue

        title = meta.get("title", "")
        if not title:
            m = re.search(r"^#\s+(.+)$", clean_body, re.MULTILINE)
            title = m.group(1).strip() if m else fpath.stem.replace("-", " ").title()

        description = meta.get("description", "") or first_description(clean_body)
        position = _safe_int(meta.get("sidebar_position", 50), 50)

        rel = fpath.relative_to(base_dir)
        section_dir = base_dir / rel.parts[0] if len(rel.parts) > 1 else base_dir
        section_label = get_category_label(section_dir) if len(rel.parts) > 1 else ""
        anc_pos = ancestor_positions(fpath, base_dir)

        pages.append({
            "title":           title,
            "description":     description,
            "url":             url,
            "position":        position,
            "section":         section_label,
            "ancestor_pos":    anc_pos,
            "depth":           len(rel.parts),
            "body":            clean_body,
            "fpath":           str(fpath),
        })

    if empty:
        listed = "\n".join(f"  {f}  ->  {u}" for f, u in empty[:10])
        raise SystemExit(
            f"generate-llms: {len(empty)} page(s) built but render no text. A page "
            f"whose only content draws client-side (a chart, a diagram component) "
            f"contributes nothing to the corpus and needs handling here.\n{listed}"
        )
    if missing:
        listed = "\n".join(f"  {f}  ->  {u}" for f, u in missing[:10])
        more = f"\n  ... and {len(missing) - 10} more" if len(missing) > 10 else ""
        hint = (
            "docs/site/build is absent — run `npm run build` in docs/site first."
            if not BUILD_DIR.is_dir()
            else "these pages have no built HTML; check they are in the sidebar and that "
                 "the build is current."
        )
        raise SystemExit(
            f"generate-llms: {len(missing)} page(s) could not be read from the built "
            f"site.\n{hint}\n{listed}{more}"
        )

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
    # The only machine-readable route from this index to the full corpus. Pages
    # advertise llms.txt via <link rel="describedby">, not llms-full.txt, so an
    # agent that arrives here would otherwise have no way to reach it.
    lines.append(
        f"The text of every page listed below is also available as a single file: "
        f"[llms-full.txt]({BASE_URL}/llms-full.txt). Pages are cleaned for machine "
        f"reading: text comes from the built site, so components are expanded and "
        f"links resolved, and a card-grid landing page keeps its prose with each "
        f"grid rendered as a link list."
    )
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
        body = strip_leading_h1(p["body"])
        # The header already ends in a blank line; a body starting with one too
        # would open every page with two.
        body = body.lstrip("\n")
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


# The build resolves `{ERIGON_VERSION}` from the GitHub Releases API
# (docusaurus.config.ts), so a page — and therefore this corpus — changes when
# Erigon ships a release, with no commit in this repo. Comparing byte-for-byte
# would turn `--check` red on every docs PR from that moment until someone
# regenerates. Version tokens are therefore masked for the staleness verdict and
# reported separately: a version-only difference is drift to fix at leisure,
# anything else is a real regression and still fails.
# Only the Erigon release the build resolved is masked, and it is discovered
# from the artifact names it appears in rather than by pattern. Masking every
# `N.N.N` would also mask a Go or library version, so a real docs edit to one of
# those would be downgraded to warning-only drift.
# No trailing \b: the version is followed by `_` in `erigon_v3.6.0_linux`, and
# `0_` is not a word boundary, so an anchored pattern matches nothing there.
_RELEASE_ANCHOR_RE = re.compile(r"erigon[:_/-]v?(\d+\.\d+\.\d+)(?!\.?\d)")


def _mask_versions(text):
    """Mask the Erigon release so a new release is drift, not staleness.

    The release token expands in places with no `erigon` next to them —
    `(e.g., v3.6.0)` and `git checkout v3.6.0` in the installation page — so
    anchoring the substitution to `erigon[:_/-]` left those behind and every
    artifact read as stale on the next release. The mask is therefore global.

    That is only safe while the release version appears nowhere else for an
    unrelated reason: an edit to a dependency pinned at the same version would
    compare equal and pass as drift. `_literal_release_uses` reports those
    positions and a test fails on them, so the hazard is detected rather than
    silently tolerated.
    """
    found = _RELEASE_ANCHOR_RE.findall(text)
    if not found:
        return text
    release = Counter(found).most_common(1)[0][0]
    # Bounded on both sides, matching the hazard scan. An unbounded pattern
    # also masks the release where it is the tail of a longer version, and an
    # unrelated pin like `13.6.0` is neither maskable nor reported by the scan.
    return re.sub(rf"(?<![.\d])v?{re.escape(release)}(?!\.?\d)",
                  "<release>", text)


def _literal_release_uses(release):
    """Source sites that hardcode `release` instead of using the version token.

    The mask is global, which is only safe while nothing unrelated is pinned at
    the same version: an edit to such a pin would compare equal and pass as
    release drift. The output cannot tell the two apart — `dependency v3.6.0`
    and `git checkout v3.6.0` are the same string — but the sources can, because
    every site the release legitimately comes from is written `{ERIGON_VERSION}`
    there. Any literal is therefore somebody else's pin. Every section that
    reaches the corpus is scanned, not just docs/: help-center is a sibling
    directory and a pin there would have gone unseen.
    """
    hits = []
    pattern = re.compile(rf"(?<![.\d]){re.escape(release)}(?!\.?\d)")
    roots = [directory for _, directory, _ in SECTIONS]
    for path in sorted(q for root in roots for q in root.rglob("*.md*")):
        for n, line in enumerate(
                path.read_text(encoding="utf-8", errors="replace").split("\n"), 1):
            if pattern.search(line):
                hits.append((path, n, line.strip()[:90]))
    return hits


def check_outputs(llms_txt, llms_full_txt):
    """Compare regenerated content to on-disk files.

    Returns (stale, version_drift): paths differing in more than version tokens,
    and paths differing only in them.
    """
    targets = [
        (OUT_DIR / "llms.txt",        llms_txt),
        (OUT_DIR / "llms-full.txt",   llms_full_txt),
        (REPO_ROOT / "llms.txt",      llms_txt),
        (REPO_ROOT / "llms-full.txt", llms_full_txt),
    ]
    stale, version_drift = [], []
    for path, expected in targets:
        try:
            actual = path.read_text(encoding="utf-8")
        except FileNotFoundError:
            actual = ""
        if actual == expected:
            continue
        if _mask_versions(actual) == _mask_versions(expected):
            version_drift.append(path)
        else:
            stale.append(path)
    return stale, version_drift


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
        stale, version_drift = check_outputs(llms_txt, llms_full_txt)
        if stale:
            print("ERROR: regenerated content differs from committed files:", file=sys.stderr)
            for path in stale:
                print(f"  {path}", file=sys.stderr)
            print("Run: python3 docs/site/scripts/generate-llms.py", file=sys.stderr)
            sys.exit(1)
        if version_drift:
            print(f"WARNING: {len(version_drift)} file(s) differ only in version "
                  f"tokens — Erigon shipped a release since these were generated. "
                  f"Regenerate when convenient; not a failure.", file=sys.stderr)
            for path in version_drift:
                print(f"  {path}", file=sys.stderr)
            print(f"OK: no differences beyond the release token "
                  f"({n_pages} pages)")
        else:
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
