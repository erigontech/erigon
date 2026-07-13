#!/usr/bin/env python3
"""Make archived doc versions self-contained.

Current docs (`docs/`) legitimately use unversioned absolute links like
`/get-started/` because the current version is served at the site root. When
`docusaurus docs:version vX.Y` freezes those pages into
`versioned_docs/version-vX.Y/`, the same absolute links resolve to the
*current* version instead of the archived one — a reader browsing the v3.4
docs is bounced into v3.6. (See erigontech/erigon#22416.)

Per archived version this rewrites, for every link-bearing syntax below:
  * same-page anchors written as absolute routes -> local `#frag`
  * every other *unversioned absolute intra-docs* link -> `/vX.Y`-prefixed

A link is left untouched when it is external, a static asset (`/img/…`), the
site root / home (`/`), the separate unversioned Help Center
(`/help-center/…`), or already version-prefixed (`/vX.Y/…`). Anything else
that is an absolute route is treated as a leak — including links into a
section that was renamed or removed since the archive was cut (that page no
longer exists at the current route, so leaving it absolute silently sends the
reader to the wrong version). The current `docs/` tree is never touched, and
links inside fenced code blocks are ignored.

Run after cutting a version:
    python3 docs/site/scripts/fix-archive-links.py            # rewrite in place
    python3 docs/site/scripts/fix-archive-links.py --check    # CI: fail if any remain
"""
import argparse
import os
import re
import sys

_VERDIR = re.compile(r"^version-(v\d+\.\d+)$")

# Link-bearing syntaxes. Each captures <pre><url><post> so the match can be
# rebuilt with only the URL changed.
_PATTERNS = (
    # Markdown inline link: ](URL) or ](URL "title") / ](URL 'title')
    re.compile(r'(?P<pre>\]\()(?P<url>/[^)\s]+)(?P<post>(?:\s+(?:"[^"]*"|\'[^\']*\'))?\))'),
    # Markdown reference definition:  [label]: URL
    re.compile(r'(?P<pre>^[ \t]*\[[^\]]+\]:[ \t]*)(?P<url>/\S+)(?P<post>)'),
    # JSX/HTML attribute: href=/to=  "URL" | 'URL' | {"URL"} | {'URL'}  (spaces ok)
    re.compile(r'(?P<pre>(?<![\w-])(?:href|to)\s*=\s*\{?\s*(?P<q>["\']))'
               r'(?P<url>/[^"\'\s}]*)(?P<post>(?P=q)\s*\}?)'),
)


def _excluded(path: str) -> bool:
    p = path.split("#", 1)[0]
    if p in ("", "/"):                                   # site root / home
        return True
    if p.startswith("/img/"):                            # static assets
        return True
    if p == "/help-center" or p.startswith("/help-center/"):  # separate unversioned instance
        return True
    if re.match(r"^/v\d+\.\d+(/|$)", p):                 # already version-prefixed
        return True
    return False


def _self_route(relpath: str) -> str:
    p = re.sub(r"\.(md|mdx)$", "", relpath)
    p = re.sub(r"(^|/)index$", "", p)
    p = p.strip("/")
    return "/" + p if p else "/"


def _transform(url: str, route: str, prefix: str):
    """Return (new_url, changed)."""
    path_part, _, frag = url.partition("#")
    frag = "#" + frag if frag else ""
    base = path_part.rstrip("/") or "/"
    # same-page anchor -> local (checked before exclusions so home-page `/#x` works)
    if frag and base == route:
        return frag, True
    if _excluded(url):
        return url, False
    return prefix + path_part + frag, True


def _iter_scannable(text: str):
    """Yield (line, is_fenced); callers skip fenced code blocks."""
    fence = None
    for ln in text.splitlines(keepends=True):
        stripped = ln.lstrip()
        m = re.match(r"(```+|~~~+)", stripped)
        if m:
            tok = m.group(1)[0] * 3
            if fence is None:
                fence = tok
            elif stripped.startswith(fence):
                fence = None
            yield ln, True
            continue
        yield ln, fence is not None


def _process_file(path, relpath, prefix, check):
    route = _self_route(relpath)
    with open(path, encoding="utf-8") as fh:
        text = fh.read()
    violations = []
    out = []

    def repl(m):
        new_url, changed = _transform(m.group("url"), route, prefix)
        if not changed:
            return m.group(0)
        violations.append(m.group("url"))
        return m.group("pre") + new_url + m.group("post")

    for line, fenced in _iter_scannable(text):
        if not fenced:
            for pat in _PATTERNS:
                line = pat.sub(repl, line)
        out.append(line)

    if violations and not check:
        with open(path, "w", encoding="utf-8") as fh:
            fh.write("".join(out))
    return violations


def scan(versioned_root, check):
    total = {}
    if not os.path.isdir(versioned_root):
        return total
    for ver in sorted(os.listdir(versioned_root)):
        m = _VERDIR.match(ver)
        if not m:
            continue
        prefix = "/" + m.group(1)  # /v3.4
        vroot = os.path.join(versioned_root, ver)
        for dp, _, files in os.walk(vroot):
            for fn in files:
                if not fn.endswith((".md", ".mdx")):
                    continue
                fp = os.path.join(dp, fn)
                v = _process_file(fp, os.path.relpath(fp, vroot), prefix, check)
                if v:
                    total[f"{ver}/{os.path.relpath(fp, vroot)}"] = v
    return total


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("versioned_root", nargs="?", help="path to versioned_docs/ (default: sibling of this script)")
    ap.add_argument("--check", action="store_true", help="report violations and exit 1; do not rewrite")
    args = ap.parse_args(argv)
    here = os.path.dirname(os.path.abspath(__file__))
    root = args.versioned_root or os.path.normpath(os.path.join(here, "..", "versioned_docs"))
    found = scan(root, args.check)
    n = sum(len(v) for v in found.values())
    if args.check:
        if found:
            print(f"FAIL: {n} unversioned absolute link(s) in archived docs across {len(found)} file(s):")
            for f, links in sorted(found.items()):
                for link in links:
                    print(f"  {f}: {link}")
            print("\nFix with: python3 docs/site/scripts/fix-archive-links.py")
            sys.exit(1)
        print("OK: all archived doc versions are self-contained")
    else:
        print(f"rewrote {n} link(s) across {len(found)} file(s)")


if __name__ == "__main__":
    main()
