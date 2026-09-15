#!/usr/bin/env python3
"""Compare the state snapshots Erigon built against the officially published ones.

Input is the TOML that ``downloader torrent_hashes --datadir <dir> --chain <chain>``
prints for the datadir under test, and the preverified TOML from
erigontech/erigon-snapshot (taken from the datadir when the node saved one there,
otherwise fetched from GitHub).

Only the state entries are compared -- ``domain/``, ``history/``, ``idx/`` and
``accessor/``. Block and Caplin snapshots are downloaded rather than built, so
their hashes say nothing about execution. ``--dirs`` and ``--exts`` narrow that
further; see STATE_DATA_EXTS for why a caller may want to drop the accessors.

A file present on only one side is reported but is not a failure: the node stops
at whatever step boundary it reached, and it keeps building past the end of the
published set once it is at the tip. A file present on both sides with a
different hash is a failure -- the state Erigon computed from genesis differs
from the published one. Nothing in common is an error: the run proved nothing.
"""
import argparse
import json
import os
import re
import sys
import urllib.request

STATE_DIRS = ("accessor", "domain", "history", "idx")

# Data files, as opposed to the accessors built over them. Accessors (.bt, .kvi,
# .kvei, .vi, .efi) are recsplit/btree structures seeded with the salt in
# salt-state.txt, so two nodes with different salts index the same data into
# different bytes. Only these extensions are comparable across nodes.
STATE_DATA_EXTS = ("kv", "v", "ef")

GITHUB_TOML_URL = "https://raw.githubusercontent.com/erigontech/erigon-snapshot/{branch}/{chain}.toml"

ENTRY_RE = re.compile(r"""^\s*['"]?([^'"\s=]+)['"]?\s*=\s*['"]([^'"]*)['"]\s*$""")

BRANCH_RE = re.compile(r'DefaultSnapshotGitBranch\s*=\s*"([^"]+)"')


class Mismatch:
    def __init__(self, name, local, published):
        self.name = name
        self.local = local
        self.published = published

    def __repr__(self):
        return f"Mismatch({self.name}, local={self.local}, published={self.published})"


class Comparison:
    def __init__(self, matched, mismatched, local_only, published_only):
        self.matched = matched
        self.mismatched = mismatched
        self.local_only = local_only
        self.published_only = published_only

    @property
    def compared(self):
        return len(self.matched) + len(self.mismatched)

    @property
    def published(self):
        return self.compared + len(self.published_only)

    @property
    def built(self):
        return self.compared + len(self.local_only)

    def __repr__(self):
        return (f"Comparison(matched={len(self.matched)}, mismatched={len(self.mismatched)}, "
                f"local_only={len(self.local_only)}, published_only={len(self.published_only)})")


class Result:
    """The result shape the QA test database expects (see upload_test_results.py)."""

    def __init__(self, outcome, reason, exit_code):
        self.outcome = outcome
        self.reason = reason
        self.exit_code = exit_code
        self.measures = dict()

    def add_measure(self, variable, value):
        self.measures[variable] = value

    def write_to_json_file(self, file):
        with open(file, "w", encoding="utf-8") as fh:
            fh.write(json.dumps(self.__dict__, indent=2))


def parse_hash_toml(text):
    entries = {}
    for line in text.splitlines():
        m = ENTRY_RE.match(line)
        if m:
            entries[m.group(1)] = m.group(2)
    return entries


def state_entries(entries, dirs=STATE_DIRS, exts=None):
    prefixes = tuple(d + "/" for d in dirs)
    suffixes = tuple("." + e for e in exts) if exts else None
    return {name: h for name, h in entries.items()
            if name.startswith(prefixes) and (suffixes is None or name.endswith(suffixes))}


def compare(local, published):
    matched, mismatched = [], []
    for name in sorted(local):
        if name not in published:
            continue
        if local[name] == published[name]:
            matched.append(name)
        else:
            mismatched.append(Mismatch(name, local[name], published[name]))
    local_only = sorted(name for name in local if name not in published)
    published_only = sorted(name for name in published if name not in local)
    return Comparison(matched, mismatched, local_only, published_only)


def subdir_of(name):
    return name.split("/", 1)[0]


def compared_by_subdir(comparison, dirs):
    names = comparison.matched + [m.name for m in comparison.mismatched]
    return {d: sum(1 for n in names if subdir_of(n) == d) for d in dirs}


def headline(comparison):
    if comparison.compared == 0:
        return "no file is present on both sides, nothing was compared"
    if comparison.mismatched:
        return (f"{len(comparison.mismatched)} of the {comparison.compared} compared files "
                f"differ from the published ones")
    return f"all {comparison.compared} compared files match the published ones"


def verdict(comparison, dirs=STATE_DIRS):
    matched, mismatched = len(comparison.matched), len(comparison.mismatched)

    if comparison.compared == 0:
        result = Result("ERROR", headline(comparison), 1)
    elif mismatched:
        names = ", ".join(m.name for m in comparison.mismatched[:5])
        suffix = ", ..." if mismatched > 5 else ""
        result = Result("FAILURE", f"{headline(comparison)}: {names}{suffix}", 1)
    else:
        result = Result("SUCCESS", headline(comparison), 0)

    result.add_measure("matched", matched)
    result.add_measure("mismatched", mismatched)
    result.add_measure("local_only", len(comparison.local_only))
    result.add_measure("published_only", len(comparison.published_only))
    for subdir in sorted(set(subdir_of(n) for n in comparison.matched)
                         | set(subdir_of(m.name) for m in comparison.mismatched)):
        result.add_measure(f"mismatched_{subdir}",
                           sum(1 for m in comparison.mismatched if subdir_of(m.name) == subdir))
    for subdir, count in compared_by_subdir(comparison, dirs).items():
        result.add_measure(f"compared_{subdir}", count)
    return result


def resolve_snapshot_branch(repo_root):
    """Read the erigon-snapshot branch Erigon itself defaults to."""
    path = os.path.join(repo_root, "db", "version", "app.go")
    with open(path, encoding="utf-8") as fh:
        m = BRANCH_RE.search(fh.read())
    if not m:
        raise RuntimeError(f"no DefaultSnapshotGitBranch in {path}")
    return m.group(1)


def load_published(args, repo_root):
    if args.published_toml and os.path.isfile(args.published_toml):
        with open(args.published_toml, encoding="utf-8") as fh:
            return fh.read(), args.published_toml
    branch = args.branch or os.environ.get("SNAPS_GIT_BRANCH") or resolve_snapshot_branch(repo_root)
    url = GITHUB_TOML_URL.format(branch=branch, chain=args.chain)
    with urllib.request.urlopen(url, timeout=60) as resp:
        return resp.read().decode("utf-8"), url


def natural_key(name):
    return [int(t) if t.isdigit() else t for t in re.split(r"(\d+)", name)]


def file_statuses(comparison, one_side_only, one_side_label):
    statuses = [("identical", n) for n in comparison.matched]
    statuses += [("DIFFERENT", m.name) for m in comparison.mismatched]
    statuses += [(one_side_label, n) for n in one_side_only]
    return sorted(statuses, key=lambda s: natural_key(s[1]))


def file_lists(comparison):
    return [(f"Built locally ({comparison.built})",
             file_statuses(comparison, comparison.local_only, "not published")),
            (f"Published ({comparison.published})",
             file_statuses(comparison, comparison.published_only, "not built"))]


def uncompared_subdirs(comparison, dirs):
    return [d for d, count in compared_by_subdir(comparison, dirs).items() if count == 0]


def format_by_subdir(comparison, dirs):
    return ", ".join(f"{d} {count}" for d, count in compared_by_subdir(comparison, dirs).items())


def render_console(result, comparison, source, chain, dirs, exts):
    def row(label, count, note=""):
        return f"  {label:<22}{count:>5}{note}"

    lines = [f"State snapshot hash check — {chain}",
             f"  published hashes from: {source}",
             f"  compared file types:   {', '.join(exts) if exts else 'all'} in {', '.join(dirs)}", ""]
    for title, statuses in file_lists(comparison):
        lines.append(f"{title}:")
        lines += [f"  {status:<14} {name}" for status, name in statuses]
        lines.append("")
    lines += [row("published:", comparison.published),
             row("built locally:", comparison.built),
             row("in both (compared):", comparison.compared,
                 f"  ->  {len(comparison.matched)} identical, {len(comparison.mismatched)} DIFFERENT"),
             f"    compared by subdir: {format_by_subdir(comparison, dirs)}",
             row("built, not published:", len(comparison.local_only), "  (not compared)"),
             row("published, not built:", len(comparison.published_only), "  (not compared)"), ""]
    uncompared = uncompared_subdirs(comparison, dirs)
    if uncompared and comparison.compared:
        lines += [f"WARNING: no {', '.join(uncompared)} file was compared, so this check says nothing about them", ""]
    lines.append(f"{result.outcome}: {headline(comparison)}")
    width = max((len(m.name) for m in comparison.mismatched), default=0)
    lines += [f"  {m.name:<{width}}  built={m.local}  published={m.published}" for m in comparison.mismatched]
    return "\n".join(lines)


def render_summary(result, comparison, source, chain, dirs, exts, cap=50):
    badge = {"SUCCESS": "✅", "FAILURE": "❌"}.get(result.outcome, "⚠️")
    file_types = ", ".join(f"`{e}`" for e in exts) if exts else "all"
    lines = [f"## {badge} State snapshot hashes — {chain}", "",
             f"**{headline(comparison)}**", "",
             f"Compared file types: {file_types} in {', '.join(f'`{d}`' for d in dirs)}. "
             f"Published hashes from `{source}`.", "",
             "| | files | |", "|---|---:|---|",
             f"| published | {comparison.published} | |",
             f"| built locally | {comparison.built} | |",
             f"| **in both (compared)** | **{comparison.compared}** | "
             f"{len(comparison.matched)} identical, {len(comparison.mismatched)} different |",
             f"| built, not published | {len(comparison.local_only)} | not compared |",
             f"| published, not built | {len(comparison.published_only)} | not compared |", "",
             f"Compared by subdir: {format_by_subdir(comparison, dirs)}", ""]
    uncompared = uncompared_subdirs(comparison, dirs)
    if uncompared and comparison.compared:
        lines += [f"> ⚠️ No {', '.join(f'`{d}`' for d in uncompared)} file was compared, "
                  f"so this check says nothing about them.", ""]
    if comparison.mismatched:
        lines += ["### Differing files", "", "| file | built | published |", "|---|---|---|"]
        for m in comparison.mismatched[:cap]:
            lines.append(f"| `{m.name}` | `{m.local}` | `{m.published}` |")
        if len(comparison.mismatched) > cap:
            lines.append(f"| ... | {len(comparison.mismatched) - cap} more | |")
        lines.append("")
    for title, statuses in file_lists(comparison):
        lines += [f"<details><summary>{title}</summary>", "", "| status | file |", "|---|---|"]
        lines += [f"| {status} | `{name}` |" for status, name in statuses]
        lines += ["", "</details>", ""]
    return "\n".join(lines)


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--local-hashes", required=True, dest="local_hashes", metavar="FILE",
                        help="output of: downloader torrent_hashes --datadir <dir> --chain <chain>")
    parser.add_argument("--chain", required=True, help="chain name, as on the erigon command line")
    parser.add_argument("--published-toml", default=None, dest="published_toml", metavar="FILE",
                        help="preverified TOML to compare against; fetched from GitHub when missing")
    parser.add_argument("--dirs", default=",".join(STATE_DIRS),
                        help="comma-separated state subdirs to compare (default: %(default)s)")
    parser.add_argument("--exts", default="",
                        help="comma-separated file extensions to compare, without the dot "
                             "(default: every extension)")
    parser.add_argument("--branch", default=None,
                        help="erigon-snapshot branch to fetch (default: $SNAPS_GIT_BRANCH, "
                             "else DefaultSnapshotGitBranch from db/version/app.go)")
    parser.add_argument("--result-file", default=None, dest="result_file", metavar="FILE",
                        help="write the QA result JSON here")
    parser.add_argument("--summary-file", default=None, dest="summary_file", metavar="FILE",
                        help="append a Markdown summary here, e.g. $GITHUB_STEP_SUMMARY")
    return parser.parse_args()


def main():
    args = parse_args()
    repo_root = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", ".."))

    dirs = tuple(d.strip() for d in args.dirs.split(",") if d.strip())
    exts = tuple(e.strip().lstrip(".") for e in args.exts.split(",") if e.strip())

    with open(args.local_hashes, encoding="utf-8") as fh:
        local = state_entries(parse_hash_toml(fh.read()), dirs, exts)
    published_text, source = load_published(args, repo_root)
    published = state_entries(parse_hash_toml(published_text), dirs, exts)

    comparison = compare(local, published)
    result = verdict(comparison, dirs)
    result.add_measure("local_state_files", len(local))
    result.add_measure("published_state_files", len(published))

    print(render_console(result, comparison, source, args.chain, dirs, exts))

    if args.result_file:
        result.write_to_json_file(args.result_file)
    if args.summary_file:
        with open(args.summary_file, "a", encoding="utf-8") as fh:
            fh.write(render_summary(result, comparison, source, args.chain, dirs, exts) + "\n")

    return result.exit_code


if __name__ == "__main__":
    sys.exit(main())
