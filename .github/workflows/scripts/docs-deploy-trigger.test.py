#!/usr/bin/env python3
"""Fixture tests for docs-deploy-trigger.py.

Run: python3 .github/workflows/scripts/docs-deploy-trigger.test.py

Every case is a YAML shape a human might write into docs-deploy.yml's
`on.push.branches`. They exist because the cutover workflow's "is this trigger
pinned?" check and its rewrite read the same list: when the two disagree, the
check reports "not pinned", the rewrite reports "already pinned", and the job
fails on every scheduled run. The negative cases matter as much as the positive
ones — a parser that accepts a shape YAML itself rejects reports a pin that the
deploy trigger does not actually have.
"""
import contextlib
import importlib.util
import io
import os
import sys
import tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
spec = importlib.util.spec_from_file_location(
    "docs_deploy_trigger", os.path.join(HERE, "docs-deploy-trigger.py"))
assert spec is not None and spec.loader is not None
mod = importlib.util.module_from_spec(spec)
spec.loader.exec_module(mod)

FAILURES = []


def check(name, got, want):
    if got != want:
        FAILURES.append(f"{name}: got {got!r}, want {want!r}")


def parse(text):
    """Branch names, or the refusal message — never an escaping SystemExit.

    A refusal must be comparable data: letting SystemExit propagate kills the
    run and hides every result gathered so far, so a regression shows up as one
    stray line instead of a report.
    """
    try:
        lo, hi = mod.branch_span(text)
        return mod.entries(text[lo:hi])
    except SystemExit as e:
        return f"REFUSED: {e}"


def run_cli(text, argv):
    """Run main() over `text`, returning (rc, stdout, resulting file)."""
    with tempfile.NamedTemporaryFile("w", suffix=".yml", delete=False) as fh:
        fh.write(text)
        path = fh.name
    os.environ["DEPLOY_YML"] = path
    sys.argv = ["docs-deploy-trigger.py", *argv]
    out = io.StringIO()
    try:
        with contextlib.redirect_stdout(out):
            rc = mod.main()
    except SystemExit as e:
        rc = e.code if isinstance(e.code, int) else 1
        if e.code and not isinstance(e.code, int):
            out.write(str(e.code))
    result = open(path).read()
    os.unlink(path)
    return rc, out.getvalue(), result


HEAD = "on:\n  push:\n"
TAIL = "\npermissions:\n  contents: read\n"
REFUSE_NO_LIST = "REFUSED: the push: trigger has no branches: list - refusing to edit blindly"
REFUSE_NO_PUSH = "REFUSED: docs-deploy.yml has no push: trigger - refusing to edit blindly"

ACCEPTED = [
    ("plain", "    branches:\n      - release/3.6\n", ["release/3.6"]),
    ("paths after branches",
     "    branches:\n      - release/3.6\n    paths:\n      - 'docs/site/**'\n", ["release/3.6"]),
    ("single quotes", "    branches:\n      - 'release/3.6'\n", ["release/3.6"]),
    ("double quotes", '    branches:\n      - "release/3.6"\n', ["release/3.6"]),
    ("trailing comment on entry",
     "    branches:\n      - release/3.6  # deploy\n", ["release/3.6"]),
    ("quoted entry, comment with no gap",
     "    branches:\n      - 'release/3.6'#c\n", ["release/3.6"]),
    ("indented comment in the list",
     "    branches:\n      # the publishing branch\n      - release/3.6\n", ["release/3.6"]),
    ("column-0 comment in the list",
     "    branches:\n# publishing branch\n      - release/3.6\n", ["release/3.6"]),
    ("blank line in the list", "    branches:\n\n      - release/3.6\n", ["release/3.6"]),
    ("dash at the key's indent", "    branches:\n    - release/3.6\n", ["release/3.6"]),
    ("tab-indented entry", "    branches:\n\t- release/3.6\n", ["release/3.6"]),
    ("extra spaces after dash", "    branches:\n      -    release/3.6\n", ["release/3.6"]),
    ("trailing whitespace", "    branches:\n      - release/3.6   \n", ["release/3.6"]),
    ("comment on the branches: key",
     "    branches:  # only the deploy branch\n      - release/3.6\n", ["release/3.6"]),
    ("'#' inside the branch name",
     "    branches:\n      - release/3.6#keep\n", ["release/3.6#keep"]),
    ("two entries", "    branches:\n      - release/3.6\n      - release/3.5\n",
     ["release/3.6", "release/3.5"]),
]

# Shapes YAML itself does not read as a branches list. Accepting one would
# report a pin the deploy trigger does not have.
REJECTED = [
    ("branches:#c — not a key in YAML",
     "    branches:#c\n      - release/3.6\n", REFUSE_NO_LIST),
    ("push: with only paths, pull_request lists branches later",
     "    paths:\n      - x\n  pull_request:\n    branches:\n      - release/3.6\n",
     REFUSE_NO_LIST),
]


def run():
    for name, body, want in ACCEPTED + REJECTED:
        check(name, parse(HEAD + body + TAIL), want)

    check("comment on the push: key",
          parse("on:\n  push:  # publish\n    branches:\n      - release/3.6\n" + TAIL),
          ["release/3.6"])
    check("push:#c — not a key in YAML",
          parse("on:\n  push:#c\n    branches:\n      - release/3.6\n" + TAIL),
          REFUSE_NO_PUSH)

    # A branch named by another trigger is not a deploy pin — in either order.
    after = ("on:\n  push:\n    branches:\n      - release/3.6\n"
             "  pull_request:\n    branches:\n      - release/3.7\n" + TAIL)
    check("pull_request after push", parse(after), ["release/3.6"])
    before = ("on:\n  pull_request:\n    branches:\n      - release/3.5\n"
              "  push:\n    branches:\n      - release/3.6\n" + TAIL)
    check("pull_request before push", parse(before), ["release/3.6"])

    # --list is the contract the workflow greps with `-Fxq`: one name per line.
    rc, out, _ = run_cli(HEAD + "    branches:\n      - release/3.6\n" + TAIL, ["--list"])
    check("--list rc", rc, 0)
    check("--list output", out, "release/3.6\n")

    # Repointing rewrites the name and nothing else.
    src = ("on:\n  push:\n    branches:\n      # publishing branch\n"
           '      - "release/3.6"  # deploy\n    paths:\n      - x\n' + TAIL)
    rc, _, result = run_cli(src, ["--repoint", "release/3.7"])
    check("repoint rc", rc, 0)
    check("repoint round trip", result, src.replace('"release/3.6"', '"release/3.7"'))

    # Already pinned is a no-op, not a failure: the workflow's step would
    # otherwise try to commit an unchanged tree and go red.
    pinned = HEAD + "    branches:\n      - release/3.7\n" + TAIL
    rc, out, result = run_cli(pinned, ["--repoint", "release/3.7"])
    check("already-pinned rc", rc, 0)
    check("already-pinned is a no-op", result, pinned)
    check("already-pinned says so", "already pins release/3.7" in out, True)

    # An ambiguous list must not be rewritten by guesswork.
    two = HEAD + "    branches:\n      - release/3.6\n      - release/3.5\n" + TAIL
    rc, out, result = run_cli(two, ["--repoint", "release/3.7"])
    check("two entries refuses", rc != 0 or "expected exactly one" in out, True)
    check("two entries unchanged", result, two)

    total = len(ACCEPTED) + len(REJECTED) + 12
    for f in FAILURES:
        print("FAIL", f)
    print(f"{total - len(FAILURES)}/{total} passed")
    return 1 if FAILURES else 0


if __name__ == "__main__":
    sys.exit(run())
