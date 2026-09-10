#!/usr/bin/env python3
"""Fixture tests for docs-deploy-trigger.py.

Run: python3 .github/workflows/scripts/docs-deploy-trigger.test.py

Every case here is a YAML shape that a human might plausibly write into
docs-deploy.yml's `on.push.branches` list. They exist because the cutover
workflow's "is the trigger pinned to this branch?" check and its rewrite must
agree on every one of them: when they disagree the check says "not pinned", the
rewrite says "already pinned", and the job fails on every scheduled run.
"""
import importlib.util
import os
import sys
import tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
spec = importlib.util.spec_from_file_location(
    "docs_deploy_trigger", os.path.join(HERE, "docs-deploy-trigger.py"))
assert spec is not None and spec.loader is not None
mod = importlib.util.module_from_spec(spec)
spec.loader.exec_module(mod)


def parse(text):
    lo, hi = mod.branch_span(text)
    return mod.entries(text[lo:hi])


HEAD = "on:\n  push:\n"
TAIL = "\npermissions:\n  contents: read\n"

# (name, on-block body, expected branches)
CASES = [
    ("plain",
     "    branches:\n      - release/3.6\n", ["release/3.6"]),
    ("paths after branches",
     "    branches:\n      - release/3.6\n    paths:\n      - 'docs/site/**'\n", ["release/3.6"]),
    ("single quotes",
     "    branches:\n      - 'release/3.6'\n", ["release/3.6"]),
    ("double quotes",
     '    branches:\n      - "release/3.6"\n', ["release/3.6"]),
    ("trailing comment on the entry",
     "    branches:\n      - release/3.6  # deploy\n", ["release/3.6"]),
    ("comment line inside the list",
     "    branches:\n      # the publishing branch\n      - release/3.6\n", ["release/3.6"]),
    ("blank line inside the list",
     "    branches:\n\n      - release/3.6\n", ["release/3.6"]),
    ("dash at the key's indent",
     "    branches:\n    - release/3.6\n", ["release/3.6"]),
    ("tab-indented entry",
     "    branches:\n\t- release/3.6\n", ["release/3.6"]),
    ("extra spaces after the dash",
     "    branches:\n      -    release/3.6\n", ["release/3.6"]),
    ("trailing whitespace on the entry",
     "    branches:\n      - release/3.6   \n", ["release/3.6"]),
    ("comment on the push: key",
     None, ["release/3.6"]),          # handled specially below
    ("comment on the branches: key",
     "    branches:  # only the deploy branch\n      - release/3.6\n", ["release/3.6"]),
    ("'#' inside the branch name",
     "    branches:\n      - release/3.6#keep\n", ["release/3.6#keep"]),
    ("two entries",
     "    branches:\n      - release/3.6\n      - release/3.5\n",
     ["release/3.6", "release/3.5"]),
]


def run():
    failures = []

    for name, body, expected in CASES:
        if body is None:                       # the push:-key comment case
            text = "on:\n  push:  # publish\n    branches:\n      - release/3.6\n" + TAIL
        else:
            text = HEAD + body + TAIL
        try:
            got = parse(text)
        except SystemExit as e:
            failures.append(f"{name}: refused with {e}")
            continue
        if got != expected:
            failures.append(f"{name}: got {got}, want {expected}")

    # A branch named only by another trigger is not a deploy pin.
    other = ("on:\n  push:\n    branches:\n      - release/3.6\n"
             "  pull_request:\n    branches:\n      - release/3.7\n" + TAIL)
    if parse(other) != ["release/3.6"]:
        failures.append(f"pull_request leak: got {parse(other)}")

    # Round trip: repointing preserves comment, quoting and indentation.
    src = ("on:\n  push:\n    branches:\n      # publishing branch\n"
           '      - "release/3.6"  # deploy\n    paths:\n      - x\n' + TAIL)
    with tempfile.NamedTemporaryFile("w", suffix=".yml", delete=False) as fh:
        fh.write(src)
        path = fh.name
    os.environ["DEPLOY_YML"] = path
    sys.argv = ["docs-deploy-trigger.py", "--repoint", "release/3.7"]
    mod.main()
    out = open(path).read()
    os.unlink(path)
    want = src.replace('"release/3.6"', '"release/3.7"')
    if out != want:
        failures.append("round trip changed more than the branch name:\n" + out)

    for f in failures:
        print("FAIL", f)
    print(f"{len(CASES) + 2 - len(failures)}/{len(CASES) + 2} passed")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(run())
