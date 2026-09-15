#!/usr/bin/env python3
"""Fixture tests for docs-deploy-trigger.py.

Run: python3 .github/workflows/scripts/docs-deploy-trigger.test.py
     python3 .github/workflows/scripts/docs-deploy-trigger.test.py --oracle

Every case is a YAML shape a human might write into docs-deploy.yml's
`on.push.branches`. They exist because the cutover workflow's "is this trigger
pinned?" check and its rewrite read the same list, and because both directions
of a wrong answer cost something: naming a branch `on.push.branches` does not
contain makes the cutover flip the deploy variable to a branch that never
publishes, and failing to see a pin that IS there makes it open a repoint pull
request that is not needed and fail while trying.

Reading is delegated to PyYAML, so these fixtures are not re-implementing a
parser's job; they pin down THIS script's contract on top of it -- which key it
reads, what it refuses, and that a rewrite touches nothing but the name.

`--oracle` additionally re-reads every fixture with an unrelated YAML
implementation (js-yaml, via docs-deploy-trigger.oracle.mjs) and requires the
two to agree. Run it whenever the reader changes: without it the fixtures are
only ever checked against the same library that produced them. It is not part
of the default run because js-yaml is a Node dependency this repo does not
otherwise need, and a check that quietly skips itself is worse than none.

    JS_YAML_FROM=docs/site python3 .../docs-deploy-trigger.test.py --oracle

JS_YAML_FROM names a directory whose node_modules has js-yaml; docs/site does
once its dependencies are installed.
"""
import contextlib
import importlib.util
import io
import json
import os
import shutil
import subprocess
import sys
import tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
spec = importlib.util.spec_from_file_location(
    "docs_deploy_trigger", os.path.join(HERE, "docs-deploy-trigger.py"))
assert spec is not None and spec.loader is not None
mod = importlib.util.module_from_spec(spec)
spec.loader.exec_module(mod)

ORACLE = os.path.join(HERE, "docs-deploy-trigger.oracle.mjs")

FAILURES = []

CHECKS = 0


def check(name, got, want):
    """Record one assertion. Counted, not hardcoded — a hand-maintained total
    drifts, and then a real failure prints a denominator nobody trusts."""
    global CHECKS
    CHECKS += 1
    if got != want:
        FAILURES.append(f"{name}: got {got!r}, want {want!r}")


def parse(text):
    """Branch names, or the refusal reason — never an escaping exception.

    A refusal must be comparable data: letting it propagate kills the run and
    hides every result gathered so far, so a regression shows up as one stray
    line instead of a report. The reason is reduced to a tag because PyYAML's
    own message wording is not this script's contract.
    """
    try:
        return [node.value for node in mod.branch_nodes(text)]
    except mod.Unreadable as exc:
        text = str(exc)
        if text.startswith("not valid YAML"):
            return "INVALID"
        if "has a non-string entry" in text:
            return "NON_STRING"
        return "NO_LIST"


def run_cli(text, argv):
    """Run main() over `text`, returning (rc, stdout, resulting file)."""
    with tempfile.NamedTemporaryFile("w", suffix=".yml", delete=False,
                                     newline="") as fh:
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
    with open(path, encoding="utf-8", newline="") as fh:
        result = fh.read()
    os.unlink(path)
    return rc, out.getvalue(), result


def oracle_read(text):
    """What an unrelated YAML implementation makes of the same document."""
    with tempfile.NamedTemporaryFile("w", suffix=".yml", delete=False,
                                     newline="") as fh:
        fh.write(text)
        path = fh.name
    try:
        proc = subprocess.run(["node", ORACLE, path],
                              capture_output=True, text=True, check=True)
    finally:
        os.unlink(path)
    return json.loads(proc.stdout)


HEAD = "on:\n  push:\n"
TAIL = "\npermissions:\n  contents: read\n"

# Shapes that name a branch, and the names they name. A real parser reads all
# of these; the earlier hand-rolled matcher did not, and each miss made the
# workflow act on a pin it could not see.
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
    ("extra spaces after dash", "    branches:\n      -    release/3.6\n", ["release/3.6"]),
    ("trailing whitespace", "    branches:\n      - release/3.6   \n", ["release/3.6"]),
    ("comment on the branches: key",
     "    branches:  # only the deploy branch\n      - release/3.6\n", ["release/3.6"]),
    ("'#' inside the branch name",
     "    branches:\n      - release/3.6#keep\n", ["release/3.6#keep"]),
    ("two entries", "    branches:\n      - release/3.6\n      - release/3.5\n",
     ["release/3.6", "release/3.5"]),
    # Round-11 findings: every one of these is a list GitHub acts on, and the
    # regex parser reported no branches for all of them.
    ("flow sequence", "    branches: [release/3.6]\n", ["release/3.6"]),
    ("flow sequence, quoted", '    branches: ["release/3.6"]\n', ["release/3.6"]),
    ("flow sequence, padded", "    branches: [ release/3.6 ]\n", ["release/3.6"]),
    ("flow sequence, two", "    branches: [release/3.6, main]\n", ["release/3.6", "main"]),
    ("flow sequence, empty", "    branches: []\n", []),
    ("folded scalar entry",
     "    branches:\n      - >-\n          release/3.6\n", ["release/3.6"]),
    ("literal scalar entry",
     "    branches:\n      - |-\n          release/3.6\n", ["release/3.6"]),
    ("explicitly tagged entry",
     "    branches:\n      - !!str release/3.6\n", ["release/3.6"]),
    ("anchored list", "    branches: &b\n      - release/3.6\n    tags: *b\n", ["release/3.6"]),
    # A plain scalar may contain spaces, so this is a one-branch list whose
    # branch happens not to exist. The workflow greps for an exact line, so it
    # reads as "not pinned" — which is right — but the name is reported as YAML
    # reads it, not silently dropped.
    ("space inside the name", "    branches:\n      - release/3.6 x\n", ["release/3.6 x"]),
]

# Documents no YAML parser reads as a branch list. Refusing is the point: these
# are the shapes where guessing invents a pin.
REFUSED = [
    # `- release/3.6` indented BELOW its own key. The compact form allows a dash
    # only at exactly the key's indentation; less than that closes the mapping
    # and leaves a stray sequence, which is why every parser rejects the
    # document. The regex parser read it as a pin, and the chain from there was
    # the worst one available: repoint considered done, variable flipped, and a
    # docs-deploy.yml GitHub cannot load at all.
    ("dash indented below the key", "    branches:\n  - release/3.6\n", "INVALID"),
    ("tab indentation", "    branches:\n\t- release/3.6\n", "INVALID"),
    ("mismatched quotes, '..\"", "    branches:\n      - 'release/3.6\"\n", "INVALID"),
    # Both quote polarities, because a check that only ever sees one of them
    # passes for a parser that handles only that one.
    ('mismatched quotes, "..\'', '    branches:\n      - "release/3.6\'\n', "INVALID"),
    # `-release/3.6` is the plain scalar "-release/3.6", so `branches` is a
    # string, not a list.
    ("no space after the dash", "    branches:\n      -release/3.6\n", "NO_LIST"),
    ("branches:#c — not a key in YAML",
     "    branches:#c\n      - release/3.6\n", "NO_LIST"),
    ("branches: with no value", "    branches:\n", "NO_LIST"),
    # A different key entirely; it must not be mistaken for `branches:`.
    ("branches-ignore only", "    branches-ignore:\n      - main\n", "NO_LIST"),
    ("push: with only paths, pull_request lists branches later",
     "    paths:\n      - x\n  pull_request:\n    branches:\n      - release/3.6\n",
     "NO_LIST"),
    # Entries that resolve to something other than a string are not branch
    # names, and comparing one to a branch is the silent mismatch this script
    # exists to prevent.
    ("numeric entry", "    branches:\n      - 3.6\n", "NON_STRING"),
    ("nested sequence entry", "    branches:\n      - - release/3.6\n", "NON_STRING"),
    ("mapping entry", "    branches:\n      - name: release/3.6\n", "NON_STRING"),
    # A duplicate key has no single right answer: js-yaml raises, and PyYAML's
    # loader keeps the LAST occurrence while a node walk would naturally take
    # the first. Taking the first would report `release/3.6` for a document
    # whose effective value is `main`, repoint the losing copy, and pass its own
    # pre-write verification. Refusing is the only reading that cannot be wrong.
    ("duplicate branches: key",
     "    branches:\n      - release/3.6\n    branches:\n      - main\n", "NO_LIST"),
]

# A tab used as separation whitespace after the dash, or a tab-only blank line.
# js-yaml accepts both; PyYAML and libyaml (the reference C implementation)
# reject both. We follow the two that reject, and record the disagreement here
# rather than leaving it to be rediscovered: the direction is the safe one. A
# refusal makes `detect` warn that the branch is not pinned and hold, and makes
# `prepare` fail loudly during a cutover; neither can invent a pin. actionlint
# and yamllint both flag tabs, so a shape like this does not survive review.
TAB_DIVERGENCE = [
    ("tab after the dash", "    branches:\n    -\trelease/3.6\n", "INVALID"),
    ("tab-only blank line inside the list",
     "    branches:\n      - release/3.6\n\t\n    paths:\n      - x\n", "INVALID"),
]

# Repoint cases: (label, body, the exact substring the rewrite must change).
# Checking a substring swap rather than a recomputed expectation keeps the
# assertion honest about the promise — one name, nothing else.
ROUND_TRIP = [
    ("plain", "    branches:\n      - release/3.6\n", "release/3.6", "release/3.7"),
    ("single quotes", "    branches:\n      - 'release/3.6'\n",
     "'release/3.6'", "'release/3.7'"),
    ("double quotes", '    branches:\n      - "release/3.6"\n',
     '"release/3.6"', '"release/3.7"'),
    ("comment preserved", "    branches:\n      - release/3.6  # deploy\n",
     "- release/3.6  # deploy", "- release/3.7  # deploy"),
    ("flow sequence", "    branches: [release/3.6]\n", "[release/3.6]", "[release/3.7]"),
    ("flow sequence, padded", "    branches: [ release/3.6 ]\n",
     "[ release/3.6 ]", "[ release/3.7 ]"),
    ("dash at the key's indent", "    branches:\n    - release/3.6\n",
     "    - release/3.6", "    - release/3.7"),
]


def run():
    oracle = "--oracle" in sys.argv
    if oracle and not shutil.which("node"):
        print("--oracle requested but node is not available; refusing to skip")
        return 1
    if oracle:
        # Fail before running 300 comparisons that would all report the same
        # missing dependency.
        # Probed with a real document, not a missing path: the oracle exits 2
        # on an unreadable file too, and a probe that cannot tell "js-yaml is
        # missing" from "that path does not exist" would report the wrong
        # reason. Requiring a parsed answer also exercises the whole path once
        # before 300 comparisons rely on it.
        with tempfile.NamedTemporaryFile("w", suffix=".yml", delete=False,
                                         newline="") as fh:
            fh.write(HEAD + "    branches:\n      - release/3.6\n" + TAIL)
            probe_path = fh.name
        try:
            probe = subprocess.run(["node", ORACLE, probe_path],
                                   capture_output=True, text=True)
        finally:
            os.unlink(probe_path)
        if probe.returncode != 0 or '"status":"ok"' not in probe.stdout.replace(" ", ""):
            detail = (probe.stderr or probe.stdout).strip()
            print(f"--oracle requested but unusable: {detail}")
            return 1

    corpus = []
    for name, body, want in ACCEPTED + REFUSED + TAB_DIVERGENCE:
        src = HEAD + body + TAIL
        check(name, parse(src), want)
        corpus.append((name, src, want))
        # Line endings are not content. Every shape must read the same with
        # CRLF, which the regex parser refused outright.
        crlf = src.replace("\n", "\r\n")
        check(f"CRLF {name}", parse(crlf), want)
        corpus.append((f"CRLF {name}", crlf, want))

    check("comment on the push: key",
          parse("on:\n  push:  # publish\n    branches:\n      - release/3.6\n" + TAIL),
          ["release/3.6"])
    # `push:#c` is a plain scalar, not a key, so the more-indented block under
    # it makes the whole document invalid. Both parsers agree.
    check("push:#c — not a key in YAML",
          parse("on:\n  push:#c\n    branches:\n      - release/3.6\n" + TAIL),
          "INVALID")
    check("no on: block at all",
          parse("jobs:\n  build:\n    runs-on: ubuntu-latest\n"), "NO_LIST")
    check("empty file", parse(""), "NO_LIST")
    # The MESSAGE, not just the refusal: `parse()` reduces both an empty file
    # and a missing `on:` to NO_LIST, so the dedicated empty-file branch could
    # be deleted with the suite still green — while docs-cutover.yml's detect
    # step has a comment that names this exact wording as the reason it does
    # not parse an empty file at all.
    rc, out, _ = run_cli("", ["--list"])
    check("empty file rc", rc, 1)
    check("empty file names the reason", "the file is empty" in out, True)
    # `on: push` with no configuration has no branch list to read.
    check("on: as a bare string", parse("on: push\n" + TAIL), "NO_LIST")
    # Quoted to dodge YAML 1.1's on/off booleans — the same trigger, and it
    # must read the same. This is read at node level precisely so that the
    # 1.1-vs-1.2 resolution of a bare `on` never enters into it.
    # Every spelling that still composes to the scalar key `on`: both quote
    # polarities and the explicit-key form. All three are the same trigger, and
    # a check that only ever sees one of them passes for a reader that handles
    # only that one.
    for label, doc in (
        ('double-quoted "on"', '"on":\n  push:\n    branches:\n      - release/3.6\n'),
        ("single-quoted 'on'", "'on':\n  push:\n    branches:\n      - release/3.6\n"),
        ("explicit key ? on", "? on\n:\n  push:\n    branches:\n      - release/3.6\n"),
    ):
        check(f"{label} key", parse(doc + TAIL), ["release/3.6"])

    # ...and a tag on the KEY is not one of them. GitHub's parser rejects an
    # unresolvable tag, but a lookup comparing only key.value accepts it, so the
    # key's own tag is checked too. A bare `on` key carries YAML 1.1's BOOLEAN
    # tag and a quoted one carries the string tag; both are the same trigger, so
    # the check has to admit both and still refuse anything else.
    for label, doc in (
        ("custom tag on the on: key", "!unknown on:\n  push:\n    branches: [release/3.6]\n"),
        ("custom tag on the push: key", "on:\n  !unknown push:\n    branches: [release/3.6]\n"),
        ("custom tag on the branches: key",
         "on:\n  push:\n    !unknown branches: [release/3.6]\n"),
    ):
        check(f"{label} refused", parse(doc + TAIL), "NO_LIST")
    # ...but capitalisation is not that difference. GitHub parses workflows
    # with the `yaml` npm package (YAML 1.2, where `on` is a string, not a
    # boolean) and matches the literal lowercase key from its own schema, so
    # `On:` is a different key and the file has no push trigger at all.
    # Reading a pin out of it would report a pin that does not publish.
    for key in ("On", "ON"):
        check(f"{key}: is not a trigger",
              parse(f"{key}:\n  push:\n    branches:\n      - release/3.6\n" + TAIL),
              "NO_LIST")

    # A tag GitHub cannot resolve makes the whole workflow unloadable, and a
    # custom-tagged mapping still composes to a MappingNode — so each node on
    # the path is checked, or the pre-write verification would bless a file
    # Actions rejects.
    for label, doc in (
        ("on", "on: !nope\n  push:\n    branches:\n      - release/3.6\n"),
        ("push", "on:\n  push: !nope\n    branches:\n      - release/3.6\n"),
        ("branches", "on:\n  push:\n    branches: !nope\n      - release/3.6\n"),
    ):
        check(f"custom tag on {label}: refused", parse(doc + TAIL), "NO_LIST")

    # ...at every level of the path, not just the innermost one.
    for label, doc in (
        ("push", "on:\n  push:\n    branches:\n      - release/3.6\n"
                 "  push:\n    branches:\n      - main\n"),
        ("on", "on:\n  push:\n    branches:\n      - release/3.6\n"
               "on:\n  push:\n    branches:\n      - main\n"),
    ):
        check(f"duplicate {label}: key refused", parse(doc + TAIL), "NO_LIST")

    # A branch named by another trigger is not a deploy pin — in either order.
    after = ("on:\n  push:\n    branches:\n      - release/3.6\n"
             "  pull_request:\n    branches:\n      - release/3.7\n" + TAIL)
    check("pull_request after push", parse(after), ["release/3.6"])
    before = ("on:\n  pull_request:\n    branches:\n      - release/3.5\n"
              "  push:\n    branches:\n      - release/3.6\n" + TAIL)
    check("pull_request before push", parse(before), ["release/3.6"])

    keyc = ("on:\n  push:\n    # which branch publishes\n    branches:\n"
            "      - release/3.6\n    paths:  # only the site\n      - x\n" + TAIL)
    check("comment at key indent, comment on closing key", parse(keyc), ["release/3.6"])

    # A second document in the stream is not a workflow GitHub would load, and
    # reading the branch list out of one of them is a guess about which.
    check("multi-document stream",
          parse(HEAD + "    branches:\n      - release/3.6\n" + TAIL + "---\non:\n  push:\n"),
          "INVALID")

    # --list is the contract the workflow greps with `-Fxq`: ONE NAME PER LINE.
    # Verified with two entries, because a single entry cannot distinguish a
    # newline-joined list from a space-joined one.
    rc, out, _ = run_cli(HEAD + "    branches:\n      - release/3.6\n" + TAIL, ["--list"])
    check("--list rc", rc, 0)
    check("--list output", out, "release/3.6\n")
    _, out2, _ = run_cli(
        HEAD + "    branches:\n      - release/3.6\n      - release/3.5\n" + TAIL, ["--list"])
    check("--list is one name per line", out2, "release/3.6\nrelease/3.5\n")

    # Repointing rewrites the name and nothing else, in every shape and under
    # both line endings.
    for label, body, old, new in ROUND_TRIP:
        for eol, join in (("LF", "\n"), ("CRLF", "\r\n")):
            src = (HEAD + body + TAIL).replace("\n", join)
            rc, _, result = run_cli(src, ["--repoint", "release/3.7"])
            check(f"repoint {label} ({eol}) rc", rc, 0)
            check(f"repoint {label} ({eol})", result,
                  src.replace(old.replace("\n", join), new.replace("\n", join)))

    # A folded entry cannot be edited in place without collapsing it, so this
    # one is checked for what it must be rather than for a substring swap: one
    # line, the new name, and a file that still parses.
    folded = HEAD + "    branches:\n      - >-\n          release/3.6\n" + TAIL
    rc, _, result = run_cli(folded, ["--repoint", "release/3.7"])
    check("repoint folded rc", rc, 0)
    check("repoint folded collapses to one entry", parse(result), ["release/3.7"])
    check("repoint folded keeps the rest of the file", result.endswith(TAIL), True)

    # Same for the styles whose node span is wider than the name itself: the
    # anchor, the explicit tag and the block header are inside the span, so
    # repointing collapses them to a plain scalar. Asserted rather than left to
    # chance, because "the edit touches only the name" is not what happens and
    # the tests should say which it is.
    for label, body, keeps in (
        ("anchored entry", "    branches:\n      - &pin release/3.6 # target\n",
         "# target"),
        ("tagged entry", "    branches:\n      - !!str release/3.6\n", None),
        ("block header with a comment",
         "    branches:\n      - |- # suppress the newline\n          release/3.6\n",
         None),
    ):
        src = HEAD + body + TAIL
        rc, _, result = run_cli(src, ["--repoint", "release/3.7"])
        check(f"repoint {label} rc", rc, 0)
        check(f"repoint {label} reads as one entry", parse(result), ["release/3.7"])
        check(f"repoint {label} keeps the rest of the file",
              result.endswith(TAIL), True)
        if keeps:
            check(f"repoint {label} keeps its trailing comment", keeps in result, True)

    # `branches: *b` IS the node the anchor named, so its source span sits
    # wherever that anchor was written. With the anchor on another key, a
    # span rewrite edits THAT key — repointing rewrote `paths` and the
    # pre-write check still passed, because the alias made branches read the
    # new value too. Refused, and the file is left alone.
    aliased_elsewhere = ("on:\n  push:\n    paths: &b [release/3.6]\n"
                         "    branches: *b\n" + TAIL)
    check("branches aliasing another key reads as unreadable",
          parse(aliased_elsewhere), "NO_LIST")
    rc, _, result = run_cli(aliased_elsewhere, ["--repoint", "release/3.7"])
    check("branches aliasing another key refuses", rc != 0, True)
    check("branches aliasing another key unchanged", result, aliased_elsewhere)

    # The mirror image must still WORK: the anchor on branches itself, aliased
    # into another key. Those really are one list, so rewriting both is what
    # the document says.
    anchor_on_branches = ("on:\n  push:\n    branches: &b\n      - release/3.6\n"
                          "    tags: *b\n" + TAIL)
    check("anchor on branches still reads", parse(anchor_on_branches), ["release/3.6"])
    rc, _, result = run_cli(anchor_on_branches, ["--repoint", "release/3.7"])
    check("anchor on branches still repoints", rc, 0)
    check("anchor on branches reads back as one entry", parse(result), ["release/3.7"])

    # The one case where dropping the anchor would change meaning: something
    # else aliases it. The rewrite is then unparseable, and the pre-write
    # verification refuses rather than landing an undefined alias.
    aliased = ("on:\n  push:\n    branches:\n      - &pin release/3.6\n"
               "  pull_request:\n    branches: [*pin]\n" + TAIL)
    rc, _, result = run_cli(aliased, ["--repoint", "release/3.7"])
    check("aliased anchor refuses", rc != 0, True)
    check("aliased anchor unchanged", result, aliased)

    # Already pinned is a no-op, not a failure: the workflow's step would
    # otherwise try to commit an unchanged tree and go red.
    pinned = HEAD + "    branches:\n      - release/3.7\n" + TAIL
    rc, out, result = run_cli(pinned, ["--repoint", "release/3.7"])
    check("already-pinned rc", rc, 0)
    check("already-pinned is a no-op", result, pinned)
    check("already-pinned says so", "already pins release/3.7" in out, True)

    # Refusals must leave the file alone. A partial rewrite of a workflow file
    # is the one outcome worse than not rewriting it.
    for label, body in (
        ("unreadable list", "    branches:\n      -release/3.6\n"),
        ("two entries", "    branches:\n      - release/3.6\n      - release/3.5\n"),
        ("empty list", "    branches: []\n"),
        ("invalid document", "    branches:\n  - release/3.6\n"),
        ("non-string entry", "    branches:\n      - 3.6\n"),
    ):
        src = HEAD + body + TAIL
        rc, _, result = run_cli(src, ["--repoint", "release/3.7"])
        # rc, not the message: a mutant that prints the refusal and returns 0
        # would let the step go on to commit an unchanged tree and fail there.
        check(f"{label} refuses", rc != 0, True)
        check(f"{label} unchanged", result, src)

    # A bad invocation must say so rather than traceback on a path it never
    # needed to read.
    os.environ["DEPLOY_YML"] = os.path.join(HERE, "does-not-exist.yml")
    for argv in ([], ["--nope"], ["--repoint"], ["--repoint", ""]):
        sys.argv = ["docs-deploy-trigger.py", *argv]
        try:
            with contextlib.redirect_stdout(io.StringIO()):
                rc = mod.main()
        except SystemExit as e:
            rc = e.code if isinstance(e.code, int) else 1
        except OSError:
            rc = "OSError"
        check(f"usage {argv}", rc, 1)

    # The pre-write verification is the guarantee that a rewrite can never land
    # a file GitHub cannot load, so it is tested directly: a branch name that
    # would not survive being spliced in as a plain scalar must be refused, not
    # written.
    src = HEAD + "    branches:\n      - release/3.6\n" + TAIL
    # The last one matters most: it splices in as a syntactically valid
    # one-entry list, so only comparing the parsed name against the requested
    # one catches it. A check of the entry COUNT alone passes and writes a
    # branch nobody asked for.
    for bad in ("release/3.7: x", "[release/3.7", "release/3.7\n      - main",
                "release/3.7 # do not deploy"):
        rc, _, result = run_cli(src, ["--repoint", bad])
        check(f"unsafe name {bad!r} refuses", rc != 0, True)
        check(f"unsafe name {bad!r} unchanged", result, src)

    if oracle:
        # The fixtures above were written from what PyYAML does. Checking them
        # against an unrelated implementation is what makes them evidence about
        # YAML rather than about PyYAML.
        known = {name for name, _, _ in TAB_DIVERGENCE}
        known |= {f"CRLF {name}" for name, _, _ in TAB_DIVERGENCE}
        for name, src, want in corpus:
            got = oracle_read(src)
            if name in known:
                # Recorded divergence: js-yaml accepts what we refuse. Assert
                # that it is still exactly this, so the day js-yaml tightens up
                # (or we stop refusing) the note stops being true out loud.
                check(f"oracle {name} still diverges", got["status"], "ok")
                continue
            if isinstance(want, list):
                check(f"oracle {name}", (got["status"], got.get("branches")),
                      ("ok", want))
            else:
                check(f"oracle {name} refused by both", got["status"] != "ok", True)

    for f in FAILURES:
        print("FAIL", f)
    print(f"{CHECKS - len(FAILURES)}/{CHECKS} passed"
          f"{'' if oracle else ' (run with --oracle to cross-check against js-yaml)'}")
    return 1 if FAILURES else 0


if __name__ == "__main__":
    sys.exit(run())
