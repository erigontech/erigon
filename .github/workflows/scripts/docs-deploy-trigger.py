#!/usr/bin/env python3
"""Read or repoint the push trigger's branch list in docs-deploy.yml.

  --list             print each push branch, one per line
  --repoint <branch> rewrite the list to that single branch

READING IS DELEGATED TO A REAL YAML PARSER.
An earlier version of this script matched the branch list with regexes and
answered differently from a real parser in four separate review rounds. Every
shape GitHub accepts has to be read the way GitHub reads it, because the cost
of disagreeing is asymmetric in both directions: reporting a pin that
`on.push.branches` does not contain makes the cutover flip the deploy variable
to a branch that never publishes, and failing to see a pin that IS there makes
it open a repoint pull request that is not needed. Flow sequences, folded and
tagged scalars, quoting styles and CRLF are all things a hand-rolled matcher
gets wrong one shape at a time; `yaml.compose` gets them right by construction,
and rejects the documents no parser accepts (a dash indented below its key, a
tab used for indentation) instead of reading a pin out of them.

WRITING STAYS A SURGICAL TEXT EDIT.
Serialising the parsed document back out would reformat the file and drop every
comment in it. Instead the node's own source span -- the parser's answer to
"where is this scalar" -- is the only region touched, so quoting style, inline
comments, line endings and the rest of the file survive byte for byte. The
rewritten text is then handed back to the same parser and must read as exactly
the requested branch BEFORE anything is written to disk: a bad edit fails
closed with the file untouched, rather than landing a workflow GitHub cannot
load.
"""
import os
import sys

import yaml

STR_TAG = 'tag:yaml.org,2002:str'
MAP_TAG = 'tag:yaml.org,2002:map'
SEQ_TAG = 'tag:yaml.org,2002:seq'


class Unreadable(Exception):
    """The file's push-branch list cannot be determined."""


def target_path():
    """File to read or edit.

    Callers point DEPLOY_YML at a temp copy fetched from a branch, or at a
    clone's working copy; it is never assumed to be this repo's own file.
    Read per call rather than at import so tests can vary it.
    """
    return os.environ.get('DEPLOY_YML', '.github/workflows/docs-deploy.yml')


def _value_of(node, *keys):
    """The value node of `keys` in a mapping node, refusing a duplicate.

    A duplicate key is the one shape where "read it the way GitHub reads it"
    has no answer to give. YAML itself calls it an error; js-yaml raises;
    PyYAML's LOADER silently keeps the LAST occurrence; and taking the first --
    the obvious thing for a node walk -- would make this script report a pin
    the effective document does not have, repoint the occurrence that loses,
    and then bless its own edit, because the pre-write verification reads by
    the same rule. That is the disagree-with-the-real-parser failure this
    script was rewritten to eliminate, so it is refused rather than resolved.
    """
    if not isinstance(node, yaml.MappingNode):
        return None
    found = [value for key, value in node.value
             if isinstance(key, yaml.ScalarNode) and key.value in keys]
    if len(found) > 1:
        raise Unreadable(f'{keys[0]}: appears {len(found)} times'
                         ' - refusing to guess which one GitHub reads')
    return found[0] if found else None


def branch_nodes(src):
    """The scalar nodes of `on.push.branches`, with their source spans.

    Raises Unreadable when the document is not valid YAML or does not have a
    push trigger with a branch list, so that callers can distinguish "no pin"
    from "cannot tell" -- and so the pre-write verification can catch its own
    failure instead of exiting the process.
    """
    try:
        root = yaml.compose(src, Loader=yaml.SafeLoader)
    except yaml.YAMLError as exc:
        raise Unreadable(f'not valid YAML: {exc}') from exc
    if root is None:
        raise Unreadable('the file is empty')

    # At node level the `on` key is the literal string 'on', whether or not it
    # was quoted to dodge YAML 1.1's on/off booleans; it is PyYAML's
    # constructor, not its parser, that would turn a bare `on` into boolean
    # true. Reading the node keeps this independent of that resolution.
    #
    # Only the exact lowercase key counts. GitHub parses workflows with the
    # `yaml` npm package -- YAML 1.2, where `on` is the string it looks like,
    # not a boolean -- and matches the literal key `on` from its own schema
    # (actions/languageservices, workflow-parser/src/workflow-v1.0.json).
    # `On:` and `ON:` are therefore different keys and not triggers at all, so
    # reading a pin out of one would report a pin on a file that has no push
    # trigger and let a repoint "succeed" on a workflow GitHub cannot load.
    trigger = _value_of(root, 'on')
    if trigger is None:
        raise Unreadable('no on: block')
    push = _value_of(trigger, 'push')
    if push is None:
        raise Unreadable('no push: trigger - refusing to edit blindly')

    branches = _value_of(push, 'branches')
    if branches is None or not isinstance(branches, yaml.SequenceNode):
        raise Unreadable('the push: trigger has no branches: list'
                         ' - refusing to edit blindly')
    # An unresolvable tag anywhere on the path is a workflow GitHub rejects
    # outright, and a custom-tagged mapping still composes to a MappingNode --
    # so without this the pre-write verification would happily bless
    # `on: !nope` as loadable.
    for label, node, tag in (('on', trigger, MAP_TAG),
                             ('push', push, MAP_TAG),
                             ('branches', branches, SEQ_TAG)):
        if node.tag != tag:
            raise Unreadable(f'{label}: has a non-standard tag ({node.tag})')

    for entry in branches.value:
        # A non-string entry is not a branch name: `- 3.6` resolves to a float,
        # and comparing that to a branch is the silent mismatch this script
        # exists to prevent. An unknown tag (`- !mine x`) is a workflow GitHub
        # would reject outright.
        if not isinstance(entry, yaml.ScalarNode) or entry.tag != STR_TAG:
            raise Unreadable(f'branches: has a non-string entry (tag {entry.tag})')
    return branches.value


def read_branches(src):
    """The branch scalar nodes, or exit with the reason they cannot be read."""
    try:
        return branch_nodes(src)
    except Unreadable as exc:
        sys.exit(f'docs-deploy.yml {exc}')


def repoint(src, new):
    """`src` with its single push branch replaced by `new`.

    Returns (patched_text, old_name), or None when it already reads `new`.
    """
    nodes = read_branches(src)
    names = [node.value for node in nodes]
    if len(names) != 1:
        sys.exit(f'expected exactly one push branch entry, found {names}')
    if names[0] == new:
        return None

    lo, hi = nodes[0].start_mark.index, nodes[0].end_mark.index
    raw = src[lo:hi]
    # The span is the scalar NODE's, which for some styles is wider than the
    # text of the name: an anchor (`&pin release/3.6`), an explicit tag
    # (`!!str release/3.6`) and a block header with its comment
    # (`|- # why\n    release/3.6`) are all inside it. Replacing the span
    # therefore collapses those styles to a plain scalar, dropping the anchor,
    # the tag and any comment on the header line. That is deliberate: the
    # result is still exactly the requested one-branch list, the deploy trigger
    # has no use for any of them, and the one case where dropping an anchor
    # would change meaning -- an anchor something else aliases -- makes the
    # rewrite unparseable and is refused below rather than written. Anything
    # narrower would mean re-deriving a style the parser already resolved,
    # which is the hand-rolled matching this script exists to avoid.
    #
    # The span of a block-style entry runs to the end of its line, so it can
    # swallow trailing whitespace and the newline itself; putting them back
    # keeps the edit off the following line. A quoted span ends at its closing
    # quote, where there is nothing to put back.
    body = raw.rstrip()
    tail = raw[len(body):]
    quote = body[0] if body[:1] in ('"', "'") else ''
    patched = f'{src[:lo]}{quote}{new}{quote}{tail}{src[hi:]}'

    # The rewrite is verified before it is written, not after: the failure this
    # guards against is a workflow file GitHub cannot load, and leaving that on
    # disk to be repaired afterwards is the outcome to avoid.
    try:
        got = [node.value for node in branch_nodes(patched)]
    except Unreadable as exc:
        sys.exit(f'refusing to write: the rewrite would not parse ({exc})')
    if got != [new]:
        sys.exit(f'refusing to write: the rewrite reads {got}, expected {[new]}')
    return patched, names[0]


def main():
    if len(sys.argv) < 2 or sys.argv[1] not in ('--list', '--repoint'):
        sys.exit(f'usage: {sys.argv[0]} --list | --repoint <branch>')
    if sys.argv[1] == '--repoint' and (len(sys.argv) < 3 or not sys.argv[2]):
        sys.exit(f'usage: {sys.argv[0]} --repoint <branch>')

    path = target_path()
    # newline='' keeps CRLF intact in both directions: read as text with
    # translation on, a CRLF file arrives with LF endings, every source span
    # shifts, and writing it back would silently reformat the whole file.
    with open(path, encoding='utf-8', newline='') as handle:
        src = handle.read()

    if sys.argv[1] == '--list':
        print('\n'.join(node.value for node in read_branches(src)))
        return 0

    new = sys.argv[2]
    result = repoint(src, new)
    if result is None:
        # Not an error: the check and this rewrite read the same list, so this
        # means the state was satisfied between the two. Degrade to a no-op
        # rather than failing the job every day.
        print(f'{path} already pins {new} - nothing to do')
        return 0
    patched, old = result

    with open(path, 'w', encoding='utf-8', newline='') as handle:
        handle.write(patched)
    print(f'repointed {old} -> {new}')
    return 0


if __name__ == '__main__':
    sys.exit(main())
