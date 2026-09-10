#!/usr/bin/env python3
"""Read or repoint the push trigger's branch list in docs-deploy.yml.

ONE parser, two modes, because the check and the rewrite disagreeing is a
failure that repeats daily: the check says "not pinned", the rewrite says
"already pinned" and exits non-zero, and the job never makes progress.

  --list             print each push branch, one per line
  --repoint <branch> rewrite the list to that single branch
"""
import os
import re
import sys

# The file to read or edit. The callers point this at a temp copy fetched from
# a branch, or at a clone's working copy; it is never assumed to be this repo's.
PATH = os.environ.get('DEPLOY_YML', '.github/workflows/docs-deploy.yml')


def _is_skippable(line):
    return not line.strip() or line.lstrip().startswith('#')


def _block_after(src, key_re, start=0):
    """Span of the lines nested under the first `key:` matching key_re."""
    m = re.compile(key_re, re.M).search(src, start)
    if not m:
        return None
    indent = len(m.group('i'))
    rest = src[m.end():]
    end = len(rest)
    for line in re.finditer(r'^(?P<i>[ \t]*)(?P<body>.*)$', rest, re.M):
        if _is_skippable(line.group(0)):
            continue                      # comments/blanks never close a block
        if len(line.group('i')) <= indent and not line.group('body').startswith('-'):
            end = line.start()            # a dash at the key's column is a member
            break
    return m.end(), m.end() + end


def branch_span(src):
    push = _block_after(src, r'^(?P<i>[ \t]*)push:[ \t]*$')
    if not push:
        sys.exit('docs-deploy.yml has no push: trigger - refusing to edit blindly')
    b = _block_after(src[:push[1]], r'^(?P<i>[ \t]*)branches:[ \t]*$', push[0])
    if not b:
        sys.exit('the push: trigger has no branches: list - refusing to edit blindly')
    return b


ENTRY = re.compile(r"^(?P<lead>[ \t]*-[ \t]*)(?P<q>['\"]?)(?P<name>[^'\"#\s]+)(?P=q)"
                   r"(?P<trail>[ \t]*(?:#.*)?)$", re.M)


def entries(block):
    return [m.group('name') for m in ENTRY.finditer(block)]


def main():
    src = open(PATH).read()
    lo, hi = branch_span(src)
    block = src[lo:hi]
    names = entries(block)

    if sys.argv[1] == '--list':
        print('\n'.join(names))
        return 0

    new = sys.argv[2]
    if len(names) != 1:
        sys.exit(f'expected exactly one push branch entry, found {names}')
    if names[0] == new:
        # Not an error: the check and this rewrite read the same list, so this
        # means the state was satisfied between the two. Degrade to a no-op
        # rather than failing the job every day.
        print(f'{PATH} already pins {new} - nothing to do')
        return 0
    patched = ENTRY.sub(lambda m: f"{m.group('lead')}{m.group('q')}{new}{m.group('q')}{m.group('trail')}", block)
    open(PATH, 'w').write(src[:lo] + patched + src[hi:])
    print(f'repointed {names[0]} -> {new}')
    return 0


sys.exit(main())
