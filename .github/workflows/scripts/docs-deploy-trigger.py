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

def target_path():
    """File to read or edit.

    Callers point DEPLOY_YML at a temp copy fetched from a branch, or at a
    clone's working copy; it is never assumed to be this repo's own file.
    Read per call rather than at import so tests can vary it.
    """
    return os.environ.get('DEPLOY_YML', '.github/workflows/docs-deploy.yml')


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
    push = _block_after(src, r'^(?P<i>[ \t]*)push:[ \t]*(?:#.*)?$')
    if not push:
        sys.exit('docs-deploy.yml has no push: trigger - refusing to edit blindly')
    b = _block_after(src[:push[1]], r'^(?P<i>[ \t]*)branches:[ \t]*(?:#.*)?$', push[0])
    if not b:
        sys.exit('the push: trigger has no branches: list - refusing to edit blindly')
    return b


# A '#' only starts a comment when whitespace precedes it (YAML requires that,
# and git permits '#' inside a branch name) — otherwise `- release/3.6#keep`
# would read as `release/3.6` and the trigger would be reported as pinned to a
# branch it does not actually name.
ENTRY = re.compile(
    r"^(?P<lead>[ \t]*-[ \t]*)"
    r"(?:(?P<q>['\"])(?P<qname>[^'\"]+)(?P=q)|(?P<name>\S+))"
    r"(?P<trail>(?:[ \t]+#.*)?[ \t]*)$", re.M)


def entries(block):
    return [m.group('qname') or m.group('name') for m in ENTRY.finditer(block)]


def main():
    path = target_path()
    src = open(path).read()
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
        print(f'{path} already pins {new} - nothing to do')
        return 0
    def swap(m):
        q = m.group('q') or ''
        return f"{m.group('lead')}{q}{new}{q}{m.group('trail')}"

    patched = ENTRY.sub(swap, block)
    open(path, 'w').write(src[:lo] + patched + src[hi:])
    print(f'repointed {names[0]} -> {new}')
    return 0


if __name__ == '__main__':
    sys.exit(main())
