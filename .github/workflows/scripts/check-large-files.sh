#!/usr/bin/env bash
# Fail when the range <base>..HEAD adds or grows a file past the size limit.
# Anything that stops the check from answering exits 2, so a broken invocation
# can never read as "no large files".
# Callers: `make check-large-files` and .github/workflows/check-large-files.yml.
set -uo pipefail

limit=${LARGE_FILE_LIMIT_BYTES:-1048576}
base=${1:-}

if [ -z "$base" ]; then
  echo "ERROR: usage: $0 <base-ref>" >&2
  exit 2
fi

if ! merge_base=$(git merge-base "$base" HEAD 2>/dev/null); then
  echo "ERROR: cannot find a merge base for '$base' and HEAD" >&2
  exit 2
fi

if ! files=$(git diff --diff-filter=ACMR --name-only "$merge_base" HEAD); then
  echo "ERROR: cannot list the files changed since '$base'" >&2
  exit 2
fi

found=0
while IFS= read -r file; do
  [ -n "$file" ] || continue
  if ! size=$(git cat-file -s "HEAD:$file" 2>/dev/null); then
    echo "ERROR: cannot read the size of '$file' at HEAD" >&2
    exit 2
  fi
  if [ "$size" -gt "$limit" ]; then
    awk -v s="$size" -v f="$file" 'BEGIN { printf "%.1f MB: %s\n", s / 1048576, f }'
    found=1
  fi
done <<<"$files"

if [ "$found" -eq 1 ]; then
  awk -v l="$limit" 'BEGIN { printf "ERROR: files exceeding %.1f MB found.\n", l / 1048576 }' >&2
  exit 1
fi
