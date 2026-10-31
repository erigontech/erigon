#!/usr/bin/env bash
# Local test harness for check-large-files.sh — builds throwaway repos and
# asserts the exit code. The bug this pins: the check used to compute an empty
# base and compare HEAD with itself, so it passed on every input.
# Run: bash .github/workflows/scripts/check-large-files.test.sh
set -uo pipefail

here=$(cd -- "$(dirname -- "$0")" && pwd)
script="$here/check-large-files.sh"
pass=0
fail=0
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT

# make_repo <dir> <bytes-on-branch> — a repo with a small file committed on the
# default branch and one file of the given size added on branch "feat",
# left checked out so the script sees it as HEAD.
make_repo() {
  local dir="$1" bytes="$2"
  mkdir -p "$dir"
  git -C "$dir" init -q -b base
  git -C "$dir" config user.email test@example.com
  git -C "$dir" config user.name test
  echo small >"$dir/small.txt"
  git -C "$dir" add -A
  git -C "$dir" commit -qm base
  git -C "$dir" checkout -q -b feat
  head -c "$bytes" /dev/zero >"$dir/added.bin"
  git -C "$dir" add -A
  git -C "$dir" commit -qm add
}

# make_repo_with_big <dir> <base-bytes> — the oversized file is already on the
# default branch, so "feat" starts from a repo that is over the limit.
make_repo_with_big() {
  local dir="$1" bytes="$2"
  mkdir -p "$dir"
  git -C "$dir" init -q -b base
  git -C "$dir" config user.email test@example.com
  git -C "$dir" config user.name test
  head -c "$bytes" /dev/zero >"$dir/big.bin"
  git -C "$dir" add -A
  git -C "$dir" commit -qm base
  git -C "$dir" checkout -q -b feat
}

# run_case <name> <want_exit> <dir> [args...]
run_case() {
  local name="$1" want="$2" dir="$3"
  shift 3
  local out rc
  out=$(cd "$dir" && bash "$script" "$@" 2>&1)
  rc=$?
  if [ "$rc" -eq "$want" ]; then
    printf 'ok   - %s (exit %d)\n' "$name" "$rc"
    pass=$((pass + 1))
  else
    printf 'FAIL - %s: want exit %d, got %d\n' "$name" "$want" "$rc"
    printf '%s\n' "$out" | sed 's/^/       | /'
    fail=$((fail + 1))
  fi
}

make_repo "$work/big" 2097152
make_repo "$work/small" 1024

run_case "a file over the limit fails" 1 "$work/big" base
run_case "a file under the limit passes" 0 "$work/small" base
run_case "an empty base is an error, not a pass" 2 "$work/big" ""
run_case "an unknown base is an error, not a pass" 2 "$work/big" no-such-ref
LARGE_FILE_LIMIT_BYTES=4194304 run_case "the limit is configurable" 0 "$work/big" base

# A file already over the limit on the base is not this range's doing. The
# check must compare against the predecessor, else any touch of one of the
# oversized files already in the tree blocks the PR.
make_repo_with_big "$work/rename" 2097152
git -C "$work/rename" mv big.bin moved.bin
git -C "$work/rename" commit -qm rename
run_case "renaming a file already over the limit passes" 0 "$work/rename" base

make_repo_with_big "$work/shrink" 2097152
head -c 1572864 /dev/zero >"$work/shrink/big.bin"
git -C "$work/shrink" commit -qam shrink
run_case "shrinking a file already over the limit passes" 0 "$work/shrink" base

make_repo_with_big "$work/grow" 2097152
head -c 3145728 /dev/zero >"$work/grow/big.bin"
git -C "$work/grow" commit -qam grow
run_case "growing a file already over the limit fails" 1 "$work/grow" base

# A symlink swapped for an oversized regular file reaches HEAD as a type
# change, which --diff-filter must not drop.
mkdir -p "$work/typechange"
git -C "$work/typechange" init -q -b base
git -C "$work/typechange" config user.email test@example.com
git -C "$work/typechange" config user.name test
echo small >"$work/typechange/small.txt"
ln -s small.txt "$work/typechange/link"
git -C "$work/typechange" add -A
git -C "$work/typechange" commit -qm base
git -C "$work/typechange" checkout -q -b feat
rm "$work/typechange/link"
head -c 2097152 /dev/zero >"$work/typechange/link"
git -C "$work/typechange" commit -qam swap
run_case "a symlink replaced by an oversized file fails" 1 "$work/typechange" base

# The failure message must name the file, else CI says nothing actionable.
# Captured first: pipefail would otherwise report the script's own exit code.
report=$(cd "$work/big" && bash "$script" base 2>&1)
if printf '%s\n' "$report" | grep -q 'added.bin'; then
  printf 'ok   - the failure names the file\n'
  pass=$((pass + 1))
else
  printf 'FAIL - the failure does not name the file\n'
  fail=$((fail + 1))
fi

printf '\n%d passed, %d failed\n' "$pass" "$fail"
[ "$fail" -eq 0 ]
