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
# default branch and one file of the given size added on branch "feat".
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

# run_case <name> <want_exit> <dir> [args...]
run_case() {
  local name="$1" want="$2" dir="$3"
  shift 3
  local out rc
  out=$(cd "$dir" && LARGE_FILE_LIMIT_BYTES="${LARGE_FILE_LIMIT_BYTES:-1048576}" bash "$script" "$@" 2>&1)
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

run_case "a file over the limit fails" 1 "$work/big" base feat
run_case "a file under the limit passes" 0 "$work/small" base feat
run_case "an empty base is an error, not a pass" 2 "$work/big" "" feat
run_case "an unknown base is an error, not a pass" 2 "$work/big" no-such-ref feat
LARGE_FILE_LIMIT_BYTES=4194304 run_case "the limit is configurable" 0 "$work/big" base feat

# The failure message must name the file, else CI says nothing actionable.
# Captured first: pipefail would otherwise report the script's own exit code.
report=$(cd "$work/big" && bash "$script" base feat 2>&1)
if printf '%s\n' "$report" | grep -q 'added.bin'; then
  printf 'ok   - the failure names the file\n'
  pass=$((pass + 1))
else
  printf 'FAIL - the failure does not name the file\n'
  fail=$((fail + 1))
fi

printf '\n%d passed, %d failed\n' "$pass" "$fail"
[ "$fail" -eq 0 ]
