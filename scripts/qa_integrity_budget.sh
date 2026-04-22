#!/usr/bin/env bash
# QA harness for `erigon seg integrity --integrity.budget`.
# Exercises the budget / sorting / exhaustion paths without waiting hours.
#
# Usage: ./scripts/qa_integrity_budget.sh <datadir>

set -uo pipefail

DATADIR="${1:-}"
if [[ -z "$DATADIR" ]]; then
    echo "usage: $0 <datadir>" >&2
    exit 2
fi
if [[ ! -d "$DATADIR" ]]; then
    echo "datadir not found: $DATADIR" >&2
    exit 2
fi

BIN="./build/bin/erigon"
if [[ ! -x "$BIN" ]]; then
    echo "binary not found: $BIN (run 'make erigon' first)" >&2
    exit 2
fi

# Safety kill-switch applied to every case: larger than the biggest
# --integrity.budget value used below, so legitimate runs finish well under it.
# Any case that hits this is a bug (either it's stuck or the check is ignoring ctx).
SAFETY_SECS="${SAFETY_SECS:-900}"

TIMEOUT_BIN=""
if command -v timeout >/dev/null 2>&1; then
    TIMEOUT_BIN="timeout"
elif command -v gtimeout >/dev/null 2>&1; then
    TIMEOUT_BIN="gtimeout"
fi
if [[ -z "$TIMEOUT_BIN" ]]; then
    echo "warning: no 'timeout' or 'gtimeout' found; safety kill-switch disabled" >&2
    echo "  install coreutils (macOS: 'brew install coreutils') for insurance" >&2
fi

OUT="$(mktemp -d)"
echo "logs -> $OUT"
echo "safety ceiling per case: ${SAFETY_SECS}s (via ${TIMEOUT_BIN:-none})"
echo

PASS=0
FAIL=0
CASES=0
CURRENT_LOG=""
CURRENT_EC=0

run_case() {
    local name="$1"; shift
    CURRENT_LOG="$OUT/${name}.log"
    echo "=== $name ==="
    echo "  cmd: $*"
    CURRENT_EC=0
    if [[ -n "$TIMEOUT_BIN" ]]; then
        "$TIMEOUT_BIN" --kill-after=30 "$SAFETY_SECS" "$@" >"$CURRENT_LOG" 2>&1 || CURRENT_EC=$?
    else
        "$@" >"$CURRENT_LOG" 2>&1 || CURRENT_EC=$?
    fi
    if [[ "$CURRENT_EC" == "124" || "$CURRENT_EC" == "137" ]]; then
        echo "  exit=$CURRENT_EC  log=$CURRENT_LOG  (KILLED by safety ceiling ${SAFETY_SECS}s)"
    else
        echo "  exit=$CURRENT_EC  log=$CURRENT_LOG"
    fi
    CASES=$((CASES + 1))
}

assert() {
    local label="$1"; local ok="$2"
    if [[ "$ok" == "1" ]]; then
        echo "  PASS  $label"
        PASS=$((PASS + 1))
    else
        echo "  FAIL  $label"
        FAIL=$((FAIL + 1))
    fi
}

grep_q() {
    if grep -Fq -- "$1" "$2"; then echo 1; else echo 0; fi
}

# 1 if `first` appears before `second` in the integrity "starting" lines.
order_before() {
    local first="$1" second="$2" logf="$3"
    local line_first line_second
    line_first=$(grep -n "\[integrity\] starting.*check=${first}\b" "$logf" | head -1 | cut -d: -f1)
    line_second=$(grep -n "\[integrity\] starting.*check=${second}\b" "$logf" | head -1 | cut -d: -f1)
    if [[ -z "$line_first" || -z "$line_second" ]]; then echo 0; return; fi
    if (( line_first < line_second )); then echo 1; else echo 0; fi
}

not()  { if [[ "$1" == "1" ]]; then echo 0; else echo 1; fi; }
eq0()  { if [[ "$1" == "0" ]]; then echo 1; else echo 0; fi; }

#-----------------------------------------------------------------------
# case 1: no flag, cheap subset -> no budget, exit 0
#-----------------------------------------------------------------------
run_case "1_no_flag_cheap" \
    "$BIN" seg integrity --datadir="$DATADIR" \
    --check=StateProgress,Publishable,HeaderNoGaps
assert "exit 0"                    "$(eq0 "$CURRENT_EC")"
assert "no '[integrity] budget'"   "$(not "$(grep_q '[integrity] budget' "$CURRENT_LOG")")"

#-----------------------------------------------------------------------
# case 2: generous budget + cheap subset -> budget log, no exhaustion
#-----------------------------------------------------------------------
run_case "2_generous_budget_cheap" \
    "$BIN" seg integrity --datadir="$DATADIR" \
    --check=StateProgress,Publishable,HeaderNoGaps \
    --integrity.budget=30m
assert "exit 0"                    "$(eq0 "$CURRENT_EC")"
assert "has '[integrity] budget'"  "$(grep_q '[integrity] budget' "$CURRENT_LOG")"
assert "no 'budget exhausted'"     "$(not "$(grep_q 'budget exhausted' "$CURRENT_LOG")")"

#-----------------------------------------------------------------------
# case 3: tight budget, full FastChecks -> expect exhaustion on tail
# runtime bounded by --integrity.budget itself (~10m)
#-----------------------------------------------------------------------
run_case "3_tight_budget_full" \
    "$BIN" seg integrity --datadir="$DATADIR" \
    --integrity.budget=10m
assert "exit 0"                      "$(eq0 "$CURRENT_EC")"
assert "has '[integrity] budget'"    "$(grep_q '[integrity] budget' "$CURRENT_LOG")"
assert "has 'budget exhausted'"      "$(grep_q 'budget exhausted' "$CURRENT_LOG")"

#-----------------------------------------------------------------------
# case 4: micro budget -> most checks get skipped
#-----------------------------------------------------------------------
run_case "4_micro_budget" \
    "$BIN" seg integrity --datadir="$DATADIR" \
    --integrity.budget=30s
assert "exit 0"                      "$(eq0 "$CURRENT_EC")"
assert "has 'skipping remaining'"    "$(grep_q 'skipping remaining' "$CURRENT_LOG")"

#-----------------------------------------------------------------------
# case 5: budget + explicit --check -> sorted cheap->heavy
#-----------------------------------------------------------------------
run_case "5_budget_plus_check_sorts" \
    "$BIN" seg integrity --datadir="$DATADIR" \
    --check=StateRootVerifyByHistory,StateProgress,HeaderNoGaps \
    --integrity.budget=5m
assert "exit 0"                                       "$(eq0 "$CURRENT_EC")"
assert "StateProgress before HeaderNoGaps"            "$(order_before StateProgress HeaderNoGaps "$CURRENT_LOG")"
assert "HeaderNoGaps before StateRootVerifyByHistory" "$(order_before HeaderNoGaps StateRootVerifyByHistory "$CURRENT_LOG")"

#-----------------------------------------------------------------------
# case 6: explicit --check, no budget -> user order preserved
#-----------------------------------------------------------------------
run_case "6_check_no_budget_preserves_order" \
    "$BIN" seg integrity --datadir="$DATADIR" \
    --check=Publishable,StateProgress
assert "exit 0"                            "$(eq0 "$CURRENT_EC")"
assert "no '[integrity] budget'"           "$(not "$(grep_q '[integrity] budget' "$CURRENT_LOG")")"
assert "Publishable before StateProgress"  "$(order_before Publishable StateProgress "$CURRENT_LOG")"

#-----------------------------------------------------------------------
# case 7: budget exhaustion with failFast=true -> still exit 0
#-----------------------------------------------------------------------
run_case "7_exhaustion_failfast" \
    "$BIN" seg integrity --datadir="$DATADIR" \
    --integrity.budget=2m --failFast=true
assert "exit 0 despite failFast"  "$(eq0 "$CURRENT_EC")"
assert "has 'budget exhausted'"   "$(grep_q 'budget exhausted' "$CURRENT_LOG")"

#-----------------------------------------------------------------------
echo
echo "==================================="
echo "cases=$CASES  pass=$PASS  fail=$FAIL"
echo "logs retained in $OUT"
echo "==================================="
if (( FAIL > 0 )); then exit 1; fi
exit 0
