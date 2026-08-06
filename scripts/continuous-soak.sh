#!/usr/bin/env bash
# continuous-soak.sh — outer loop that alternates leg P (preverified
# fallback) and leg M (manifest bootstrap via local master publisher)
# unwind-fresh-sync-then-soak cycles indefinitely (or CYCLES times).
#
# Per [[soak-two-mode-bootstrap-2026-08-03]]: mode-B/C unwind changes
# require BOTH bootstrap modes to be soaked. A defect that surfaces
# under only one is still a defect. Alternating within one wrapper is
# the ratchet — every code change gets both legs before the next lands.
#
# Between leg-M cycles the publisher is wiped and re-synced so every
# leg exercises the publisher's fresh-sync-plus-publish path. Between
# leg-P cycles the publisher is left alone (leg P doesn't touch it).
#
# On leg M, post-run scan of the consumer log for the preverified-
# fallback line MUST NOT match — that indicates the manifest path
# failed silently and the cycle would then be running against the
# same source as leg P, defeating the point. Such cycles fail hard.
#
# Env:
#   CYCLES               how many cycles to run (default: 0 = forever)
#   START_LEG            first leg to run: "P" or "M" (default: P)
#   REFRESH_BEFORE_LEG_M refresh publisher before each leg-M cycle (default: 1)
#   PUB_HTTP_PORT        publisher HTTP RPC port (default: 19845)
#   ITER                 sub-iters per cycle (forwarded to unwind-fresh-sync-then-soak; default: 5)
#   RANDOMIZE_DEPTHS     forwarded to unwind-fresh-sync-then-soak (default: true)
#   RESULTS_DIR          per-cycle result dirs live here (default: /erigon/tmp/continuous-soak-results)

set -eu

CYCLES="${CYCLES:-0}"
START_LEG="${START_LEG:-P}"
REFRESH_BEFORE_LEG_M="${REFRESH_BEFORE_LEG_M:-1}"
PUB_HTTP_PORT="${PUB_HTTP_PORT:-19845}"
ITER="${ITER:-5}"
RANDOMIZE_DEPTHS="${RANDOMIZE_DEPTHS:-true}"
RESULTS_DIR="${RESULTS_DIR:-/erigon/tmp/continuous-soak-results}"

mkdir -p "$RESULTS_DIR"

next_leg() {
  # $1 = current leg letter
  if [[ "$1" == "P" ]]; then echo "M"; else echo "P"; fi
}

wait_publisher_tip() {
  # Poll caplin/execution progress on the publisher until head advance
  # stalls (already at tip). Cheap heuristic: two consecutive polls with
  # the same head block number, 60s apart.
  local prev="" cur=""
  echo "[continuous-soak] waiting for publisher to reach hoodi tip..."
  for i in $(seq 1 60); do  # up to 60 minutes
    cur=$(curl -sS -m 5 -X POST -H "Content-Type: application/json" \
      --data '{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1}' \
      "http://127.0.0.1:${PUB_HTTP_PORT}" 2>/dev/null | jq -r '.result // empty' || true)
    if [[ -z "$cur" ]]; then
      echo "[continuous-soak] publisher RPC still not answering (attempt $i)..."
      sleep 30
      continue
    fi
    if [[ -n "$prev" && "$prev" == "$cur" ]]; then
      echo "[continuous-soak] publisher head stable at $cur — treating as at-tip"
      return 0
    fi
    prev="$cur"
    sleep 60
  done
  echo "[continuous-soak] FAIL: publisher didn't reach a stable tip within 60m" >&2
  return 1
}

run_leg_p() {
  local cycle="$1"
  local out="$RESULTS_DIR/cycle-$(printf '%03d' "$cycle")-legP"
  mkdir -p "$out"
  echo "[continuous-soak] cycle $cycle leg P → $out"
  env -u PUBLISHER_ENR -u PUBLISHER_TRUST_ROOT \
    ITER="$ITER" RANDOMIZE_DEPTHS="$RANDOMIZE_DEPTHS" \
    LAUNCH_CMD=scripts/erigon-launch-hoodi-soak.sh \
    DATADIR=/erigon/tmp/erigon-hoodi-soak.continuous \
    scripts/unwind-fresh-sync-then-soak.sh \
    >"$out/soak.log" 2>&1
  local rc=$?
  echo "$rc" >"$out/exit-code"
  return "$rc"
}

run_leg_m() {
  local cycle="$1"
  local out="$RESULTS_DIR/cycle-$(printf '%03d' "$cycle")-legM"
  mkdir -p "$out"
  echo "[continuous-soak] cycle $cycle leg M → $out"

  if [[ "$REFRESH_BEFORE_LEG_M" == "1" ]]; then
    echo "[continuous-soak] refreshing publisher before leg M..."
    scripts/refresh-publisher.sh || return 1
  fi
  wait_publisher_tip || return 1

  eval "$(scripts/publisher-info.sh)"
  echo "[continuous-soak] publisher ENR=$ENR" >"$out/publisher-info.txt"
  echo "[continuous-soak] publisher TRUST_ROOT=$TRUST_ROOT" >>"$out/publisher-info.txt"

  PUBLISHER_ENR="$ENR" PUBLISHER_TRUST_ROOT="$TRUST_ROOT" \
    ITER="$ITER" RANDOMIZE_DEPTHS="$RANDOMIZE_DEPTHS" \
    LAUNCH_CMD=scripts/erigon-launch-hoodi-soak.sh \
    DATADIR=/erigon/tmp/erigon-hoodi-soak.continuous \
    scripts/unwind-fresh-sync-then-soak.sh \
    >"$out/soak.log" 2>&1
  local rc=$?
  echo "$rc" >"$out/exit-code"

  # Post-check: manifest path MUST have been the actual bootstrap route.
  # If the "P2P manifest discovery timed out — falling back to preverified"
  # line appears, this leg silently degraded to leg P and is invalid.
  if grep -q "P2P manifest discovery timed out — falling back to preverified" "$out/soak.log"; then
    echo "[continuous-soak] FAIL leg M cycle $cycle: manifest bootstrap fell back to preverified" | tee -a "$out/verdict.txt"
    return 2
  fi
  return "$rc"
}

leg="$START_LEG"
cycle=1
while [[ "$CYCLES" == "0" || "$cycle" -le "$CYCLES" ]]; do
  case "$leg" in
    P) run_leg_p "$cycle" ;;
    M) run_leg_m "$cycle" ;;
    *) echo "unknown leg $leg" >&2; exit 1 ;;
  esac
  rc=$?
  if [[ "$rc" != "0" ]]; then
    echo "[continuous-soak] cycle $cycle leg $leg FAILED with rc=$rc — stopping" >&2
    exit "$rc"
  fi
  echo "[continuous-soak] cycle $cycle leg $leg OK"
  leg=$(next_leg "$leg")
  cycle=$((cycle + 1))
done

echo "[continuous-soak] completed $((cycle - 1)) cycles"
