#!/usr/bin/env bash
# fuse_compare.sh — run one Erigon binary on /erigon-data/chiado_commit_gate
# with .kvei/.efi accessor files removed, so the fusefilter build path
# (WriterSharded vs legacy Writer) is exercised end-to-end. Polls Prometheus
# memory metrics into a CSV so two runs can be compared side by side.
#
# Usage:
#   scripts/fusefilter-bench/fuse_compare.sh <binary-path> <label>
#
# Example:
#   scripts/fusefilter-bench/fuse_compare.sh ./build/bin/erigon_main    main
#   scripts/fusefilter-bench/fuse_compare.sh ./build/bin/erigon_fusesharded sharded
#
# Stop the run with Ctrl+C — the script SIGINTs erigon, waits up to 60s for a
# clean shutdown, then SIGKILLs as a fallback. A summary (peak RSS, peak heap)
# is printed at exit and the CSV path is reported.

set -euo pipefail

if [[ $# -ne 2 ]]; then
  echo "usage: $0 <binary-path> <label>" >&2
  exit 2
fi

BIN="$1"
LABEL="$2"

if [[ ! -x "$BIN" ]]; then
  echo "binary not found or not executable: $BIN" >&2
  exit 2
fi

DATADIR=/erigon-data/chiado_commit_gate
LOGDIR=/erigon-logs
METRICS_HOST=127.0.0.1
METRICS_PORT=6061
METRICS_URL="http://${METRICS_HOST}:${METRICS_PORT}/debug/metrics/prometheus"
POLL_INTERVAL=3                 # seconds between metric samples
SHUTDOWN_GRACE=60               # seconds to wait after SIGINT before SIGKILL
HARD_TIMEOUT_MIN=30             # safety cap — script self-stops after this many minutes if user forgets Ctrl+C

mkdir -p "$LOGDIR"

TS=$(date +%Y%m%d_%H%M%S)
LOG="$LOGDIR/erigon-${LABEL}-${TS}.log"
CSV="$LOGDIR/fuse-compare-${LABEL}-${TS}.csv"
PIDFILE="$LOGDIR/erigon-${LABEL}.pid"

# ---------- 1. Pre-flight: refuse if a previous run is still alive ---------- #
if [[ -f "$PIDFILE" ]] && kill -0 "$(cat "$PIDFILE")" 2>/dev/null; then
  echo "previous erigon with label '$LABEL' still running (PID $(cat "$PIDFILE")). kill it first." >&2
  exit 3
fi

# ---------- 2. Wipe .kvei and .efi accessor files ---------- #
echo "==> wiping .kvei and .efi accessor files under $DATADIR"
if [[ ! -d "$DATADIR" ]]; then
  echo "datadir does not exist: $DATADIR" >&2
  exit 2
fi
KVEI_COUNT=$(find "$DATADIR" -type f -name '*.kvei' | wc -l | tr -d ' ')
EFI_COUNT=$(find "$DATADIR" -type f -name '*.efi'  | wc -l | tr -d ' ')
echo "    found: .kvei=$KVEI_COUNT  .efi=$EFI_COUNT"
find "$DATADIR" -type f \( -name '*.kvei' -o -name '*.efi' \) -delete
echo "    deleted."

# ---------- 3. Launch erigon ---------- #
echo "==> launching erigon ($BIN) — log: $LOG"
COLLECT_TABLE_SIZES_FREQUENCY=3s nohup "$BIN" \
  --datadir "$DATADIR" \
  --chain=chiado \
  --private.api.addr=127.0.0.1:0 \
  --metrics.port=$METRICS_PORT --metrics --metrics.addr=0.0.0.0 \
  --nat=stun \
  --torrent.download.rate 10G --torrent.upload.rate=1k \
  --pprof --pprof.port=6062 \
  --prune.mode=archive --persist.receipts \
  --db.pagesize=4k \
  --sync.loop.block.limit=10_000_000 \
  --batchSize=512m --http=false \
  --prune.experimental.include-commitment-history \
  --database.verbosity=3 --log.console.verbosity=4 \
  > "$LOG" 2>&1 &
ERIGON_PID=$!
echo "$ERIGON_PID" > "$PIDFILE"
echo "    erigon PID=$ERIGON_PID"

# ---------- 4. Metrics poller (background) ---------- #
# Prometheus exposition is line-oriented `metric_name{labels} value timestamp`.
# We grep for the exact metric names (no labels on these) and awk out the value.
echo "ts_unix,ts_iso,elapsed_sec,label,rss_bytes,vsz_bytes,heap_alloc_bytes,heap_inuse_bytes,heap_sys_bytes,sys_bytes,next_gc_bytes,alloc_total_bytes,gc_sys_bytes,goroutines,cpu_seconds" > "$CSV"

START_UNIX=$(date +%s)

poller() {
  local pid=$1
  while kill -0 "$pid" 2>/dev/null; do
    # Wait briefly for the metrics endpoint to come up on first sample.
    local body
    body=$(curl -fsS --max-time 2 "$METRICS_URL" 2>/dev/null || true)
    if [[ -n "$body" ]]; then
      local now_unix now_iso elapsed
      now_unix=$(date +%s)
      now_iso=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
      elapsed=$((now_unix - START_UNIX))
      # Extract scalar metrics. Empty if missing.
      local rss vsz heap_alloc heap_inuse heap_sys sys next_gc alloc_total gc_sys goroutines cpu
      rss=$(printf '%s\n' "$body"          | awk '/^process_resident_memory_bytes /  { print $2; exit }')
      vsz=$(printf '%s\n' "$body"          | awk '/^process_virtual_memory_bytes /   { print $2; exit }')
      heap_alloc=$(printf '%s\n' "$body"   | awk '/^go_memstats_heap_alloc_bytes /   { print $2; exit }')
      heap_inuse=$(printf '%s\n' "$body"   | awk '/^go_memstats_heap_inuse_bytes /   { print $2; exit }')
      heap_sys=$(printf '%s\n' "$body"     | awk '/^go_memstats_heap_sys_bytes /     { print $2; exit }')
      sys=$(printf '%s\n' "$body"          | awk '/^go_memstats_sys_bytes /          { print $2; exit }')
      next_gc=$(printf '%s\n' "$body"      | awk '/^go_memstats_next_gc_bytes /      { print $2; exit }')
      alloc_total=$(printf '%s\n' "$body"  | awk '/^go_memstats_alloc_bytes_total /  { print $2; exit }')
      gc_sys=$(printf '%s\n' "$body"       | awk '/^go_memstats_gc_sys_bytes /       { print $2; exit }')
      goroutines=$(printf '%s\n' "$body"   | awk '/^go_goroutines /                  { print $2; exit }')
      cpu=$(printf '%s\n' "$body"          | awk '/^process_cpu_seconds_total /      { print $2; exit }')
      printf '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n' \
        "$now_unix" "$now_iso" "$elapsed" "$LABEL" \
        "${rss:-}" "${vsz:-}" "${heap_alloc:-}" "${heap_inuse:-}" "${heap_sys:-}" "${sys:-}" \
        "${next_gc:-}" "${alloc_total:-}" "${gc_sys:-}" "${goroutines:-}" "${cpu:-}" \
        >> "$CSV"
    fi
    sleep "$POLL_INTERVAL"
  done
}
poller "$ERIGON_PID" &
POLLER_PID=$!

# ---------- 5. Hard-timeout watchdog ---------- #
watchdog() {
  sleep $((HARD_TIMEOUT_MIN * 60))
  if kill -0 "$ERIGON_PID" 2>/dev/null; then
    echo "==> hard timeout (${HARD_TIMEOUT_MIN}m) reached — sending SIGINT" >&2
    kill -INT "$ERIGON_PID" 2>/dev/null || true
  fi
}
watchdog &
WATCHDOG_PID=$!

# ---------- 6. Cleanup trap ---------- #
cleanup() {
  local exit_code=$?
  set +e
  echo
  echo "==> stopping erigon (PID $ERIGON_PID)..."
  if kill -0 "$ERIGON_PID" 2>/dev/null; then
    kill -INT "$ERIGON_PID" 2>/dev/null
    # Wait up to SHUTDOWN_GRACE seconds for clean shutdown.
    for ((i=0; i<SHUTDOWN_GRACE; i++)); do
      kill -0 "$ERIGON_PID" 2>/dev/null || break
      sleep 1
    done
    if kill -0 "$ERIGON_PID" 2>/dev/null; then
      echo "    erigon did not exit after ${SHUTDOWN_GRACE}s — SIGKILL"
      kill -9 "$ERIGON_PID" 2>/dev/null || true
    fi
  fi
  kill "$POLLER_PID" 2>/dev/null || true
  kill "$WATCHDOG_PID" 2>/dev/null || true
  wait "$POLLER_PID" "$WATCHDOG_PID" 2>/dev/null || true
  rm -f "$PIDFILE"

  echo "==> summary (label=$LABEL)"
  echo "    log: $LOG"
  echo "    csv: $CSV"
  # Quick peak summary from the CSV.
  if [[ -s "$CSV" ]]; then
    awk -F, 'NR>1 && $5!="" {
      if ($5+0>r) r=$5+0;
      if ($6+0>v) v=$6+0;
      if ($7+0>ha) ha=$7+0;
      if ($8+0>hi) hi=$8+0;
      if ($9+0>hs) hs=$9+0;
      n++
    } END {
      if (n==0) { print "    no metric samples captured"; exit }
      printf "    samples=%d  peak_rss=%.1f MB  peak_vsz=%.1f MB  peak_heap_alloc=%.1f MB  peak_heap_inuse=%.1f MB  peak_heap_sys=%.1f MB\n",
        n, r/1048576, v/1048576, ha/1048576, hi/1048576, hs/1048576
    }' "$CSV"
  fi
  exit "$exit_code"
}
trap cleanup INT TERM EXIT

# ---------- 7. Tail the log so the user can monitor and Ctrl+C ---------- #
# Filter to INFO only. Erigon uses log15-style level prefixes
# ("INFO[...]", "WARN[...]", "DBUG[...]", "EROR[...]", "TRCE[...]", "CRIT[...]").
# --line-buffered keeps each line flushed in real time.
echo "==> tailing log (INFO only — Ctrl+C to stop the run)"
echo
tail -F "$LOG" | grep --line-buffered '^INFO' &
TAIL_PID=$!
# Wait for erigon to exit OR user to Ctrl+C.
wait "$ERIGON_PID" 2>/dev/null || true
kill "$TAIL_PID" 2>/dev/null || true
