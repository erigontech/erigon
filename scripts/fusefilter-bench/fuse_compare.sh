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
METRICS_URL=""                  # discovered after erigon starts (step 3a)
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

# ---------- 2. Wipe chaindata and recsplit accessor files ---------- #
# The PR's WriterSharded path is reached via recsplit's flushExistenceFilter,
# which embeds a fusefilter blob inside the recsplit index file (.efi/.kvi/.vi).
# .kvei is a separate file written by existence.Filter — currently always in
# bloom-filter mode (useFuse=false everywhere), so deleting it does NOT exercise
# the PR. The recsplit indexes are what we need to force-rebuild.
#
# Removing chaindata/ on top forces erigon to re-execute and re-index from
# snapshots, which is what triggers BuildMissedAccessors → recsplit → our
# fusefilter writer.
if [[ ! -d "$DATADIR" ]]; then
  echo "datadir does not exist: $DATADIR" >&2
  exit 2
fi

CHAINDATA="$DATADIR/chaindata"
echo "==> wiping $CHAINDATA"
if [[ -d "$CHAINDATA" ]]; then
  CHAINDATA_SIZE=$(du -sh "$CHAINDATA" 2>/dev/null | awk '{print $1}')
  echo "    size before: $CHAINDATA_SIZE"
  rm -rf "$CHAINDATA"
  echo "    removed."
else
  echo "    (already absent)"
fi

echo "==> wiping recsplit accessor files (.efi/.kvi/.vi) under $DATADIR"
EFI_COUNT=$(find "$DATADIR" -type f -name '*.efi' | wc -l | tr -d ' ')
KVI_COUNT=$(find "$DATADIR" -type f -name '*.kvi' | wc -l | tr -d ' ')
VI_COUNT=$(find  "$DATADIR" -type f -name '*.vi'  | wc -l | tr -d ' ')
echo "    found: .efi=$EFI_COUNT  .kvi=$KVI_COUNT  .vi=$VI_COUNT"
find "$DATADIR" -type f \( -name '*.efi' -o -name '*.kvi' -o -name '*.vi' \) -delete
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

# ---------- 3a. Discover the working metrics endpoint ---------- #
# Erigon may bind to IPv4-only (0.0.0.0), IPv6-only ([::]), or both, and the
# metrics path may differ between versions. Try the cartesian product of
# candidate hosts × paths until one returns Prometheus-shaped content.
#
# Observed gotcha: with --metrics.addr=0.0.0.0 erigon sometimes ends up bound
# to [::]:6061 and the kernel won't accept an IPv4 connection to 127.0.0.1
# unless net.ipv6.bindv6only=0. `curl 0.0.0.0:port` works around it via the
# kernel's "connect-to-0.0.0.0 means 127.0.0.1" quirk; `localhost` resolves to
# both IPv4 and IPv6 so curl falls through to whichever socket is live.
CANDIDATE_HOSTS=(127.0.0.1 localhost 0.0.0.0 ::1)
CANDIDATE_PATHS=(/debug/metrics/prometheus /debug/metrics /metrics)

probe_metrics_endpoint() {
  local host path url code
  for host in "${CANDIDATE_HOSTS[@]}"; do
    for path in "${CANDIDATE_PATHS[@]}"; do
      if [[ "$host" == "::1" ]]; then
        url="http://[${host}]:${METRICS_PORT}${path}"
      else
        url="http://${host}:${METRICS_PORT}${path}"
      fi
      code=$(curl -sS -m 2 -o /tmp/fuse_probe.$$ -w '%{http_code}' "$url" 2>/dev/null || echo 000)
      if [[ "$code" == "200" ]] && grep -aq '^# HELP\|^# TYPE\|^process_resident_memory_bytes' /tmp/fuse_probe.$$ 2>/dev/null; then
        rm -f /tmp/fuse_probe.$$ 2>/dev/null || true
        echo "$url"
        return 0
      fi
    done
  done
  rm -f /tmp/fuse_probe.$$ 2>/dev/null || true
  return 1
}

echo "==> probing metrics endpoint (port $METRICS_PORT, 60s upfront budget)"
METRICS_URL=""
for ((wait_s=0; wait_s<60; wait_s++)); do
  if url=$(probe_metrics_endpoint); then
    METRICS_URL="$url"
    echo "    metrics live at $METRICS_URL (after ${wait_s}s)"
    break
  fi
  if ! kill -0 "$ERIGON_PID" 2>/dev/null; then
    echo "    erigon exited before metrics came up (PID $ERIGON_PID gone)" >&2
    break
  fi
  sleep 1
done
if [[ -z "$METRICS_URL" ]]; then
  echo "    metrics not up in 60s — poller will keep retrying lazily."
  echo "    manual check: curl -v http://0.0.0.0:${METRICS_PORT}/debug/metrics/prometheus  /  ss -ltnp | grep $METRICS_PORT"
fi

# ---------- 4. Metrics poller (background) ---------- #
# Prometheus exposition is line-oriented `metric_name{labels} value timestamp`.
# We grep for the exact metric names (no labels on these) and awk out the value.
echo "ts_unix,ts_iso,elapsed_sec,label,rss_bytes,vsz_bytes,heap_alloc_bytes,heap_inuse_bytes,heap_sys_bytes,sys_bytes,next_gc_bytes,alloc_total_bytes,gc_sys_bytes,goroutines,cpu_seconds" > "$CSV"
POLLER_LOG="$LOGDIR/poller-${LABEL}-${TS}.log"
echo "    poller log: $POLLER_LOG"

START_UNIX=$(date +%s)

poller() {
  # Disable strict-mode in the poller. The outer script runs with `set -euo pipefail`;
  # in the poller we'd rather log a single bad sample (e.g. transient metrics blip,
  # awk match failure, transient curl error) than have the entire loop die silently.
  set +e
  # Send stderr of every command in this function to a debug log so any unexpected
  # failure leaves a trace; otherwise the function could exit on a hidden non-zero
  # status with no signal.
  exec 2>>"$POLLER_LOG"
  local pid=$1
  local first=1
  while kill -0 "$pid" 2>/dev/null; do
    # Late-binding: if upfront discovery didn't find a URL, keep trying every
    # tick. Once found, latch it (subsequent ticks reuse the discovered URL).
    if [[ -z "$METRICS_URL" ]]; then
      if METRICS_URL=$(probe_metrics_endpoint); then
        printf '[%s] late discovery: %s\n' "$(date +%H:%M:%S)" "$METRICS_URL" >> "$POLLER_LOG"
      else
        sleep "$POLL_INTERVAL"
        continue
      fi
    fi

    local body http_code curl_err
    # Capture body + HTTP status + curl stderr separately so we can diagnose
    # any failure mode (connection refused, timeout, wrong path, empty body).
    body=$(curl -sS -m 3 -w '\n__HTTP__%{http_code}\n' "$METRICS_URL" 2>"$POLLER_LOG.curl_err" || true)
    curl_err=$(cat "$POLLER_LOG.curl_err" 2>/dev/null || true)
    http_code=$(printf '%s\n' "$body" | awk -F'__HTTP__' '/__HTTP__/ { print $2; exit }')
    body=$(printf '%s' "$body" | awk '/__HTTP__/ { exit } { print }')

    if [[ "$first" -eq 1 ]]; then
      printf '[%s] first probe → http_code=%s body_bytes=%s curl_err=%q\n' \
        "$(date +%H:%M:%S)" "${http_code:-NONE}" "${#body}" "$curl_err" >> "$POLLER_LOG"
      first=0
    fi

    if [[ -n "$body" && "$http_code" =~ ^2 ]]; then
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
    else
      printf '[%s] sample fail: http_code=%s body_bytes=%s curl_err=%q\n' \
        "$(date +%H:%M:%S)" "${http_code:-NONE}" "${#body}" "$curl_err" >> "$POLLER_LOG"
    fi
    sleep "$POLL_INTERVAL"
  done
}
poller "$ERIGON_PID" &
POLLER_PID=$!

# ---------- 5. Hard-timeout watchdog ---------- #
# Use SIGTERM rather than SIGINT — empirically erigon under nohup doesn't react
# to SIGINT sent from this script (works fine when delivered from htop or a
# parent shell), but SIGTERM triggers the normal graceful-shutdown path.
watchdog() {
  sleep $((HARD_TIMEOUT_MIN * 60))
  if kill -0 "$ERIGON_PID" 2>/dev/null; then
    echo "==> hard timeout (${HARD_TIMEOUT_MIN}m) reached — sending SIGTERM" >&2
    kill -TERM "$ERIGON_PID" 2>/dev/null || true
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
    # SIGTERM, not SIGINT — SIGINT under nohup doesn't reach erigon's signal
    # handler reliably; SIGTERM (same signal htop sends on default kill) does.
    kill -TERM "$ERIGON_PID" 2>/dev/null
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
  [[ -n "${POLLER_PID:-}" ]]   && kill "$POLLER_PID"   2>/dev/null || true
  [[ -n "${WATCHDOG_PID:-}" ]] && kill "$WATCHDOG_PID" 2>/dev/null || true
  wait ${POLLER_PID:-} ${WATCHDOG_PID:-} 2>/dev/null || true
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
# Erigon log lines look like: `[INFO] [05-11|09:49:10.400] message...`
# Run tail+grep in the foreground: Ctrl+C kills them first, then the EXIT/INT
# trap stops erigon cleanly. --line-buffered flushes each line immediately.
#
# Background watcher: if erigon exits on its own (crash, OOM, completed run),
# send SIGINT to this script so we don't hang in tail forever.
( while kill -0 "$ERIGON_PID" 2>/dev/null; do sleep 2; done; kill -TERM $$ 2>/dev/null ) &
EXIT_WATCH_PID=$!

echo "==> tailing log (INFO only — Ctrl+C to stop the run)"
echo
# -a forces grep to treat the input as text. Without it, the first NUL byte in
# the stream (e.g. raw bytes Caplin/p2p occasionally log) flips grep to
# "binary file matches" mode and it stops printing matching lines.
tail -F "$LOG" | grep --line-buffered -a '^\[INFO\]' || true
kill "$EXIT_WATCH_PID" 2>/dev/null || true
