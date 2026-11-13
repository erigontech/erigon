---
name: bal-devnet-ab-test
description: A/B test erigon's BAL parallel-execution scheduling on any BAL devnet (bal-devnet-N). Uses launch-devnet to bring up the primary instance, then spins up a second instance with IGNORE_BAL=true on a different port offset so throughput can be compared head-to-head. Use this when the user wants to compare BAL vs non-BAL parallel execution.
allowed-tools: Bash, Read, Write, Edit, Glob
allowed-prompts:
  - tool: Bash
    prompt: start, stop, and manage two parallel erigon+CL instances for BAL A/B testing
---

# BAL A/B Testing Wrapper

Compare erigon's parallel-execution throughput with vs without BAL (Block Access List) scheduling on a `bal-devnet-N` network. The flow is:

- **Instance A (BAL)** — default behaviour. Uses BAL hints from blocks to pre-populate version maps and schedule transactions optimistically.
- **Instance B (No-BAL)** — same code, but with `IGNORE_BAL=true` set in the environment, forcing the dependency-tracking scheduling path.

Both instances sync the same chain so `gas/s`, `repeat%`, `abort`, and `invalid` counters can be compared directly at chain tip.

## Prerequisite: bring up Instance A

Use the [`launch-devnet`](../launch-devnet/SKILL.md) skill with the BAL devnet URL the user provides (e.g. `https://bal-devnet-3.ethpandaops.io`). That skill discovers chain id, forks, bootnodes, and CL image from the network's config service, generates start/stop/clean scripts, and monitors progress. **Do not duplicate that logic here.**

When `launch-devnet` finishes, you'll have:
- `$WORKDIR/` — Instance A working directory (default: `~/<devnet-name>/`)
- `$WORKDIR/devnet-info.txt` — chain id, fork schedule, port offset chosen for Instance A
- `$WORKDIR/inventory.json` — bootnodes & peer endpoints (reused by Instance B)
- `$WORKDIR/genesis.json`, `$WORKDIR/testnet-config/` — config artefacts (reused by Instance B)
- A running erigon (BAL) + CL pair

Confirm Instance A is past genesis and importing payloads before starting Instance B.

## Set up Instance B (No-BAL)

`launch-devnet` writes Instance A's chosen offset to `$WORKDIR/devnet-info.txt` as `port_offset: <N>` (key/value format). Read it instead of guessing, then derive Instance B's offset by adding `+300` (leaves room for `+200`/`+300` ephemeral nodes the user might also be running). Verify the candidate ports with `lsof` (cross-platform — `ss` is Linux-only) before committing to the offset.

```bash
NOBAL_DIR="${WORKDIR}-nobal"

OFFSET_A=$(awk -F': *' '$1=="port_offset"{print $2}' "$WORKDIR/devnet-info.txt")
OFFSET_B=$(( OFFSET_A + 300 ))

# Re-check the +OFFSET_B family (TCP + UDP). Bump by +100 and retry on conflict.
for p in <each B port at +OFFSET_B>; do
  lsof -nP -iTCP:$p -sTCP:LISTEN >/dev/null 2>&1 && echo "TCP $p in use"
done
for p in <each B UDP port at +OFFSET_B>; do
  lsof -nP -iUDP:$p >/dev/null 2>&1 && echo "UDP $p in use"
done

mkdir -p "$NOBAL_DIR/erigon-data" "$NOBAL_DIR/cl-data"
cp    "$WORKDIR/genesis.json"     "$NOBAL_DIR/"
cp -r "$WORKDIR/testnet-config"   "$NOBAL_DIR/"
cp    "$WORKDIR/inventory.json"   "$NOBAL_DIR/"
cp    "$WORKDIR/devnet-info.txt"  "$NOBAL_DIR/devnet-info.txt"
# Update Instance B's metadata to record the new offset and IGNORE_BAL flag
sed -i.bak "s/^port_offset:.*/port_offset: $OFFSET_B/" "$NOBAL_DIR/devnet-info.txt" && rm "$NOBAL_DIR/devnet-info.txt.bak"
echo "ignore_bal: true" >> "$NOBAL_DIR/devnet-info.txt"

./build/bin/erigon init --datadir "$NOBAL_DIR/erigon-data" "$NOBAL_DIR/genesis.json"
```

Generate Instance B's start scripts the same way `launch-devnet` does, with these changes:

1. **`start-erigon.sh`** — add `export IGNORE_BAL=true` at the top of the script, before the `erigon` invocation. All other env vars stay the same as Instance A. Keep the `exec ./build/bin/erigon …` ending so the captured PID is the erigon PID (used by Cleanup).
2. **All ports** — use the `+OFFSET_B` family computed above (or whichever offset survived port-conflict checks). Update both the erigon flags and the CL flags.
3. **CL container name** — use `${DEVNET}-nobal-cl` to avoid colliding with Instance A's container.
4. **CL `--execution-endpoint`** — point at Instance B's authrpc port, not A's.
5. **CL `--disable-enr-auto-update`** — required when running two CLs on the same host.

Start in the same order: erigon first, save its PID, **poll** for `jwt.hex` and authrpc bind (same loop as `launch-devnet` Step 8 — do not `sleep`), then CL.

```bash
cd "$NOBAL_DIR" && nohup bash start-erigon.sh > erigon-console.log 2>&1 &
echo $! > "$NOBAL_DIR/erigon.pid"

AUTHRPC_B=<authrpc port at +OFFSET_B>
for i in $(seq 1 60); do
  if [ -f "$NOBAL_DIR/erigon-data/jwt.hex" ] \
      && lsof -nP -iTCP:"$AUTHRPC_B" -sTCP:LISTEN >/dev/null 2>&1; then
    break
  fi
  sleep 1
done
[ -f "$NOBAL_DIR/erigon-data/jwt.hex" ] || { echo "Instance B jwt.hex not produced after 60s"; exit 1; }

cd "$NOBAL_DIR" && nohup bash start-cl.sh > cl-console.log 2>&1 &
```

## Compare at chain tip

Wait until both instances are caught up (compare `eth_blockNumber` against the public RPC). Once both are at-head, the steady-state metrics in the execution log lines are what matter:

| Metric | Source | What it measures |
|--------|--------|------------------|
| `gas/s` | erigon execution log lines | Raw execution throughput |
| `repeat%` | erigon execution log lines | Speculative re-execution rate (lower = better dependency prediction) |
| `abort` | erigon execution log lines | Transactions aborted per batch |
| `invalid` | erigon execution log lines | Transactions invalidated by conflict detection |
| `blk/s` | erigon execution log lines | Block processing rate |

**Expected**: BAL instance should have lower `repeat%` and `abort` because BAL pre-populates the version map, reducing false conflicts. The `gas/s` delta is the net throughput impact.

Side-by-side log snapshot:

```bash
echo "=== BAL (A) ===" && \
  grep -E "parallel (executed|done)" "$WORKDIR/erigon-data/logs/erigon.log" | tail -3
echo
echo "=== No-BAL (B) ===" && \
  grep -E "parallel (executed|done)" "${WORKDIR}-nobal/erigon-data/logs/erigon.log" | tail -3
```

For longer comparisons, sample both logs at fixed intervals and aggregate (mean ± stdev) rather than eyeballing the tail.

## Cleanup of Instance B only

```bash
NOBAL_DIR="${WORKDIR}-nobal"

docker stop "${DEVNET}-nobal-cl" 2>/dev/null || true
docker rm   "${DEVNET}-nobal-cl" 2>/dev/null || true

# Stop Instance B's erigon by PID (saved at start time) — avoids pkill -f regex risks.
if [ -f "$NOBAL_DIR/erigon.pid" ]; then
  PID=$(cat "$NOBAL_DIR/erigon.pid")
  kill "$PID" 2>/dev/null || true
  rm -f "$NOBAL_DIR/erigon.pid"
fi

# Optional — wipe Instance B's working directory.
# Refuse if WORKDIR is empty, NOBAL_DIR doesn't end with `-nobal`, or
# the directory doesn't carry the marker file we wrote at setup time.
# These guards prevent `rm -rf` from acting on an unrelated path if a
# variable is unset or has been overwritten.
if [ -n "$WORKDIR" ] \
    && [[ "$NOBAL_DIR" == *-nobal ]] \
    && [ -f "$NOBAL_DIR/devnet-info.txt" ] \
    && grep -q '^ignore_bal: true' "$NOBAL_DIR/devnet-info.txt"; then
  rm -rf "$NOBAL_DIR"
else
  echo "refusing to wipe '$NOBAL_DIR' — sanity check failed; clean it up by hand"
fi
```

Instance A is unaffected; use its own `stop.sh` / `clean.sh` to manage it.

## Investigating discrepancies

If A and B disagree on block contents or state root (rather than just on throughput), this is **not** a BAL-vs-no-BAL throughput question — it's a correctness divergence. Stop the comparison and follow the failure-investigation flow in [`launch-devnet`](../launch-devnet/SKILL.md) ("Investigating failures — finding the absolute truth"). The two instances are running the same erigon binary with one env-var difference, so a state-root diff between them points squarely at the BAL scheduling path.
