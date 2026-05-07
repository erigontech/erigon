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

Pick a second port offset that doesn't collide with Instance A. If Instance A uses `+100`, Instance B should use `+400` (leaves room for `+200`/`+300` ephemeral nodes the user might also be running). Verify with `ss -tlnp` before committing to it.

```bash
NOBAL_DIR="${WORKDIR}-nobal"
mkdir -p "$NOBAL_DIR/erigon-data" "$NOBAL_DIR/cl-data"
cp    "$WORKDIR/genesis.json"     "$NOBAL_DIR/"
cp -r "$WORKDIR/testnet-config"   "$NOBAL_DIR/"
cp    "$WORKDIR/inventory.json"   "$NOBAL_DIR/"
cp    "$WORKDIR/devnet-info.txt"  "$NOBAL_DIR/devnet-info.txt"   # then edit to record the new offset

./build/bin/erigon init --datadir "$NOBAL_DIR/erigon-data" "$NOBAL_DIR/genesis.json"
```

Generate Instance B's start scripts the same way `launch-devnet` does, with two changes:

1. **`start-erigon.sh`** — add `export IGNORE_BAL=true` at the top of the script, before the `erigon` invocation. All other env vars stay the same as Instance A.
2. **All ports** — use the `+400` family (or whichever offset survived port-conflict checks). Update both the erigon flags and the CL flags.
3. **CL container name** — use `<devnet>-nobal-cl` to avoid colliding with Instance A's container.
4. **CL `--execution-endpoint`** — point at Instance B's authrpc port, not A's.
5. **CL `--disable-enr-auto-update`** — required when running two CLs on the same host.

Start in the same order: erigon first, wait for `jwt.hex` and authrpc port binding, then CL.

```bash
cd "$NOBAL_DIR" && nohup bash start-erigon.sh > erigon-console.log 2>&1 &
# wait for jwt.hex + authrpc bind
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
docker stop "<devnet>-nobal-cl" 2>/dev/null
docker rm   "<devnet>-nobal-cl" 2>/dev/null
pkill -f "datadir.*${WORKDIR}-nobal/erigon-data"
# Optional — wipe disk
rm -rf "${WORKDIR}-nobal"
```

Instance A is unaffected; use its own `stop.sh` / `clean.sh` to manage it.

## Investigating discrepancies

If A and B disagree on block contents or state root (rather than just on throughput), this is **not** a BAL-vs-no-BAL throughput question — it's a correctness divergence. Stop the comparison and follow the failure-investigation flow in [`launch-devnet`](../launch-devnet/SKILL.md) ("Investigating failures — finding the absolute truth"). The two instances are running the same erigon binary with one env-var difference, so a state-root diff between them points squarely at the BAL scheduling path.
