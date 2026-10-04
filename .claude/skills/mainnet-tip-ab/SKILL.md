---
name: mainnet-tip-ab
description: A/B test two erigon binaries against each other on mainnet tip, driven by a single Prysm CL through an engine-mux. Use this when comparing an unmerged PR (or any candidate erigon build) against a baseline (main HEAD, merge-base, another PR) on a real mainnet workload, with matched per-payload latency, RAM, and correctness signals. Trigger on requests like "A/B PR X vs main", "rig vs main", "engine_newPayload latency for PR X", or "overnight comparison".
allowed-tools: Bash, Read, Write, Edit, Glob, Monitor, TaskStop
---

# Mainnet-tip A/B testing with Prysm + engine-mux

Compare two erigon binaries on live mainnet payloads. The rig fans every Prysm-driven `engine_newPayloadV*` / `engine_forkchoiceUpdated*` / `engine_getBlobs*` call to **both** ELs, records the per-EL response latency + status in a CSV, and the analyst computes matched-pair statistics from steady-state-tip windows.

The rig lives at `/home/andrew/erigon-node/tmp/elbench/` and the per-EL datadirs at `/mnt/giant/`. It's NOT a generic setup — it depends on a specific host (i9-13900KS) and two pre-synced mainnet datadirs. Don't try to build it from scratch; only adapt the binaries / flags / labels for the comparison at hand.

## Prerequisites — verify before touching anything

```bash
# Both datadirs synced (or close to tip).
ls /mnt/giant/erigon3/ethereum/{chaindata,snapshots}     >/dev/null   # slot A
ls /mnt/giant/erigon3-main/ethereum/{chaindata,snapshots} >/dev/null  # slot B
ls /mnt/giant/prysm/mainnet                              >/dev/null

# The mux + prysm binaries already built.
ls /home/andrew/erigon-node/tmp/elbench/bin/{engine-mux,beacon-chain}

# JWT secret shared between the ELs and Prysm.
ls /mnt/giant/erigon3/ethereum/jwt.hex
```

If any of those are missing, **stop and tell the user** — building a fresh rig from zero requires snapshot syncing both datadirs and is out of scope.

## Rig layout

| Component | Where | Role |
|---|---|---|
| **Slot A (control)** | `/mnt/giant/erigon3/ethereum`, ports 8551/8545/6060/30303/42069 | Default ports/cores; usually a built-from-main binary |
| **Slot B (candidate)** | `/mnt/giant/erigon3-main/ethereum`, ports 8651/8645/6160/30403/42169 | +100 offset everywhere; the PR / candidate binary |
| **engine-mux** | `127.0.0.1:8561`, CSV → `tmp/elbench/data/mux.csv`, stats → `127.0.0.1:9100` | Fans CL calls to both ELs; records per-EL response time + status |
| **Prysm** | `127.0.0.1:3500` beacon API, points at the **mux** as the EL | Single CL drives both ELs through the mux |
| **CPU pinning** | Slot A → cores 0-7, slot B → 8-15, mux/Prysm/etc. → 16-31 (E-cores) | i9-13900KS hybrid: cores 0-15 are P-core hyperthreads, 16-31 are E-cores |

Variable names in `env.sh` use historical `MAIN`/`R34` prefixes — **slot A = MAIN, slot B = R34**, regardless of what either is actually running. Don't rename them; just remember the mapping.

## CSV schema (`tmp/elbench/data/mux.csv`)

13 comma-separated fields per row, **no header**:

| Idx (awk `$N`) | Field | Notes |
|---:|---|---|
| `$1` | unix ms timestamp | mux receives request |
| `$2` | JSON-RPC method | `engine_newPayloadV4`, `engine_forkchoiceUpdatedV3`, `engine_getBlobsV2`, `eth_getBlockByNumber`, … |
| `$3` | mux req id | monotonic |
| `$4` | block number | 0 for non-payload methods |
| `$5` | block hash | empty for non-payload |
| `$6` | gas used | 0 for non-payload |
| `$7` | tx count | 0 for non-payload |
| `$8` | blob count | 0 for non-payload |
| `$9` | **primary latency ms** (slot A / main) | float |
| `$10` | primary status | `VALID` / `INVALID` / `SYNCING` / `ACCEPTED` / `?` (for non-engine methods) / `ERR:...` |
| `$11` | **secondary latency ms** (slot B / candidate) | float |
| `$12` | secondary status | same vocabulary |
| `$13` | "agree" flag | unused; ignore |

**Matched-pair stats** are computed over rows where `$2=="engine_newPayloadV4"` AND `$10=="VALID"` AND `$12=="VALID"`. This filters out catch-up (`SYNCING`), background queries (`eth_getBlockByNumber`), and any case where the two sides disagreed on the verdict (worth flagging separately — see "Correctness signal" below).

## Standard workflow (one PR vs one baseline)

### 1. Pick the merge-base for a clean A/B

```bash
git fetch origin <pr-branch> main
BASE=$(git merge-base origin/<pr-branch> origin/main)
PR_HEAD=$(git rev-parse origin/<pr-branch>)
```

`$BASE` is the commit that both binaries share; build the control from there so the only difference between A and B is the PR's own commits. If the PR is far behind main, **ask the user** whether to use `merge-base` or fresh `origin/main` for the control — different question, different control choice.

### 2. Build both binaries with distinct, descriptive names

```bash
git checkout $BASE          ; make erigon && cp build/bin/erigon build/bin/erigon-main-${BASE:0:8}
git checkout $PR_HEAD       ; make erigon && cp build/bin/erigon build/bin/erigon-pr<NUMBER>
git checkout -               # back to where you were
```

Name binaries by **content**, not by role. `erigon-main-ea6e9f12` is good; `erigon-baseline` is not — when you come back tomorrow with a different PR, you won't know which baseline the `baseline` binary was built from.

### 3. Wire the rig

Edit `tmp/elbench/env.sh`:

```bash
ERIGON_MAIN_BIN=.../erigon-main-<short-sha>     # control
ERIGON_R34_BIN=.../erigon-pr<NUMBER>            # candidate
```

Edit `tmp/elbench/run-mux.sh` — set descriptive `-primary-name` / `-secondary-name`:

```bash
-primary-name main      -secondary-name pr<NUMBER>
```

These labels show up in `engine-mux` stats and any tool that reads the CSV — they're worth setting correctly.

**Flags on slot B** go in `tmp/elbench/run-erigon-r34.sh`. Common ones:

| Flag | Effect |
|---|---|
| `--exec.serial=true` | Forces serial execution; used to A/B parallel vs serial exec |
| `--experimental.parallel-commitment=true` | Enables PR #21238's `ParallelPatriciaHashed` |

If a PR is gated behind an env var (e.g. `EXEC3_NONCE_PREWRITES`), set it inline in slot B's launch script. If both sides need a non-default flag (e.g. user explicitly asks for parallel-exec on BOTH), edit both `run-erigon-main.sh` and `run-erigon-r34.sh` — don't only edit one and trust the default.

### 4. Restart the rig

```bash
# Always archive the previous run's CSV — its rows from before the binary swap will pollute matched-pair stats.
[ -f tmp/elbench/data/mux.csv ] && mv tmp/elbench/data/mux.csv tmp/elbench/data/mux.csv.<prev-label>.bak

bash tmp/elbench/stop-all.sh
bash tmp/elbench/start-all.sh
```

`start-all.sh` brings up: erigon-main → erigon-r34 → engine-mux → prysm (in that order, with short sleeps for engine API readiness).

`stop-all.sh` SIGTERMs all four (never `-9`; both erigon and prysm need a clean shutdown to avoid db corruption / chaindata growth bugs).

Confirm both binaries are the ones you intended:

```bash
readlink /proc/$(cat tmp/elbench/logs/erigon-main.pid)/exe
readlink /proc/$(cat tmp/elbench/logs/erigon-r34.pid)/exe
```

### 5. Wait for steady-state at tip

Both ELs need to be **at tip** AND **processing similarly** before stats are meaningful. Default predicate:

- both eth heads in sync within ±2 blocks of each other
- slot B has ≥ 2 p2p peers (slot A's torrent/discovery is usually fine; the slot-B-just-restarted case is the bottleneck)
- the 5 most recent both-VALID `engine_newPayloadV4` matched pairs all have main + candidate latency < 300 ms (i.e., both are actually executing at tip cadence, not bulk-replaying)

A flat "diff ≤ 4 blocks" check is **not enough** — both can be 4 blocks behind tip and replay the gap at 5 ms/block, which doesn't reflect tip latency. The "5 recent both <300 ms" check catches this.

### 6. Measure

Three common shapes:

- **Quick check / canary (n ≈ 20-200 pairs, ≈ 10-40 min):** confirm a candidate isn't crashing or producing INVALID before committing to a long run. Loop sleeping 30s until `awk -F, ... | wc -l ≥ N`.
- **Standard latency comparison (n ≈ 200 pairs, ≈ 40 min):** matched-pair percentiles + delta. Adequate for a perf headline.
- **Overnight / correctness comparison (≥ 60 min at tip + sampler + log monitor):** for PRs where the concern is "does it stay correct over a long run?" — sampler logs latency + RAM + status counts every 5 min and bails on any process death; log-tail monitor fires on `panic|fatal|invalid block|bad block|state root mismatch|commitment mismatch|wrong root|Could not validate block`.

Always **anchor the window with a `AGE_MS` timestamp captured at steady-state**, then filter the CSV `awk -F, -v s=$AGE_MS '$1>=s ...'`. Stats over the whole CSV will pull in catch-up rows and pre-restart noise.

### 7. Analyze

Matched-pair latency:

```python
import csv, statistics
rows=list(csv.reader(open('/tmp/pairs.csv')))   # already filtered to engine_newPayloadV4 + both VALID + $1>=AGE_MS
m=[float(r[8])  for r in rows]   # primary = main
p=[float(r[10]) for r in rows]   # secondary = candidate
delta=[a-b for a,b in zip(m,p)]
def pct(x,q):
    x=sorted(x); k=int(len(x)*q); k=min(k,len(x)-1); return x[k]
# Report mean / p50 / p90 / p99 of both sides + delta + ratio.
# Always also print the stdev of `delta` and len(rows) so the reader can sanity-check
# whether the mean delta is bigger than its SE = stdev/sqrt(n).
```

A 0.2 ms p50 delta on a 76 ms p50 with stdev 33 ms over n=2000 is **statistically indistinguishable from zero** — say so explicitly rather than reporting the 0.3% as a "win".

RAM: sample `VmRSS` from `/proc/<pid>/status` every 5 min into a CSV. Peak RSS varies considerably run-to-run (MDBX page-cache warm-up, aggregator-flush timing) — **don't report small RAM deltas as load-bearing without multiple runs**.

### 8. Correctness signal

For PRs that touch execution / commitment, watch for:

| Signal | What to count | Where |
|---|---|---|
| `invalid_any` | rows where either side returned `INVALID` | CSV |
| `status_mismatch` | rows where `$10!=$12` (main says VALID, candidate says SYNCING/INVALID, or vice versa) | CSV |
| Log patterns | `panic`, `fatal`, `invalid block`, `bad block`, `state root mismatch`, `commitment mismatch`, `wrong root`, `Could not validate block` (case-insensitive) | candidate's `erigon-r34.log` (slot B is where the PR runs) |
| Process death | `[ -d /proc/<pid> ]` | per-PID |

Use a `Monitor` (persistent tail-grep) on `erigon-r34.log` so each match fires an event in your session.

## Reusable measurement template (≥ 1 h at tip + sampler + monitor)

Inline a Bash script like the one below, run it `run_in_background: true`, and arm a `Monitor` for log events:

```bash
#!/bin/bash
set -u
MAIN_PID=<from logs/erigon-main.pid>
PR_PID=<from logs/erigon-r34.pid>
CSV=/home/andrew/erigon-node/tmp/elbench/data/mux.csv
OUT=/home/andrew/erigon-node/tmp/elbench/data/measure_<label>.log
MIN_DURATION_SEC=3600

# 1. Wait for steady-state (predicate above).
while true; do
  m=$(curl -s -X POST -H 'content-type: application/json' \
      --data '{"jsonrpc":"2.0","id":1,"method":"eth_blockNumber"}' \
      http://127.0.0.1:8545 | jq -r '.result // ""' | awk '{printf "%d", $1}')
  h=$(curl -s -X POST -H 'content-type: application/json' \
      --data '{"jsonrpc":"2.0","id":1,"method":"eth_blockNumber"}' \
      http://127.0.0.1:8645 | jq -r '.result // ""' | awk '{printf "%d", $1}')
  peers=$(curl -s 127.0.0.1:6161/debug/metrics/prometheus | awk '/^p2p_peers / {print int($2)}')
  if [ -n "$m" ] && [ -n "$h" ] && [ -n "$peers" ] && \
     [ "$peers" -ge 2 ] && [ $((m-h)) -le 2 ] && [ $((m-h)) -ge -2 ]; then
    fast=$(awk -F, '$2=="engine_newPayloadV4" && $10=="VALID" && $12=="VALID" \
                    {p=$9+0; s=$11+0; print p, s}' "$CSV" 2>/dev/null \
           | tail -5 | awk '$1<300 && $2<300' | wc -l)
    [ "$fast" -eq 5 ] && break
  fi
  sleep 30
done
AGE=$(date +%s); AGE_MS=${AGE}000

# 2. Sample latency + RAM + status counts every 5 min; bail on death or after N hours.
while true; do
  ts=$(date +%s)
  total=$(awk -F, -v s=$AGE_MS '$1>=s && $2=="engine_newPayloadV4"' "$CSV" | wc -l)
  vv=$(awk -F, -v s=$AGE_MS '$1>=s && $2=="engine_newPayloadV4" && $10=="VALID" && $12=="VALID"' "$CSV" | wc -l)
  invalid=$(awk -F, -v s=$AGE_MS '$1>=s && $2=="engine_newPayloadV4" && ($10=="INVALID" || $12=="INVALID")' "$CSV" | wc -l)
  mismatch=$(awk -F, -v s=$AGE_MS '$1>=s && $2=="engine_newPayloadV4" && $10!=$12 && $10!="" && $12!=""' "$CSV" | wc -l)
  m_rss=$(awk '/^VmRSS:/{print $2}' /proc/$MAIN_PID/status 2>/dev/null || echo 0)
  p_rss=$(awk '/^VmRSS:/{print $2}' /proc/$PR_PID/status 2>/dev/null || echo 0)
  [ -d /proc/$MAIN_PID ] || { echo "main died"; break; }
  [ -d /proc/$PR_PID  ] || { echo "candidate died"; break; }
  # ... percentile math via python heredoc on filtered CSV ...
  echo "ts=$ts vv=$vv invalid=$invalid mismatch=$mismatch m_rss=${m_rss}kB p_rss=${p_rss}kB ..." >> "$OUT"
  [ $((ts - AGE)) -ge "$MIN_DURATION_SEC" ] && [ "$vv" -ge 200 ] && break
  sleep 300
done
```

Pair with:

```
Monitor(
  description="<label> errors",
  persistent=true,
  command="tail -F /home/andrew/erigon-node/tmp/elbench/logs/erigon-r34.log \
           | stdbuf -oL grep -iE 'panic|fatal\b|invalid block\b|bad block\b|\
wrong root|commitment mismatch|state root mismatch|root hash mismatch|\
Could not validate block'"
)
```

The `stdbuf -oL` is critical — without it, `tail | grep` buffers up to a page and you get no events for minutes.

## Gotchas

- **Stop the rig before changing binaries.** `start-all.sh` writes PIDs to `tmp/elbench/logs/*.pid`; if you swap the binary in `env.sh` without stopping, the next `stop-all.sh` kills the wrong process.
- **The candidate's P2P enode may collide with main's.** Both erigon datadirs were cloned from the same source, so they share `nodekey` and advertise the same `enode://` ID. Outbound peer discovery still works (eventually) but inbound is impaired. If a candidate is stuck at 0 peers for > 5 min after restart, that's why; the fix is to delete `/mnt/giant/erigon3-main/ethereum/nodekey` before the first restart on a new candidate. (Don't do this casually — it loses any peer-pool warming.)
- **`Could not validate block` is not always candidate-side.** The mux logs both sides; check the timestamp and which port (8545 vs 8645) is reporting before assuming the candidate is wrong.
- **`engine_newPayload` latency includes block exec + state root + block hash check.** A PR that touches *only* block-decode or *only* commitment shows up in the same number, so don't overinterpret which subsystem is responsible from the rig measurement alone — use pprof on the running candidate to attribute.
- **Always archive `mux.csv` before a restart.** Per-PR archives keep historical comparisons available: `mv mux.csv mux.csv.<prev-label>.bak`.
- **The rig is single-host.** Background work (snapshot retiring, downloader churn) affects both ELs but not symmetrically (one's cores might be busier with their own bg flush). For variance-sensitive measurements, run the same A/B at different times of day before concluding.
- **Run-to-run RAM variance is large.** A 1-2 GB delta in peak RSS between two runs of the *same* binary is normal. Don't report RAM as a feature win without ≥ 3 runs.

## When NOT to use this rig

- **Devnet / testnet** comparisons → use the `bal-devnet-ab-test` skill (uses Kurtosis / ethpandaops infra).
- **Throughput / MGas-per-second** measurements on a fixed corpus → use `benchmarkoor`.
- **Hive / EEST** protocol-conformance checks → use `erigon-test-hive`.
- **Quick local sanity check** of a single binary → use `erigon-ephemeral`.
