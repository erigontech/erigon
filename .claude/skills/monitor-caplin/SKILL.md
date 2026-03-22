---
name: monitor-caplin
description: "Health check for any erigon+Caplin node. Usage: /monitor-caplin (uses defaults) or /monitor-caplin --el-port 8745 --beacon-port 5755 --log /path/to/log. Works with any network: devnets, testnets, mainnet."
allowed-tools: Bash, Read, Glob, Grep
allowed-prompts:
  - tool: Bash
    prompt: monitor and health-check erigon with caplin
---

# Monitor Caplin

One-shot health check for any erigon+Caplin (embedded CL) node.

## Usage

```
/monitor-caplin                                          # defaults: EL 8545, Beacon 5555, auto-detect log
/monitor-caplin --el-port 8745 --beacon-port 5755        # custom ports
/monitor-caplin --log /root/epbs-devnet-0/erigon-console.log  # custom log
```

Can also read from an epbs devnet config:
```
/monitor-caplin epbs-devnet-0    # reads config from devnet/configs/epbs-devnet-0.yaml
```

## Step 0: Resolve Parameters

**Option A — devnet name given** (e.g. `epbs-devnet-0`):
Read `/root/work/erigon/.claude/skills/devnet/configs/<name>.yaml` and extract `el_http`, `cl_beacon_api`, `workdir`.

**Option B — explicit flags given**:
Use the provided `--el-port`, `--beacon-port`, `--log` values.

**Option C — no args (defaults)**:
- EL HTTP: `8545`
- Beacon API: `5555`
- Log: auto-detect by finding the erigon process and its `--datadir`, then look for `erigon-console.log` in the parent dir. If not found, skip log checks.

Final variables:
- `$EL_PORT` — EL JSON-RPC port
- `$BEACON_PORT` — Beacon API port
- `$LOGFILE` — log file path (may be empty)

## Checks

Run ALL checks. Present a concise summary.

### 1. Process alive

```bash
pgrep -fa "erigon.*--" | grep -v grep | head -5
```

If not running, report **DOWN** and stop.

### 2. EL block height

```bash
curl -s --max-time 3 http://localhost:${EL_PORT} -X POST -H "Content-Type: application/json" \
  -d '{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1}'
```

Parse hex to decimal.

### 3. CL sync status

```bash
curl -s --max-time 3 http://localhost:${BEACON_PORT}/eth/v1/node/syncing
```

Report `head_slot`, `sync_distance`, `is_syncing`, `is_optimistic`.

### 4. CL head

```bash
curl -s --max-time 3 http://localhost:${BEACON_PORT}/eth/v1/beacon/headers/head
```

Report slot and root.

### 5. CL peers

```bash
curl -s --max-time 3 http://localhost:${BEACON_PORT}/eth/v1/node/peer_count
```

Report connected count.

### 6. Log error scan (last 2000 lines)

Skip if `$LOGFILE` is empty.

Scan last 2000 lines for:

| Pattern | Meaning |
|---------|---------|
| `missing segment` | Envelope not found — EL/CL gap |
| `EL could not process payload` | errELBehind path hit |
| `EL behind` | EL lagging behind CL |
| `parent's total difficulty not found` | EL parent block missing |
| `panic` | Go panic (critical) |
| `ERR` or `ERROR` | Errors |
| `FCU error` or `forkchoice updated error` | Fork choice update failure |
| `failed to add pending EL payload` | blockCollector failure |
| `head updated` | Head advancement (health signal) |
| `Starting forward sync` | Forward sync entry |

### 7. Recent head progression

```bash
grep "head updated" $LOGFILE | tail -5
```

If last update > 2 minutes ago, flag **STALLED**.

## Output

```
== Caplin Health Check ==
Process:    RUNNING (PID xxxxx)
EL block:   102345
CL head:    slot 112890
CL syncing: false (sync_distance=0)
CL peers:   12 connected

== Recent Errors (last 2000 lines) ==
missing segment:     0
EL behind:           3
panic:               0
head updated:        47
(...)

== Last 5 head updates ==
...

== Verdict: HEALTHY / DEGRADED / DOWN ==
```

Verdict:
- **DOWN**: process not running
- **DEGRADED**: panic > 0, or head stalled > 2 min, or missing segment > 10, or 0 peers
- **HEALTHY**: everything else
