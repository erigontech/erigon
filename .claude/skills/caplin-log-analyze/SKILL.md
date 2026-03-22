---
name: caplin-log-analyze
description: Analyze Caplin (CL) logs for known issues. Detects EL/CL race conditions, missing segments, FCU failures, forward sync loops, head stalls, and other common problems. Works with any devnet or mainnet log file.
allowed-tools: Bash, Read, Glob, Grep
allowed-prompts:
  - tool: Bash
    prompt: analyze caplin and erigon log files for errors and patterns
---

# Caplin Log Analyzer

Analyze Caplin/Erigon log files to diagnose common issues. Pass a log file path as argument, or it defaults to `/root/epbs-devnet-0/erigon-console.log`.

## Usage

```
/caplin-log-analyze                              # default log file
/caplin-log-analyze /path/to/erigon-console.log  # custom log file
```

## Analysis Categories

### 1. EL/CL Race Condition (forward sync to chain tip)

**Root cause**: After forward sync switches to chain tip, gossip envelopes hit `newPayload` before EL has finished digesting the `Flush()` blocks. EL returns error (parent TD not found), envelope is discarded, creating a permanent gap.

**Detection patterns**:
```
# EL can't process payload (new errELBehind path)
"EL could not process payload"
"EL behind"

# Old behavior: hard failure on newPayload
"parent's total difficulty not found"

# Consequence: envelope missing from disk
"missing segment"

# Forward sync re-entry (cyclic pattern)
"Starting forward sync"
"chainTipSync"
"Flush complete"
```

**Diagnosis**: Look for this cycle:
1. `Starting forward sync` -> `Flush complete` -> `chainTipSync`
2. Shortly after: `parent's total difficulty not found` or `EL could not process payload`
3. Then: `missing segment` errors
4. Then: back to `Starting forward sync`

If this cycle repeats, the EL/CL race condition is active.

### 2. Missing Segment Analysis

```bash
grep "missing segment" $LOGFILE | tail -20
```

Extract slot numbers from missing segment errors. Check if they form contiguous ranges (indicates a single gap from one lost envelope) or scattered (indicates repeated occurrences).

### 3. Head Progression

```bash
grep "head updated" $LOGFILE | awk '{print $1, $2}' | tail -20
```

Check:
- Is head advancing steadily?
- Are there long gaps (>2 minutes) between head updates?
- Is the block number increasing monotonically?

Plot a rough timeline: extract timestamps and block numbers from `head updated` lines.

### 4. FCU (Fork Choice Updated) Errors

```bash
grep -i "forkchoice\|FCU" $LOGFILE | grep -i "err\|fail\|invalid" | tail -20
```

FCU failures often follow missing segments -- the CL can't build a valid chain if intermediate blocks are missing.

### 5. Forward Sync / Chain Tip Transitions

```bash
grep -E "forward sync|chainTipSync|Flush|Starting downloading" $LOGFILE | tail -30
```

Count transitions between forward sync and chain tip. Frequent switching (more than once every 5 minutes) indicates instability.

### 6. Peer Health

```bash
grep -i "peer\|connected\|disconnected" $LOGFILE | tail -20
```

Low peer count can cause missed gossip messages.

### 7. Panic / Fatal Errors

```bash
grep -iE "panic|fatal|SIGSEGV|runtime error" $LOGFILE | head -20
```

Any panic is critical. Report full context (surrounding 10 lines).

### 8. Pending EL Payloads (new fix)

```bash
grep -E "pending EL payload|DrainPending|pendingELPayloads" $LOGFILE | tail -20
```

If the errELBehind fix is deployed, these logs indicate:
- Payloads being queued (good -- fix is working)
- Failures adding to collector (bad -- investigate)

### 9. blockCollector Activity

```bash
grep -iE "blockCollector|AddGloasBlock|Flush.*blocksInserted|batch inserted" $LOGFILE | tail -20
```

Check that Flush is inserting blocks and that the pending payloads are being drained.

## Output Format

```
== Caplin Log Analysis ==
Log file:  /path/to/log
Lines:     123456
Time span: [first timestamp] -> [last timestamp]

== Summary ==
| Category                  | Count | Status  |
|---------------------------|-------|---------|
| head updated              | 234   | OK      |
| missing segment           | 15    | WARNING |
| EL behind                 | 8     | INFO    |
| parent TD not found       | 0     | OK      |
| panic                     | 0     | OK      |
| FCU errors                | 3     | WARNING |
| forward sync transitions  | 2     | OK      |
| pending EL payloads       | 8     | INFO    |

== EL/CL Race Condition ==
Status: DETECTED / NOT DETECTED / MITIGATED (errELBehind fix active)
Evidence: ...

== Head Progression ==
Latest head: block 102345 at [timestamp]
Advancing: YES / STALLED (last update X minutes ago)
Avg block time: ~12s

== Recommendations ==
1. ...
2. ...
```

## Known Issues Reference

| Issue | Symptoms | Fix |
|-------|----------|-----|
| EL/CL race (pre-fix) | missing segment bursts after forward sync, cyclic re-entry | Deploy errELBehind fix (on_execution_payload.go + chain_tip_sync.go) |
| EL/CL race (post-fix) | "EL behind" logs but head keeps advancing, no missing segments | Working as intended |
| Stale bootnodes | 0 peers, no gossip | Refresh bootnodes from ethpandaops inventory API |
| Checkpoint sync failure | Can't start, no genesis state | Check checkpoint-sync URL reachability |
| blockCollector gap panic | panic in Flush() on non-sequential blocks | Pending EL payloads may arrive out of order; needs sorted insert |
