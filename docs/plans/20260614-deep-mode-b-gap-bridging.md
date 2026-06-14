# Deep Mode-B recovery: the post-snapshot-tip gap-bridging problem

**Status**: open finding, surfaced live during the 5-iter soak on
2026-06-13 (iter 2 mode_b, depth 10k).

**Severity**: blocks the original Gate-1 soak profile (5k/10k/30k/60k/30k)
from completing past iter 2.

**Branch**: `feat/snapshot-flow-app-integration`

## Symptoms

After Mode-B unwind to a target ≥ ~10,000 blocks below the
pre-unwind head, the Caplin re-anchor + EL forward-sync recovery
wedges with:

```
[sync] limited big jump from=3001377 to=3011447 amount=5000 padding=2
error executing clstage app=caplin stage=ForkChoice
  err="failed to compute and notify services of new fork choice:
   failed to run forkchoice: execution Client RPC failed to retrieve
   ForkChoiceUpdate response, err: append with gap blockNum=3010000,
   but current height=3001377"
```

EL head stays at the unwind target (`3001377`); Caplin keeps retrying
with the same FCU target (`3011447`); recovery never converges.

The 30-minute soak recovery window times out; soak aborts.

## Mode-A and shallow Mode-B work — what's special about deeper?

Iter 1 of the same soak ran cleanly:
- mode_a (depth 50): 28s, recovered.
- mode_a2 (depth 300): 68s, recovered.
- mode_b (depth 5,000): 468s, recovered.

Iter 2 mode_b (depth 10,000) wedged.

The working depth ceiling is somewhere between 5k and 10k.

## Root cause: Case C's snapshot assumption

`cl/phase1/execution_client/block_collector/persistent_block_collector.go:480-487`
documents the design assumption:

> Case C fires a single ForkChoiceUpdate at the lowest cached
> block's hash and KEEPS the cache. Erigon's engineapi
> HandleForkChoice resolves the hash via the snapshot-backed
> BlockReader **(the blocks above the post-unwind EL head live in
> snapshot files after a mode-B unwind)** and, when headNum >
> finishProgressBefore by more than the smallBlockJumpThreshold,
> runs the Execution stage forward from elHead through the
> snapshot-backed blocks.

The parenthetical assumption — *"the blocks above the post-unwind
EL head live in snapshot files"* — holds ONLY when the unwind
target is at or below the snapshot tip.

On hoodi during this soak, the relevant numbers:
- Snapshot tip (`frozenBlocks`): 2,993,999 → highest snapshot
  block 2,993,998.
- Iter 1 mode_b target: 3,006,333. Gap [3,006,334..3,011,338]
  ≈ 5,005 blocks. **Past the snapshot tip.**
- Iter 2 mode_b target: 3,001,377. Gap [3,001,378..3,009,999]
  ≈ 8,622 blocks. **Past the snapshot tip.**

Both are past the snapshot tip. The 5k case happened to work because
Caplin's post-anchor forward-sync window extended far enough down to
cover the entire gap on its own (the gap-prune Case C path was a
no-op or only nudged across a small contiguous range). The 10k case
exceeded Caplin's natural cache window.

Where the gap blocks live:
| Block range | In snapshots? | In chaindata? | In Caplin cache? |
|---|---|---|---|
| `[0, snapshot_tip)` | Yes | (pruned in minimal) | No (below cache anchor) |
| `[snapshot_tip, unwind_target]` | No | Survived mode-B (≤ target) | No |
| `[unwind_target+1, Caplin_anchor)` | **No** | **Wiped by mode-B** | **No (this is the gap)** |
| `[Caplin_anchor, chain_tip]` | No | (will be pushed by Caplin) | Yes |

The middle "gap" row is the wedge: blocks that have no source for
the EL to read from when forkchoice walks back via parent links.

## Why "append with gap" specifically

Caplin's FCU points at the high block (e.g. 3,011,447) past the gap.
The EL's `updateForkChoice` reorg path walks back via parent hashes
collecting "new canonicals" until it reaches a parent whose hash is
canonical. For each step it calls `blockReader.Header(parent_hash,
parent_num)`. If `parent_num` is in the gap range, the header is
missing — which should return `MissingSegment`.

In our wedge it returns "append with gap" instead, suggesting the
walk *succeeded* through the gap range somehow. The likely
explanation: Caplin's BlockCollector cache extended down into the
gap by enough that the walk found headers there (Caplin's
checkpoint-sync downloads a window of beacon blocks, and the
forward-sync extends it downward and upward). The walk produced a
sparse-but-extant header chain through the gap. Then
`AppendCanonicalTxNums` failed because the bodies (or TxNum
entries) weren't filled for the gap blocks, only the headers.

The mechanism details matter less than the conclusion: gap blocks
have NO complete data source. Whichever specific failure surfaces
first, the soak is wedged.

## Three possible fixes

### Option A — Precondition refusal

Add a check at the SetHead entry: refuse mode-B unwinds whose target
would create an unbridgeable gap.

```go
// Hard floor on mode-B targets: refuse if (Caplin's expected
// post-restart anchor block) − target > maxBridgeableGap (constant
// derived from Caplin's forward-sync window, e.g. 5000).
maxGap := 5000
if currentHead - targetBlock > maxGap && targetBlock > frozenBlocks {
    return fmt.Errorf("setHead target %d would create a %d-block gap " +
        "past snapshot tip %d that exceeds Caplin's bridgeable window %d; " +
        "shallower targets or fresh-sync required",
        targetBlock, currentHead-targetBlock, frozenBlocks, maxGap)
}
```

**Pros**: zero architectural change; matches the
"refuse-loudly-not-wedge-silently" pattern of the existing orphan
precondition.

**Cons**: caps the achievable Mode-B depth; soak profile 5k/10k/30k/60k/30k
fails immediately for the deeper iterations. The whole point of Mode B is
to handle deep unwinds; capping it at 5k defeats the design.

### Option B — Caplin backward-fetch on demand

When the gap-prune Case C path fires AND the gap exceeds Caplin's
existing cache, dispatch a Caplin-side "backward-fetch" that pulls
beacon blocks for slots corresponding to the gap range. The fetched
beacon blocks carry execution payloads which contain header + body;
those get pushed into chaindata, closing the gap.

**Pros**: lifts the gap-size cap. Aligns with how Caplin sources data
for arbitrary slots (beacon p2p `BeaconBlocksByRange` / `BlobSidecarsByRange`).

**Cons**: significant Caplin-side work. The current BlockCollector
cache is filled by forward-sync, not on-demand fetch. Requires a new
fetch path with its own retry / backoff / spec-compliant rate-limiting.
Bounded by the weak-subjectivity window — peers aren't required to
serve beacon blocks deeper than that.

### Option C — EL self-bridges the gap

When the EL's forkchoice walk reaches the gap range and finds no
header, return a structured status that tells Caplin "fetch blocks
for slot range X, push them in order, then re-FCU." Caplin's gap-prune
issues the corresponding fetches.

**Pros**: cleaner separation — EL says what it needs, Caplin sources
it. Avoids EL pretending to understand beacon slots.

**Cons**: requires a new EL → CL signaling path; the
`ExecutionStatusMissingSegment` exists but doesn't carry the missing
range. Extending it to do so is a protocol-ish change.

## Recommendation

**Option A as a stopgap, Option B as the medium-term fix.**

Option A unblocks the immediate soak (run with depths ≤ 5k for
iter 2+) and gives operators a clear refusal rather than a silent
wedge. Document the cap as a known constraint of the experimental
delivery.

Option B is the architecturally correct fix and aligns with how the
mode-B re-anchor primitive was originally pitched (see memory pin
"MODE-B CL-REWIND MVI BUILT + ANCHOR-FLOOR FINDING"). It belongs in
the same iteration-2 cycle as Proposals 1, 2, 3 — it changes the
trust / fetch contract enough to deserve a proposal of its own.

## Soak profile implications

Until Option B lands, the realistic soak profile is shallower:
- `5000, 5000, 5000, 5000, 5000` — five 5k mode-Bs with mode_a/mode_a2
  shoulders. Validates the unwind correctness path without exercising
  the deep-recovery gap.
- The original profile `5000, 10000, 30000, 60000, 30000` becomes the
  post-Option-B target profile, not the pre-PR gate profile.

The pre-PR gate should be:
1. The 5-iter 5k soak passes.
2. Kill-mid soak passes.
3. Fresh-sync-then-soak passes.
4. Deeper soaks documented as an Option-B blocker for production.

## Reference

- Live wedge: `/tmp/unwind-soak-v3-driver.log` and
  `/tmp/erigon-hoodi.log` at 23:05+ on 2026-06-13.
- Soak result CSV: `/tmp/unwind-soak-v3-20260613.csv` — five out of
  six attempted phases passed; deep mode-B wedged on the sixth.
- Memory pin: "MODE-B CL-REWIND MVI BUILT + ANCHOR-FLOOR FINDING
  (2026-06-09 evening)" predicted this finding ("Caplin's forwardSync
  startSlot is bounded below by anchorSlot") and called Phase 2
  needed for deep mode-B targets. This document is the live capture
  of Phase 2 being needed.
