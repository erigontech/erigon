# Admin SetHead Unwind — Design

**Date:** 2026-05-25
**Branch:** `feat/snapshot-flow-app-integration`
**Status:** Design phase — awaiting approval before commit 2

## Context

This document captures the design for the *administrative* SetHead path
(triggered via `debug_setHead` JSON-RPC, fork-from CLI, future operator
commands) in **block-aligned mode**, where the unwind target may sit
arbitrarily far below the current head — including past the diffset
retention window and into snapshot file ranges.

The work was prompted by two related observations:

1. The CLAUDE.md rule "Unwind beyond data in snapshots not allowed" is a
   placeholder for the code in this document — the missing primitive
   that knows how to coordinate DB unwind + snapshot file trim +
   commitment anchor at an arbitrary block.
2. The user flagged a load-bearing concern (2026-05-25): *"you can't do
   snapshot unwind without db unwind, otherwise you leave a wedge of data
   in the db."* Provider.Unwind in its current shape (commit `0244c4e000`)
   only handles the commitment-entry sub-op; if we add snapshot-trim
   without also handling DB teardown, the DB keeps orphan TxNums /
   canonical hashes / stage progress / diffsets pointing at blocks that
   no longer exist in snapshots.

## Scope and non-goals

**In scope:**

- The aligned-mode admin SetHead path that handles unwinds at any depth.
- The coupling between snapshot-trim and DB-unwind (must run together).
- A precondition contract that lets us implement this cleanly in the
  short term without solving mid-execution reconciliation.

**Out of scope (separate workstreams):**

- The exec-stage unwind path remains diffset-bounded
  (`CanUnwindBeforeBlockNum` / `unwindExec3`); no changes here.
- **Diffset removal proper** lands as part of the execution component
  cutover, not this work. This design *facilitates* future diffset
  removal but does not preempt it.
- **Mid-execution SetHead** (live SharedDomains, in-flight execution) is
  a long-term refinement, captured below but not built in the short
  term.
- Non-aligned chains keep the existing CanUnwindBeforeBlockNum-bounded
  SetHead path. No regressions, no new behavior.

## Two modes — within-diffset (incremental) vs past-diffset (reset)

SetHead picks a mode based on toBlock's relationship to the diffset
retention window (2026-05-25):

> *"For SetHead it has two modes, if within the diffset — use the
> current incremental model, beyond that do the larger reset."*

### Mode A — within-diffset (incremental)

If `toBlock >= ReadLowestUnwindableBlock(tx)` (and the diffset chain
for `toBlock+1..head` is intact), SetHead takes the existing path
unchanged:

- `pipelineExecutor.UnwindTo(toBlock, ...)`
- `pipelineExecutor.RunUnwind(sd, tx)` — applies diffsets backwards via
  `unwindExec3` and `AggregatorRoTx.Unwind`
- TxNums / canonical-hashes / stage-progress truncation
- `sd.Flush + tx.Commit`

No quiescence requirement, no Provider.Unwind call. This is the
zero-risk path covering the common case (chain reorgs, near-tip
admin rewinds). The exec-stage diffset machinery is the source of
truth.

### Mode B — past-diffset (quiescent-DB-reset)

Engaged when:

- aligned-mode is on (`cfg.Snapshot.BlockAlignedBoundaries` true), AND
- `toBlock < ReadLowestUnwindableBlock(tx)` OR the diffset chain has a
  gap in `toBlock+1..head`

Then SetHead delegates the whole flow to Provider.Unwind, which runs
the quiescent-DB-reset sequence below.

The clean simplification the user proposed (2026-05-25):

> *"We can only do SetHead when there are no SharedDomains in flight.
> I.e. no execution and all previous execution is flushed to the DB. If
> we are there we can delete all of the database entities including
> diffsets and start from scratch with what is in effect a newly
> initialized db. This avoids mid execution reconciliations."*

### Precondition (mode B only) — wait, don't error

Before any Provider.Unwind work happens, SetHead must observe a
quiescent DB:

1. **No SharedDomains in flight.** No execution stage is running; no
   block builder or fork-validator is holding a SharedDomains; no
   background commit is mid-flush.
2. **All previous execution flushed to the DB.** Whatever state
   SharedDomains was about to write has been committed; nothing is
   buffered.

**SetHead waits for these conditions; it does not fail on first
contact.** Operators invoking `debug_setHead` shouldn't see spurious
"busy" errors just because a stage happens to be running — the natural
expectation is that the call blocks briefly until the chain settles,
then proceeds.

Implementation: SetHead waits on a quiescence signal with a bounded
timeout. The wait composes:

- Acquire the existing `ExecModule.semaphore` (serializes against
  forward execution; existing 5-second acquire timeout extended for
  the admin path).
- After semaphore acquired, wait for `currentContext == nil` and
  `publishedSD` empty (no SharedDomains pointer is live anywhere) via
  a condition variable or channel-close signal published by the last
  stage's cleanup.
- Total wait bounded by a configurable timeout (default proposal:
  120 seconds — long enough for any in-flight stage to flush, short
  enough that a hung pipeline still surfaces as an error).

Only if the bounded wait expires does SetHead return an error
("execution did not become quiescent within N seconds"). The common
case is a few-millisecond wait while the current stage completes.

### The reset sequence

With the precondition satisfied, Provider.Unwind is the single primitive
that runs the complete admin unwind. Sub-ops in order:

1. **Snapshot-trim past toBlock** — delete or truncate snapshot
   segments whose `To > toBlock`; drop the corresponding torrents via
   `Inventory + DownloaderClient`; republish chain.toml. After this
   step the on-disk snapshot tip is `toBlock`.

2. **DB reset past toBlock** — delete *all* database entities pertaining
   to blocks `> toBlock`, plus all transient working-set entities that
   would otherwise survive into a fresh execution from `toBlock`:

   - `TxNums` truncated to `toBlock + 1`.
   - Canonical hashes for `> toBlock` removed.
   - Stage progress (Headers / Bodies / BlockHashes / Execution) reset
     to `toBlock`.
   - **All diffsets** (`ChangeSets3`) covering `> toBlock` removed.
     For aligned-mode chains we go further: diffsets covering
     `<= toBlock` may also be cleared since the snapshots are now the
     authoritative source. (Optional — see "Diffset retention" below.)
   - Any other writable-domain content in MDBX that the next forward
     execution would otherwise see as inconsistent.

   The intent: after this step, the DB is effectively *newly
   initialized* relative to the snapshot tip — what's on disk in
   snapshot files is the only state; what's in MDBX is empty (or holds
   only invariants like genesis).

3. **Commitment anchor at toBlock** — write a commitment state entry at
   toBlock via the existing `WriteCommitmentEntryAtBlock` helper
   (commit `0244c4e000`). The trie state for this entry is read from
   the latest commitment entry in snapshots (which sits at the nearest
   step boundary `<= toBlock` — for aligned-mode chains this is exactly
   `toBlock` once aligned-mode publishers write per-block commitment
   entries; for now it may be the most recent step boundary, with
   re-execution from there to `toBlock` filling the gap on next
   startup).

4. **Atomic commit.** All four steps land in a single MDBX transaction
   (modulo the filesystem operations in step 1 which are not
   transactional — see "Atomicity gap" below).

### Why this is correct

After SetHead completes:

- The on-disk snapshot set is `[0, toBlock]` (well-formed, aligned).
- The MDBX DB has no entries for blocks `> toBlock` (no wedge).
- The commitment anchor at `toBlock` lets a subsequent
  `NewSharedDomains` call seek to a valid state and re-execute forward.
- There is no SharedDomains positioning *during* SetHead — that's
  precisely what the precondition avoided. The next forward execution
  cycle picks up the new tip cold-start.

### Equivalence to the cold-start "processed frozen blocks" state

The post-mode-B state is **observably identical** to the state of a
freshly-started node that has just processed the frozen blocks up to
toBlock and has not yet executed anything past that point. Same
snapshot set on disk; same empty writable working set in MDBX; same
commitment anchor as the source-of-truth pointer; same expectation
that forward execution starts cold from this point.

This equivalence is load-bearing for reasoning about correctness:
**if cold-start works, mode B works** — anything that goes wrong in
mode B that doesn't go wrong at cold-start is a coordination gap, not
a fundamental issue.

### CL forkchoice coordination

The equivalence has a corollary on the consensus-layer (CL) side:
mode B has **effectively reversed the DB for the CL as well**.
Whatever forkchoice state the CL was tracking — head, safe, finalized
hashes — almost certainly references blocks past `toBlock` that no
longer exist on the EL side. After mode B, the CL's forkchoice state
is stale and the next Engine API call from CL → EL (FCU, NewPayload)
will reference unknown hashes.

Two open questions, both requiring testing:

1. **Does the CL recover naturally?** At cold-start, both EL and CL
   come up together and the CL's forkchoice state is built from
   scratch (checkpoint sync, etc.). After mode B mid-run, the CL is
   already-running but its forkchoice state is invalid. Does the
   CL → EL Engine API surface a "head not found" response that the CL
   knows how to recover from (re-anchor to the snapshot tip)? Or does
   it loop / error / wedge?

2. **What does Caplin (Erigon's embedded CL) actually do?** External
   CL clients (Prysm, Lighthouse, Teku, Lodestar) each have their own
   recovery semantics. Caplin is in-process and may be able to take a
   shortcut signal we publish from the storage component. The
   behaviour needs to be characterised explicitly — relying on
   "probably works" here is exactly the wedge the broader design is
   trying to avoid.

#### External CL is the primary test target

It would be easy to make mode B "work" against Caplin by inventing an
in-process EL ↔ CL signal — a shared bus event, a direct callback —
that other clients don't have. That signal would pass our tests and
silently break every operator running Erigon EL with Prysm /
Lighthouse / Teku / Lodestar.

**The test matrix must include an external CL** so the coordination
contract is observable purely through the standard Engine API. Any
behaviour that depends on internal Erigon EL/CL knowledge does not
count as "mode B works" — it counts as a Caplin-only optimization
that has to layer on top of an external-compatible baseline.

#### Resolution path

If the CL does *not* recover naturally via standard Engine API
responses, SetHead grows a coordination step before returning success
— but the coordination has to be expressible in standard Engine API
terms (e.g., the EL surfaces `INVALID` / `SYNCING` on the next FCU
with a known-good `latestValidHash`, and the CL re-syncs from there).
A bespoke Erigon-only signal is not an acceptable resolution.

For commit 2: implement mode B without any EL/CL coordination
(matching the cold-start equivalence). Test against **both** an
external CL and Caplin. If the test surfaces a wedge against the
external CL, design the coordination as a standard-Engine-API
response in a follow-up commit; if both pass, the cold-start
equivalence is self-coordinating and no extra code is needed.

### Atomicity gap

Snapshot-file deletion is filesystem-level and not part of the MDBX
transaction. The risk: snapshot trim succeeds, MDBX commit fails, and
we're left with files missing but DB still pointing at them.

Mitigation order:
- Do the MDBX work first (in a single transaction; commit it).
- Then do the snapshot trim. If snapshot trim fails after MDBX commit,
  the DB is consistent (points at no blocks past toBlock) and the
  snapshot tip is *larger* than the DB — recoverable on restart by
  re-trimming.

Alternative (cleaner but more code): snapshot-trim writes a
"pending-trim" record into MDBX *inside* the transaction; the trim
itself runs after commit; on restart, any pending-trim record is
replayed to make the filesystem catch up. Worth considering for v2; the
ordering-based approach is fine for v1.

### Diffset retention

The user's framing: *"This change should facilitate diffset removal
not preempt it. We will do diffset removal as part of the execution
component cutover."*

For the short-term model, we have a choice:

- **Clear all diffsets past toBlock only.** Diffsets `<= toBlock` are
  retained for as long as the exec-stage cares about them. Conservative.
- **Clear all diffsets entirely on aligned-mode SetHead.** Goes further
  in the "newly initialized DB" direction; eliminates the wedge concern
  for diffsets too. May surprise the exec-stage if forward execution
  later expects pre-toBlock diffsets to be present.

Recommendation: clear `> toBlock` only in commit 2. The wider "clear
everything" mode can land later, gated on a flag, when the exec
component cutover lands.

## Long-term refinement — mid-flight SetHead

The user's note (2026-05-25):

> *"In the longer term unwind with an sd in flight should involve just
> resetting the db's maxtx for reads, and then tearing down existing
> data and replacing it with new data on flush. This will be a
> refinement of what we have already tested here."*

Sketch (not built in this workstream):

- SetHead does not require quiescence. SharedDomains may be live; one
  or more execution stages may be running.
- The reset operates by lowering the DB's "maxtx for reads" to
  `toBlock`'s last txNum. Subsequent reads against the DB see only
  pre-toBlock content, even if the writable working set still holds
  later-block changes in memory.
- The tear-down of `> toBlock` data is deferred to the next flush:
  whatever SharedDomains was about to flush is reconciled against the
  new tip — entries above the new maxtx are dropped, entries at or
  below are flushed normally.
- New forward execution from toBlock starts immediately; the live
  SharedDomains either receives a Restart signal to refresh, or the
  refinement is engineered so the live SharedDomains can transition
  without a full restart.

This is conceptually clean and a natural extension of the short-term
primitive — the precondition relaxes; the sub-op sequence stays the
same.

## Future direction — Flush as a storage operation

The user noted (2026-05-25): *"I expect that shared domain flush will
become an operation on the storage component."*

Today `sd.Flush(ctx, tx)` lives in `db/state/execctx` and is called
directly by exec-stage code. The architectural direction is for Flush
to be invoked through the storage component instead, aligning with the
broader storage componentization that already owns
inventory / publish / retire / unwind. This is a future workstream;
the present design facilitates it as follows:

- Mode B (past-diffset reset) does no Flush at all — by precondition
  there is no SharedDomains in flight, so there is nothing to flush.
  When Flush moves to the storage component, mode B is unaffected.
- Mode A (within-diffset incremental) still calls `sd.Flush` via the
  existing code path. When Flush moves to the storage component, mode
  A's flush call site changes; nothing structural in this design has
  to be unwound.
- The long-term refinement (mid-flight SetHead) explicitly invokes a
  flush-with-tear-down: that flush is naturally one of the operations
  the storage component would own.

In short: by routing aligned-mode admin unwinds through the storage
component (Provider.Unwind) from day one, this design lands the
storage-as-orchestrator pattern that storage-owned Flush will
generalize.

## Why this facilitates diffset removal

The short-term model is built around a principle: *the snapshot files
are the source of truth; the DB is the writable working set*. Once
SetHead can reset the DB to "what's in snapshots" without consulting
diffsets, the architecture no longer depends on diffsets for
admin-driven state changes.

When the execution component cutover later removes diffsets entirely:

- Provider.Unwind does not change shape. It already runs without
  needing diffsets.
- The exec-stage path (which today does need diffsets for
  unwindExec3) is rewritten in that cutover to either (a) take the
  same snapshot-driven approach as Provider.Unwind, or (b) be
  replaced by a new exec architecture that doesn't unwind at all
  (reorgs handled differently).

Either path is unblocked by having Provider.Unwind in place first.

## Implementation breakdown (commits)

Pending design approval. Roughly:

**Commit 2 — Two-mode SetHead with Provider.Unwind for mode B**

- Add mode detection in SetHead: compute
  `minUnwindable := changeset.ReadLowestUnwindableBlock(tx)` (already
  the basis of `CanUnwindToBlockNum`), branch on
  `toBlock >= minUnwindable` (mode A) vs `toBlock < minUnwindable`
  with aligned mode on (mode B).
- Mode A: existing `pipelineExecutor.UnwindTo + RunUnwind + truncate +
  Flush + Commit` path. Untouched.
- Mode B: delegate to Provider.Unwind. Provider.Unwind grows three
  sub-ops:
  - Quiescence check (no SharedDomains in flight; precondition for
    mode B only).
  - Snapshot-trim (Inventory + AllSnapshots traversal, file delete,
    torrent drop, chain.toml republish).
  - DB reset (`TxNums.Truncate`, `TruncateCanonicalHash`, stage
    progress reset, diffsets `> toBlock` clear).
  - Existing commitment-anchor sub-op (from `0244c4e000`).
- Provider.Unwind unit tests extended for the new sub-ops; SetHead E2E
  adds a mode-B test with a fake snapshot fixture.
- Non-aligned chains: mode B never engages (the aligned-mode condition
  fails); they keep getting the legacy `CanUnwindToBlockNum` rejection
  for deep targets, same as today.

**Commit 3 — Scenario 3 E2E (real snapshot fixture)**

- Extend execmoduletester to produce real snapshot files.
- Add `TestSetHead_E2E_AlignedDeepUnwind_IntoFiles` that exercises the
  full short-term model end-to-end: aligned chain, snapshot files
  present, target inside snapshot range, SetHead succeeds, files
  trimmed, DB reset, subsequent execute past new head works.

## Open questions

These should be resolved before commit 2.

1. **Quiescence wait mechanism.** What's the most reliable signal to
   wait on for "no SharedDomains in flight"? Options:
   - A counter on ExecModule incremented at SharedDomains acquire /
     decremented at release; SetHead waits on a `sync.Cond` signaled
     when the counter hits zero.
   - A `chan struct{}` "quiescent" channel that the last stage's
     cleanup closes when it releases its SharedDomains; SetHead
     waits on the channel + a timeout context.
   - Polling `currentContext == nil && publishedSD == nil` under
     `ExecModule.lock` with backoff.

   Whatever the mechanism, the bound is a configurable timeout
   (default ~120s) — well past the longest stage flush, well below
   "infinite hang."

2. **Commitment anchor trie state.** For aligned-mode chains, do
   publishers already write per-block commitment entries, or only at
   step boundaries? If only step boundaries, what does SetHead do for a
   toBlock between step boundaries — read the latest entry `<= toBlock`
   and re-execute forward to toBlock on next startup, or write a
   synthetic entry at toBlock with the same trie state as the previous
   step boundary?

3. **Snapshot-trim atomicity v1 vs v2.** Order-based recovery (MDBX
   commit then filesystem trim) is simplest; pending-trim-record
   replay is cleaner but more code. Pick one for commit 2.

4. **Aligned-mode detection.** SetHead needs to know whether the chain
   is aligned. Thread `cfg.Snapshot.BlockAlignedBoundaries` into
   ExecModule, or expose it on the Unwinder interface? (Provider has
   access via its Config and could expose a `BlockAligned() bool`
   method.)

5. **CL forkchoice coordination.** See the
   "CL forkchoice coordination" section above. Resolution is
   test-driven: implement mode B without coordination; characterise
   what an external CL and Caplin actually do when EL state reverses
   under them; design a coordination signal only if testing surfaces
   a wedge. Slot-aligned CL-side snapshots are a separate concern in
   the broader aligned-mode workstream, not a SetHead-specific issue.
