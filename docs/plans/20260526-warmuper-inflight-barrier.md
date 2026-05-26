# Warmuper in-flight barrier — fix arena data race (EncodeKeyV2 panic)

## Overview

The commitment `Warmuper` hands async worker goroutines a key slice that aliases a reused
bump-allocator arena (`Updates.byteArena`). At each 10k-key batch boundary `HashSort` drops only
*queued* warmups (`DrainPending`) and then resets the arena — but workers already executing
`warmupKey` keep reading the old slice while the next batch overwrites those bytes. A genuine
data race on shared mutable memory.

On `main` the warmup encoder is `HexToCompact`, which tolerates garbage silently, so the race is
invisible (at worst a wasted prefetch). PR #21146 switched the warmup encoder to `EncodeKeyV2`,
which validates that every byte is a nibble (≤0x0F) and **panics** otherwise — turning the
latent race into a hard crash:

```
panic: nibbles v2: nibble at index 68 is 0xff, must be in [0x00, 0x0F]
  EncodeKeyV2()            nibbles/nibbles_v2.go:43
  (*Warmuper).warmupKey()  commitment/warmuper.go:198
```

Reproduced on mainnet at block 24,832,920 after ~5h46m (48GB-cap A/B bench). **Not** an OOM,
**not** a V2 logic bug — V2 only added the assertion that exposes a pre-existing race.

**Fix (brainstormed Option B — zero-copy barrier):** add a barrier primitive that drops queued
warmups, waits until every worker has finished its in-flight key (parks them), runs the caller's
arena reset at that safe point, then releases the workers. Chosen over copy-per-key / pooled-copy
because warmup is a hot path (~500k keys per commitment flush) and we want to avoid per-key
allocation.

## Context (from discovery)

Files/components involved:
- `execution/commitment/warmuper.go` — `Warmuper`, `warmupWorkItem`, worker loop (`Start`),
  `WarmKey` (enqueues `hashedKey` by reference, no copy), `DrainPending`, `CloseAndWait`.
- `execution/commitment/commitment.go` — `Updates.HashSort`, `arenaAlloc`/`arenaEnsureCap`,
  the two in-loop batch boundaries that call `DrainPending()` + reset `byteArena`.
- `execution/commitment/hex_patricia_hashed.go` — the only warmup-enabled `HashSort` caller
  (creates warmuper, `Start()`, `defer CloseAndWait()`); a second caller passes `nil` warmuper.
- `execution/commitment/commitment_test.go` — existing warmuper test + `noopPatriciaContext`.

Related patterns / dependencies:
- `warmupKey` uses `hashedKey[depth]` as a nibble index into a `uint16` child bitmap — confirms
  `hashedKey` is *meant* to be unpacked nibbles; `0xff` is corruption, not a valid value.
- Single producer: `WarmKey` and the batch boundary both run on the `HashSort` callback
  goroutine, so nothing enqueues real keys while the barrier runs.
- `noopPatriciaContext.Branch` returns `nil` → `warmupKey` breaks immediately (never in-flight),
  which is exactly why the existing test never caught this.
- Line numbers below are from `origin/awskii/nibblesv2-main`; **re-confirm at edit time** (they drift).

## Branch base (IMPORTANT)

Implement on a branch off **fresh `origin/main`** (`awskii/warmup-inflight-barrier`), NOT
nibblesv2-main. The arena race is a **latent bug on main**: there the warmup encoder is
`HexToCompact` (warmuper.go ~:198), which tolerates the corrupted bytes silently — so on main
there is **no panic**; the only observable symptom is wasted prefetches. This fix removes the race
at its source. PR #21146 (nibblesv2), which swaps the encoder to the validating `EncodeKeyV2`
where the same race surfaces as the `nibble at index N is 0xff` panic, inherits the fix on merge.

Consequence for tests: the **`-race` detector is the sole signal** (the panic does not exist on
main). Keys are valid nibbles, so nothing relies on `EncodeKeyV2`. The `-race` regression test
must fail on pre-fix main (data race on `t.byteArena`) and pass after.

## Development Approach

- **Testing approach: TDD (tests first)** — mandated by project CLAUDE.md for bug fixes
  ("reproduce the bug as a failing test before touching the fix"). The `-race` integration test
  is the red→green driver.
- Complete each task fully before the next; small focused changes.
- Every task includes its tests; all tests pass before starting the next task.
- Maintain backward compatibility (the `nil`-warmuper path and existing API are unchanged).

## Testing Strategy

- **Unit tests** (`execution/commitment/commitment_test.go`):
  - A `gatedPatriciaContext` helper implementing `PatriciaContext`, with two modes:
    - **gated** (contract test): `Branch` signals `entered` then blocks on `release` — timing is
      channel-driven, so the returned blob can be the trivial `[]byte{0,0,0,0}`.
    - **slow** (`-race` test): `Branch` returns a ≥4-byte blob and `time.Sleep`s ~50–100µs (more
      reliable than a bare `runtime.Gosched()`) so a worker is provably still inside `warmupKey`
      holding its (arena-backed) `hashedKey` when the main goroutine hits the batch-boundary reset.
      Note: a plain `{0,0,0,0}` blob has an empty bitmap, so `warmupKey` breaks after one shallow
      iteration — the sleep, not descent depth, is what widens the window. (Optional stronger
      variant: return a blob whose bitmap bit matches the key's next nibble so `warmupKey` descends
      several levels, widening it further — only if the sleep proves insufficient.)
  - Keys are valid nibbles (0x00–0x0F) so the *fixed* code never corrupts — the detector is
    `-race`, not the panic.
  - Test 2 (integration, `-race`): the true regression test — reproduces the race on pre-fix
    code, green after the fix.
  - Test 1 (contract, deterministic): pins the new method's guarantee.
- **e2e tests**: n/a (no UI).
- Intended commands: `go test -race -run 'TestHashSort_WarmupArenaNoRace|TestWarmuper_WaitForInFlightKeysThenRun' ./execution/commitment/ -count=20`,
  plus `make lint && make erigon integration`.

## Progress Tracking

- mark completed items `[x]` immediately; add ➕ for new tasks, ⚠️ for blockers.
- keep this file in sync if scope changes.

## Solution Overview

A `warmupBarrier{reached, resume chan struct{}}` sentinel is pushed through the existing work
channel, one per worker. A worker that pulls a barrier marker signals `reached` and then blocks
on `resume`, guaranteeing it has finished its current real key and cannot run ahead. Once all N
workers are parked (all `reached` received), no worker is reading any caller buffer — the safe
point. The caller's reset runs there, then `resume` is closed.

Parking (not merely counting markers) is required: a fast worker could otherwise consume two
markers while a slow worker is still mid-`warmupKey`, so a plain count could hit N while a worker
still holds a slice.

## Technical Details

New types (warmuper.go):
```go
type warmupBarrier struct {
    reached chan struct{} // worker signals it has finished its in-flight key
    resume  chan struct{} // closed to release all parked workers
}
type warmupWorkItem struct {
    hashedKey  []byte
    startDepth int
    barrier    *warmupBarrier // non-nil ⇒ barrier marker, not a real key
}
```

Worker loop branch (in `Start`'s goroutine, before the real-key handling):
```go
if item.barrier != nil {
    item.barrier.reached <- struct{}{}
    select {
    case <-item.barrier.resume:
    case <-w.ctx.Done():
    }
    continue
}
```

New method:
```go
// WaitForInFlightKeysThenRun drops queued warmups, waits until every worker has
// finished its in-flight key (parking them at a barrier), runs fn at that safe
// point — when no worker still references a previously submitted key slice — then
// releases the workers. Used to reset the shared key arena between batches.
//
// Contract: call only from the single HashSort producer goroutine (the same one
// that calls WarmKey). NOT safe to call concurrently with Close()/CloseAndWait(),
// which close w.work — a send on a closed channel would panic. ctx cancellation
// is safe (it does not close w.work).
func (w *Warmuper) WaitForInFlightKeysThenRun(fn func()) {
    if !w.started.Load() || w.numWorkers <= 0 || w.closed.Load() {
        fn()
        return
    }
    w.DrainPending() // drop queued keys (fn has already processed them)
    // One shared barrier reused for all N markers (avoid per-marker alloc on a hot path).
    b := &warmupBarrier{
        reached: make(chan struct{}, w.numWorkers),
        resume:  make(chan struct{}),
    }
    sent := 0
    for i := 0; i < w.numWorkers; i++ {
        select {
        case w.work <- warmupWorkItem{barrier: b}:
            sent++
        case <-w.ctx.Done():
            close(b.resume)
            fn()
            return
        }
    }
    for i := 0; i < sent; i++ {
        select {
        case <-b.reached:
        case <-w.ctx.Done():
            close(b.resume)
            fn()
            return
        }
    }
    fn()             // SAFE POINT: all workers parked
    close(b.resume)  // release; workers resume the range loop and pick up the next batch
}
```

Note: the barrier orders only the *async warmup* readers. The synchronous trie-processing
`fn(...)` loop at the batch boundary runs to completion on this same producer goroutine *before*
`WaitForInFlightKeysThenRun` is called, so it is already serialized and needs no barrier.

Wiring (commitment.go), at BOTH in-loop batch boundaries (ModeDirect ~1850-1853,
ModeUpdate ~1919-1922):
```go
if warmuper != nil {
    warmuper.WaitForInFlightKeysThenRun(func() {
        t.batchSlab = t.batchSlab[:0]
        t.byteArena = t.byteArena[:0]
    })
} else {
    t.batchSlab = t.batchSlab[:0]
    t.byteArena = t.byteArena[:0]
}
```

Untouched (already safe): final-batch path (arena not reused before the deferred
`warmuper.CloseAndWait()` at hex_patricia_hashed.go:2784 drains workers); post-`HashSort`
`DrainPending()` at hex_patricia_hashed.go:2885; nil-warmuper caller at hex_patricia_hashed.go:2623.

## What Goes Where

- **Implementation Steps** (checkboxes): test harness, the barrier primitive, the two wiring
  sites, the contract test, verification, docs.
- **Post-Completion** (no checkboxes): re-run the 48GB A/B bench on the host; consider porting the
  barrier to `main` (latent race there); consider making `EncodeKeyV2` return an error instead of
  panicking in the best-effort warmup path.

## Implementation Steps

### Task 1: Failing regression test + gated mock context (TDD red)

**Files:**
- Modify: `execution/commitment/commitment_test.go`

- [x] add `gatedPatriciaContext` (implements `PatriciaContext`) with both modes + a `CtxFactory`
      for each. **slow**: realised as a *straggler* — `slowCtxFactory` makes worker 0 sleep per
      `Branch` and return a descending blob (`{0,0,0,1,0,0}`) so it keeps re-reading one
      arena-backed key for ~60 levels while the other 3 workers run fast. A *uniform* delay (the
      original plan's `{0,0,0,0}`+50–100µs) provably cannot race: every worker lags equally and no
      two ever touch the same arena offset concurrently (confirmed empirically). The plan
      anticipated this ("stronger variant: descend several levels"). **gated** (`gatedCtxFactory`):
      `Branch` signals `entered` then blocks on `release`.
- [x] add `TestHashSort_WarmupArenaNoRace`: 20k valid-nibble keys (one in-loop batch reset + final
      batch), `HashSort` with a warmuper (NumWorkers 4, **slow/straggler** ctx) so one worker stays
      mid-`warmupKey` across the reset; assert no panic and all 20k keys visited; covers **both**
      ModeDirect and ModeUpdate (two subtests).
- [x] confirmed it FAILS on current code: `go test -race -run TestHashSort_WarmupArenaNoRace ./execution/commitment/ -count=20`
      → `-race` report: write in `arenaAlloc` (commitment.go:1470) vs read in `warmupKey`, in both
      modes. Stall is 2ms/level because `-race` slows the producer ~10x while `time.Sleep` is
      wall-clock — a short stall lets the straggler finish before any reset, so no overlap.
- [x] test is **red until Task 3** (intended bug repro); passes without `-race` (no panic, all keys
      visited), fails with `-race` on this pre-fix code.
- [x] ran the test - RED before Task 3 (consistent across count=20, ~6.4s).

### Task 2: Barrier primitive in the Warmuper

**Files:**
- Modify: `execution/commitment/warmuper.go`
- Modify: `execution/commitment/commitment_test.go`

- [ ] add `warmupBarrier` type and the `barrier *warmupBarrier` field on `warmupWorkItem`.
- [ ] add the barrier branch to the worker loop in `Start` (reached → select resume/ctx → continue).
- [ ] add `WaitForInFlightKeysThenRun(fn func())` (drop queued, enqueue N markers, await N reached,
      run fn at safe point, close resume) with ctx-cancel handling in every select.
- [ ] write `TestWarmuper_WaitForInFlightKeysThenRun` (contract): NumWorkers 1, gated `Branch`
      signals `entered` then blocks; `WarmKey`; wait `entered`; call the method in a goroutine;
      assert it has NOT returned while the worker is in `Branch`; release gate; assert `fn` ran
      only AFTER the in-flight key completed (atomic flag) and the method returns.
- [ ] write a **deterministic** ctx-cancel case: with the gated ctx holding a worker inside
      `Branch` (so it has NOT yet reached the barrier), cancel the context, then call the method;
      assert it returns promptly via the `<-w.ctx.Done()` arm of the marker-send/await loop and
      that `fn` still runs (no hang, no leak). Do NOT assert on the worker-side `<-resume` vs
      `<-ctx.Done()` interleaving — that race is benign and not deterministically reachable; the
      method always closes `resume` on every early return, so no worker is stranded.
- [ ] run tests - contract test must pass before Task 3.

### Task 3: Wire HashSort batch boundaries (TDD green)

**Files:**
- Modify: `execution/commitment/commitment.go`

- [ ] re-confirm the two batch-boundary line ranges (ModeDirect, ModeUpdate); replace
      `DrainPending()` + `batchSlab[:0]` + `byteArena[:0]` with the
      `WaitForInFlightKeysThenRun`/`else` block shown in Technical Details.
- [ ] leave the final-batch path, post-HashSort `DrainPending`, and nil-warmuper caller untouched.
- [ ] run `TestHashSort_WarmupArenaNoRace` `-race -count=20` — must now be GREEN (both modes).
- [ ] run the full commitment package tests `-race` - must pass before next task.

### Task 4: Verify acceptance criteria

- [ ] verify the Overview repro no longer panics and the race is gone:
      `go test -race -run 'TestHashSort_WarmupArenaNoRace|TestWarmuper_WaitForInFlightKeysThenRun' ./execution/commitment/ -count=20`
- [ ] `make lint` (repeat until clean — linter is non-deterministic).
- [ ] `make erigon integration` (build both binaries).
- [ ] `go test ./execution/commitment/...` (full package, no `-short`) passes.

### Task 5: [Final] Docs & wrap-up

- [ ] update CLAUDE.md only if a new pattern warrants it (likely not).
- [ ] write a short PR note for #21146: "warmuper: copy-free in-flight barrier so arena reset
      can't race async warmup reads (EncodeKeyV2 panic was the symptom; latent on main via
      HexToCompact)".
- [ ] move this plan to `docs/plans/completed/`.

## Post-Completion
*Manual / external — no checkboxes*

**Manual verification:**
- Re-run the aligned 48GB A/B bench on host `arb1-dev` (datadirs at step 8703) past blk
  24,832,920 to confirm B no longer panics there, then on to the blk-25.0M / step-9000 memory
  region — the original (now unblocked) purpose of the 48GB run.

**Follow-ups (separate PRs, not this fix):**
- Confirm PR #21146 (nibblesv2) picks up this fix on merge from main — that's where the race
  currently surfaces as the `EncodeKeyV2` panic; once merged, re-run the 48GB bench.
- Defense-in-depth: have `EncodeKeyV2` return an error (and the warmuper skip the key) instead of
  `panic` in the best-effort warmup path.
