# Warmuper Arena Buffer Ring (replace in-flight barrier)

## Overview

PR #21432 (`erigontech/erigon`, branch `awskii/warmup-inflight-barrier`) fixes a data
race in commitment `HashSort` warmup. The hashed key handed to async warmup workers
aliases the producer's grow-only byte arena; at each batch boundary the producer resets
the arena (`t.byteArena = t.byteArena[:0]`) and the next batch overwrites the backing
array while an in-flight worker is still reading the key inside `warmupKey`.

The original PR fixed this with an **in-flight barrier** that parked every worker before
each reset. This plan **replaces that barrier with a ring of arena buffers**: each batch
generation writes into its own buffer, and a buffer is only reused once every warm item
from its previous occupant generation has completed. The producer blocks at a boundary
only when a straggler from `gen-K` is still running — normally instant.

Benefits over the barrier: no per-batch park-all-workers handshake; the common path is a
single atomic load; workers are never parked, only the (rare) lapping producer waits.

## Context (from discovery)

- **Files involved:**
  - `execution/commitment/commitment.go` — `Updates` struct + `byteArena` (1449),
    `arenaAlloc` (1458), `arenaEnsureCap` (1476), `HashSort` (1797–1945),
    `hashSortBatchSize = 10_000` (1789), `WarmKey` submission sites (1830, 1898),
    `byteArena` reset/assign sites: **1808** (ModeDirect entry), **1850** (ModeDirect batch
    boundary), **1875** (ModeUpdate entry), **1919** (ModeUpdate batch boundary), **1963**
    (inside `Reset()`); `arenaEnsureCap` prealloc calls at 1807 (`*192`) / 1874 (`*144`).
    There is **no** post-loop reset inside `HashSort`; the final batch reuses the last
    generation's buffer.
  - `execution/commitment/warmuper.go` — `Warmuper` struct, `warmupWorkItem` (~81),
    `Start` worker loop (channel cap `numWorkers*64`), `warmupKey`, `WarmKey` (272),
    `DrainPending` (310), `Close`/`Wait`.
  - `execution/commitment/hex_patricia_hashed.go:2885` — an **external `DrainPending`
    caller** (post-fold queue drain). `DrainPending` must be KEPT for this caller.
  - `execution/commitment/commitment_test.go` — `TestHashSort_WarmupArenaNoRace` (125,
    keep), three barrier-contract tests calling the removed method at lines
    **190/242/281** (delete early — they block test-compilation), helpers
    `gatedCtxFactory`/`gatedPatriciaContext`/`slowCtxFactory` (80–104, reuse).
- **Related patterns:** prior approach documented in
  `docs/plans/completed/20260526-warmuper-inflight-barrier.md`.
- **Current repo state (clean):** `HEAD` is the plan-doc commit on top of `ce25e2e0ae`
  ("Merge branch 'main' into awskii/warmup-inflight-barrier" — the old PR tip, **barrier
  still intact**, before the latest main). No merge is in progress; working tree is clean.
  `origin/main` is fetched and ~38 commits ahead of the merge base; merging it conflicts in
  exactly one file: `execution/commitment/warmuper.go` (the worker loop). Task 1 performs
  this merge from scratch. Reference backups of a prior working session exist at
  `/tmp/warmup-drainpending-wip.patch` and git stash `drainpending-wip-21432` (placeholder
  `DrainPending` edits — not required, the ring supersedes them).

## Development Approach

- **Testing approach: TDD** (repo CLAUDE.md mandates Red → Green → Refactor for behavior
  changes and forbids automated agents adding `t.Skip`).
- Complete each task fully (code + tests passing) before the next.
- Small, focused changes; keep the `-race` acceptance test green throughout.
- `make lint` is non-deterministic — run repeatedly until clean before any push.
- Update this plan's checkboxes as work lands; add ➕ for new tasks, ⚠️ for blockers.

## Testing Strategy

- **Unit/contract tests** (required each task):
  - Keep `TestHashSort_WarmupArenaNoRace` as the `-race` acceptance test; it now exercises
    the ring. Must be GREEN under `-race` after the ring lands.
  - Delete the three barrier-contract tests that call the removed
    `WaitForInFlightKeysThenRun`; replace with one `WaitBufferFree` contract test (a
    straggler holding a key from generation `g` blocks `WaitBufferFree(g%K)` until it
    finishes, then it returns).
  - **Coverage gap to close:** `TestHashSort_WarmupArenaNoRace` (20 000 keys = 2
    generations, buffers 0/1 never reused) proves *distinct-slot separation* but never
    exercises the lap/block path — it would still pass even if `WaitBufferFree` were a
    no-op. So the `WaitBufferFree` contract test is the real guard. Additionally add an
    integrated lap test (e.g. ≥30 000 keys → ≥3 generations with K=2, plus a slow
    straggler) that forces an actual buffer reuse and proves the producer blocks until the
    straggler drains — run under `-race`.
- **Race detector** is the primary signal for the arena race.
- **Run:**
  ```
  cd /Users/awskii/org/wrk/erigon-warmup-barrier
  go test -race -run 'HashSort_WarmupArena|WaitBufferFree' ./execution/commitment/
  make lint        # repeat until clean
  make erigon integration
  ```
- No UI/e2e tests apply.

## Progress Tracking

- mark `[x]` immediately when done; ➕ for discovered tasks; ⚠️ for blockers.
- keep this file in sync with actual work.

## Solution Overview

**Ring of K arena buffers (K=2), generation-tagged, block-on-lap.**

- `Updates` owns `arenas [K][]byte`, a `curArena int` index, and a monotonic `gen uint64`.
  `arenaAlloc` allocates from `arenas[curArena]`. Both the warmuper's hashed key (`hk`) and
  the producer-only `plainKey` (`pk`) come from the current buffer.
- `Warmuper` owns `outstanding [K]atomic.Int64` plus a `sync.Mutex`+`sync.Cond` used only
  on the rare drain-to-zero / wait paths. `warmupWorkItem` carries `gen uint64`.
- `WarmKey(hk, depth, gen)` does `outstanding[gen%K].Add(1)` then enqueues. A worker does
  `outstanding[item.gen%K].Add(-1)` after `warmupKey` returns and `Broadcast`s the cond
  **only when the count reaches 0** — the per-key path stays lock-free.
- `WaitBufferFree(slot int)`: fast-path `outstanding[slot].Load() == 0` → return; else
  lock, `for outstanding[slot].Load() != 0 { cond.Wait() }`, unlock.
- At each `HashSort` batch boundary: `gen++; slot = gen%K;
  warmuper.WaitBufferFree(slot); reset arenas[slot] to [:0]; curArena = slot`.
- **Entry init:** `gen`/`curArena` persist on `Updates` across `HashSort` calls. At each
  `HashSort` entry (both ModeDirect 1808 and ModeUpdate 1875), call
  `warmuper.WaitBufferFree(curArena)` before resetting `arenas[curArena]`, so a prior
  HashSort's stragglers can't still hold the entry buffer. (The repro test calls
  `warmuper.Wait()` between runs, which already drains, but the invariant must hold without
  relying on the caller.) `Reset()` (1963) resets the whole ring.

**Race-free argument:** batch `N` writes only `arenas[N%K]`; the buffer at slot `b` is
reused for generation `N` only after generation `N-K` (the previous occupant of `b`) has
fully drained, enforced by `WaitBufferFree`. Distinct live generations at any instant
(`N-K`..`N-1`, ≤ K of them) map to distinct slots, so the `outstanding` slot for a
generation is never concurrently incremented for `gen` while being decremented for
`gen-K` (we block until it reads 0 before reusing). In the repro test (20 000 keys /
batch 10 000 = 2 generations → buffers 0 and 1, never reused) no overwrite can occur.

**Cond correctness:** producer checks the predicate under the mutex in the `for`-loop;
workers `Broadcast` under the same mutex after the atomic decrement. Either the producer
reads `0` (decrement already visible) and skips waiting, or it reads non-zero, enters
`Wait` (releasing the mutex), and the worker's later mutex-protected `Broadcast` wakes it —
no lost wakeup.

## Technical Details

- **K = 2**, declared as a named const (e.g. `arenaRingSize = 2`); bumping to 3 only changes
  memory headroom and stall frequency, not correctness.
- Memory: `K ×` the previous single arena's capacity. `arenaEnsureCap` is currently called
  with `hashSortBatchSize*192` (ModeDirect, 1807) and `hashSortBatchSize*144` (ModeUpdate,
  1874) to size ONE buffer; each ring slot needs its own capacity → `K×` total.
- `arenaAlloc` keeps its existing overflow fallback (independent allocation when a single
  key exceeds buffer capacity) per buffer; that path is already race-safe (a fresh slice).
- `DrainPending` is **kept** — it is still called by `hex_patricia_hashed.go:2885` to
  discard now-useless queued warmups after fold completes (unrelated to arena safety). Do
  not remove it. It is simply no longer relied on for the arena race.
- `WarmKey` retains its channel backpressure (`select` on `w.work <-` / `ctx.Done()`); the
  ring sits underneath it for byte-lifetime safety, independent of channel depth.

## What Goes Where

- **Implementation Steps** (checkboxes): merge commit, ring data structures, reclamation,
  `HashSort` wiring, test replacement, lint/build — all in this repo.
- **Post-Completion** (no checkboxes): pushing to update PR #21432 and watching CI are the
  operator actions after implementation; a reviewer sign-off on the concurrency change.

## Implementation Steps

### Task 1: Merge origin/main, drop the barrier, unblock test-compilation

**Files:**
- Modify: `execution/commitment/warmuper.go` (resolve conflict; remove barrier)
- Modify: `execution/commitment/commitment.go` (placeholder call sites)
- Modify: `execution/commitment/commitment_test.go` (delete 3 stale barrier tests)

- [x] from the clean HEAD, run `git merge --no-edit origin/main`; expect ONE conflict in
      `execution/commitment/warmuper.go` (the `Start` worker loop)
- [x] resolve the worker-loop conflict by taking origin/main's version: `warmupKey` is
      called directly inside the `case item, ok := <-w.work:` select arm; delete the old
      post-select barrier block (the `if item.barrier != nil { ... }` + second `warmupKey`
      that sits between the conflict markers)
- [x] remove the barrier entirely (the ring replaces it): delete the `barrier
      *warmupBarrier` field from `warmupWorkItem`, the `warmupBarrier` struct, and the
      `WaitForInFlightKeysThenRun` method from `warmuper.go`
- [x] delete the three barrier-contract tests in `commitment_test.go` (~190/242/281:
      `TestWarmuper_WaitForInFlightKeysThenRun`, `_CtxCancel`, `_ParksAllWorkers`) so the
      test binary compiles for Tasks 2–4 (new `WaitBufferFree` tests arrive in Task 3)
- [x] switch the two `HashSort` call sites (`commitment.go` ~1846/1919) from
      `WaitForInFlightKeysThenRun(func(){ reset })` to placeholder `if warmuper != nil {
      warmuper.DrainPending() }` followed by the plain `t.batchSlab=…[:0]` /
      `t.byteArena=…[:0]` reset (Tasks 2–4 replace this with the ring)
- [x] confirm no conflict markers remain and `git diff --name-only --diff-filter=U` is empty
- [x] sanity build + vet + test-compile: `go build ./execution/commitment/`,
      `go vet ./execution/commitment/`, `go test -run xxxNONExxx ./execution/commitment/`
      (compile-only)
- [x] stage all and commit the merge with a neutral message
      `merge origin/main into awskii/warmup-inflight-barrier` (merge touches the whole repo;
      the package-prefix convention is for change commits, not merges)
- [x] (no new behavior tests this task; ring tests come in Tasks 2–4)

### Task 2: Introduce the K-buffer arena ring in `Updates`

**Files:**
- Modify: `execution/commitment/commitment.go`

- [x] add `const arenaRingSize = 2`
- [x] replace the single `byteArena []byte` field with `arenas [arenaRingSize][]byte`,
      add `curArena int` and `gen uint64` to `Updates`
- [x] update `arenaAlloc` to allocate from `arenas[curArena]` (keep the over-capacity
      independent-allocation fallback), returning a slice into the current buffer
- [x] update `arenaEnsureCap` (1476) to size the relevant ring buffer(s); the two callers
      at 1807 (`*192`) and 1874 (`*144`) now reserve `K×` total
- [x] update all other `byteArena` readers/reset sites to operate on `arenas[curArena]`
      (`grep -n byteArena` → 1469, 1478, 1808, 1850, 1875, 1919, 1963; the `HashSort`
      boundary/entry resets are rewired in Task 4, but `Reset()` at 1963 must reset the
      whole ring here)
- [x] write/extend a unit test for `arenaAlloc` proving sequential allocations within a
      buffer return non-overlapping slices and the overflow fallback still works
- [x] run `go test -run 'arena|Updates' ./execution/commitment/` — must pass before Task 3
      (compiles now that Task 1 removed the stale barrier tests)

### Task 3: Add generation reclamation to `Warmuper`

**Files:**
- Modify: `execution/commitment/warmuper.go`

- [x] add `outstanding [arenaRingSize]atomic.Int64`, a `sync.Mutex`, and a `*sync.Cond`
      (initialized in `NewWarmuper`) to `Warmuper`; share the `arenaRingSize` const
- [x] add `gen uint64` to `warmupWorkItem`
- [x] change `WarmKey` signature to `WarmKey(hashedKey []byte, startDepth int, gen uint64)`;
      `outstanding[gen%arenaRingSize].Add(1)` before the enqueue `select`
- [x] in the worker loop, after `warmupKey` returns, `outstanding[item.gen%arenaRingSize]
      .Add(-1)`; when it reaches 0, lock the mutex and `Broadcast` the cond
      (factored into `releaseGen`, also called on the `ctx.Done()` enqueue-abort path so the
      counter never leaks)
- [x] add `WaitBufferFree(slot int)`: fast-path atomic `Load()==0` returns; else lock and
      `for Load()!=0 { cond.Wait() }`
- [x] write a contract test `TestWarmuper_WaitBufferFree_BlocksUntilStragglerDone` using
      `gatedCtxFactory`: submit one key for gen 0, gate the worker inside `Branch`, assert
      `WaitBufferFree(0)` does not return; release; assert it returns and `outstanding[0]==0`
- [x] write a test that `WaitBufferFree(slot)` returns immediately when the slot is already
      drained (fast path)
- [x] run `go test -race -run 'WaitBufferFree' ./execution/commitment/` — must pass

### Task 4: Wire the ring into `HashSort` batch boundaries

**Files:**
- Modify: `execution/commitment/commitment.go`

- [ ] pass `t.gen` to both `WarmKey` submission sites (1830, 1898)
- [ ] **batch-boundary** sites **1850** (ModeDirect) and **1919** (ModeUpdate): replace the
      placeholder `DrainPending()` + `byteArena=[:0]` reset with
      `t.gen++; slot := int(t.gen % arenaRingSize); if warmuper != nil {
      warmuper.WaitBufferFree(slot) }; t.arenas[slot] = t.arenas[slot][:0];
      t.curArena = slot`
- [ ] **entry** sites **1808** (ModeDirect) and **1875** (ModeUpdate): before resetting the
      entry buffer, `if warmuper != nil { warmuper.WaitBufferFree(t.curArena) }`, then reset
      `t.arenas[t.curArena]` (so a prior HashSort's stragglers can't hold the entry buffer)
- [ ] DO NOT remove `DrainPending` — it is still called by `hex_patricia_hashed.go:2885`;
      leave that call and the method intact
- [ ] confirm `TestHashSort_WarmupArenaNoRace` (kept) still passes under `-race`
- [ ] add an integrated **lap test** (≥30 000 keys → ≥3 generations with K=2 + a slow
      straggler via `slowCtxFactory`) that forces a buffer reuse and proves the producer
      blocks until the straggler drains; assert no race and correct visited count
- [ ] run `go test -race -run 'HashSort_WarmupArena|HashSort_WarmupLap' ./execution/commitment/`
      — must pass

### Task 5: Consolidate and run the full test suite

**Files:**
- Modify: `execution/commitment/commitment_test.go` (if any cleanup needed)

- [ ] confirm the 3 stale barrier-contract tests are gone (deleted in Task 1) and no
      reference to `WaitForInFlightKeysThenRun`/`warmupBarrier` remains in tests
- [ ] confirm the `WaitBufferFree` contract tests (Task 3) and the integrated lap test
      (Task 4) are present and green
- [ ] ensure helpers (`gatedCtxFactory`, `slowCtxFactory`, `gatedPatriciaContext`) are all
      still used (no dead-code / unused-symbol lint)
- [ ] run `go test -race ./execution/commitment/` — full package must pass

### Task 6: Lint, build, verify acceptance criteria

- [ ] `make lint` — run repeatedly until clean (non-deterministic scanner)
- [ ] `make erigon integration` — must build
- [ ] verify the Overview race scenario is closed: keys never overwritten while in-flight
      (acceptance = `TestHashSort_WarmupArenaNoRace` green under `-race`)
- [ ] verify no `WaitForInFlightKeysThenRun` / `warmupBarrier` references remain
      (`grep -rn 'WaitForInFlightKeysThenRun\|warmupBarrier' execution/commitment/`)
- [ ] `go test -race ./execution/commitment/` full-package green

### Task 7: Commit, push, move plan

- [ ] commit the ring implementation:
      `commitment: replace warmup in-flight barrier with arena buffer ring`
- [ ] `git push` to `origin awskii/warmup-inflight-barrier` (updates PR #21432)
- [ ] move this plan to `docs/plans/completed/` (`mkdir -p docs/plans/completed`)

## Post-Completion
*Items requiring external action — no checkboxes.*

**Operator actions:**
- After push, watch CI on PR #21432 (`gh pr checks 21432 --watch`) until terminal; address
  any red checks (the `-race` job is the relevant signal for this change).
- The branch is a draft PR; mark ready / request review once CI is green.

**Review considerations:**
- This is a concurrency redesign — a reviewer should sanity-check the `outstanding`/cond
  reclamation and the `K=2` headroom assumption against production batch/worker counts.
