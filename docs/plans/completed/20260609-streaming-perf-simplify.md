# Streaming commitment: CPU/alloc optimization + simplification

Cut streaming-commitment CPU per the 1M-whale profile and simplify the code.
Correctness is already done — every parity test must stay green; no root-hash change.

Package: `execution/commitment`. Branch: `awskii/parallel_prepare_fold`.

## Invariants (never break)

- Green at workers 1/4/8: `TestStreaming_MultiDepthSplitParity`,
  `TestStreaming_MultiDepthCollapseParity`, `TestStreaming_FullCollapseParity`,
  `TestStreaming_StorageCollapseAcrossSplit`, and the `requireIncrementalEquiv`
  suite (`wide_nested_parallel_test.go`).
- **Only hard gate: streaming root + stored branches stay byte-identical to
  `ModeDirect` and `ModeParallel`.** ns/op is informational. Validate every task
  against `ModeParallel`.
- Inner loop: `-race -count=3`. Final verify (Task 5): `-race -count=20`.
- KISS: no new abstractions; comments per `CLAUDE.md` (one line). When trimming,
  **keep** comments that state a correctness invariant (condensed to one sentence);
  only delete restatements, backstory, dates/PR refs, and test-seam narration.

## Baseline (record before, compare after)

`go test -run=^$ -bench='Benchmark_TrieVariants_1MWhale/Streaming-batch$' -benchmem -cpuprofile=base.prof ./execution/commitment/`
— note ns/op, allocs/op, B/op, `go tool pprof -top`.

### Task 1: Flush metrics once per Process (drop per-key counters)

Profile: `prometheus.counter.Inc` 3.4% + `Metrics.Updates` 1.2%.

**Files:** `hex_patricia_hashed.go`, `metrics.go`

- [x] The 3.4% is the **global** prometheus counters `mxTrieStateLoadRate.Inc()` /
      `mxTrieStateSkipRate.Inc()` called per key (`hex_patricia_hashed.go:373,378,
      1005,1095,1173,1249`). Coalesce them into per-Process accumulate-then-single-Add,
      preserving the emitted totals (they are process-wide; one Add per Process keeps
      the value). Done: removed the 6 per-key `.Inc()`; `flushTrieStateRates()`
      publishes the monotonic `hadToLoad`/`skippedLoad` atomics' delta to the
      prometheus counters once per Process (mutex-guarded, exact, race-safe).
- [x] Also move the per-worker `Metrics` atomics (`metrics.go` `loadAccount.Add`
      etc.) / `Metrics.Updates` off the per-key path the same way. Done: per-key path
      bumps non-atomic `*N` fields; `flushHot()` folds them into the atomics before
      any read (AsValues/logMetrics/Values), zeroed in Reset.
- [x] Parity green; benchmark shows the metrics cost gone, roots byte-identical.
      `TestStreaming_*Parity`, `TestStreaming_StorageCollapseAcrossSplit`,
      `TestVerifyParallel*` green `-race -count=3` at workers 1/4/8; `after.prof`
      `-top` shows zero prometheus/Metrics frames.

### Task 2: Cut per-fold allocations (GC) — do NOT rebuild the arena

Profile: ~30% in `madvise`/`mallocgc`/GC; 13.7M allocs/op.

**Files:** `streaming_commitment.go`, `streaming_deep_fold.go`

- [x] The prefix-trie arena already exists and resets per block (`prefixArena`/
      `allocNode`/`resetArena` in `prefix_trie.go`, `prefixTrie.Reset()` at
      `endBlock`). **Leave it** — its slab pointer-stability is load-bearing for
      `*prefixNode` refs. Do not rebuild it. Done: untouched.
- [x] Aim at the real allocators: per-key key copies in `collectSplitKeys` /
      `collectStorageNibbleKeys` (`append([]byte(nil), hk...)` per touched key) and
      the `touchedKey` slices themselves; the per-call `make([]byte, 0, 144)` path /
      `childPrefix` slices in `storageRootLocal` / `foldSplit`. Reuse pooled scratch
      instead of per-call allocation. Done: added a chunked `keyArena` (64KB chunks,
      new chunk only when the next key won't fit so prior slices never move) so both
      collect funcs replace one `append([]byte(nil), hk...)` per key with O(bytes/64KB)
      chunk allocs. The `make([]byte,0,144)`/`childPrefix`/`accPrefix` slices are
      ≤16-per-account and absent from the alloc profile, so left as-is per KISS.
- [x] Verify deferred updates are already pooled (`deferredUpdatePool`); reuse, don't
      re-pool. Done: `get/putDeferredUpdate` use the pool; `foldSplit`/`storageRootLocal`
      reuse via `TakeDeferredUpdates`/`appendDeferred`, no re-pool added.
- [x] Parity + `-race -count=3`; allocs/op drops materially; roots byte-identical.
      Done: `TestStreaming_*Parity`, `StorageCollapseAcrossSplit`, `TestVerifyParallel*`
      green `-race -count=3` (roots byte-identical to ModeParallel). 1M-whale benchmark
      allocs/op 13.82M → 12.92M (~900K per-key copies eliminated; remaining allocs are
      benchmark update-builder/MockState harness + the load-bearing prefix-trie arena).

### Task 3: Goroutine coordination — investigate, simplify only if a clean equivalent exists

Profile: ~28% in `cond_wait`/`usleep`/`cond_signal`/`atomic`; cores idle (sync-bound).

**Files:** `streaming_commitment.go`, `streaming_split_fold.go`

- [x] Map the fan-out: `errgroup` + `sem` channel + `workerPool` Get/Put per storage
      subtree (`storageRootLocal`, `foldPresentSplits`). Note the double coordination
      (`sem` and `errgroup`) and pool churn. Done: `foldPresentSplits` already uses a
      single `errgroup.WithContext` + `SetLimit(numWorkers)` + pooled workers — clean.
      `storageRootLocal` carried the double coordination: a `sem` channel bounding
      concurrency AND a separate unlimited `errgroup`, plus a dead `sem<-`/`<-sem`
      guard around the post-`Wait()` `aggregateStorageRoot` (the sem is fully drained
      once `g.Wait()` returns, so it guarded nothing). Pool churn is sync.Pool
      Get/Put — the right tool, left as-is.
- [x] **Load-bearing invariant to preserve:** one fold = one disjoint subtree prefix
      (`storageRootLocal`'s write-isolation comment) — concurrent folds write only
      their own prefix to the shared ctx, so a collapse self-flush never races another
      fold. Any worker-model change must keep this; `-race` will NOT catch a logical
      double-apply, so validate specifically against `TestStreaming_MultiDepthCollapseParity`
      and `TestStreaming_StorageCollapseAcrossSplit`. Done: each `g.Go` body still folds
      exactly one first-nibble subtree into its own `childPrefix` — invariant untouched;
      both named collapse-parity tests green `-race -count=3`.
- [x] If a simpler equivalent preserves that invariant + parity (e.g. one bounded
      pool/queue instead of errgroup+sem, or batching small first-nibbles), apply it.
      **Otherwise leave the model unchanged — it is correct — and add one line saying
      why.** Do not trade correctness for CPU. Done: dropped the `sem` channel in
      `storageRootLocal` for `g.SetLimit(sc.numWorkers)` — the same bound, matching
      `foldPresentSplits`, with fewer parked goroutines and no dead post-`Wait` guard.
      `foldPresentSplits` was already single-mechanism, so left unchanged. Roots
      byte-identical; full `TestStreaming|TestVerifyParallel|TestDeepFold` suite green
      `-race -count=3` at the in-test worker counts; `make lint` clean.

### Task 4: Simplify — dead code, comments, tests

**Files:** `execution/commitment/*`

- [x] Remove genuinely-dead `storageSplits` field + `StorageSplits()` (no `.Add`
      site anywhere). Done: dropped both from `streaming_commitment.go`.
- [x] `isSplitPoint`, `foldSubtreeAtPrefix`, `foldChildSubtree`, `foldStorageChildCell`
      are reachable only from tests. Remove each with the specific test functions that
      reference it — **not** whole test files (those files also assert live seams like
      `DeepLocalFolds`). Order: remove `foldStorageChildCell` (+ its tests) **before**
      `foldChildSubtree` — the latter's only caller is the former, so it's dead only
      after that. `stripLeadingChildExt` is **live** (called by production folds) — keep it.
      Done with one corrected deviation: removed `isSplitPoint` (+ `TestIsSplitPoint`),
      `foldSubtreeAtPrefix` (+ `TestFoldSubtreeAtPrefix_MatchesDepth64`), and the
      redundant 1-line wrapper `foldStorageChildCell` (consumers now call
      `foldChildSubtree` directly). KEPT `foldChildSubtree`: its real consumer is the
      live test helper `foldWhaleStorageChildren`, which backs the live-code regression
      tests `TestAggregateStorageRoot_ResetsDestinationCell` and
      `TestDeepFold_StorageRootParity` (they exercise production `aggregateStorageRoot`
      / deep-fold parity) — removing it would delete that coverage. This also keeps
      `stripLeadingChildExt` live as the plan requires (it is called ONLY by
      `foldChildSubtree`, so the "keep stripLeadingChildExt" instruction is only
      self-consistent if `foldChildSubtree` stays). `stripLeadingChildExt` is in fact
      not reached from production today; it is live via that test helper chain.
- [x] Background scheduler (`StartScheduler`, `foldSplitBg`, `foldKeys`, `enqueue`,
      `scheduleWorker`, `overlayContext`, `foldDirtySplits`, the eager-fold gate, the
      scheduler-only `splitState` fields) is dead in production but test-covered.
      **Scope it OUT of this plan** (KISS, leave-correct-code-alone) — it is a separate
      removal task if wanted, not part of comment trimming. Done: left untouched.
- [x] Trim comments per the invariant-preserving rule above; collapse redundant tests.
      Done: removed the now-stale "StorageSplits seam is not asserted" trailing sentences
      in three test docstrings; removed the two dead test functions above.
- [x] Build, lint, parity green. Done: `go build`/`go vet` clean; `make lint` 0 issues;
      `TestStreaming|TestDeepFold|TestAggregate|TestVerifyParallel` green `-race -count=3`.

### Task 5: Verify

- [x] `go test -run 'TestStreaming|TestDeepFold|TestVerifyParallel|TestAggregate' -race -count=20` green.
      Done: full suite green `-race -count=20` (501s).
- [x] `make lint` clean; `make erigon integration` builds. Done: lint 0 issues; both binaries build.
- [x] Re-run the baseline benchmark; record ns/op + allocs/op delta. Roots byte-identical to ModeParallel is the pass gate.
      Done: 1M-whale Streaming-batch 914ms ns/op, 12.92M allocs/op (down from 13.82M baseline);
      roots byte-identical to ModeParallel (parity tests green).

## Post-Completion
- No `git push` task — pushing to the PR stays manual.
