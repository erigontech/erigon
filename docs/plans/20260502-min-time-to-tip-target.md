# Min time to tip — target + process

## Target

**Metric:** wall-clock time from `erigon` process start to the first
`head validated ... age=0` log line. That's "reached tip" — node is
processing fresh blocks as they arrive.

**Two modes to track separately:**

  - **Minimal** (`--prune.mode=minimal`): aggressive pruning, smallest
    disk footprint. Fastest possible sync.
  - **Archive** (`--prune.mode=archive`): full historical state.
    Slowest sync; storage-driven lifecycle does the most work.

Track both because:

  - Minimal is the operator-best-case (what's possible if you only
    need to follow chain).
  - Archive is the storage-component-stress-case (most files, most
    transitions, most opportunity for the lifecycle path to misbehave).

The DELTA between the two tells us where the lifecycle path's cost
scales with retained data.

## Baseline (to be measured)

Initial measurement target on hoodi:

| Run | Prune | Flag | Time-to-tip | Peers (Caplin/eth) | Disk |
|---|---|---|---|---|---|
| Smoke 1 | default (full) | off | ~3h | 127/32 | ~30 GB |
| Smoke 2 | default (full) | on | ~47 min | 97/29 | ~30 GB |
| Item 1 | default (full) | on + tight gate | ~44 min | 97/64 | ~30 GB |
| **Minimal** | minimal | on + tight gate | TBD | TBD | TBD |
| **Archive** | archive | on + tight gate | TBD | TBD | TBD |

Smoke 1 vs Smoke 2 difference (3h vs 47m) is mostly network /
peer-warmup variance, not architectural — both produced snapshots,
same end state. To remove that noise, all measured runs should ideally
run within a few hours of each other (similar peer availability).

## Known cost observations

From the item 1 test (44m to tip, default mode, flag-on, tight gate):

  - **3901 BuildMissedIndices invocations vs 188 advanced-to-Indexed
    transitions.** Each sweep iterates `LifecycleDownloaded` files
    and calls BuildMissedIndices ONCE PER FILE even though
    BuildMissedIndices is a global rebuild that handles all missing
    files in one pass. 6 files at Downloaded → 6 invocations →
    6× the work for the same outcome.
    - **Fix:** debounce/coalesce per-sweep. The first invocation
      should rebuild everything missing; subsequent invocations
      within the same sweep should observe "nothing missing" and
      no-op.
    - **Easier fix:** call BuildMissedIndices once per sweep at the
      sweep level, after iterating but before advancing. The
      per-file dispatch then just verifies the deps are now present
      and advances state — no per-file invocation of the global
      builder.
    - **Effort:** small; ~30 LOC change in driver + builder.

  - **Sweep cadence is 60s.** Means the lifecycle is at most 60s
    behind reality. For files arriving rapidly, that's a 60s tail
    on the Downloaded → Advertisable trip. For minimum time-to-tip,
    a shorter cadence (e.g. 5s during initial sync, 60s after) would
    pull the tail in.
    - **Fix:** adaptive cadence. Start aggressive, back off as the
      Downloaded backlog drains.
    - **Effort:** medium; ~50 LOC + tuning.

  - **Inventory startup population scans every visible file
    individually.** Wire A's `AllSnapshots.Files()` and
    `Aggregator.Files()` enumeration loops AddFile for each entry,
    each AddFile fires a ChangeSet, each ChangeSet wakes a sweep.
    O(N) AddFile and O(N) ChangeSet for N files. At hoodi tip
    (~thousands of files) this is fast; at mainnet archive (~tens
    of thousands) it could be a measurable startup cost.
    - **Fix:** batch AddFile path that adds N files and emits one
      ChangeSet at the end.
    - **Effort:** small; ~40 LOC.

These are the visible inefficiencies as of commit `da3e5defb9`. The
benchmark runs (minimal + archive) will surface more.

## Process to get to min time-to-tip

A loop, not a one-shot:

### Phase 1 — Establish baseline

  1. Run the minimal-mode test on hoodi, measure time-to-tip + lifecycle
     counts.
  2. Run the archive-mode test on hoodi, same.
  3. Record numbers in this doc's table above. Compare vs the
     completion-phase smoke results.

### Phase 2 — Diagnose the long pole

For each mode, identify which phase dominates wall-clock:

  - **Snapshot download phase** — bytes/sec, peer count, parallelism.
    Bottleneck is typically network or torrent-client behavior.
  - **Index-build phase** — CPU-bound, parallelism is `IndexWorkers`.
    Bottleneck is likely the per-file BuildMissedIndices loop
    (the 3901 vs 188 issue above).
  - **Block execution phase** — CPU-bound on EVM execution.
    Storage-component changes don't directly affect this; mentioned
    here because it's a chunk of total wall-clock.
  - **State catch-up phase** — bringing the in-DB state forward to
    the latest snapshot's `ToStep`. Touches Aggregator heavily.
  - **CL slot catch-up** — Caplin downloading historical beacon
    data + executing payloads forward. Often the long pole on
    fresh starts.

For each, measure: wall-clock seconds spent + bytes/CPU consumed.

### Phase 3 — Apply fixes in order of expected impact

Order by `expected speedup / effort`:

  1. **Coalesce per-sweep BuildMissedIndices** (above). Highest
     leverage if the index-build phase dominates.
  2. **Adaptive sweep cadence** (above). Reduces the
     Downloaded → Advertisable tail.
  3. **Batch AddFile in startup population** (above). Helps archive
     mode startup but probably small.
  4. **Phase 0 / Phase 1 download-order verification** (completion
     plan item 3). If the production path doesn't actually do
     latest-first, fix it. Could meaningfully reduce time-to-tip
     because Phase 0 latest-state files are what unblock the sync
     cycle.
  5. **Anything Phase 2 surfaces** — diagnose-driven.

After each fix, re-run minimal + archive. Track the table above.
Stop when measured time-to-tip is approximately as fast as the
network can deliver bytes (i.e. download phase is the dominant cost
and there's no architectural slack left).

### Phase 4 — Lock in

Once we have stable times:

  - Add an automated benchmark CI run (long-running, scheduled
    rather than per-PR).
  - Document the baseline numbers in this doc and in
    `docs/plans/20260501-storage-lifecycle-spec.md` as the
    "default-on flip" precondition.
  - The `LifecycleDrivenByStorage` flag's flip from default-false to
    default-true gates on these numbers being acceptable.

## Out of scope for this target

  - Cross-chain comparison (hoodi vs sepolia vs mainnet). Hoodi is
    the development target; mainnet measurements come once the path
    is proven.
  - End-to-end RPC latency. Time-to-tip is about chain progression;
    consumer reads (HeaderByNumber etc.) are a separate target.
  - Network-side optimization (tuning torrent client, peer
    discovery). Outside the storage component's scope.

## Reference

  - Completion plan: `docs/plans/20260502-app-integration-completion.md`
    item 6 (performance measurements) — this target IS that item,
    formalized as an ongoing process rather than a one-off.
  - Lifecycle spec: `docs/plans/20260501-storage-lifecycle-spec.md`
    — the architecture this target measures.
  - In-flight test memory: `app-integration-test-in-flight.md` — the
    item 1 run that surfaced the 3901 vs 188 perf observation.
