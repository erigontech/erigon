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

## Companion scenario: minimum-viable-download for clean start

A different question, related target: **what is the smallest set of
snapshot files a node can clean-start against and still enter the
sync cycle?** Time-to-tip measures speed; this measures *minimum-data
floor*.

**Why it matters now:** future lazy loading needs the system to
operate against an incomplete file set. Knowing today's
minimum-viable-set teaches us where the architecture currently
demands all-or-nothing, and what the lazy path will have to provide
for free. The testing methodology is the same as the time-to-tip
runs (fresh datadir, hoodi, observe sync state); the input variable
is "what files are present at start" rather than "what prune mode".

**Process:**

  1. Run a normal fresh sync (e.g. minimal mode). Stop SIGINT after
     each phase boundary the production log identifies.
  2. At each stop, copy the datadir, then restart with the
     downloader disabled (`--snap.nodownload=true` or equivalent).
     Observe whether the node enters the sync cycle.
  3. The earliest stop point where a clean-restart (downloader off)
     reaches `head validated age=0` is the minimum-viable-set.
  4. The set's content (which files are present, which aren't)
     defines the floor.

**Expected boundary (sharper take):** the minimum is the **latest
state slice** — the most recent step's `.kv` + `.kvi` per domain
(accounts, storage, code, commitment) plus their existence-filter
accessors. That set is enough for "normal state running":

  - Execute new blocks as they arrive (read latest state, apply tx,
    write new state).
  - RPC queries about *current* state (eth_getBalance, eth_call at
    latest, etc.).

Block files (`.seg` headers/bodies/transactions) and history files
(`.v` / `.ef` / `.efi`) are **NOT** needed for normal state running.
They unlock distinct capabilities:

  - Block files: historical block queries, block-by-hash, peer
    serving of historical blocks via eth/68.
  - History files: archive-style queries (`eth_getBalance` at past
    block).

Both can be lazy-loaded on demand once the architecture supports it
— the minimum-viable-set test should confirm that reads against
absent historical files return Pending (or whatever soft-fail
shape) rather than crashing the node.

If the test confirms this, the storage-component's "Ready for
normal operation" gate is fundamentally simpler than today's
"download everything before doing anything" assumption: latest
state slice is the floor, everything else is enhancement.

**Outputs to record:**

  - File names + total bytes in the minimum-viable-set
  - The phase/step the production log claims it's in when restarted
  - Time from clean-start to tip (compared against full-sync time)
  - Any errors that appear if the set is one file short of viable

This is a distinct target from min-time-to-tip but shares the
testing harness. Run it after the minimal/archive baseline runs
complete so we have a fair comparison point.

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
