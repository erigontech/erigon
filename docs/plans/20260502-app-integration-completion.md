# App-integration completion plan

## Status

This branch is **not complete**. To be complete it must be a
**working optimised system** — that's the bar. Two smoke tests
on hoodi told us the basics don't crash; they did NOT tell us the
system is working as designed.

## What the smoke tests showed

  - **Test 1 (no regression, flag off).** Synced to tip in ~3h,
    produced new snapshots via stage-driven path. Default-off
    behaviour unchanged from main. ✅
  - **Test 2 (storage-driven, flag on).** Synced to tip in ~47m,
    produced new snapshots — but `productionIndexBuilder` NEVER
    FIRED (zero `[lifecycle]` log lines). Snapshots were produced
    by retire's inline `BuildMissedIndicesIfNeed`, which we did
    not gate. The lifecycle driver ran but only did tracking
    work. ⚠️

So in flag-on mode the storage-driven path is **structurally
present but operationally inert**. The system is not yet doing
what it was designed to do. That's not "complete".

## What completion requires

A working optimised system means, at minimum:

### 1. The storage-driven path actually drives

`productionIndexBuilder` must fire for real index-build work in
flag-on mode. Today retire's un-gated inline build pre-empts it.
Two paths to fix:

  - **Tight gate.** Also gate retire's inline
    `BuildMissedIndicesIfNeed`
    ([db/snapshotsync/freezeblocks/block_snapshots.go:474](../db/snapshotsync/freezeblocks/block_snapshots.go#L474)).
    With flag on, retire creates `.seg` only; storage driver
    does the rest. Validates the lifecycle driver end-to-end.
    Risk: timing assumptions in retire (does it require indexes
    before publishing visibility?). Bor has its own retire path
    that may need the same treatment.
  - **Move retire into storage.** The deeper fix from the
    lifecycle spec — storage owns retire too. Bigger scope but
    matches the architectural intent.

Decision needed; tight gate is the cheaper path to validate the
current code; move-into-storage is the right long-term home.

### 2. Observability so we can SEE what the system does

Without log lines we are guessing. Required:

  - `Driver.Start` — Info: "lifecycle driver started", interval,
    snap-dir.
  - `Driver.dispatch` — Debug per dispatch: file + transition.
  - `Driver.discoverNewFiles` — Debug per add; Info summary on
    bulk discovery.
  - `productionIndexBuilder.BuildMissedIndices` — Info on
    invocation with elapsed + result. Distinct prefix
    (`[storage-lifecycle]` or similar) so log readers can
    distinguish driver-fired calls from stage/retire-fired ones.
  - `Inventory.AdvanceTo` — optional Trace.

Small change (~50–100 LOC). Blocking everything else: without
observability we cannot verify any of the items below.

### 3. Download order verified in production sync

Phase 0 latest-first + Phase 1 backfill is exercised by
in-process scenarios (`TestP2P_LatestDownloadBackfill`). Real
hoodi sync must show the same ordering in the production log:

  - State files before block files
  - Within state, `ToStep` descending
  - `InitialStateReady` after Phase 0 completes

Requires a structured log on every `DownloadRequested` plus a
post-test analysis. If the order is wrong in production we have
a real bug.

### 4. Fast-start tested

Hoodi-test-fixture (85 GB pre-populated, per memory at
`/erigon/hoodi-test-fixture/snapshots/`) gives us a ~5-min
iteration cycle vs the smoke test's 47-min. Required for any
serious development cadence. Verifies wire A's startup
population on real non-empty data.

### 5. Failure-injection scenarios pass

The storage-driven path's value is precisely that it handles
failure cases the stage path doesn't. Two classes of tests:

**Clean-stop scenarios** (SIGINT — process exits gracefully):

  - **Mid-sync restart.** Stop SIGINT during initial download or
    state catch-up, restart, lifecycle resumes cleanly.
  - **Mid-retire restart.** Stop while a retire batch is mid-flight,
    restart, retire resumes from where it stopped.
  - **Long soak.** 24h+ run, no leaks/drift/missed transitions.

**Abnormal-termination scenarios** (SIGKILL / OOM / power loss —
process dies without cleanup):

  - **Kill during download.** Partial `.seg` left on disk. Restart
    must detect "this isn't a complete file" and either
    re-download or finish the partial — not treat the partial as
    Downloaded. Today's `discoverNewFiles` adds any file with a
    known extension at LifecycleDownloaded; a half-written file
    would be misclassified. Either tighten discoverNewFiles
    (size/checksum sanity vs torrent metadata) or rely on the
    downloader's resume path. Test confirms we get a clean
    restart, not a wedged inventory.
  - **Kill during index build.** `.idx` partially written or
    missing despite `.seg` present. Restart should retry the
    build; the retry should succeed (BuildMissedIndices is
    idempotent for most index types, but verify).
  - **Kill during retire-merge.** Constituents and merged file
    coexist on disk for a window; if killed during the window,
    restart sees both. Held-view discipline + recalcVisibleFiles
    should resolve cleanly, but verify across all lifecycle
    states.
  - **Kill while loading to tip.** Process dies during initial
    sync (download phase, index-build phase, state-execution
    phase). Each phase has different "in-flight" state on disk.
    Restart must reach a clean-start state — node either picks up
    where it left off or rolls back to a known-good earlier state,
    no halfway-zombie.
  - **Corruption-detected re-download.** Delete an `.idx` post-
    sync; storage driver detects on next sweep and rebuilds.
  - **Hardware-flush failure.** Snapshot file fully written from
    erigon's perspective but lost in OS page cache before fsync
    (simulate via dropping cache + truncating file by N bytes).
    Restart should detect partial content and re-download.

The clean-stop set validates the lifecycle's normal recovery path.
The abnormal-termination set is what makes the storage-driven path
worth the complexity — the stage path today struggles with these
scenarios because it doesn't model file-level lifecycle state at
all.

Each abnormal-termination test follows the same shape: launch
fresh sync → reach intended phase → SIGKILL → restart same datadir
→ observe `[storage-lifecycle]` log lines + final sync state →
declare pass/fail.

**Connection to existing validation infrastructure.** The built-in
per-file validators we already shipped (`SizeMatchesTorrent`,
`ContentNotEmpty`, `NameNotEmpty`, `RangeOrdering`,
`KindConsistencyFromName` — see
`node/components/storage/validation/builtins.go`) are precisely the
right shape for partial-file detection. They don't run as a startup
phase today, but they could:

  - **Wire A** (Provider startup population) runs a per-file
    Validator chain over each on-disk entry before AddFile-ing it.
    A partial or corrupt file (size mismatch with torrent metadata,
    empty content where there should be bytes) gets rejected →
    AddFile skipped or entered at a quarantined state → next sweep
    triggers a re-download.
  - **Wire E** (driver disk scan) similarly: validate before
    classifying a freshly-discovered file as Downloaded. A
    SIGKILL-induced partial fails `SizeMatchesTorrent` and stays
    out of the Ready pipeline.

This reuses existing infrastructure (the extension-point validation
framework) for a use case it was designed for but isn't yet applied
to. No new validators needed — just running the existing chain at
the right moments. The abnormal-termination tests will tell us
whether this wiring is sufficient or whether additional validators
are needed for cases the current built-ins don't cover (e.g.
half-written indexes that pass size check but fail recsplit-parse).

If the abnormal-termination tests don't pass, the storage-driven
path doesn't justify its existence vs the stage path; we'd be
carrying complexity without recovery payoff.

### 6. Performance characteristics measured

"Optimised" means we measured and the numbers are acceptable:

  - Sweep CPU cost on a populated Inventory (cheap? per-sweep
    allocations?)
  - Memory footprint of Inventory at hoodi tip (~thousands of
    files)
  - Index-build latency: stage-driven vs storage-driven (should
    be comparable; if storage-driven is slower, we have a
    regression)
  - End-to-end sync time: flag-off vs flag-on (test 2 was
    47m, test 1 was 3h — but conditions differed; need a fair
    comparison)

If any number is meaningfully worse than the stage path, the
flag should not flip on by default until fixed.

## Sequencing for completion

  1. **Observability (item 2)** — first; everything else is
     guesswork without it. ~half-day.
  2. **Fast-start (item 4)** — second; gives us a cheap iteration
     loop for the rest. ~half-day to get running cleanly.
  3. **Download-order verification (item 3)** — runs against the
     fast-start setup. Confirm production matches scenarios.
     ~day for instrumentation + analysis.
  4. **Tight retire gate decision + implementation (item 1)** —
     pair with download verification; the decision affects what
     we observe. ~day for the decision + implementation +
     re-test.
  5. **Failure scenarios (item 5)** — once the basics are
     instrumented and verified. ~few days, parallelisable.
  6. **Performance measurements (item 6)** — last; compares
     stable storage-driven path against stage-driven. ~day for
     measurements + analysis.

Total estimate: 1–2 weeks of focused work to call this complete.

## Open decisions

These need resolution before / during the work above:

  - **Tight gate vs move-retire-into-storage** for item 1. Tight
    gate first (cheaper, tests current code); move later as a
    separate branch.
  - **Bor retire path.** Same gate question as the main path —
    does Bor's retire need the same treatment? Likely yes for
    consistency. Worth checking before flipping flag-on default.
  - **Default flag value.** When does
    `LifecycleDrivenByStorage` flip from default-false to
    default-true? After items 1–6 pass cleanly + a soak window.
    Probably another release cycle.
  - **Observability vs framework event bus.** Today's plan adds
    `log.Logger` lines. The storage-views spec hints at
    `flow.InventoryChanged{ChangeSet}` events on the framework
    event bus. Should the observability hook into that instead?
    Probably yes long-term, but log lines first for immediate
    debugging.

## Out of scope for completion

These remain deferred to follow-up branches:

  - **Read-method wait-or-pending integration** in
    RoSnapshots/Aggregator. Setter is wired but no read methods
    consult `Awaiter` yet. Separate phase, scoped against
    consumer-side scenarios (downloading peers).
  - **Step 11 — legacy stage-code cleanup.** Stays deferred
    until `LifecycleDrivenByStorage` has been default-on across
    multiple releases without incident.
  - **Cross-file consistency validators.** Stage 2 of validation
    framework. Plug into `runValidation`'s chain.

## What stays from the current 25 commits

All of it. The foundation is correct; completion adds
instrumentation, validation, gate tightening, and measurements.
Nothing gets reworked. Specifically:

  - The five connective wires (A–E) STAY.
  - The CLI flag + gate STAY. Default-off remains the safe
    initial position.
  - All specs and EXTENDING.md STAY.
  - The Awaiter setters on RoSnapshots/Aggregator STAY (their
    read-method integration is a follow-up phase).

Completion is **additive on top of these 25 commits**, not a
rework.
