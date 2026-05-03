# Min time to tip — target + process

## Target

**Metric:** wall-clock time from `erigon` process start to the first
`head validated ... age=0` log line. That's "reached tip" — node is
processing fresh blocks as they arrive.

**The architectural target: time-to-tip should be CONSTANT across
pruning modes.**

Pruning mode determines what's retained AFTER sync, not what's
required to REACH tip. Once a node has the latest state slice
(Phase 0 from the orchestrator design) + the ability to receive
new blocks from peers, it's at tip. Anything else — historical
blocks, archive history — is enhancement that pruning mode chooses
whether to keep.

Therefore:

  - **Minimal** (`--prune.mode=minimal`): keep latest state, prune
    historical files aggressively. Time-to-tip should = baseline.
  - **Full** (`--prune.mode=full`, default): keep blocks + some
    history. Time-to-tip should = baseline.
  - **Archive** (`--prune.mode=archive`): keep everything. Time-to-tip
    should = baseline.

If the measurements show meaningful differences, the architecture is
downloading / processing / blocking on data that isn't required for
"ready", and the gap IS the target for optimization. Each mode's
time-to-tip should converge to the time the network takes to deliver
the latest state slice + Caplin's catch-up to the beacon head.

We track all three modes separately and watch the delta close as we
optimize. Constant time-to-tip across modes = architectural success.

**The actual difference between modes is which RPCs are answerable
afterward, not when sync completes:**

  - **Minimal:** current-state RPCs only (`eth_getBalance` at latest,
    `eth_call` at latest, `eth_getBlockByNumber` for very recent
    blocks). Historical block / state queries return Pending or
    NotFound.
  - **Full:** + historical block queries (`eth_getBlockByNumber` for
    any block, `eth_getTransactionByHash`, `eth_getLogs` over recent
    history). Archive-style state queries (`eth_getBalance` at past
    block) NOT available.
  - **Archive:** + all archive-style state queries.

Pruning mode = RPC capability surface. It is NOT a knob for "how
fast can I sync". Today the modes appear to affect sync time
because they affect what gets downloaded *before* tip is declared;
the architectural fix is to defer historical-only downloads until
AFTER tip while still allowing them to catch up in the background.

## Baseline (to be measured)

Initial measurement target on hoodi:

| Run | Prune | Flag | Time-to-tip | Peers (Caplin/eth) | Disk | Lifecycle counts |
|---|---|---|---|---|---|---|
| Smoke 1 | default (full) | off | ~3h | 127/32 | ~30 GB | n/a (flag off) |
| Smoke 2 | default (full) | on | ~47 min | 97/29 | ~30 GB | unobserved (pre-observability) |
| Item 1 | default (full) | on + tight gate | ~44 min | 97/64 | ~30 GB | 3901 BMI / 188 idx / 188 adv |
| **Minimal** | minimal | on + tight gate | **18 min 30 s** | 127/n.a. | TBD | 1705 BMI / 209 idx / 209 adv |
| **Archive** | archive | on + tight gate | **24 min 27 s** | 127/n.a. | TBD | 3822 BMI / 216 idx / 216 adv |

BMI = BuildMissedIndices invocations; idx = advanced-to-Indexed
transitions; adv = advanced-to-Advertisable transitions. With the
tight gate confirmed (zero `RetireBlocks`-prefixed BuildMissed
lines in the minimal run), every BMI invocation is storage-driven.

**Minimal mode observations** (run 2026-05-02 17:05:30 → 17:24:00):

  - Time-to-tip 18 min 30 s. ~2.4× faster than default mode (item 1).
    Less data to retain → faster sync.
  - Lifecycle ratio: 1705 BMI / 209 advances = ~8 invocations per
    transition. Smaller ratio than item 1's ~21:1 (3901/188), but
    same architectural inefficiency (per-Downloaded-file invocation
    of a global builder).
  - Tight gate working as intended; storage-driven path is the
    only builder.

**Archive mode observations** (run 2026-05-02 21:48:09 → 22:12:36):

  - Time-to-tip 24 min 27 s. **~32% slower than minimal (+5 min 57 s
    over baseline).** Architectural target = constant time-to-tip
    across modes; this gap IS the optimization target.
  - Lifecycle counts: 3822 BMI, 216 idx, 216 adv. Transitions ~7 more
    than minimal (consistent with archive's larger expected file
    set). BMI more than 2× (3822 vs 1705) confirms the
    per-Downloaded-file invocation issue scales with retained data.
  - The ~6-min gap maps to download time for archive-mode-only files
    (`.v` history, `.ef` / `.efi` accessors) that minimal mode skips.
    Per the architectural target, those should download in the
    BACKGROUND after tip is declared, not before.

**Direct minimal vs archive comparison:**

| Metric | Minimal | Archive | Delta |
|---|---|---|---|
| Time-to-tip | 18 min 30 s | 24 min 27 s | +5 min 57 s (+32%) |
| BuildMissedIndices | 1705 | 3822 | +2117 (+124%) |
| Advanced to Indexed | 209 | 216 | +7 |
| Advanced to Advertisable | 209 | 216 | +7 |

The transition delta (+7) is small — the ADDITIONAL files archive
keeps don't dominate the lifecycle work. The time delta (+6 min)
is mostly download time. Optimization target: defer those
historical-file downloads until after tip.

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

**Architectural framing.** What this scenario actually defines is
the **Phase 0 download set** — the file set that must arrive
BEFORE the node enters its sync cycle. Phase 1 is everything else,
which downloads in the background.

**Phase 0 is the trigger, not the gating set.** What phase 0
defines is:

  - the ORDER files are requested in (latest-first within domain,
    state before block, etc.)
  - the TRIGGER for sync to start (when the phase 0 subset is local,
    sync cycle can begin)

Phase 0 does NOT determine which files the publisher supplies, or
which files the downloader fetches. The publisher offers whatever
it has (a coherent set, ideally archive-shaped); the downloader
fetches everything in that set. Time-to-tip is measured by when
the phase-0 subset arrives, even though the rest continues to
download in the background and the node is happily processing new
blocks while it does.

Net implications:

  - A correctly-configured publisher offers a complete coherent set
    (no internal gaps relative to the canonical chain). Archive mode
    is the safe default.
  - A pruning publisher (minimal/full) is not necessarily wrong, but
    the merged published manifest must remain coherent — a publisher
    that drops mid-history files from BOTH disk AND its published
    chain.toml is broken from a downstream consumer's perspective
    because the consumer can't index ranges covered by the gap.
  - V2 nodes don't filter the publisher's offering; they fetch it
    all. The phase-0 ordering decides which arrive first, and the
    sync-cycle trigger fires once that subset is local.

**Operators can extend the phase-0 trigger set.** The default is
the minimum-viable subset (latest state slice + recent blocks) so
nodes reach "ready" as quickly as possible. Operators with stricter
"ready" requirements specify additional files via env / flag
(shape TBD when the configurable mechanism lands):

  - RPC operator who wants historical block queries answerable
    without Pending: extend phase 0 to include some range of block
    files.
  - Archive operator who wants archive-style queries answerable
    immediately: extend phase 0 to include history (.v / .ef / .efi).
  - Test fixture: pin an explicit phase 0 set for deterministic
    measurement.

Extending phase 0 doesn't change what gets downloaded — the full
publisher offering still arrives. It changes only what's gated on
before sync starts. Trade-off: stricter phase 0 → longer
time-to-tip (more files must arrive first); looser phase 0 →
faster tip but more reads return Pending until backfill catches up.

**The 2026-05-03 V2 test result.** All the V2 mechanisms worked
end-to-end (discovery, manifestReady gate, registry replacement).
What broke wasn't the V2 wiring — it was the test environment:

  - The publisher (minimal mode + bootstrap=true) advertised
    preverified ∪ local-files = mostly preverified. The test node
    received a manifest only 12 entries different from preverified.
  - Download proceeded against that merged set; webseed served
    nearly everything (peer-download stayed 0 B/s). 422/423 files
    landed.
  - One file didn't complete and/or the storage-lifecycle driver
    started BuildMissedIndices retries against ranges that had
    incomplete coverage; 7942 "not all snapshot segments are
    available" warnings accumulated.
  - The test was stopped before tip because the loop wasn't making
    forward progress.

The takeaway is procedural rather than architectural: a meaningful
V2 minimal-set test requires a publisher whose offered set is
coherent AND meaningfully smaller than preverified. A
fully-archive publisher with bootstrap=false would advertise just
its local files; that's the comparison run that would surface a
real time-to-tip difference. The minimal-mode bootstrap publisher
we used didn't meet that bar.

**There IS a real engineering finding from the test:** the
lifecycle driver's reaction to "not all snapshot segments are
available" is wrong. It just hammers BuildMissedIndices retry
after retry (7942 times in our test) instead of reasoning about
why the build failed. The proper behaviour should be:

  - On build failure, identify the missing segments.
  - If the missing segments are in the active download queue:
    SUSPEND the build for that range. Resume when the downloads
    complete (e.g. wait on inventory ChangeSet for the names).
  - If the missing segments are not yet in any queue but are
    available from a publisher / preverified / webseed: TRIGGER
    a download for them via the storage component's lifecycle
    driver, then suspend until they arrive.
  - If the missing segments are unavailable anywhere: log a
    structured error, stop retrying, surface the gap to the
    operator. This is the "invalid manifest / broken publisher"
    case the operator needs to know about.

Today's behaviour (tight retry loop with no reasoning) wastes CPU,
floods the log, and provides no signal to the operator. The fix
moves the driver from "blind retry" to "informed dispatch". This
is a follow-up engineering item, not a one-line tweak — requires:

  - Structured error from BuildMissedIndices listing missing
    segments (currently it's an opaque error string).
  - Driver-side inventory consultation to classify each missing
    segment (queued / available-but-unqueued / unavailable).
  - Per-classification action: suspend / download-and-suspend /
    log-and-stop.

Worth tracking as a completion-plan addendum.

**Source-of-truth rule:**

Bootstrap is **opt-in via flag**, not the default. Behaviour matrix:

  | V2 (`--snap.p2p-manifest`) | bootstrap flag | Source |
  |---|---|---|
  | off | any | preverified.toml (legacy) |
  | on  | off (default) | peer-discovered chain.toml only |
  | on  | on | preverified.toml + augment from peers |

  - Default V2 node: peer-discovered only. preverified ignored.
  - Bootstrap publisher: opts in via
    `--snap.bootstrap-from-preverified`. Used for initial chain
    rollout / recovery.
  - Today's behavior is the third row by accident — V2 augments
    preverified rather than replacing it. See completion plan §5b
    for the required correction.

The Phase 0 set therefore = whatever the publisher peers currently
advertise via chain.toml. With a tip-running publisher, that set is
just the latest step's state files + recent block range. Bounded.
Constant.

This set should ultimately be **configurable** by the operator
(env var or flag). Different deployments have different "ready to
sync" thresholds:

  - A consumer-facing RPC node may want stricter Phase 0 (latest
    state + recent blocks) so reads don't surface Pending.
  - A relay / forwarding node may accept thinner Phase 0 (latest
    state only) and lazy-load everything else on demand.
  - A test fixture may define an explicit minimum to validate
    behaviour without varying environmental conditions.

Configurability also gives us a forward-compatible knob for lazy
loading: as more code paths handle absent files via Pending, the
default Phase 0 set shrinks. Operators can opt into a thinner set
ahead of the default.

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
