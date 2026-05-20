# Storage owns the full post-download pipeline (end-state)

Date: 2026-05-18
Status: design — to implement after the (A) short-term patch confirms no other surprises in the run-to-tip path.

## Why

The componentization directive is: **staged-sync should run execution only.**
Today, staged-sync's OtterSync stage still owns three things that aren't
"execution":

1. **Open file handles** on snapshots after download (OpenSegments,
   OpenFolder, agg.OpenFolder).
2. **Seed MDBX with per-block metadata** derived from snapshot data
   (`FillDBFromSnapshots` — populates `kv.HeaderTD`, canonical hash
   pointers, etc.) so downstream consumers can do MDBX-backed lookups.
3. **Bridge step ordering** between download, index-build, and
   open-then-seed via the OtterSync `forward` function's sequential
   structure.

The 2026-05-18 live run exposed the cost of this awkward split: the
storage component fired `flow.InitialStateReady` representing "state-domain
phase complete" — but block-segment `.idx` accessors were still
mid-Indexing for 155 ms after the signal fired. OtterSync's `OpenSegments`
ran in that window and lstat-failed on a not-yet-built `.idx`. The
publisher's execution service didn't start. `FillDBFromSnapshots` never
ran. Caplin's `BlockCollector.Flush` then failed forever on a missing
parent TD for block 25,069,999 (the snapshot tip) — `kv.HeaderTD` had no
entry because the routine that would populate it never ran.

The signal-and-run architecture needs to be rethought. The signal that
"state is ready" must mean "ready for **everything** downstream relies
on" — files open, indices built, MDBX seeded.

## What changes

Move post-download MDBX seeding and file-open bookkeeping out of
`execution/stagedsync/stage_snapshots.go` and into the storage component.
The storage component becomes responsible for:

- Downloading snapshot files (already there)
- Per-file lifecycle: Declared → Downloading → Downloaded → Indexing →
  Indexed → Validating → Advertisable (already there)
- **Opening file handles** post-Indexed (Snapshots.OpenFolder,
  agg.OpenFolder, OpenSegments) — NEW for storage
- **Seeding MDBX** with the per-block metadata derived from snapshots
  (`FillDBFromSnapshots` equivalent: header TD, canonical hashes,
  body-count) — NEW for storage
- Emitting **one** signal: `flow.InitialStateReady` — meaning "every
  file in the minimal set is on disk + Indexed + opened + MDBX seeded;
  stages 2-6 may proceed without further checks"

Staged-sync's OtterSync becomes a no-op under `--snap.lifecycle-driven-by-storage`:
- Wait on `flow.InitialStateReady`
- Return DONE

No further work, no OpenFolder, no FillDBFromSnapshots, no retries. The
stage's only job is the wait.

## Layering

```
┌─────────────────────────────────────────────────────────────────┐
│  Storage component (node/components/storage/)                   │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │  Lifecycle driver — per-file state machine                │   │
│  │   • Downloaded → Indexing (BuildMissedIndices)           │   │
│  │   • Indexing → Indexed (.idx on disk)                    │   │
│  │   • Indexed → Validating (cryptographic checks)          │   │
│  │   • Validating → Advertisable                            │   │
│  └──────────────────────────────────────────────────────────┘   │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │  Orchestrator — gap-fill + signal emission                │   │
│  │   • Tracks phase-1 (state-domain + meta + salt + block-  │   │
│  │     header) and phase-2 sets                              │   │
│  │   • Wait for ALL phase-1 files to reach Indexed           │   │
│  │   • Wait for post-Indexed open-and-seed step to complete  │   │
│  │   • Fire InitialStateReady ONCE                          │   │
│  └──────────────────────────────────────────────────────────┘   │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │  Post-Indexed open-and-seed step (NEW)                    │   │
│  │   • Snapshots.OpenFolder()                                │   │
│  │   • agg.OpenFolder()                                      │   │
│  │   • FillDBFromSnapshots equivalent: walk frozen headers,  │   │
│  │     write HeaderTD + canonical-hash + block-count to MDBX │   │
│  │   • Verify: rawdb.ReadTd(parent of snapshot.tip) ≠ nil    │   │
│  │     after seeding (canary against silent failures)        │   │
│  └──────────────────────────────────────────────────────────┘   │
│                            │ on completion                       │
│                            ▼                                     │
│             flow.InitialStateReady — fire once                   │
└────────────────────────────────────────────────────────────────┬┘
                                                                 │
                                                                 ▼
┌─────────────────────────────────────────────────────────────────┐
│  Staged-sync (execution/stagedsync/)                           │
│  Stage 1 (OtterSync) under --snap.lifecycle-driven-by-storage:  │
│   • Wait on flow.InitialStateReady                              │
│   • Return DONE                                                 │
│  Stages 2-6 run normally (read MDBX-seeded HeaderTD, etc.)      │
└─────────────────────────────────────────────────────────────────┘
```

## Migration plan

Done in incremental commits so each step is independently testable.

### Step 1 — Move OpenFolder out of OtterSync into storage

The `Snapshots.OpenFolder()`, `agg.OpenFolder()`, and
`OpenSegments(headers, bodies, true, false)` calls currently in
`stage_snapshots.go:306-348` move into the storage component, fired as
the next lifecycle step after the last phase-1 file reaches
LifecycleIndexed.

The new internal contract: orchestrator waits for `phase1Indexed == 0`,
then calls a `postIndexed()` callback that runs OpenFolder. Only after
OpenFolder returns does the orchestrator fire InitialStateReady.

OtterSync's storageDriven path stops calling OpenFolder/OpenSegments
(they've already run by the time InitialStateReady fires).

### Step 2 — Move FillDBFromSnapshots out of OtterSync into storage

`FillDBFromSnapshots` (currently in
`execution/stagedsync/rawdbreset/reset_stages.go:217`, invoked from
`stage_snapshots.go:423`) moves into the storage component's
`postIndexed()` callback. Runs after OpenFolder, before InitialStateReady
fires.

This populates `kv.HeaderTD`, canonical hashes, etc. for the entire
snapshot range. After this, `rawdb.ReadTd` for any block in
0..snapshot.tip returns a valid TD.

Add a canary: after FillDBFromSnapshots completes, do a
`rawdb.ReadTd(tx, snapshotTipHash, snapshotTipNumber)` and assert it's
non-nil. If nil, fail-loud at startup with a clear error message
("MDBX seeding incomplete") rather than letting Caplin discover it
later as "parent's total difficulty not found".

### Step 3 — Move the `cfg.afterDownload` callback into storage

`afterDownload` (currently invoked at `stage_snapshots.go:333`) does
work like agg.OpenFolder + reload + retire-up-to-frozen-tip. Move what
belongs to storage (file-handle opening) into the new postIndexed
step; what belongs to exec (retire bookkeeping) stays in exec.

### Step 4 — Make OtterSync's storageDriven path strictly a no-op

After steps 1-3, OtterSync's storageDriven path is just `<-initialStateReady; return nil`.

Document this in the OtterSync code as the canonical V2 path.

### Step 5 — Granular signals (optional, future)

If different consumers want different readiness levels (Caplin needs
`BlockHeadersReady` early to set its download destination; exec needs
the full `InitialStateReady`), keep emitting the granular signals
alongside the integrated one. Already partially done — `BlockHeadersReady`
exists and Caplin subscribes to it. Document the signal contracts in
a single place.

### Step 6 — Incremental TD seeding (optimization, future)

Today's `PostIndexedSeed` runs `FillDBFromSnapshots` as a single sweep
inside the postIndexed callback, after the LAST phase-1 file reaches
LifecycleIndexed. That's correct but pessimistic: TD only needs the
header files (it walks `blockReader.HeadersRange` and writes
`kv.HeaderTD` per header). Header files are tiny (~MB) compared to
state-domain files (~GB). So waiting for state to finish before
seeding TD wastes parallelism.

The optimization: hook the lifecycle's "headers .seg advanced to
LifecycleIndexed" transition. As each header-range is Indexed, seed
TD for that range immediately. By the time the orchestrator fires
InitialStateReady (after the last phase-1 file lands), `kv.HeaderTD`
is already fully populated — the postIndexed callback's
FillDBFromSnapshots becomes a no-op (every stage's progress is
already at blocksAvailable).

Shape: add a per-header-range hook in the storage component, fed from
the same inventory ChangeSet subscription the orchestrator already
uses. The hook calls a TD-seed-this-range function (new, narrower
than `FillDBFromSnapshots`) on each ChangeSet where a headers .seg
just reached LifecycleIndexed. Concurrent invocations serialise on a
mutex; each runs in its own `temporalDb.Update`. Idempotent — re-
seeding a range that already has TD is cheap (`if progress >= range
{ continue }`).

Net: time-to-tip for a fresh consumer drops by the time it takes to
download state-domain files (hours), because TD seeding fully overlaps
state downloading and is done by the time state arrives.

This step is post-(C)-stable: get the synchronous version (Step 3)
working end-to-end first, then optimize.

## What this is NOT

- Not a chaindata migration. Existing datadirs continue to work; the
  storage component owns the post-download steps that previously ran
  inside OtterSync but on the same data.
- Not a public API change for consumers other than OtterSync. Stages
  2-6 keep using `rawdb.ReadTd` etc. unchanged; the data they read is
  the same, just populated by a different actor.
- Not a Caplin change. Caplin keeps subscribing to `BlockHeadersReady`
  and reading TD via `rawdb.ReadTd`. The fix is on the producer side
  (who populates TD), not the consumer side.

## Risk

Concentrating the post-download pipeline in storage means the storage
component owns more correctness-critical work. Failure-mode hardening:

- Each step (open, seed) must be idempotent — re-runnable on a partial
  failure.
- The integrated signal must hold even if InitialStateReady has fired
  before from a prior session — i.e. on restart, if the work has
  already been done, the storage component still fires the signal
  (idempotent emission).
- Fail-loud canaries: assert TD is queryable for snapshot.tip before
  firing InitialStateReady; assert OpenFolder succeeded; assert all
  phase-1 files in inventory are at LifecycleIndexed or beyond.

## Open question

`FillDBFromSnapshots` currently lives in `execution/stagedsync/rawdbreset/`.
The storage component shouldn't directly import staged-sync. Either:
- Move the function into a neutral package (e.g. `db/rawdb` or
  `db/snapshotsync`) that both can import.
- Or wire it as a callback (like `cfg.afterDownload`): storage calls
  out to a function provided at construction.

Callback shape is the lightweight option and matches existing patterns
(manifestSelfCheck, manifestSigner already use this).

## Verification (after migration)

A fresh-datadir `--prune.mode=minimal` publisher launched against
mainnet must:

1. Download snapshots + state files
2. Reach `LifecycleIndexed` for all phase-1 files
3. Storage runs OpenFolder + FillDBFromSnapshots + canary checks
4. `InitialStateReady` fires exactly once, AFTER step 3 succeeds
5. OtterSync wakes, returns DONE
6. Stages 2-6 run; exec ingests Caplin's FCUs; tip advances cleanly
7. No "parent's total difficulty not found", no `lstat`-on-missing-`.idx`

Time-to-first-exec-block after `InitialStateReady` should be the
straight-line cost of stages 2-6, with no wait-and-retry latency.
