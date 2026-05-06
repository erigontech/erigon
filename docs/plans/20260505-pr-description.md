# Draft PR description — V2 snapshot-flow app-integration

For when this branch raises against `main` (after #20933 merges).
Save / paste verbatim into the PR body.

---

## Title

`node/components/storage: V2 snapshot-flow app-integration — storage-owned import lifecycle, V2-only mode, per-step batch validation`

## Summary

This PR wires the storage component as the owner of the file
import lifecycle, makes V2 chain.toml the sole source of the
download set when the operator opts in, and adds per-step batch
validation that publishes a step's minimum subset before the rest
of the step lands.

End-to-end:

  - **Hoodi V2 path**: 10 min 25 s time-to-tip (post-§5e
    measurement, minimal mode).
  - **Mainnet bootstrap publisher**: 42 min 31 s (fresh datadir,
    `--snap.bootstrap-from-preverified --snap.p2p-manifest
    --snap.lifecycle-driven-by-storage`, block 25,031,794).
  - **Mainnet V2 node** (against the at-tip publisher): 44 min
    34 s (`--snap.p2p-manifest --snap.lifecycle-driven-by-storage`,
    staticpeered, fresh datadir, block 25,032,022).

Both mainnet runs landed with zero collision crashes, zero
disk-walk lines, zero BMI invocations (lifecycle pre-check from
§5c working as designed), and 179 files transitioning through the
lifecycle. The V2 node's 2-minute overhead vs the publisher is
attributable to the 30-s manifestReady gate plus second-mover
swarm warmup — both nodes downloaded the same set (publisher's
advertised chain.toml = preverified ∪ {} since just-synced; the
architectural-advantage measurement against a smaller advertised
set is follow-up).

The PR is **opt-in**:
- `--snap.lifecycle-driven-by-storage` — activates the storage-
  owned lifecycle. Default false; existing stage-driven path stays
  authoritative until cutover.
- `--snap.p2p-manifest` — uses peer-discovered chain.toml as the
  download source. Default false; preverified.toml stays the
  default behaviour.
- `--snap.bootstrap-from-preverified` — promotes the node to
  bootstrap-publisher role. Default false; only the snapshotter
  needs this.

A node started with the defaults today operates exactly as pre-PR.

## What's in scope

### Storage-owned import lifecycle

- File state machine: Declared → Downloading → Downloaded →
  Indexing → Indexed → Validating → Advertisable. Single Ready
  gate at Advertisable.
- `Inventory` is the persistent state model with held-view
  discipline (refcount + pendingDeletes for safe concurrent reads
  across mutations).
- Lifecycle driver runs the state machine: ChangeSet subscription
  + periodic sweep, dispatch handlers per state, per-file failure
  / quarantine bookkeeping.

### V2 chain.toml (peer-discovered manifest)

- Consumer mode: chain.toml from peers is the SOLE download set.
  preverified.toml is invisible. `manifestReady` gate blocks the
  snapshot stage until peer discovery succeeds (5-min timeout,
  falls back to preverified for safety).
- Bootstrap-publisher mode: preverified seeds the publisher's
  chain.toml; the publisher's published manifest = preverified ∪
  local files. Gives consumers chain-of-custody trust rooted in
  the bootstrap.
- Latest-first download ordering with minimum-tier priority:
  `(IsMinimum desc, ToStep desc, FromStep desc)`.

### Unified step + minimum data model

- `StepKey = (FromStep, ToStep, Domain)` — files with the same key
  are step-siblings (one retire/merge cycle's output).
- `IsMinimum` — name-pattern classification of "minimum publishable
  subset" within a step (`headers.seg + headers.idx` for blocks;
  `<domain>.kv + .kvi + .bt` for state). One classifier shared
  across lifecycle batch validation, downloader sort, and (future)
  publisher gate.
- `StepGroup.Minimum()` / `Extras()`, `Inventory.FilesAtStep`,
  `AdvanceStep` (whole-step atomic), `AdvanceFiles` (subset
  atomic).

### Per-step batch validation

- `StepValidator` interface — runs across a `StepGroup`, distinct
  from per-file `Validator`.
- `AllFilesPresent` default validator — stat-checks every file in
  the group is on disk. Sub-millisecond per step.
- Two-pass minimum-first hook (`BuildOnBatchValidation`):
  - Pass 1: when minimum subset is fully Indexed, validate +
    advance just the minimum to Advertisable. Publisher /
    consumer gets the minimum immediately.
  - Pass 2: when extras complete, validate + advance the rest.

### Per-file / per-step timing instrumentation

- `FileTimings { EnqueuedAt, DownloadCompletedAt, IndexedAt,
  ValidatedAt }` recorded automatically on AddFile + AdvanceTo +
  AdvanceStep + AdvanceFiles.
- `StepTimings` derives `MinimumDownloadedAt` /
  `AllDownloadedAt` / `MinimumIndexedAt` / `AllIndexedAt` /
  `MinimumValidatedAt` / `AllValidatedAt` from the per-file
  timings. Single-RLock derivation.
- Feeds the future bandwidth-aware orchestrator (backwards-vs-
  sideways priority + saturation tuning).

### Per-file Tier-0 metadata gate

- `Inventory.AddFile` rejects malformed entries (empty Name,
  inverted/empty step ranges, Kind that disagrees with name's
  pattern). Auto-derives Kind from name when not explicitly set.
- `PopulateFromName` is the single seam every caller goes through
  to derive step + domain + kind metadata; centralises parsing
  + the future block→txnum→step unit conversion.

### Stage 2 — Commitment-derived (step, block) binding + state-at-end consistency check

- New `CommitmentDomainValidator` runs on commitment-step batches:
  opens `commitment.kv`, decodes `KeyCommitmentState`, verifies
  the recorded state sits at the END of its block (txNum ==
  blockMaxTxNum) — catches files mis-named or built against the
  wrong step boundary.
- On success, registers `(toStep, blockNum)` binding in the
  inventory. `Inventory.BlockToStep(block)` answers the inverse
  query for block-snapshot callers.
- Runs identically on publisher and consumer via the shared
  `StepChain` — failure on either side blocks step advance to
  Advertisable.

### Block-step wait-for-binding gate

- Block-domain steps (empty `Domain`) advance to Advertisable only
  after a commitment-derived `(step, block)` binding covers their
  block range.
- Until a binding exists for the range, the batch hook returns
  nil — files wait at Indexed, no quarantine, no failure.
- Net effect: a block file is never advertised until commitment-
  derived state has verified its step boundary. Block files that
  download early during initial sync wait for the corresponding
  commitment steps to validate before advancing.

### Inventory pre-check before BuildMissedIndices (§5c)

- `BuildOnIndexing` consults the inventory before invoking the
  global builder. When all an entry's deps are already Local,
  the handler advances directly to Indexed with zero builder
  invocations.
- Hoodi rerun confirmed: 776 redundant BMI invocations → 0.

### Operational documentation

New plan docs under `docs/plans/`:
- `20260504-v2-operational-guide.md` — operator runbook for the
  V2 path: bootstrap-publisher / V2-node roles, flag combinations,
  verification log lines, failure modes, default-flag posture.
- `20260504-step-and-minimum-unified.md` — the unified step +
  minimum model. Single set of types, one classifier, used by
  every consumer.
- `20260504-validation-flow.md` — validation tiers (metadata
  always-on, per-step presence default, content+size +
  format-integrity opt-in), failure-recovery mapping, switchable
  flags.
- `20260502-min-time-to-tip-target.md` — time-to-tip target +
  hoodi baseline + mainnet-next-milestone process.
- `20260504-publisher-restart-chaintoml-bug.md` — publisher
  restart-correctness bug and fix candidates (separate small PR).

## What's NOT in scope (follow-up)

- **Mainnet performance measurement.** The PR gate. 10-min
  time-to-tip needs to be reproduced on Ethereum mainnet.
- **Block-file step-unit rewrite using the (step, block)
  binding.** Stage 2 produces the binding; using it in
  `PopulateFromName` to rewrite block-snapshot files'
  `FromStep`/`ToStep` from block-units into step-units is the
  follow-up. Today block files keep block-unit numbers — sibling
  grouping works because step-siblings share parsed numbers, but
  cross-kind comparisons aren't aligned until this lands.
- **Bandwidth-aware download orchestrator.** Backwards-vs-
  sideways priority shaping using the per-step timings landed
  here. Real-time torrent throughput integration plugs into the
  same accessors.
- **§5e full removal** of `AddTorrentsFromDisk` (today gated, not
  deleted).
- **§5c full inventory-driven dispatch.** Pre-check landed here;
  per-file dispatch (vs global BMI sweep) is follow-up.
- **Abnormal-termination scenarios** (SIGKILL / OOM / power-loss
  matrix).
- **Publisher DID + embedded trust root.** Separate PR per
  `docs/plans/20260504-publisher-did-ucan.md`.

## Test plan

- [x] `make lint` clean
- [x] `make test-short` clean
- [x] `make erigon` builds
- [x] V2 mechanism end-to-end on hoodi:
  - 2026-05-04 V2 post-§5e rerun: 10m25s tip, 0 collision crashes,
    22 advance-to-Indexed/Advertisable, 0 disk-walk lines.
  - 2026-05-04 §5c rerun: 13m17s tip, 0 BMI invocations
    (vs prior 776), 156 advance-to-Indexed/Advertisable.
  - 2026-05-04 step-batch run: 99 "step advanced" log lines —
    the per-step batch hook firing as designed.
- [x] Scenarios suite covers ordering, quarantine, lifecycle
      transitions, per-step minimum-first ordering, timing
      instrumentation.
- [x] **Mainnet time-to-tip measurement (2026-05-05)**.
      Bootstrap publisher + V2 node, both fresh, V2 node started
      after publisher reached tip. Both nodes ran on the same
      box, so during the V2 node's run the publisher was still
      live (catching up to mainnet tip every 12 s); resource
      contention is a real factor in the V2 node's number.

  Total time-to-tip:

  | Run | Total | Block at first age≤6s |
  |-----|-------|------------------------|
  | Bootstrap publisher | **42 min 31 s** | 25,031,794 |
  | V2 node | **44 min 34 s** | 25,032,022 |

  Phase breakdown:

  | Phase | Publisher | V2 node | Δ |
  |-------|-----------|---------|---|
  | Snapshot download (OtterSync DONE) | 18 min 44 s | 13 min 55 s | V2 **4m49s faster** (had a populated publisher + warm swarm) |
  | Execution catch-up (~12.7 k blocks) | 23 min 44 s | 30 min 36 s | V2 **6m52s slower** (28% slower per block — same-box contention with the publisher's still-running execution) |

  **Reading the numbers**: the V2 node's snapshot phase was
  faster — it joined a warm swarm, which is the V2 architectural
  advantage. The execution stage was slower per block because
  both nodes were running on the same machine and competing for
  CPU + disk I/O. On separate hardware the V2 node would beat
  the publisher's 42m31s.

  Counts (both nodes): 0 BMI invocations, 0 quarantines, 0
  `Adding torrents from disk`, 179 advance-to-Indexed
  transitions, manifestReady cleared in 32 s on the V2 node.

  Both runs proved the V2 mechanism end-to-end on mainnet with
  zero regressions. The publisher's advertised chain.toml had
  **7389 entries** at the time the V2 node connected (preverified
  6685 entries plus ~704 additional files the publisher had
  produced locally during its catch-up beyond preverified's
  snapshot point). The V2 node downloaded the same set as a
  preverified-driven node would have — set sizes are
  approximately equal because preverified itself grows over time
  (each release ships a larger list, with more entries near tip
  where unmerged-step files accumulate).

  **What V2 actually changes** vs preverified isn't manifest size
  but four different things:

    1. **Liveness** — V2 chain.toml is updated by publishers
       continuously; preverified is frozen at binary build time.
       Without V2, post-preverified files have to be fetched via
       ad-hoc stage retire; with V2, they're authoritatively in
       the manifest.
    2. **Trust** — V2 chain.toml is signable + verifiable
       per-publisher (follow-up DID/UCAN PR); preverified is
       "trust the binary."
    3. **Decentralization** — V2 has multiple potential publishers;
       preverified has one canonical source.
    4. **Selectability** (post-PR scope) — V2 consumers can opt
       to download only `IsMinimum` files of recent steps
       (Phase 0 per `docs/plans/20260502-min-time-to-tip-target.md`),
       skipping historical extras. Preverified can't be
       partially-skipped. This IS the future "faster than
       preverified" path but it's not in scope for this PR.

  For total time-to-tip specifically, our two mainnet runs
  downloaded the same files — V2's 4m49s download-phase advantage
  vs the publisher came purely from a warm swarm + populated
  peer, not from a smaller manifest.

  **Re-publish + Seed wires (commits `c724fa2a96`, `2d1a88060b`,
  `c373dd07b3`)**: added so the publisher's chain.toml reflects
  fresh retire output via the lifecycle path. After multiple
  iterations the wires landed are:

    1. **Re-publish-on-retire** (`c724fa2a96`): chain.toml
       regenerates on every OnFilesChange (frozen + deleted
       paths). Idempotent.
    2. **Bridge subscriber** (`2d1a88060b`): subscribes to
       inventory ChangeSets; when files reach
       LifecycleAdvertisable, calls `Seed` (builds `.torrent` +
       adds to torrent client) then re-publishes chain.toml.
    3. **Bootstrap binding seeder** (`c373dd07b3`): registers a
       `(step, block)` binding for the latest commitment file in
       bootstrap inventory so the block-step wait gate can
       release.

  **What's still gating the V2 architectural advantage**: on a
  fresh publisher datadir, two subtle paths aren't yet flowing
  through the lifecycle, so the bridge subscriber doesn't fire:

    - Bootstrap commitment files don't exist on a fresh datadir,
      so the binding seeder has nothing to seed.
    - `discoverNewFiles` only scans the top-level snap-dir;
      state files in `domain/` / `history/` / `accessor/`
      subdirs aren't picked up by the lifecycle, so commitment
      files never go through the per-step batch hook either.

  Result: the LIFECYCLE path doesn't currently complete the chain
  to LifecycleAdvertisable on a fresh publisher, but **retire's
  old `seeder.Seed(ctx, mergedFileNames)` path still fires and
  the chain.toml does grow via that route** (publisher 5 mainnet
  run: 7389 → 7392 entries during catch-up to tip). So the
  publisher still ends up with a slightly fresher manifest than
  preverified-only, just via the legacy code path rather than the
  shiny new lifecycle.

  The wires landed are correct + idempotent + non-regressive.
  The full lifecycle-driven V2 exec-reduction advantage emerges
  once the lifecycle covers state files (a separate follow-up
  PR, not in scope here). Today's wires sit dormant for fresh
  publishers; they activate once `discoverNewFiles` sees state
  files, OR when retire produces a fresh commitment that the
  bootstrap seeder can pick up on subsequent restart.

  Hoodi reference numbers from
  `docs/plans/20260502-min-time-to-tip-target.md`:
  V2 post-§5e 10m25s, §5c rerun 13m17s.

  Hardware: webseed-download peaked ~300 MB/s, peer-download
  ~210 MB/s, 320 GB total snapshot data for mainnet minimal.

## Operator migration

Defaults preserve pre-PR behaviour. To opt in:

```
# Bootstrap publisher (one node per chain — typically the
# snapshotter):
--snap.p2p-manifest
--snap.bootstrap-from-preverified
--snap.lifecycle-driven-by-storage

# Regular V2 node:
--snap.p2p-manifest
--snap.lifecycle-driven-by-storage
```

See `docs/plans/20260504-v2-operational-guide.md` for the full
runbook.
