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

End-to-end on hoodi: 10 min 25 s time-to-tip in minimal mode (start
to first `head validated age=0`), zero collision crashes, zero
disk-walk lines, 22 files transitioning Indexed → Advertisable
through the lifecycle. Mainnet measurement is the next milestone.

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
- **Stage 2 — commitment-file block→step binding.** A batch
  validator that opens the commitment.kv during validation,
  reads `KeyCommitmentState` to extract the canonical block
  number for the step's end, and registers a verifiable
  `(step, block)` binding in the inventory.
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
- [ ] Mainnet time-to-tip measurement (next milestone, gates
      moving the PR out of draft).

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
