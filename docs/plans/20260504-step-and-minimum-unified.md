# Step and minimum — unified model

Three planning streams have been talking about file groupings using
overlapping but slightly-different vocabulary. This doc nails the
terms down so the code lands one set of types, one grouping
algorithm, one definition of "minimum", and the same semantics
across:

  - Phase 0 download ordering ([20260502-min-time-to-tip-target.md](20260502-min-time-to-tip-target.md))
  - Time-to-tip readiness ("minimum-viable download for clean
    start", same doc)
  - Step-batch validation ([20260504-validation-flow.md](20260504-validation-flow.md))
  - Step-minimum publication (publisher advertises minimum subset
    of a step first; same doc)

If these end up with separate type definitions and grouping
functions, the codebase grows scenarios that drift. One model,
applied consistently, keeps the surface small.

## Core concepts

### Step

A **step** is the natural unit of files produced by one retire /
merge cycle: a coherent set of files covering one half-open
`[FromStep, ToStep)` range, optionally scoped to a `Domain`.

  - **Block step:** all files for one block range —
    `headers.seg`, `bodies.seg`, `transactions.seg` plus their
    index files (`headers.idx`, `transactions.idx`,
    `transactions.efi`). `Domain` is empty.
  - **State step (per Domain):** all files for one state range of
    one domain — `accounts.kv`, `accounts.kvi`, `accountsHistory.v`,
    `accountsHistory.ef`, `accountsHistory.efi`. `Domain` is the
    domain identifier.

**Step key** is `(FromStep, ToStep, Domain)`. Files with the same
step key are step-siblings. The grouping algorithm is one function
on `FileEntry`: `e.StepKey()` returns the tuple.

Files that don't carry step semantics (caplin, meta, salt) have
zero `FromStep` and `ToStep` and are not subject to step-batch
operations — they're singletons.

### Minimum within a step

Within a step, files have roles. The **minimum** is the subset of
files that constitutes a publishable / usable unit on its own; the
**rest** are extras that complete the step.

| Step kind   | Minimum files                          | Extras                              |
|-------------|----------------------------------------|-------------------------------------|
| Block step  | `headers.seg`, `headers.idx`           | `bodies.seg`, `bodies.idx`, `transactions.seg`, `transactions.idx`, `transactions.efi` |
| State step  | `<domain>.kv`, `<domain>.kvi`          | `<domain>History.v`, `<domain>History.ef`, `<domain>History.efi` |

The minimum is "smallest publishable subset that consumers can
actually use." For block steps that's the headers + their hash
index — enough for a light client / header-only consumer. For
state steps that's the domain primary + accessor — enough for
current-state queries.

Files outside the minimum are usable but not standalone (bodies
without headers are useless; history without domain primary is
useless).

This mapping is **the** definition of minimum, applied uniformly:

  - `FileEntry.IsMinimum()` returns the boolean.
  - `StepGroup.Minimum()` returns the minimum subset.
  - `StepGroup.Extras()` returns the rest.
  - One implementation; no per-feature variations.

### Minimum-viable download set (network-level)

The **minimum-viable download set** is the cross-step minimum:
across all steps the consumer needs, it's the union of each step's
minimum subset, restricted to recent steps (the latest-step slice
the consumer needs to operate at tip).

  - Block: latest-N-step `headers.seg` + their indexes.
  - State: latest-step domain primaries + accessors for every
    domain.

This is what the time-to-tip target measures: how fast the
consumer can have the minimum-viable download set locally + indexed
+ at-tip.

The same `IsMinimum()` flag drives this: the downloader's Phase 0
ordering picks files where `IsMinimum()` is true AND the file
belongs to a recent step. Phase 1 picks everything else.

## Where each existing concept maps

### Phase 0 download ordering

Phase 0 is **trigger + order**, not a gating set
(per [20260502-min-time-to-tip-target.md](20260502-min-time-to-tip-target.md)).

  - **Order:** download minimum-set files before extras; within
    minimum, latest steps before older.
  - **Trigger:** download starts as soon as chain.toml is
    discovered; nothing waits for a "minimum complete" signal.
  - **Implementation:** sort the download list by
    `(IsMinimum desc, ToStep desc, Name)`. No filtering — every
    file goes into the queue, just in priority order.

### Time-to-tip readiness

A node is "at tip" when:

  - Latest minimum-viable download set is locally Indexed (every
    minimum file at LifecycleIndexed or above).
  - Caplin has caught up to the beacon head.
  - Stage execution has reached the latest finalised block.

Validation isn't part of this gate — minimum files at Indexed are
sufficient to start operating, even if the step's batch validation
hasn't run yet.

### Step-batch validation

Batch validation runs **per step**, with two passes:

  1. When the step's minimum subset is fully Indexed → run batch
     chain across the minimum → on pass, advance minimum files to
     Advertisable.
  2. When the step's extras are also Indexed → run batch chain
     across the extras → on pass, advance extras to Advertisable.

The same batch chain runs for both passes; the only difference is
the file subset it operates on. For the initial implementation
both passes happen at the same hook point — when the WHOLE step
is Indexed, run validation across the whole step. The two-pass
split lands as a follow-up once Phase 0 ordering reliably delivers
minimum-first (otherwise minimum + extras arrive together and the
two passes collapse).

### Step-minimum publication (availability)

Publisher's chain.toml entries are gated on Advertisable. With
two-pass validation, minimum files become Advertisable first → get
published first → consumers see them in chain.toml before the
extras are published. Net effect: minimum-set availability is
optimised.

This falls out of the per-step batch model + the minimum/extras
split; no new mechanism needed.

## Data model

```go
// In node/components/storage/snapshot:

// StepKey identifies a step group: files with the same StepKey
// are step-siblings.
type StepKey struct {
    FromStep, ToStep uint64
    Domain           Domain
}

// IsMinimum reports whether this file is part of its step's
// minimum publishable subset. The mapping is fixed by file kind +
// name pattern (see "Minimum within a step" table).
func (f *FileEntry) IsMinimum() bool { … }

// StepKey returns the file's step group identifier. Zero step
// (FromStep=0, ToStep=0) for non-stepped files (caplin, meta, salt).
func (f *FileEntry) StepKey() StepKey { … }

// On Inventory:

// StepGroup is the set of FileEntries that share a step key.
type StepGroup struct {
    Key   StepKey
    Files []*FileEntry
}

// Minimum returns the entries marked IsMinimum.
func (g StepGroup) Minimum() []*FileEntry { … }

// Extras returns the entries NOT marked IsMinimum.
func (g StepGroup) Extras() []*FileEntry { … }

// FilesAtStep returns the StepGroup for a key.
func (i *Inventory) FilesAtStep(key StepKey) StepGroup { … }

// AdvanceStep atomically transitions every file in the group to
// the target lifecycle state. Returns the names that actually
// changed state (idempotent).
func (i *Inventory) AdvanceStep(key StepKey, to LifecycleState) []string { … }
```

One set of types. Every consumer (downloader ordering, lifecycle
batch hook, publisher gate) uses these.

## Sequencing

  1. **Land the data model.** `StepKey`, `IsMinimum`,
     `StepGroup`, `FilesAtStep`, `AdvanceStep`. Tests for grouping
     + minimum classification.
  2. **Wire the lifecycle batch hook.** Driver dispatches per-file
     at LifecycleIndexed, but the per-file handler delegates to
     a step-grouping batch logic that:
       - Returns nil (wait) if the step's minimum isn't fully
         Indexed.
       - Runs the batch chain across the minimum + extras (single
         pass for now).
       - Calls `AdvanceStep(key, LifecycleAdvertisable)`.
  3. **Default batch validator: `AllFilesPresent`.** Stat-checks
     every file in the group is openable.
  4. **Phase 0 ordering uses `IsMinimum`.** Updates the
     downloader's sort to prioritise minimum + latest-step.
  5. **Two-pass split** (follow-up): minimum advances at one hook
     fire, extras at a later one — independent transitions.
  6. **Tier 2 / Tier 3 batch validators** (opt-in flags, per
     [20260504-validation-flow.md](20260504-validation-flow.md)).

Items 1–4 land in this PR; 5–6 follow.

## Block-to-step unit conversion (staged)

The current naming has `FileEntry.FromStep` / `ToStep` overloaded:

  - State files: literal step numbers (e.g. 0, 256, 512).
  - Block files: block numbers (multiplied by 1000 by
    `snaptype.ParseFileName` for the `<from>-<to>-<type>` pattern).

For sibling grouping within a step, this works — siblings share the
parsed numbers. For cross-kind comparisons (block step vs state
step), the units don't agree.

The proper conversion uses the **commitment file as the source of
truth for block ↔ step**:

  - Each commitment domain file at step `[N, M)` records the
    state at the END of step `M`.
  - The state at that point includes the canonical block number.
  - Reading the commitment file's last block number yields a
    concrete `(step, block)` pair.
  - Multiple commitment files give multiple boundary pairs; block
    ranges interpolate.

This is a legitimate batch-validation operation: it's exactly the
moment we have the commitment file open with full read context.

### Stage 1 (this PR)

`snapshot.PopulateFromName` derives `FromStep` / `ToStep` / `Domain`
/ `Kind` from a file's name via `snaptype.ParseFileName`. Block
files end up in block-units, state files in step-units. Mixed but
sufficient for sibling grouping (the batch hook only needs same
key for siblings, not cross-kind unit alignment). Wires into
`discoverNewFiles` (lifecycle driver), bootstrap (provider), and
populate.go (LiveInventory).

Validates the end-to-end per-step batch flow on hoodi.

### Stage 2 (follow-up)

`CommitmentBlockBinding` batch validator: when a commitment domain
step batch validates, it opens `commitment.kv` and reads the block
number for the step's end. Stores the boundary in the inventory
(`Inventory.RegisterStepBlockBoundary(step, block)`).

`Inventory.BlockToStep(block) (step, ok)` answers the conversion
query. `PopulateFromName` for block files calls this and rewrites
`FromStep` / `ToStep` to canonical step units. After this lands,
all `FileEntry` step fields are in one unit and cross-kind
operations are meaningful.

Bootstrapping: until at least one commitment-step batch has
validated, block files use block-unit fallback. This is acceptable
because cross-kind operations don't fire during initial bootstrap
(the consumer is downloading files, not comparing them).

  - A separate "minimum-set" type defined in the time-to-tip plan
    that doesn't share the inventory model.
  - The lifecycle driver having its own grouping logic that
    differs from the downloader's ordering logic.
  - Any "step" definition that doesn't reduce to
    `(FromStep, ToStep, Domain)`.
  - `IsMinimum` having multiple definitions in different
    subsystems.

If a future feature needs a different grouping (e.g. cross-step
validators), it's an explicit extension on top of `StepKey` /
`IsMinimum` — not a parallel taxonomy.
