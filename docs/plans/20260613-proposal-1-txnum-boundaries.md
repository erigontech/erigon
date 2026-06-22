# Proposal 1 — txNum file boundaries (functional)

**Status**: draft, iteration-2 work item. Must be resolved before
production adoption of the experimental snapshot-flow + mode-B
unwind implementation.

**Prerequisite for**: Proposal 2 (content-addressed filenames),
Proposal 3 (chain definition / transports split).

## TL;DR

Today snapshot files are bounded by *step indices* — a derived
quantity that's a function of `txNum / stepSize`. This proposal
replaces step indices with raw `txNum` boundaries on the file's
canonical surface: the filename, the manifest, the aggregator's
internal addressing, the consensus-relevant identity of a file's
range.

The change is *functional* — it changes what the file *means*, not
just what it's called. The current "step index" is implicit and
derived; `txNum` is the canonical aggregator coordinate. Conflating
them works as long as `stepSize` stays constant, but the conflation
is conceptually wrong and locks in operational behaviour that
shouldn't be load-bearing.

This proposal lays out the motivation, the semantic equivalence
between the two encodings, the migration path, and the open
questions reviewers need to resolve before implementation can start.

## Background — what step indices are today

In the current implementation:

- The aggregator's unit of work is the `txNum` — every committed
  transaction (including system txs) increments `txNum`.
- Snapshot state files are named by *step*: `v2.0-accounts.0-1.kv`
  covers `[step 0, step 1) == [txNum 0, txNum stepSize)`.
- `stepSize` is a chain-config constant (currently 390625 on
  hoodi, larger on mainnet) chosen so a 1k-block aligned cut
  corresponds to a single step on average.
- The mapping is `step = txNum / stepSize` (integer division), so
  a step name implicitly commits to a particular `stepSize`.

Aggregator internals (RecomputeAtTxNumWithoutSD, boundary-step file
regeneration, the writable-shadow's diff-replay) all operate in
`txNum` terms. The step encoding is purely how the surface (names,
manifest entries, log lines) presents that work.

## Motivation — why this isn't just a rename

### 1. `stepSize` is a chain-config constant that *shouldn't be*

Today the chain-config picks `stepSize` once and freezes it. Files
produced before the constant changes can never be re-aligned without
re-encoding their names. Changing `stepSize` to optimize for newer
hardware, denser blocks, or different prune strategies requires a
hard schema flag day.

With `txNum` boundaries the constant disappears from the file
surface. The aggregator can change its internal `stepSize` (the
collation cadence) without touching any file's identity. Files'
canonical names continue to commit to `txNum` boundaries; the
internal step machinery becomes a pure performance/cadence choice.

### 2. Step boundaries lie about cut points

A "step boundary" is a coarse alignment that *usually* falls between
block boundaries but doesn't have to. The current unwind path
handles mid-step cuts (see WipeWritableShadowPast) precisely because
the step boundary != block-boundary equivalence is leaky. By
exposing `txNum` on the surface, the equivalence becomes explicit:
"the file covers `[txNum_A, txNum_B)`," and the boundary-step
diff-replay disappears from being a special case.

### 3. Cross-network comparison is impossible today

`step 273` on hoodi and `step 273` on mainnet refer to wildly
different ranges (different `stepSize`, different chain ages). A
filename or a manifest entry that names a step encodes a
chain-config-dependent meaning that's only legible *with* the chain
config. `txNum` is chain-config-independent within a chain; a file's
identity becomes self-describing.

### 4. It's a prerequisite for content-addressed identity (Proposal 2)

Proposal 2 makes a file's identity its content hash. Content hashes
have to be over a canonical byte sequence including the metadata.
Step indices are derived metadata, not canonical. Migrating to
content-addressed identity *while* keeping step encoding means the
hash commits to the derived value, locking in `stepSize`-dependence
of the identity. `txNum` boundaries make the metadata canonical so
the content hash commits to facts about the file, not facts about
the chain config that produced it.

## Semantic equivalence — what stays the same

For any well-formed snapshot file produced with `stepSize = S`:

```
old name:  v2.0-accounts.fromStep-toStep.kv      with fromStep,toStep ∈ ℕ
new name:  v2.0-accounts.<fromTxNum>-<toTxNum>.kv with fromTxNum = fromStep × S,
                                                       toTxNum   = toStep   × S
```

The bytes inside the file are unchanged. The coverage range
`[fromTxNum, toTxNum)` is the same. The accessor (`.bt`, `.kvi`)
points at the same offsets. Reading the file is byte-identical
between the two encodings; only the name changes.

The aggregator's collation cadence (the choice of `stepSize`) is
demoted from a chain-config constant to an internal scheduling
parameter — files can be produced at any `txNum`-aligned cadence
without consumers needing to know which cadence the publisher chose.

## Proposed grammar

```
old:  v<format-version>-<domain>.<fromStep>-<toStep>.<kind>
new:  v<format-version>-<domain>.<fromTxNum>-<toTxNum>.<kind>
```

- `format-version` — unchanged (`v2.0` for state files, `v1.1` for
  block snapshots).
- `domain` — unchanged (`accounts`, `storage`, `code`, `commitment`,
  `receipts`, ...).
- `fromTxNum` / `toTxNum` — base-10 in this proposal; encoding choice
  is Proposal 2's surface concern (see §"Encoding deferred").
- `kind` — unchanged (`kv`, `bt`, `kvi`, `kvei`).

The `.<fromTxNum>-<toTxNum>.` infix replaces `.<fromStep>-<toStep>.`
1:1. No reordering, no new fields, no field removal.

## Encoding deferred — base-10 here, base-N in Proposal 2

This proposal keeps `txNum` in base-10 to keep the semantic change
visible and independent of cosmetic re-encoding. Proposal 2's
field-order / hash-in-name work may also re-encode `txNum` as
base64-url for compactness; that's a layered change against this
proposal's semantic baseline, not bundled into it.

## Migration path — three options

### Option A — flag day with conversion utility

On upgrade, the node walks the snapshots dir and renames every file
from `<fromStep>-<toStep>` to the equivalent `<fromTxNum>-<toTxNum>`.
The rename is in-place (atomic via `os.Rename`). The `.torrent` files
are regenerated for the new names; old `.torrent`s are deleted.

**Pros**: clean break, no dual-encoding code path lingers.
**Cons**: an upgrade-time forced rename of every snapshot file. For
a publisher node with thousands of files this is non-trivial; for a
consumer node the .torrent regeneration may not be safe to do without
re-attesting the file. Coordination with peers serving the old names
needs to be deliberate.

### Option B — dual-encoding read, single-encoding write

Read path accepts both old (`fromStep-toStep`) and new
(`fromTxNum-toTxNum`) names. Write path always emits the new form.
Over time (next merge cycle), old files get superseded and the dual
path can be removed.

**Pros**: no forced flag day. Existing files keep working.
**Cons**: dual-encoding code path persists until the merge cadence
removes all old files. For mainnet's 100k-chunk cadence this could
be months. The dual path is a maintenance tax during the transition.

### Option C — manifest-level mapping table

Files on disk keep their existing names. A new manifest entry
(`txnum-aliases.toml`) maps each step-named file to its
`(fromTxNum, toTxNum)`. The aggregator and unwind paths consult the
alias table when interpreting names; new files are produced with
txnum names directly.

**Pros**: zero existing-file disturbance. Lowest operational risk.
**Cons**: introduces a separate canonical-mapping artifact. The
aliasing layer is itself state to maintain and validate. It also
*doesn't fully solve* the motivation: file identity is still step-
based on disk; the alias table only fixes the manifest's reading.

**Recommendation**: **Option B**. The dual-encoding read tax is
small (one parser variant) and the upgrade cost is zero; the
write-side is purely additive. Option A is too operationally
painful; Option C doesn't actually settle the canonical question.

## Open questions

### 1. Block snapshot files (`v1.1-*`)

This proposal targets state files (`v2.0-*`). Block snapshots are
already named by raw block numbers (`v1.1-002900-003000-headers.seg`),
so the conceptual issue ("derived index in the surface") doesn't
apply to them. **Recommendation**: leave block snapshots unchanged.

### 2. Mainnet's existing pre-existing files

Mainnet publishers have *years* of step-named files in preverified
manifests. Forcing a rename or even a parallel-encoding ingestion on
those is the migration's biggest pain point. **Suggested answer**:
preverified files retain their original step-named identity
indefinitely; new files post-upgrade use `txNum` names. The dual
read path of Option B handles this naturally.

### 3. Manifest schema impact

`chain.toml` and `preverified.toml` list names verbatim. With
Option B, consumers parse both forms; the manifest's grammar gets a
new alternative production. With Option A, manifests get rewritten
at upgrade time. With Option C, manifests gain a new section.
**Decision needed alongside Option B**: parser is forgiving; both
forms produce the same `(domain, fromTxNum, toTxNum)` triple
internally.

### 4. Test fixtures

Existing test fixtures with step names need to keep working through
the migration window. Per Option B's dual read, this is automatic;
no fixture changes needed for the read path. Write-path tests need
new fixtures expressing the new grammar.

### 5. Coordination with the aggregator's internal `stepSize`

If `stepSize` is no longer chain-config-pinned, then choosing it
becomes a publisher-side decision. Do all publishers on a network
need to agree on `stepSize`? Probably no — consumers can read files
regardless of the publisher's chosen cadence — but this needs
explicit confirmation against the aggregator's collation /
boundary-step semantics.

## What this proposal does NOT change

- File contents (bytes-on-disk are identical).
- Accessor (`.bt`, `.kvi`) formats.
- The unwind/setHead semantics or the mode-A/mode-B split.
- Consensus state commitments (Keccak state roots are untouched).
- `chain.toml` and `preverified.toml` schemas beyond the file-name
  grammar above.
- Step indices as an *internal* aggregator concept — collation
  cadence still organizes work in steps internally; only the
  *surface* (filename, manifest entry) changes.

## Recommendation

**Adopt Option B**: dual-encoding read, single-encoding write,
applied to state files (`v2.0-*`) only. Block snapshots (`v1.1-*`)
out of scope.

This is the minimum-disruption path that settles the canonical
question of what a file's range actually means without forcing an
upgrade-time data migration.

## Sequencing

This proposal is a prerequisite for Proposal 2 (content-addressed
filenames). Proposal 2's hash-in-name commits to file metadata; that
metadata must be canonical before the hash makes sense. Proposal 2's
implementation can begin only after this proposal is accepted and
its write path is in production.

Proposal 3 (chain definition / transports split) is downstream of
both; it doesn't directly depend on this proposal, but the split's
"chain definition" artifact is much simpler once filenames carry
their canonical metadata directly.
