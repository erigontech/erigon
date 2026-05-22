# Canonical layer — revised spec

**Status**: agreed 2026-05-22. Folds three converging design concerns into one
corrected model for the snapshot-flow canonical layer.

**Revises**: the Layer 1 and Layer 2 sections of
`20260520-chaintoml-ucan-flow-spec.md`. That document remains the contract for
Layer 3 (per-node advertisement), the two-UCAN authentication, and the staged-
adoption mechanism; its Layer 1 / Layer 2 text is superseded by this one.

## Why this revision

Three concerns surfaced after the 2026-05-20 spec, and all three rewrite the
same `CanonicalView` properties — keying, monotonicity, and the version
counter. Patching the view three times against a shifting model would not
converge, so they are folded here:

1. **The canonical chain can go backwards.** A reorg deep enough to orphan
   blocks already retired into snapshots can happen. Retire is gated on a fixed
   depth heuristic, not finality; and finality itself is probabilistic — a
   catastrophic reorg *can* cross it. The 2026-05-20 spec's "the view only ever
   adds, Layer 1 never demotes" is therefore false.
2. **Reversioning.** A snapshot file-format version bump (`v1.0-…` → `v2.0-…`)
   renames every file and changes every info-hash. The canonical view, keyed by
   exact name, would treat the reversioned set as entirely missing and trigger a
   whole-archive re-download.
3. **Residual gaps** in the 2026-05-20 canonical model — the version counter is
   not globally consistent, and the spec describes signed checkpoints that were
   never built.

## 1. Corrected canonical model

Canonical is a **consumer-computed view**, not a published authority file —
unchanged from Phase 6, and still the right call (a published `canonical.toml`
re-introduces a single signer to compromise).

What changes: **the view is not monotonic.** It can *demote* (a rewind orphans
entries) and it holds *multiple variants per logical file* (reversioning). The
2026-05-20 "monotonic, never demotes" property is retired.

The view is a function of three inputs, not two:

```
canonical = f(verified peer advertisements, quorum, the node's own consensus view of the chain)
```

The third input is new and load-bearing — see §5. **Quorum decides which bytes
serve a block range; consensus decides which block ranges are canonical at
all.** Consensus is the higher authority: a quorum of advertisements can
*promote* an entry, but the node's consensus layer can *invalidate* it.

## 2. Canonical view keying — logical slots

The view is keyed by **logical slot**, not by exact file name:

```
slot = (kind, base, step-or-block range)        — version prefix stripped
```

`downloadertype.ParseFileName` already separates the version prefix from the
rest of the name, so the slot key is mechanically derivable.

Each slot holds a **set of canonical variants** — `slot → {(name, hash), …}`.
A slot with more than one variant is the normal state during a reversioning
window (a `v1.0` and a `v2.0` form of the same logical file, both format-valid).

**Slot satisfaction**: a node has no gap for slot `S` if it holds *any*
canonical variant of `S`. Holding `v1.0-X` while `v2.0-X` is also canonical is
*not* a gap — this is what stops a reversioning bump from triggering a
whole-archive re-download.

Merge transitions need no special case under slot keying: `X.0-1024`,
`X.1024-2048` and the merged `X.0-2048` are three *different slots* (different
ranges). They accumulate quorum independently and are all canonical
simultaneously; the consumer's existing coverage logic picks a covering set.

## 3. Promotion, demotion, and the three name/coverage cases

**Promotion** is unchanged from Phase 6: an entry is promoted into a slot's
variant set when observed in `≥ Q` distinct Authority-UCAN-verified
advertisements, `Q = max(Q_floor, ceil(F · N_authorised))`.

**The verdict classifier** (`CheckOwnAdvertisement`) must distinguish three
"different name / overlapping coverage" cases — today it only handles the
first:

- **Minority** — same slot, the node's variant hash differs from a
  quorum-promoted variant's hash → adopt the canonical hash (staged adoption,
  existing mechanism).
- **Merge** — a different slot whose range is the union of ranges the node
  holds → adopt the compaction (also existing staged adoption; it is just a
  different-range slot).
- **Reversion** — the *same* slot key, a *different version prefix*, both forms
  format-valid → **keep both, do not adopt.** Reversioning is driven by the
  binary's supported `FileVersion` set, not by quorum: a node fetches the
  `v2.0` variant only when its binary drops support for `v1.0`. Quorum's role is
  only to make the `v2.0` variant *available*.

**Demotion** is consensus-driven — see §5.

**Retention**: a superseded merge form is GC'd from the view once no verified
publisher has advertised it for a configured window (~24h). A superseded
*reversion* form is retained far longer — until no verified publisher
advertises it at all (release-cycle scale, months); the 24h merge window is far
too short for a format migration.

## 4. Canonical identity (replaces the version counter)

The 2026-05-20 spec's monotonic `v<N>` counter is **retired**. It was never
sound:

- It is observation-order dependent — `CanonicalView.version` increments once
  per `Observe` call that promotes, so two consumers seeing the same
  advertisements in different order or batching reach different `N` for
  identical canonical content. "Are you on canonical v5?" is not a shared
  vocabulary.
- Demotion (§5) and reversion (§2/§3) both break monotonicity outright.

Replace it with a **canonical digest**: a content hash over the sorted
`slot → {sorted variants}` map. Properties:

- **Globally consistent** — same canonical content yields the same digest on
  every node, regardless of observation history.
- **Identity, not order** — "are you on canonical `<digest>`?" is an equality
  check. There is no total order on canonical states (a rewind moves it
  "backward"), so no monotonic counter can exist; an identity is what is
  actually needed.
- Changes correctly on promotion, demotion, and reversion.

The staging-directory tag used by adoption (`adoption-<tag>`) is a *separate
local concern* — it needs only per-node uniqueness and can stay a local
counter or use a digest prefix. It is not the canonical identity.

### 4.1 Stream identity sits above the digest

The digest identifies canonical *content* — but only *within one stream*. Which
stream a canonical view belongs to is a separate, higher identity: the pair
`(forkid, lineage)`, where `forkid` is the EIP-2124 fork ID (consensus rules)
and `lineage` is the `[view]` branch point. This is specified in the
*Identification* section of `erigon-documents` →
`ethereum/design/erigon-archive/fork-spec.md`.

Consequences for this spec:

- The canonical view, its slots, and its digest are all per-`(forkid, lineage)`.
  Two streams never share a canonical view; their digests are not comparable.
- `forkid` is an early-reject filter: a received manifest whose fork ID is not
  EIP-2124-compatible with the local node's is dropped before any entry reaches
  promotion/demotion logic here.
- Which stream a node follows is fixed by its chain configuration, never by
  quorum. Quorum decides bytes within a stream (§3); configuration decides the
  stream. A node on a minority fork stays on it regardless of how many
  publishers advertise the other side.
- A contentious split shares all pre-split history: pre-cut epochs draw quorum
  witnesses from *every* lineage that shares them; post-cut epochs are
  stream-specific.

## 5. Rewind and recovery — the liveness section

This is the foundational correctness section, not an appendix.

### 5.1 The problem is network liveness

In the distributed model a deep reorg is a *network-liveness* failure, not a
contained per-node one. The canonical view is a shared, quorum-derived artifact.
If it cannot demote: publishers keep advertising orphaned snapshots,
quorum-promoted entries stay canonical, consumers and every joining node keep
syncing the dead branch, and the swarm cannot converge onto the live branch —
**the whole network stops.** The ~24h GC window is far too slow to be the
recovery.

### 5.2 The mechanism — canonical view subordinate to consensus

A catastrophic reorg is resolved by consensus — honest nodes' consensus layers
*do* converge on the corrected chain; that is a property the network already
has. The canonical view rides on top of it:

- Each node's storage observes its **own** consensus view of the chain.
- When consensus reports the canonical head moving backward, storage demotes
  the canonical entries covering the orphaned range.
- Publishers re-retire the corrected branch and advertise the new entries;
  quorum re-forms on them.
- New / joining nodes sync consensus first, so their canonical view is computed
  under the corrected chain from the start.

No human-coordinated "everyone delete X" — the swarm converges on the demotion
*because consensus converges*. That is the liveness guarantee.

### 5.3 Recovery boundary — the weak-subjectivity period

The recovery path's scope boundary is the **weak-subjectivity period**. Within
it the canonical head can rewind and recovery must handle it; beyond it the
staked validator set is economically bankrupt — the chain is non-recoverable /
needs a fresh weak-subjectivity checkpoint — and that is out of scope.

The weak-subjectivity period is an **estimate, not a constant.** It is a
derived, variable quantity — a function of validator-set size and churn rate —
and it grows as the validator set grows. The current working figure is ~300,000
blocks (~41 days on Ethereum mainnet), but that is an estimate under today's
conditions. **It must be derived live from the consensus layer and carried on
the consensus→storage signal — never embedded as a constant.** Wherever this
spec or the code states the boundary, it states it as "the (estimated,
validator-set-dependent) weak-subjectivity period," so the assumption stays
visible. Hard-coding a number would repeat the exact omission that hid this
whole issue — treating a conditional estimate as an absolute.

### 5.4 The retire gate

The current retire gate is a fixed depth heuristic (`keep = 1024` blocks behind
head, in `db/snapshotsync/freezeblocks/block_snapshots.go`; the in-code comment
already flags this as an unfinished `FullImmutabilityThreshold` TODO). Replace
it with a gate relative to the consensus finalized checkpoint / the
weak-subjectivity horizon.

This is **window reduction, not a guarantee.** It makes invalidation require a
finality reversal rather than a routine deep reorg, but — because finality is
probabilistic — it cannot make invalidation impossible. The retire margin is a
*frequency knob*; it does not remove the need for the recovery path. A larger
margin costs more hot (un-retired) data in the mutable DB — the cost the
`keep = 1024` TODO refers to.

### 5.5 Distribution-layer response — unwind complete files to a boundary

The distribution layer's unwind unit is the **complete snapshot file** (plus its
dependent set) — never partial-file surgery. A rewind drops *whole files* and
lands on a **well-defined file/step boundary**.

The unwind target is the first boundary at or below the rewind point where the
*retained* set has verified **complete good coverage across all file types and
all domains** — which may be *deeper* than the rewind point itself. The unwind
keeps dropping complete files backward until coverage is known-good; it does not
stop early.

This is the exact mirror of the partial-block problem
(`docs/plans/…partial-block…`, the forward case): forward, the layer does not
advance or advertise until it reaches a complete confirmed boundary; backward,
it does not stop unwinding until it reaches a complete good-coverage boundary.
One principle — **the distribution layer only ever rests on complete confirmed
boundaries.**

"Complete good coverage" is not new logic: it is the orchestrator's existing
`coverageForRoleLocked` / `IsComplete` predicate plus the phase-1 rule that
`FrozenBlocks` collapses to `min(headers, bodies, transactions)`. The
reverse-unwind predicate is "walk back until `IsComplete` holds for the retained
set across every role and domain" — the same predicate, read in the other
direction. The boundary is therefore *deterministic*: two nodes given the same
rewind compute the same retained boundary, which is what lets the swarm
converge.

### 5.6 Scope — the consensus→storage signal is the scope line

Define one explicit event: `CanonicalHeadRewound{ToBlock: B}` (name TBD). It is
the scope boundary of this work.

- **Above the signal — OUT OF SCOPE:** real reorg detection, real consensus
  wiring, and the EL un-retire / unwind-into-snapshot-range path. That last one
  is a known storage gap that is deliberately *not* being fixed yet; this work
  does not depend on it.
- **Below the signal, in the distribution layer — IN SCOPE, must be complete and
  tested:** the canonical view demotes orphaned entries (§5.5); the download
  process drops the orphaned torrents and re-fetches the corrected branch when
  it is re-advertised; the publisher's manifest reflects the corrected set.

This is genuinely separable and still *completes* the distribution-layer half:
the distribution layer is a network concern. A reorg-hit node's EL is stuck on
the deferred un-retire gap — that node is locally broken, which is acceptable —
but the *network* must still converge, and the manifest + download process
being rewind-capable is exactly what keeps the network alive independent of any
one node's EL gap.

### 5.7 Retirement ordering — unadvertise first

Retiring a snapshot file — whether merge-superseded, rewind-orphaned (§5.5), or
rolled below the minimal-mode retention horizon — proceeds in a fixed order:

1. **Unadvertise.** Remove the file from the publisher's inventory and republish
   the manifest without it; evict any rolling generation that still lists it.
   After this step no current manifest points at the file.
2. **Remove locally.** Drop the torrent, then delete the data file and its
   `.torrent` sidecar from disk.

Unadvertise is *first*, and is a distinct step. Removing the file (step 2)
before unadvertising leaves a window where the advertised manifest points
consumers at a file the publisher no longer has or seeds — failed fetches and
swarm noise, violating the stable-manifest requirement.

Between the two steps the file may stay seedable for a short grace window, so a
consumer download already in flight against the prior manifest can complete
before the local delete.

All three retirement triggers (merge, rewind, minimal-horizon roll) share this
ordering; the retire mechanism (`RetireFiles`) implements it once. Note: the
existing merge `onDel` path already does inventory-removal + republish before
the torrent drop — incidentally correct — but the ordering was never explicit;
this section makes it a requirement.

## 6. Genesis pinning

Canonical genesis (`v0`) is a pinned `preverified.toml` snapshot — the content
present when the first authoritative publisher for the chain launched, recorded
as a definite artefact (content + hash) committed to `snapcfg` or distributed
as `canonical.v0.toml`. A node whose embedded `preverified.toml` is newer than
genesis anchors on the pinned `v0` and treats the extra entries as later
promotions — it never adopts its own binary's `preverified.toml` as genesis.

Process decision (to ratify): the erigon release that first ships
authoritative-publisher support also ships the pinned `canonical.v0.toml` for
each supported chain. For a second-generation chain with no prior authoritative
publisher, `v0` is the chain's embedded `preverified.toml` at the moment that
support ships.

## 7. Removed from the spec — signed checkpoints

The 2026-05-20 spec described optional signed checkpoints
(`canonical.<enr-fp>.toml` / `canonical.v<N>.toml`) — a publisher publishing a
snapshot of its derived view as a cold-start hint. These were **not built**:
Phase 6d chose `CanonicalView` restart-persistence
(`canonical.view-state.json`) instead. The checkpoint concept is removed from
the canonical-layer design. (A consumer still re-verifies every entry to quorum
regardless, so a checkpoint was only ever a performance hint.)

## 8. Test plan

All scenarios run against the real `storage.Provider` via
`NewP2PNodeWithStorageProvider`, with the rewind signal *injected* by the
harness — no real chain, no reorg, no EL. Simulate-first methodology; the
canonical view and orchestrator under test are the real components, only the
signal is synthetic, so a test cannot pull the EL un-retire gap into scope.

1. **Rewind demotes** — seed and quorum-promote a canonical slot; inject
   `CanonicalHeadRewound{ToBlock: B}`; assert the canonical view demoted the
   entries above `B`, the orphaned torrents were dropped, and a re-advertised
   corrected variant is fetched.
2. **Unwind to a complete-coverage boundary** — seed a file set where the rewind
   point falls such that stopping there would leave incomplete coverage; assert
   the unwind continues to the first complete-coverage boundary (deeper than the
   rewind point), the retained set satisfies `IsComplete`, and it landed exactly
   on a file boundary (never mid-file).
3. **Reversioning keeps both** — advertise a `v1.0` and a `v2.0` variant of the
   same slot; assert both stay canonical, the slot is satisfied by holding
   either, and no whole-set re-download is triggered.
4. **Merge multi-canonical** — pre-merge and merged forms both canonical; covered
   by existing scenarios, referenced here for completeness.

## 9. Implementation sequencing

A `CanonicalView` rewrite is the core: slot keying (§2), the variant set,
demotion (§3/§5), and the canonical digest (§4). Then the rewind signal and the
downloader's unwind-to-boundary response (§5.5/§5.6), then the WS-relative
retire gate (§5.4), then the verdict classifier's reversion case (§3), then
genesis pinning (§6). The signed-checkpoint removal (§7) is a deletion.

**Out of scope** for this work: the EL un-retire path, real reorg detection, and
the production wiring of the consensus→storage signal's *source*. In scope: the
canonical view, the manifest, the download process, and their response to the
injected signal — all harness-tested.
