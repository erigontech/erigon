# Fork testing — design decisions pin (2026-07-18)

**Companion to**: [`20260630-fork-testing-scenarios.md`](20260630-fork-testing-scenarios.md).

Records the design decisions taken in the 2026-07-18 planning conversation so
downstream test authors and implementers don't need to re-open the questions.

## F-0 progress (2026-07-18)

| Scenario | Status | Landed as |
|---|---|---|
| Schema — MinForkUnwindBlock | ✓ | `chain.Config.MinForkUnwindBlock` + `ParentSection.MinForkUnwindBlock` |
| E.2 — genesis + fork-ID cross-checks | ✓ | `ParentSection.ParentGenesisHash` + `ParentSection.ParentForks` + `ValidateParentIdentity` |
| A.4 — UCAN binding-at-fork-from picker | ✓ | `snapshotauth.PickIssuerFromAcceptSet` |
| B.4 — parent-trust-root-rotation-immutable | ✓ | `TestForkAuthorityCascade_ParentRotationDoesNotWidenAcceptSet` |
| E.5 — malformed forked-from at cascade | ✓ | `TestForkAuthorityCascade_MalformedForkedFromRejects` |
| E.1 — consumer-side pre-cut filter | ✓ | Already covered in `parentcut_validate_postcut_test.go` (plan doc's ✗ was stale) |
| A.2 — jagged-cut PendingReplacement supersession | ⏸ | Needs jagged-name parser support (`PopulateFromName` extension) |
| E.3 — UCAN wrong-root integration | ⏸ | Needs multi-process harness |
| C.4 — arbitrary depth cascade walker | ⏸ | Needs cascade-walker code (recursion + circularity) |

All landed items are additive-only — no code paths shared with the running unwind soak. The branch `mh/fork-testing-f0` isolates them until the soak signs off.

## D-1 — B.3 fallback: hard fail

When a fork follower can't locate the parent's V2 manifest by
`ParentManifestHash` on the swarm, the follower **hard-fails with a clear
error**. No silent fallback to bootstrap-from-preverified, no indefinite wait.

Rationale: the swarm's inability to serve the pinned parent manifest is an
operator-visible problem (add a parent publisher, or the fork's declared
lineage is wrong). Silent fallback makes the gap invisible; indefinite wait
looks the same as a slow start-up.

## D-2 — D.5 across-cut unwind: refuse-by-default + configurable floor

`debug_setHead(target)` on a fork chain with `target < CutBlock` is
**refused by default**. Operators who want to allow deeper unwind — for
example, transitioning a fork node back to the parent's canonicity — set a
new fork-only chain.Config field:

```go
Config.MinForkUnwindBlock uint64  // JSON: "minForkUnwindBlock,omitempty"
```

Semantic: the absolute block-number floor a setHead target must be `>=` on
this fork chain. Zero-value is interpreted at runtime as `CutBlock` (only-
current-fork unwind — the safe default). A positive value below `CutBlock`
authorises unwind down to that block; the extreme `1` (or parent's
genesis+1) permits unwind all the way back to parent start. The field is
mirrored on `ChainTomlV2.ParentSection.MinForkUnwindBlock` so followers read
the same floor from the V2 manifest.

**Convenience RPC — planned, not yet implemented**: `debug_setHeadToCut()` —
a no-arg helper that unwinds the current fork to `CutBlock` without the
operator having to look up the block number.

## D-3 — E.2 wrong-parent detection: both genesis + fork-ID cross-checks

Beyond the existing `ParentManifestHash` equality check (B.2), a
fork-follower additionally verifies:

- **Genesis-hash cross-check**: the peer's advertised
  `ParentSection.Chain` resolves to a known chain whose actual genesis hash
  matches an expected value carried on the manifest.
- **Fork-ID derivation check**: recompute EIP-2124 ForkID from
  `ParentSection.Forks[]` and verify it chains from the declared parent's
  genesis. Catches parents claiming a fork schedule inconsistent with the
  named chain.

Both checks run at manifest-accept time; a mismatch on either rejects the
manifest before the UCAN chain is walked.

## D-4 — C.4 fork-of-a-fork depth: arbitrary depth ALLOWED

The UCAN cascade supports **arbitrary depth**. A grandchild fork whose
`Parent` is itself a fork validates: grandchild content UCAN → grandchild
authority UCAN → parent authority UCAN (grandchild's parent) → grandparent
trust root.

This reverses the earlier "depth-1 only" position noted in
`memory/fork-identification-design-pickup-2026-05-23.md`. Test surface
gains a depth-≥2 fixture; implementation gains cascade-walk termination on
non-fork parent (walk until the parent chain.Config carries no `Parent`).

## D-5 — B.4 parent trust-root rotation: pre-existing forks stay valid

A parent chain's later trust-root rotation does **not** invalidate
previously-created forks. `ValidParentTrustRoots` is captured immutably at
fork-from time; the parent's rotation is future state that has no bearing
on already-anchored fork lineage.

Adopted as-is from the `20260630-fork-testing-scenarios.md` author's read.

## D-6 — F.4 parent forwards-compatibility: adopted as-is

The parent chain adding a new continuous fork to its schedule
(Shanghai+1, Cancun+1, etc.) is informational for existing forks; the
fork's lineage anchor (`ParentManifestHash` + captured `Parent.Forks[]`
snapshot) stays fixed and unaffected.

Adopted as-is from the plan doc.
