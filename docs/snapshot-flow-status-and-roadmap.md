# Snapshot Flow — Status and Roadmap

**Branch:** `feat/snapshot-flow-app-integration`
**Date:** 2026-06-30
**Scope:** Full snapshot-flow branch (571 files changed vs `main`, ~98k LOC)

This document is a comprehensive internal status note. It pairs with:

- [`docs/product-overview-snapshot-flow.md`](product-overview-snapshot-flow.md) — public-facing feature matrix (✓/◐/✦)
- [`docs/plans/20260430-snapshot-flow-objective.md`](plans/20260430-snapshot-flow-objective.md) — the top-level objective ("continuous validated publication")
- [`docs/plans/20260504-v2-operational-guide.md`](plans/20260504-v2-operational-guide.md) — operator-facing flag/log reference
- [`docs/plans/20260515-three-layer-snapshot-distribution.md`](plans/20260515-three-layer-snapshot-distribution.md) — the L1/L2/L3 distribution model

Audience: Erigon team + collaborators tracking this branch toward main integration.

---

## 1. Why this branch exists

Pre-V2 Ethereum nodes rely on out-of-band trust for the data behind the consensus head: an operator trusts the binary's embedded `preverified.toml` hashes, syncs from scratch, or pulls a snapshot from a single source. Three structural problems:

1. **One publisher per chain in practice.** A single `preverified.toml` author has months of wide window to push a wrong-or-compromised file before anyone notices. The trust boundary is "did you compile from the right tag," not "does the swarm agree."
2. **Sync time grows with chain age, not with chain tip.** A new node downloads the full historical file set even if it only intends to operate at tip. This scales poorly as chains age.
3. **Recovery requires rebuild-from-scratch.** A node that fell behind, took the wrong side of a transition, or needs to roll back has no in-place healing path. The operator wipes the datadir and re-syncs.

This branch addresses all three with a unified model:

- **Multiple publishers, swarm-converged canonicality.** Publishers retire blocks independently. Their outputs converge (or diverge — detectably) on the network. Canonicality is computed by consumers, not asserted.
- **Per-tip download set.** Most nodes only fetch what's needed to operate at tip — a small state slice + recent blocks — discovered via peer-published manifests, not a central registry.
- **Operator-grade recovery primitives.** `admin_setHead` against arbitrary blocks (across the changeset boundary AND into snapshots), minority-publisher detection, staged file replacement, in-place CL re-anchor.

The substrate is reusable. The same primitives — UCAN trust chain, manifest exchange, validity-bounded advertisements, atomic adoption — become the foundation for forthcoming federated-history networks across clients.

---

## 2. Architectural foundations

The branch organises around four conceptual layers. Each is implementable independently of the others (different files, different test surfaces) but they interlock at well-defined seams.

### 2.1 Three-layer distribution (L1/L2/L3)

Spec: [`docs/plans/20260515-three-layer-snapshot-distribution.md`](plans/20260515-three-layer-snapshot-distribution.md).

| Layer | Concern | Mechanism |
|---|---|---|
| **L1 — file content** | Per-file integrity | BitTorrent `.torrent` info-hashes |
| **L2 — file set** | Which files this node has + serves | `chain.v2.<enr-fp>.<seq>.toml` manifest, advertised by info-hash on ENR |
| **L3 — canonical set** | Which files everyone agrees are real | Quorum across UCAN-authorised publishers |

L1 is unchanged from pre-V2 (BitTorrent already integrity-protects file content). L2 and L3 are new.

### 2.2 Two-UCAN trust chain

Spec: [`docs/plans/20260520-chaintoml-ucan-flow-spec.md`](plans/20260520-chaintoml-ucan-flow-spec.md), [`docs/plans/20260516-two-ucan-shape.md`](plans/20260516-two-ucan-shape.md).

Each manifest a publisher emits is bound to two signed delegations:

- **Authority UCAN** (`chain.ucan.authority.<enr-fp>.<rev>.bin`) — long-lived (months). Root trust authority → operator pubkey. Capability `snapshot.publish:<chain>`. Info-hash carried on the V2 manifest as `AuthorityUCANHash`.
- **Content UCAN** (`chain.v2.<enr-fp>.<seq>.ucan`) — short-lived (per generation). Operator → self. Capability `chain.v2:hash:<sha256(toml)>`. Parent-by-hash link to the Authority UCAN.

Standard secp256k1 + BLAKE-equivalent chains. No bespoke crypto. The legacy `.sig` sidecar was removed in the same commit the Content UCAN landed — no fallback path.

### 2.3 Manifest format and naming

- **ENR-keyed naming**: `chain.v2.<enr-fp>.<seq>.toml`. `<enr-fp>` = first 16 hex chars of `sha256(enr-bytes)`. Constant-width, peer-unique, no collision risk between publishers.
- **Generation counter** (`<seq>`): multiple advertisement generations from the same node co-exist during publish/republish — peers that captured an older ENR snapshot at handshake time can still fetch the older info-hash for a window.
- **Typed schema**: `BlockFileEntry` for block-range files, `[entries]` map for state-domain files. ENR carries `V2InfoHash` + `MinStep` trailing fields for fast peer discovery without a full manifest fetch.
- **Validity-based eviction** (not count-capped): an old generation stays live as long as its advertised name-set ⊆ current inventory.

### 2.4 Storage component model

The execution stack no longer owns snapshot lifecycle. A new `node/components/storage/` package owns: inventory, file lifecycle (retire → merge → trim → rebuild), publish coordination, recovery primitives. Execution consumes storage through an interface (`storage.Provider`).

Verified architectural decoupling 2026-05-23: no `node/eth` imports from `node/components/{storage,downloader,manifest_exchange,snapshotauth}/`. The component boundaries are real.

---

## 3. What landed — Network layer

### 3.1 Two-UCAN authentication ✓

Every V2 manifest emission goes through `RollingV2Publisher.Publish` which requires:
- An Authority UCAN bytes source (`SetDelegationSource`)
- A Content UCAN minter (`SetContentUCANMinter`)
- A node ENR fingerprint (`SetSelfENRFingerprint` or via `Downloader.SelfENRFingerprint()`)

If any are missing, the publish errors and no manifest is written. Consumers running with `TrustConfig` verify the UCAN chain to a configured trust root.

Tests: `db/downloader/chaintoml_v2_ucan_test.go`, `db/downloader/chaintoml_v2_rolling_test.go`, `db/downloader/parentcut_publisher_integration_test.go`.

### 3.2 Per-node advertisement manifest ✓

`RollingV2Publisher` (in `db/downloader/chaintoml_v2_rolling.go`) writes successive generations of the V2 manifest. Triggered by:
- Retire / merge / boundary-regen completion events on the storage event bus
- `flow.TrustPromoted` events (when a previously-pending file passes validation)

The publisher is wired in `node/eth/backend.go` via `Downloader.SetInventory(...)` + `SetSelfENRFingerprint(...)` + the producer-side `SetManifestSelfCheck` callback (which runs `snapshotsync.CheckOwnAdvertisement` against the canonical view).

### 3.3 Network quorum + canonical view ✓

Spec: `docs/plans/20260524-canonical-layer.md` (parked plan, summarised here).

Canonicality is computed: an entry `(name, hash)` is admitted to a consumer's `CanonicalView` when it's been observed in ≥ Q distinct UCAN-authorised publisher advertisements.

- `Q = max(Q_floor, ceil(F * N_authorised))` — population-relative, sybil-resistant.
- Sybil boundary is the UCAN audience pubkey, not the ENR or peer-id. Minting ENRs is free; minting UCAN audiences that chain to a configured trust root is not.
- **Pinned canonical v0**: every chain has a definite genesis canonical snapshot (a recorded `preverified.toml` content + its hash). Version numbers (`v<N>`) are globally meaningful across binary releases.

`db/snapshotsync/canonical_view.go` holds the runtime canonical view. Production paths (`backend.go` self-check, snapshot retire, manifest exchange) all read from this view, NOT from the compiled-in `cfg.Preverified.Items`. Three sites that fell back to preverified at runtime were tightened in commit `6f48f4232c` — preverified is bootstrap-only.

### 3.4 Manifest exchange ✓

`node/components/manifest_exchange/` — the consumer-side service that discovers peer manifests via ENR `V2InfoHash`, fetches them by BitTorrent info-hash, validates the UCAN chain, and hands the result to the canonical-view aggregator.

### 3.5 Tip-filtered discovery ✓ *(landed this session)*

`db/downloader/chaintoml_consumer.go:filterDiscoveredByLocalTip` + `db/snapshotsync/preverified_filter.go:filterPreverifiedByLocalTip`.

Why: after a mode-B unwind, local processing can be far behind peer-advertised tips. Peers' chain.tomls still advertise files in the unwound range. Without a filter, the downloader re-fetches files the unwind just swept, wedging the recovery.

Rule: at both ingress sites (chain.toml peer discovery + SyncSnapshots reconcile), drop block entries whose `To > localTip + 1`. Cold-start (`localTip == 0`) is pass-through. CL / state / meta entries pass through. The `+1` accounts for the next-canonical block.

Tests: `db/downloader/chaintoml_consumer_test.go`, `db/snapshotsync/preverified_filter_test.go`.

### 3.6 Validity-based eviction ✓

`RollingV2Publisher` keeps a generation alive as long as `manifest.names ⊆ inventory.local`. When inventory shrinks (mode-B trim), generations whose name-set is no longer a subset are evicted from local seeding.

This is the right rule: the publisher should not advertise files it can no longer serve. Count-based eviction was tried earlier and produced wedges where peers held a stale ENR but the corresponding manifest was already cleaned up.

---

## 4. What landed — Recovery layer

The biggest functional change. `admin_setHead(N)` now works for any `N`, including `N` in the snapshot range.

### 4.1 The three recovery modes

Implemented in `node/components/storage/provider_unwind*.go`. Triggered from `execution/forkchoice.go:setHead`.

| Mode | When | Path |
|---|---|---|
| **Mode-A** | `target ≥ minUnwindableBlock` (within MDBX changeset) | Changeset-only rollback. Fast (<1s). EL re-execs the unwound blocks immediately. |
| **Mode-A2** | `target < minUnwindableBlock` but `target > frozenBlocksTip` (past changeset, within DB) | "Lite" mode-B: state unwind via commitment recompute. No snapshot trim. ~3min. |
| **Mode-B** | `target ≤ frozenBlocksTip` (into snapshot territory) | Full path: snapshot trim, straddler rebuild (if `target` lands mid-chunk), state unwind, Caplin re-anchor. 5min → 60min scaling with depth. |

### 4.2 Snapshot trim + straddle rebuild

When mode-B targets land mid-1k-chunk (the historical 1k retire granularity), the chunk containing the target needs to be replaced with a narrower file ending exactly at `target`. `node/components/storage/provider_unwind_snapshot_rebuild.go:rebuildBlockStraddles`.

The branch landed two iterations of this:

1. **First version (iter-4 wedge, 2026-06-27)**: rebuild wrote new `.seg` files to disk via `seg.NewCompressor` but did NOT call `Inventory.AddFile`. Inventory drifted from disk. `LocalBlockTip`'s contiguity walk ran over a stale view. Caplin's `DownloadHistoricalBlocks` stop bound dropped to 0 (or stale). Recovery never filled the gap.
2. **Fix (commit `6f48f4232c`)**: every rebuild output is announced to Inventory via `Inventory.AddFile(...)`. Inventory becomes the point-of-truth users see; disk and Inventory move together. Includes a new `db/snapshotsync/fileset/` module with pure-rule predicates (`StalePastTip`, `StaleNonMaximal`, `CullPlan`) used by sweep, `findOverlaps`, and `calcVisibleFiles`.

### 4.3 Caplin re-anchor ✓ (in-process), ◐ (external CL)

Spec: `memory/mode-b-cl-rewind-mvi-shipped-2026-06-09.md`, `memory/mode-b-external-cl-followup.md`.

Caplin's `anchorSlot` floor at `cl/phase1/forward_sync.go:286` blocked deep mode-B setHead below `anchorSlot`. Fixed: deep `UnwindCompleted` events trigger a CL re-anchor primitive that walks anchorSlot down to match the new EL head.

Open: **external CLs** (Lighthouse, Prysm, Teku, Nimbus, Lodestar over JSON-RPC) — EL must return `{INVALID, latestValidHash}` on FCU after mode-B setHead so the CL re-anchors via Engine API. Deferred post-release.

### 4.4 Persistent flow + change-detection

Three V2-related flags are persisted in MDBX via `db/kv/kvcfg/accessors_config.go`:

- `SnapLifecycleDrivenByStorage` ← `--snap.lifecycle-driven-by-storage`
- `SnapP2PManifest` ← `--snap.p2p-manifest`
- `SnapBootstrapFromPreverified` ← `--snap.bootstrap-from-preverified`

Persistence is **for change-detection, not for flag omission**. The snapModes loop at `node/eth/backend.go:371-380` calls `EnsureNotChanged` for each. CLI value ≠ persisted value → erigon refuses to start with a clear error. New tests in `db/kv/kvcfg/accessors_config_test.go` pin this behaviour across all three flags. *(Tests added this session.)*

This is the same policy as `SnapTrustFingerprint` (trust-root universe change refuses startup). Other persistent flags use weaker policies: `--persist.receipt` warns + accepts, `--prune.mode` has compat shims for retention bumps.

---

## 5. What landed — Storage layer

### 5.1 Inventory write-through

`node/components/storage/snapshot/inventory.go` is the runtime point-of-truth for "which files this node has." Every disk-mutating code path (retire output, merge output, mode-B rebuild output, mode-B sweep) calls `Inventory.AddFile` or `Inventory.RemoveFile` synchronously.

The branch landed several wedges where Inventory drifted from disk:
- `removeOldFiles` silent error → Inventory dropped entries for files still on disk
- `removeOldFiles` `ErrNotExist` not counted as success
- `sweepBlockOrphansPastBlock` using wrong predicate (`From > toBlock` missed straddlers)
- Straddle rebuild not calling `AddFile` (the iter-4 wedge)

All folded into one rule module + write-through invariant in commit `6f48f4232c`.

### 5.2 Unified file-set rules

`db/snapshotsync/fileset/rules.go` (new this branch). Two pure rules + a tiebreaker.

```go
// StalePastTip returns indices of items violating the tip invariant.
//   I is stale iff I.To > tip.
// Covers past-tip orphans (I.From ≥ tip) AND straddlers (I.From < tip < I.To).
func StalePastTip(items []Tagged, tip uint64) []int

// StaleNonMaximal returns indices of items violating maximality.
//   M-A (single-dominator): I is a proper subset of some J. Result: I removed.
//   M-B (union-cover): I's range is tiled by {J_k} all ProducedByRegen=true.
//     Result: I removed (narrower regen tiling wins).
func StaleNonMaximal(items []Tagged) []int

// CullPlan = union of past-tip + non-maximal indices, iterated to fixpoint.
func CullPlan(items []Tagged, tip uint64) []int
```

13 unit tests in `rules_test.go` encode every observed wedge as a deterministic fixture (iter-5 narrow-straddler, the M-B union-cover from the 2026-06-25 `v2.0-accounts.272-280` wedge, etc.). Five sites delegate: `sweepBlockOrphansPastBlock`, `collectFilesPastBlock`, `findOverlaps[T SortedRange]`, `calcVisibleFiles`, `straddleBlockFileForType`.

### 5.3 Storage event bus

`node/components/storage/flow/` — the storage component publishes events (`DownloadComplete`, `TrustPromoted`, `RetireComplete`, `MergeStarted`, `UnwindCompleted`, etc.) on a typed event bus. Subscribers include the V2 publisher, the canonical-view aggregator, integrity validators, and the lifecycle driver.

Open: `MergeStarted`/`MergeEnded` events from `db/state` (the authoritative merge ledger) still need wiring. See `memory/merge-events-design.md`.

### 5.4 Block-slot-aligned storage model

Design pin: `memory/block-slot-aligned-storage-model-2026-05-24.md`.

Storage steps align on atomic units (block for EL, slot for CL). Fork (fold) + unwind become standard operations on the same boundary granularity. A unified `storage.Provider.Unwind(toBlock)` consolidates db unwind + snapshot unwind + commitment-entry insertion.

Status: model agreed; implementation in pieces (the rounding-removal work is folded into this).

---

## 6. What landed — Operational tooling

### 6.1 Soak test driver

`scripts/unwind-soak.sh` (428 lines) drives `admin_setHead` against a running erigon in repeatable iterations. Each iteration runs three scenarios:

1. **mode_a** — depth 50 (within changeset)
2. **mode_a2** — depth 300 (past changeset, within DB)
3. **mode_b** — configurable depth (e.g. 5k, 10k, 30k, 60k, 30k across 5 iters)

Per iteration: pre-head, setHead call, recovery polling (head OR log-bytes growth), error-pattern grep, CSV row. The driver aborts on any iter failure.

This session hardened the liveness model:
- Originally: head-only polling. False-failed when Caplin was actively downloading but head pinned at target.
- Iteration 1: added Caplin `DownloadHistoricalBlocks progress` signal.
- Iteration 2: added `BlockCollector Inserted blocks progress`.
- Iteration 3: added `[4/6 Execution] parallel executed blk=N`.
- Iteration 4: added `[chaintoml] regenerated` count.
- **Current (working)**: log file byte growth — any erigon log write counts as "alive."
- **Plus soft-wedge abort**: forbidden patterns (`invalid block`, `parent's total difficulty not found`, `Could not start execution service`, `halting process`) → abort immediately. Catches the wedges that the log-bytes gate would miss (e.g. an invalid-block retry loop keeps the log growing but isn't making real progress).

### 6.2 Fresh-sync-then-soak harness

`scripts/unwind-fresh-sync-then-soak.sh`. Wipes datadir, launches erigon, waits for sync to live tip (with Caplin-aware liveness gate + hard deadline), then runs the soak.

Catches first-time-bootstrap interactions that the running-datadir soak misses: initial OtterSync + manifest exchange + first Inventory snapshot + first retire/merge cycle into the new setHead path.

### 6.3 Erigon launcher

`scripts/erigon-launch-hoodi-soak.sh`. Standard flag set, used by the manual restart path AND the soak harness — single source of truth for V2-mode launches.

**Today's fix**: launcher omitted `--snap.p2p-manifest`. Effect: backend gate at `node/eth/backend.go:1118` skipped ENR-updater wiring → `SetSelfENRFingerprint` never fired → 860 V2 publish failures over 2.5h on a clean run. Fix: add the flag. Persistence (§4.4) protects against accidental drop on subsequent runs.

---

## 7. What landed — Safety + change-detection

### 7.1 Producer-side self-check

`backend.go:676` wires `SetManifestSelfCheck` on every `RollingV2Publisher.Publish`. The callback runs `snapshotsync.CheckOwnAdvertisement` against the canonical view; failure returns an error from `Publish`, the caller logs `Warn` and skips this generation.

Phase 7c minority-detection trigger: when the self-check finds this node advertising a non-canonical hash for a quorum-promoted file, it hands the verdict to `triggerAdoption`, which runs staged adoption out of band (network fetch + validation + cutover must not block the publish path).

Spec: `docs/plans/20260516-phase-7bc-staged-adoption.md`, `docs/plans/20260524-canonical-layer.md`.

### 7.2 Adoption grace gate

`snapshotsync.NewAdoptionGraceGate` (configurable via `--snapshot.adoption-grace`). A minority verdict must persist for the grace window before staging is triggered, so a transient swarm disagreement (a brief quorum flap, or this node's own fresh publish not yet observed by peers) settles instead of kicking off a network fetch + cutover.

### 7.3 Publisher startup pre-flight

Spec: [`docs/plans/20260522-publisher-startup-preflight.md`](plans/20260522-publisher-startup-preflight.md).

The first manifest a publisher emits after startup must be known-good. Three guarantees enforced via the lifecycle driver chain:
- All advertised names are present on disk (no inventory rows asserting `Local`/`Verified` without re-verification)
- Each name's infohash matches what's actually compressed at that path
- A settle-watcher holds back the first publish until inventory has stabilised through validation

Implemented 2026-05-22. Replaces an empty-manifest-at-startup bug observed on a live two-publisher sepolia run 2026-05-21.

---

## 8. Observed wedges + remaining unknowns

This branch surfaced and closed a long list of wedges. The session log entries in `memory/` capture each one. Highlights:

| Wedge | Root cause | Status |
|---|---|---|
| Mode-B BranchCache wedge (#21386) | Stale cache entries past UnwindTo's maxValidTxN | Workaround landed (`USE_STATE_CACHE=false` in launcher); proper fix in #21386 PR |
| Iter-4 straddle rebuild not added to Inventory | `seg.NewCompressor` wrote to disk without `Inventory.AddFile` | Fixed in `6f48f4232c` |
| Iter-5 narrow-straddler not swept | `sweepBlockOrphansPastBlock` used `From > toBlock` | Fixed via `StalePastTip` rule |
| 2026-06-25 v2.0-accounts.272-280 union-cover | `calcVisibleFiles isProperSubsetOf` missed M-B case | Fixed via `StaleNonMaximal` rule with `ProducedByRegen` tiebreaker |
| Chain.toml drift across iters | Peer discovery + reconcile re-introduced swept files | Fixed via tip-filters at both sites |
| 860 V2 publish failures over 2.5h | Launcher missing `--snap.p2p-manifest` → ENR fingerprint never set | Fixed (this session): launcher flag + backend-side gate symmetry |
| Soak false-fails on healthy deep recoveries | Head-only liveness; 180s gate too tight | Fixed (this session): log-bytes growth + soft-wedge abort |

### 8.1 Open: iter-4 mode-B state-divergence at block 3,110,001

**Status: under investigation. Not yet a fix.**

In the publish-gap soak run, iter-4 mode-B at depth 60k hit:
```
invalid block, block=3110001, gas used by execution: 1691540, in header: 58499447
parallel exec loop exited with 32 block(s) still pending (reason=ctx-done-drain)
```

Block 3,110,001 lives in MDBX past the snapshot frontier. After deep mode-B unwind to 3,054,862 + Caplin re-push, exec computed gasUsed=1.69M for the block but the header demands 58.5M. Symptom: truncated tx view (44 txs processed, header expects ~2,800).

Hypotheses (not yet validated):
- MDBX block 3,110,001's tx data was partially purged by retire and the snapshot file for that range never landed locally
- Caplin's re-push delivered the header but not the full tx body for blocks in the gap
- A race between mode-B trim and a concurrent retire output

With chain.toml publishing now working, the post-recovery file-set view should be consistent for the next soak run; that's the natural next-test point. If iter-4 mode-B still wedges with chain.toml publishing healthy, the truncated-tx state-divergence becomes a standalone investigation against a known-good publish layer.

### 8.2 Open: external CL re-anchor

In-process Caplin re-anchors cleanly on deep mode-B `UnwindCompleted`. External CLs (Lighthouse, Prysm, Teku, Nimbus, Lodestar) talk to EL via Engine API; the spec for "EL just dropped its head 60k blocks" is FCU returning `{INVALID, latestValidHash}`. Erigon currently returns `{VALID}` after mode-B setHead, which doesn't trigger the CL to re-anchor.

Deferred post-release. Spec referenced in `memory/mode-b-external-cl-followup.md`.

### 8.3 Open: design-gap — partial-block commitment validation

Partial-block commitments have no consensus anchor: the commitment computed mid-block isn't part of any header. The defensive workaround is "pause until block-segment is Advertisable." Long-term fix: design a partial-block validation primitive or accept that mode-B targets must be block-aligned.

See `memory/design-gap-partial-block-validation.md`.

### 8.4 Open: reversioning canonical design

A mass `v1.0` → `v2.0` reversioning (e.g. a file-format bump) MUST NOT trigger whole-snapshot re-download. Currently it would: peers advertise new-version names, consumers don't have them, BitTorrent fetches the new files. The right design is either co-existence (consumers accept either version) or a content-equivalence proof (the version-bump is a no-op on bytes).

Deferred until end of chain.toml effort.

### 8.5 Open: chain.toml v2 `[[webseeds]]` coverage-bound section

Pre-V2 `webseeds.toml` had range-bounded HTTP fallback. V2 manifest schema doesn't yet have `[[webseeds]]`. Cloud operators using HTTP fallback alongside BitTorrent need this.

Deferred post-soak.

### 8.6 Open: exec-during-download gap

V2 currently does sequential `download then exec`. The V2 consumer should be able to exec the minimal set (current state slice) while still downloading historical files for archive operations. This is a sequencing change in the lifecycle driver — not blocked on protocol design.

See `memory/exec-during-download-gap.md`.

---

## 9. Project ordering — the sequence

Pinned 2026-05-23 (`memory/PROJECT ORDERING`). The path from where this branch is now to mainstream Erigon integration:

```
Phase 2: Fork + Unwind (active — current soak validates this)
   ↓
Post-functional:
   • Security audit
   • Performance
   • Scaling
   • PR integration to main
   ↓
History-network integration (federated):
   • caplin / EL era files
   • ss2era (shipped)
   • era2ss + ss2rpc (proposed)
   • cross-client federation
   ↓
Core Erigon
```

Security audit is post-functional, NOT concurrent with Phase 2. Reasoning: an audit against a moving target is wasted spend.

History-network integration: the snapshot-flow infrastructure becomes a BitTorrent + UCAN substrate for federated history archives. Caplin contributes its EL era files; the same trust-quorum-validity model works for the slot-aligned CL data.

---

## 10. Roadmap — concrete next steps

### 10.1 Immediate (this week)

- **Land current soak iteration cleanly.** All 5 iters incl iter-4 mode-B at depth 60k. With chain.toml publishing working + the liveness/soft-wedge gates this session, iter 1-3 pass cleanly in the active run; iter-4 is the remaining test.
- **If iter-4 still wedges**: standalone investigation of the block-3,110,001 truncated-tx state-divergence (§8.1). Working hypothesis is something around the snapshot-frontier / MDBX-tx-data interaction post deep unwind.
- **Commit the uncommitted changes** from this session: chain.toml tip-filter, SyncSnapshots tip-filter, V2 publish gate, launcher flag, liveness gate, pin tests, error-message fix in `snapModes`.

### 10.2 Next 2-4 weeks

- **Multi-node hardening**. Per `memory/feedback-multi-node-hardening.md`: multi-node + churn surfaces hardening bugs single-node misses. Validate:
  - 20+ publisher quorum harness
  - Manifest+UCAN-only flow (no centralised snapshot file download)
  - Publisher restart preflight under load
  - Real-network surprises encoded back as deterministic scenarios
- **External-test methodology**. `memory/feedback-external-reference-tests.md`: swarm tests must be driven from real `preverified.toml` / live `chain.toml` files, never synthesised fixtures.
- **PR description draft.** `docs/plans/20260606-pr-description-v2-snapshot-flow-app-integration.md` is the draft. Refine with current soak data.

### 10.3 Phase 2 wrap (1-2 months)

- **Componentization push**: extract execution as a component (per `memory/component-actor-model-refactor.md`). HIGH PRIORITY because the single-goroutine actor that serialises state transitions is what currently blocks execution-component extraction. Fuzz tests at `fuzz_test.go` (currently `t.Skip`'d) are the verification suite.
- **Snapshot-flow scenarios as merge gate**: per `memory/snapshot-flow-merge-gate.md`. Scenarios must demonstrate trusted+validated files via latest-download + backfill before merge.

### 10.4 Post-functional (after Phase 2)

- **Security audit** of the UCAN flow, quorum aggregator, manifest signing, BitTorrent integration, mode-B unwind path.
- **Performance**: time-to-tip benchmarks on mainnet (target: matches or beats pre-V2 — V2 should be FASTER once the per-tip download set is the norm).
- **Scaling**: archive-operator scenario (full preverified set + V2 bootstrap publisher mode), publisher infrastructure (Erigon Tech's snapshotter as the canonical mainnet bootstrap publisher).
- **PR integration to main**. Phased rollout per [`docs/plans/20260505-v2-rollout-sequence.md`](plans/20260505-v2-rollout-sequence.md): default flags preserve pre-V2 behaviour; opt-in only. Erigon Tech snapshotter becomes the first bootstrap publisher.

### 10.5 History-network integration

Pinned in `memory/history-network-integration-2026-05-23.md`.

- **ss2era**: shipped (block-snapshot → era file converter)
- **era2ss**: proposed (reverse)
- **ss2rpc**: proposed (era → JSON-RPC archive serving)
- **Cross-client federation**: the same UCAN + quorum model works for any client that emits era files. Reth, Geth, Nethermind, Besu — each can be a publisher in the quorum.

This is post-PR-integration. The snapshot-flow infrastructure on `main` is the substrate.

### 10.6 Core Erigon

After history-network. The final sequence: **functional → post-functional → history-network → core Erigon**, in that order. Core means: replacing pre-V2 sync as the default path, deprecating `preverified.toml`-only mode, simplifying the lifecycle driver now that storage owns the post-download pipeline.

---

## 11. Operational invariants (lessons captured)

These are battle-tested from the soak runs. Document for the team so the next contributor doesn't re-discover them.

### 11.1 "Inventory is the point-of-truth"

Source: user direction 2026-06-28. Every disk-mutating path must update Inventory synchronously. Disk-scan reconciliation as a recovery mechanism is allowed (and exists, in `provider_unwind.go:sweepBlockOrphansPastBlock`) but is a **self-heal** not a primary update path. If Inventory drifts in normal operation, that's a bug to fix at the write site, not at a periodic-reconcile site.

### 11.2 "Persistence is for change-detection, not omission"

Source: user direction 2026-06-30. Three V2 mode flags are persisted in MDBX. The purpose is to ERROR at startup if the CLI value doesn't match the persisted value — protecting against accidental flag drop. It is NOT to allow operators to omit the flag and inherit the persisted value (which would also be safe, but isn't the contract). See §4.4.

### 11.3 "Received torrents must match chain.toml"

Source: user direction. The downloader's per-fetch sidecar verifies that BitTorrent torrents arrive bearing manifest entries we asked for. A torrent that doesn't match the manifest we requested is dropped. Prevents the "swarm offers a different file than the one the canonical view expects" attack.

### 11.4 "Don't process incoming downloads until local processing has advanced beyond them"

Source: user direction (the tip-filter rule from this session). After a mode-B unwind, peer chain.tomls advertise files in the unwound range; without a filter, those files come back via download and re-wedge the recovery. See §3.5.

### 11.5 "The assumption is that a fail is a transient disk issue, which will recover; if it's a logic fail — which it should NOT — that's a bug"

Source: user direction. Retire-stage error handling: keep retrying transient errors (disk full, brief I/O hiccup). NEVER swallow logic errors. Every logic error during retire is a bug to fix at the source, not a recoverable transient.

### 11.6 "There should be no need to change the current sync flow"

Source: user direction. The default (non-`--snap.p2p-manifest`) path must continue to work exactly as before. V2 is opt-in. The gate added in §3.5 strictly tightens an existing one-sided gate (the off-mode becomes silent instead of error-spammy) but doesn't change behaviour when the flag is off.

### 11.7 "Tests are the spec"

Source: `memory/feedback-engineering-standards.md`. Every observed wedge gets a pinned test fixture. The unified file-set rules module (§5.2) is the cleanest example: 13 unit tests encode every historical wedge as a deterministic scenario. Future refactors that break the policy fail the test, not silently re-introduce the wedge.

### 11.8 "Never silently downgrade"

Source: live-rig discovery 2026-06-02. Earlier, omitting `--snap.lifecycle-driven-by-storage` silently left `Provider.Inventory` nil and the new mode-B snapshot-trim path was a no-op. Now: the three snap-mode flags are persisted + change-detected, so a missing flag against a `true`-persisted datadir errors at startup. The same principle applies elsewhere: if a feature is on, removing it must be explicit.

---

## 12. Where to find this in the code

| Concept | Primary file(s) |
|---|---|
| L2 manifest emission | [`db/downloader/chaintoml_v2_rolling.go`](../db/downloader/chaintoml_v2_rolling.go), [`chaintoml_v2_publish.go`](../db/downloader/chaintoml_v2_publish.go), [`chaintoml_v2.go`](../db/downloader/chaintoml_v2.go) |
| L2 manifest consume | [`db/downloader/chaintoml_consumer.go`](../db/downloader/chaintoml_consumer.go), [`db/snapshotsync/preverified_filter.go`](../db/snapshotsync/preverified_filter.go) |
| Two-UCAN trust chain | [`node/components/snapshotauth/`](../node/components/snapshotauth/) |
| Manifest exchange (P2P) | [`node/components/manifest_exchange/`](../node/components/manifest_exchange/) |
| Canonical view + quorum | [`db/snapshotsync/canonical_view.go`](../db/snapshotsync/canonical_view.go), [`db/snapshotsync/check_own_advertisement.go`](../db/snapshotsync/check_own_advertisement.go) |
| Storage component | [`node/components/storage/`](../node/components/storage/) |
| Inventory write-through | [`node/components/storage/snapshot/inventory.go`](../node/components/storage/snapshot/inventory.go) |
| Unified file-set rules | [`db/snapshotsync/fileset/rules.go`](../db/snapshotsync/fileset/rules.go) + `rules_test.go` |
| Mode-B unwind | [`node/components/storage/provider_unwind*.go`](../node/components/storage/) |
| Caplin re-anchor | [`cl/phase1/forward_sync.go`](../cl/phase1/forward_sync.go) (anchorSlot rewind) |
| Snap-mode persistence + check | [`node/eth/backend.go:355-380`](../node/eth/backend.go), [`db/kv/kvcfg/accessors_config.go`](../db/kv/kvcfg/accessors_config.go), [`accessors_config_test.go`](../db/kv/kvcfg/accessors_config_test.go) |
| V2 publish wiring (backend) | [`node/eth/backend.go:610-758`](../node/eth/backend.go), [`backend.go:1118`](../node/eth/backend.go) |
| Soak harness | [`scripts/unwind-soak.sh`](../scripts/unwind-soak.sh), [`scripts/unwind-fresh-sync-then-soak.sh`](../scripts/unwind-fresh-sync-then-soak.sh), [`scripts/erigon-launch-hoodi-soak.sh`](../scripts/erigon-launch-hoodi-soak.sh) |

---

## 13. Canonical file format

The chain manifest is the central artefact of V2 distribution — what publishers emit, what consumers verify, what the swarm converges on. The format is documented in two places that operate at different abstraction levels.

### 13.1 Current canonical declaration — `ChainTomlV2` Go struct + TOML wire

Today the Go struct in [`db/downloader/chaintoml_v2.go:54`](../db/downloader/chaintoml_v2.go#L54) is the canonical declaration; TOML is the wire serialization. Top-level shape:

```go
type ChainTomlV2 struct {
    Version int                          // protocol version (currently 2)
    GenesisFork string                   // hex(CRC32(genesis_hash)) — identity-tree anchor
    Forks []ForkActivation               // activated continuous fork schedule
    Parent *ParentSection                // fork-only: lineage + cut + CL config
    AuthorityUCANHash string             // infohash of the Authority UCAN
    Blocks []BlockFileEntry              // block-snapshot files
    Meta map[string]string               // erigondb.toml-like config files
    Salt map[string]string               // hash-derivation salts
    Domains map[string]*DomainManifest   // state-domain files (kv/history/idx/accessor)
    Caplin []CaplinFileEntry             // beacon-archive files
}
```

Per-file entries (`BlockFileEntry`, `DomainFileEntry`, `CaplinFileEntry`) carry the same shape: `Name`, `Range [2]uint64`, `Hash`, `Trust`, optional `ProofRoot` (the state-trie root recorded inside the file, closing the cryptographic chain to block headers), optional `AtBlock` / `AtTxNum` (the chain-timeline position the proof refers to), and optional `PendingReplacement` (transitional fork-cut files that must skip canonical promotion).

The wire form is `[[blocks]]` array-of-tables. The parser also accepts the legacy `[blocks]` flat map (name → hash) for back-compat, normalising it into typed entries with range derived from the filename.

### 13.2 Future canonical declaration — SSZ schema

Spec: [`docs/plans/20260524-chain-ssz-schema.md`](plans/20260524-chain-ssz-schema.md) — Chain manifest SSZ schema — derivation + cutover plan.

> "The speced ssz container is the chain format designed rather than evolved through the process. We should assume it will eventually be agreed through consensus." — user, 2026-05-24.

The end-state model is **SSZ-on-the-wire as the canonical manifest format**, with TOML reserved for human-readable printing. Three-layer separation:

1. **Schema** — what fields exist + their bounds + their semantics. SSZ container declaration. Subject to EIP-style consensus review.
2. **Wire** — how a publisher actually serialises the schema for distribution. SSZ today; an ENR-embeddable hash + a torrent-distributed SSZ artefact.
3. **Print** — how to render the schema for humans. TOML for readability.

Design discipline (from the spec):

- The SSZ container IS the chain manifest format, not a serialization of `ChainTomlV2`. Fields are derived from first principles, not preserved from the V1→V2 evolution.
- Schema is the authority; Go struct becomes a representation.
- Designed for an external review audience (strict naming, tight bounds, deliberate enumerations).
- Per-operator trust assessments (UCAN chains, local trust roots, per-file confidence) live **outside** the manifest as side-channels keyed to manifest identity.
- Forward-compatibility via `ProgressiveContainer` — V3 can add fields without changing V2 Merkle gindices.

Cutover plan: dual-publish at PR-integration-to-main (TOML + SSZ for one release), then deprecate TOML wire one release later. The Print layer stays TOML indefinitely.

### 13.3 Pending format proposals (must land before production)

Three proposals in [`docs/plans/`](plans/) gate production adoption of the experimental snapshot-flow + mode-B unwind implementation. All three are marked "iteration-2 work item":

| # | Doc | What it changes |
|---|---|---|
| **P1** | [20260613-proposal-1-txnum-boundaries.md](plans/20260613-proposal-1-txnum-boundaries.md) | Replace *step indices* (derived from `txNum / stepSize`) with raw `txNum` boundaries on the canonical surface — filenames, manifest, aggregator addressing. Functional change: redefines what a file means, not just what it's called. |
| **P2** | [20260613-proposal-2-content-addressed-names.md](plans/20260613-proposal-2-content-addressed-names.md) | Make a file's identity its content hash, not its filename string. Closes the "publisher serves a corrupted file under the right name" attack. Depends on P1 (txnum-canonical metadata must come first). |
| **P3** | [20260613-proposal-3-chain-definition-transports-split.md](plans/20260613-proposal-3-chain-definition-transports-split.md) | Split `chain.toml` into two concerns: (a) **what the chain IS** — authoritative canonical file set, signed via UCAN; (b) **how the chain GETS HERE** — torrent infohashes, webseeds, transport details. Hinges on P2's content-addressed identity for the split. |

These proposals are deliberately ordered: P1 → P2 → P3. None has landed yet. They're load-bearing for the SSZ schema (§13.2) — the SSZ format needs to know whether boundaries are txnum-based and whether identity is content-addressed before it can be specified as a final consensus artefact.

---

## 14. Future-state design docs

Beyond the canonical-format work in §13, the branch carries a substantial set of "where we want to get to" specs. These are NOT speculative roadmaps — they're concrete designs that have been worked through, often pinned by user direction, and are gated on prerequisite work landing.

### 14.1 Storage end-state

| Doc | What it specifies |
|---|---|
| [20260430-storage-views-spec.md](plans/20260430-storage-views-spec.md) | Storage views contract — the read surface storage exposes to consumers |
| [20260501-storage-lifecycle-spec.md](plans/20260501-storage-lifecycle-spec.md) | Storage-owned snapshot import lifecycle |
| [20260518-storage-owns-post-download-pipeline.md](plans/20260518-storage-owns-post-download-pipeline.md) | **End-state**: storage owns the full post-download pipeline. Staged-sync runs execution only. Lifts `FillDBFromSnapshots`, `OpenSegments`, index-build coordination out of the OtterSync stage. |
| [20260525-lockfree-file-reclamation-spec.md](plans/20260525-lockfree-file-reclamation-spec.md) | Lock-free snapshot-file reclamation via generation-chained bundle refcounts |

The 20260518 doc is the load-bearing end-state — "storage owns the full post-download pipeline" — and most other storage-component work tracks toward it.

### 14.2 Mode-B + recovery end-state

| Doc | What it specifies |
|---|---|
| [20260525-admin-sethead-unwind-design.md](plans/20260525-admin-sethead-unwind-design.md) | Admin SetHead Unwind — top-level design |
| [20260527-sethead-external-cl-test-rig.md](plans/20260527-sethead-external-cl-test-rig.md) | SetHead mode B — external CL conformance test rig (covers §8.2's open work) |
| [20260530-mode-b-functional-completeness.md](plans/20260530-mode-b-functional-completeness.md) | Mode-B — functional completeness checklist |
| [20260603-mode-b-boundary-step-regen-plan.md](plans/20260603-mode-b-boundary-step-regen-plan.md) | Mode-B boundary-step commitment file regeneration |
| [20260609-mode-b-cl-rewind-gap.md](plans/20260609-mode-b-cl-rewind-gap.md) | Mode-B Deep SetHead — CL-side rewind gap (CL component MVI) |
| [20260614-deep-mode-b-gap-bridging.md](plans/20260614-deep-mode-b-gap-bridging.md) | Deep Mode-B recovery: post-snapshot-tip gap-bridging |
| [20260510-partial-block-validation-model.md](plans/20260510-partial-block-validation-model.md) | Partial-block commitment validation — model and edge cases (covers §8.3) |

### 14.3 Trust + canonical-view end-state

| Doc | What it specifies |
|---|---|
| [20260520-phase7-staged-adoption-design.md](plans/20260520-phase7-staged-adoption-design.md) | Phase 7b/7c — staged canonical adoption design |
| [20260522-canonical-layer-revision.md](plans/20260522-canonical-layer-revision.md) | Canonical layer — revised spec |
| [20260522-fork-identification-impl.md](plans/20260522-fork-identification-impl.md) | Fork identification — implementation plan |

### 14.4 Performance + scale targets

| Doc | What it specifies |
|---|---|
| [20260502-min-time-to-tip-target.md](plans/20260502-min-time-to-tip-target.md) | **Headline target: 10-min time-to-tip on mainnet**. Validated 10:25 on hoodi with bootstrap publisher + V2 manifest. Mainnet validation is the next milestone. |
| [20260607-coverage-baseline.md](plans/20260607-coverage-baseline.md) | Erigon unit-test coverage baseline + tracking |

### 14.5 Boundary / experimental rules

| Doc | What it specifies |
|---|---|
| [20260613-experimental-boundary.md](plans/20260613-experimental-boundary.md) | Experimental boundary statement — what's locked vs what can still change. The companion to P1/P2/P3 — defines what's a snapshot-flow API contract vs an internal detail. |

### 14.6 Architectural anchors (user-pinned 2026-05-23/24, not yet in `docs/plans/`)

These were established as load-bearing design anchors in conversation and are referenced from `/erigon/.claude/projects/.../memory/`. They should be promoted to `docs/plans/` before PR integration to main:

| Anchor | Why load-bearing |
|---|---|
| **Chain has two axes of change** (time + value) | Negotiated-variable model (quorum + per-chain trust roots + UCAN) is the framework. Consensus enshrinement is one possible future, not required. Vocabulary: `archive_state` / `archive_root` / `archive_transition`. |
| **Block/slot-aligned storage** | Storage steps align on atomic units (block for EL, slot for CL). Fork (fold) + unwind become standard ops on the same boundary. Unifies db unwind + snapshot unwind + commitment-entry insertion under `storage.Provider.Unwind(toBlock)`. |
| **Fork trust-root model** | Per-chain trust roots; fork picks `ValidParentTrustRoots[]`; fork-authority UCAN embeds parent trust root that vetted `ParentManifestHash`. Lite vs belt-and-braces verification. |
| **Manifest-driven, not convention-driven** | Chain.toml v2 carries every property `ParseFileName` previously derived from filenames. Code with manifest access looks up there; `ParseFileName` shrinks to legacy fallback. |
| **Three-layer model: schema / wire / print** | Cutover at PR-integration-to-main via dual-publish (§13.2). |

---

## 15. References — design docs (chronological)

The full set, sorted by date. Marked entries are referenced inline from sections above.

| Date | Doc | Topic |
|---|---|---|
| 2026-04-30 | [snapshot-flow-objective](plans/20260430-snapshot-flow-objective.md) | Top-level objective: continuous validated publication |
| 2026-04-30 | [storage-views-spec](plans/20260430-storage-views-spec.md) | Storage views contract |
| 2026-05-01 | [storage-lifecycle-spec](plans/20260501-storage-lifecycle-spec.md) | Storage-owned snapshot import lifecycle |
| 2026-05-02 | [min-time-to-tip-target](plans/20260502-min-time-to-tip-target.md) | Performance target + process |
| 2026-05-04 | [v2-operational-guide](plans/20260504-v2-operational-guide.md) | Operator-facing flag/log reference |
| 2026-05-04 | [publisher-did-ucan](plans/20260504-publisher-did-ucan.md) | Publisher DID + embedded trust root |
| 2026-05-04 | [publisher-restart-chaintoml-bug](plans/20260504-publisher-restart-chaintoml-bug.md) | Postmortem |
| 2026-05-05 | [v2-rollout-sequence](plans/20260505-v2-rollout-sequence.md) | Phased mainnet rollout |
| 2026-05-10 | [partial-block-validation-model](plans/20260510-partial-block-validation-model.md) | Partial-block commitment validation |
| 2026-05-15 | [three-layer-snapshot-distribution](plans/20260515-three-layer-snapshot-distribution.md) | L1/L2/L3 model |
| 2026-05-16 | [two-ucan-shape](plans/20260516-two-ucan-shape.md) | Authority + Content UCAN design |
| 2026-05-18 | [storage-owns-post-download-pipeline](plans/20260518-storage-owns-post-download-pipeline.md) | End-state storage ownership |
| 2026-05-20 | [chaintoml-ucan-flow-spec](plans/20260520-chaintoml-ucan-flow-spec.md) | Central reference: publisher emits + consumer verifies |
| 2026-05-20 | [phase7-staged-adoption-design](plans/20260520-phase7-staged-adoption-design.md) | Phase 7b/7c staged canonical adoption |
| 2026-05-22 | [publisher-startup-preflight](plans/20260522-publisher-startup-preflight.md) | First-manifest-must-be-known-good |
| 2026-05-22 | [canonical-layer-revision](plans/20260522-canonical-layer-revision.md) | Canonical layer revised spec |
| 2026-05-22 | [fork-identification-impl](plans/20260522-fork-identification-impl.md) | Fork identification implementation |
| 2026-05-24 | [chain-ssz-schema](plans/20260524-chain-ssz-schema.md) | **SSZ schema — end-state canonical format** |
| 2026-05-25 | [admin-sethead-unwind-design](plans/20260525-admin-sethead-unwind-design.md) | Admin SetHead unwind |
| 2026-05-25 | [lockfree-file-reclamation-spec](plans/20260525-lockfree-file-reclamation-spec.md) | Lock-free file reclamation |
| 2026-05-27 | [sethead-external-cl-test-rig](plans/20260527-sethead-external-cl-test-rig.md) | External CL conformance test rig |
| 2026-05-30 | [mode-b-functional-completeness](plans/20260530-mode-b-functional-completeness.md) | Mode-B completeness checklist |
| 2026-06-03 | [mode-b-boundary-step-regen-plan](plans/20260603-mode-b-boundary-step-regen-plan.md) | Boundary-step regen |
| 2026-06-07 | [coverage-baseline](plans/20260607-coverage-baseline.md) | Unit-test coverage |
| 2026-06-09 | [mode-b-cl-rewind-gap](plans/20260609-mode-b-cl-rewind-gap.md) | CL-side rewind gap |
| 2026-06-13 | [experimental-boundary](plans/20260613-experimental-boundary.md) | Experimental boundary statement |
| 2026-06-13 | [proposal-1-txnum-boundaries](plans/20260613-proposal-1-txnum-boundaries.md) | **P1: txNum boundaries** |
| 2026-06-13 | [proposal-2-content-addressed-names](plans/20260613-proposal-2-content-addressed-names.md) | **P2: content-addressed names** |
| 2026-06-13 | [proposal-3-chain-definition-transports-split](plans/20260613-proposal-3-chain-definition-transports-split.md) | **P3: chain definition / transports split** |
| 2026-06-14 | [deep-mode-b-gap-bridging](plans/20260614-deep-mode-b-gap-bridging.md) | Deep mode-B gap bridging |

---

## 16. Open questions for review

Before PR integration to main, these are worth surfacing for collective decision:

1. **Default-flag posture**. Current: V2 is opt-in via `--snap.p2p-manifest`. Should this flip to default-on at some point in the rollout, or stay opt-in indefinitely? The rollout sequence (§10.4) currently says "default preserves pre-V2"; that's a deliberately conservative phase-1 stance.
2. **Adoption policy default**. `--snapshot.adoption-policy` controls how minority verdicts trigger staged adoption. Defaults vary by chain.
3. **Trust-root universe management**. Trust roots are compiled-in per-chain + optionally overridden by `--snapshot.trust-roots`. Trust-root rotation policy is currently "reset the datadir." Long-term we may want a graceful rotation (overlap window where old + new roots both validate).
4. **Webseeds in V2 manifest**. Cloud-operator HTTP fallback isn't yet expressed in V2. Decision: a `[[webseeds]]` section, or a separate sidecar? Likely deferred to P3 (chain-definition/transports split, §13.3).
5. **Reversioning policy**. Mass version bumps (§8.4) must not trigger full re-download. Open: design.
6. **P1/P2/P3 sequencing vs SSZ cutover.** Should the txnum-boundaries + content-addressed-names changes (P1/P2 in §13.3) land before, with, or after the SSZ schema cutover (§13.2)? They're independent in principle, but a single cutover may be operationally simpler than three.
7. **What's the next protocol-level breaking change?** Some open items (external CL re-anchor §8.2, partial-block commitments §8.3) may require coordinated upgrades. Worth flagging now which ones go into a "future protocol-version" bucket vs ones that ship within this branch.
8. **Architectural-anchor promotion** (§14.6). The five user-pinned anchors are referenced from memory but not yet in `docs/plans/`. Promoting them is a small writing exercise but worth deciding the canonical home before PR integration.
