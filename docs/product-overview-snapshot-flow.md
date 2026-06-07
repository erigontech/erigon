# Erigon Snapshot Flow — Product Overview

Network-native chain distribution + operator-grade recovery for the Erigon execution client.

---

## Executive brief

Today's Ethereum nodes typically rely on out-of-band trust for the data behind the consensus head: an operator trusts the binary's embedded preverified hashes, syncs from scratch, or pulls a snapshot from a single source. That works, but it scales poorly and gives any single publisher a wide window where a wrong-or-compromised retire can propagate unnoticed.

This branch of Erigon ships a different model:

- **Chain data is published, advertised, and validated on a peer-to-peer swarm.** Multiple publishers retire blocks independently. Their outputs converge (or diverge — detectably) on the network, not in private.
- **Canonicality is *computed* by consumers, not asserted by a publisher.** A consumer admits an entry into its canonical view only after independently observing it from `Q` distinct authorised publishers. The rule is sybil-resistant (boundary = signing-key identity, not network address) and population-relative (Q scales with the size of the authorised publisher set).
- **A node that fell behind, took the wrong side of a transition, or needs to be rolled back can heal in place.** Minority-publisher detection, staged file replacement, admin-SetHead arbitrary-block unwind, and standard Engine-API CL coordination cover the recovery paths an operator actually needs without "rebuild from scratch."
- **The substrate is reusable.** The same primitives (UCAN trust chain, manifest exchange, validity-bounded advertisements, atomic adoption) become the foundation for forthcoming federated-history networks across clients — not Erigon-specific plumbing.

The features below cover what this branch already delivers (✓), what's in active integration (◐), and the two near-term extensions (✦) that flesh out the model.

## Status

**Local proof-of-concept is near-complete.** The features marked ✓ below run end-to-end on a single-machine swarm and on a live testnet (hoodi): publisher reaches tip, consumers join, validate, and converge; admin SetHead unwinds against real chain data with a spec-compliant CL re-anchor; the substrate components import cleanly without leaking back into the execution stack.

**Next:**
1. **Network testing** — multi-node swarms on real testnets (sepolia, hoodi) with mixed Erigon + external-CL configurations.
2. **Integration into Erigon mainstream**.

Federated-history-network integration (#6) follows both — it consumes the substrate the network-testing + mainstream-integration phases harden.

---

## Network features

These work across any node implementation that speaks the same advertisement and trust model — they're not Erigon-specific.

### 1. Two-UCAN Authentication ✓

Every chain.v2 manifest a publisher emits is bound to two signed delegations so consumers can verify the publisher's authority + the manifest's integrity in one chain.

- **Authority UCAN** (`chain.ucan.authority.<enr-fp>.<rev>.bin`): root trust authority → operator pubkey. Long-lived (months). Carries the capability `snapshot.publish:<chain>`. Info-hash advertised on the manifest as `AuthorityUCANHash`.
- **Content UCAN** (`chain.v2.<enr-fp>.<seq>.ucan`): operator → self. Short-lived (per advertisement generation, expires next regeneration). Capability `chain.v2:hash:<sha256_of_toml>`. Parent-by-hash link to the Authority UCAN.
- **Standard cryptography**: secp256k1, BLAKE-equivalent hash chains; no bespoke crypto.
- **No fallback to unsigned**: the legacy `.sig` sidecar was removed in the same commit the Content UCAN landed. Manifests without a valid UCAN chain are dropped.

### 2. Per-Node Advertisement Manifest ✓

Each publisher emits a self-describing manifest that says "this is the file set I currently hold and will seed."

- **ENR-keyed naming**: `chain.v2.<enr-fp>.<seq>.toml` where `<enr-fp>` = first 16 hex chars of `sha256(enr-bytes)`. Constant-width, peer-unique, not collision-prone.
- **Mandatory generation counter** (`<seq>`): multiple advertisement generations from the same node co-exist on the swarm during publish/republish without aliasing or torrent collisions.
- **Validity-based eviction**: the publisher's local generations stay live as long as their advertised set remains a subset of inventory. No arbitrary count cap.
- **Typed schema**: `BlockFileEntry` for block-range files, `[entries]` map for state-domain files, ENR carries `V2InfoHash` + `MinStep` trailing fields for fast peer discovery without a full manifest fetch.

### 3. Network Quorum + Canonical View (with archive-state SSZ end goal) ✓

Canonicality is what the swarm agrees on, not what any single publisher asserts.

- **Deterministic quorum promotion**: an entry `(name, hash)` is admitted to a consumer's canonical view when it's been observed in ≥ Q distinct **UCAN-authorised** publisher advertisements. Q = `max(Q_floor, ceil(F * N_authorised))` — population-relative, sybil-resistant.
- **Sybil boundary is the UCAN audience pubkey**, not the ENR or peer-id. Minting ENRs is free; minting UCAN audiences that chain to a configured trust root is not.
- **Pinned canonical v0**: every chain has a definite genesis canonical snapshot (a recorded `preverified.toml` content + its hash) so version numbers (`v<N>`) are globally meaningful across binary releases. v0 is a verifiable artefact, not "whatever the binary embedded."
- **Versioned canonical**: `v<N>` = v0 ∪ entries quorum-promoted through the N-th batch. Definite content, independently recomputable, no authority needed.
- **Optional signed checkpoints** (`canonical.v<N>.toml`): cold-start consumers can seed their view from a checkpoint hint but **must independently re-verify every entry to Q**. Checkpoints are a performance hint, never an authority.
- **Archive state model — the schema beneath the wire format**:
  - The chain has two axes (time + value); the canonical archive captures both.
  - Four entry families (Values + History + Indexes + Salt) form the archive state.
  - `archive_root = hash_tree_root(archive_state)`; `archive_state(N+1) = archive_transition(archive_state(N), block(N+1))`.
  - Five transition classes — time, value, retire, merge, aging — cover every state change the model needs to express.
- **Wire format: TOML today, SSZ end goal**: the SSZ migration brings hash-tree-rootability, Merkle-provability of single-entry membership without a full download, and consensus-bindability (so a beacon chain can reference an archive_root directly). TOML stays as a human-inspection print format. Schema, wire, and print are explicitly separated.

### 4. Minority Healing — Staged Canonical Adoption ✓

A publisher whose retire output lost the quorum race (older retire algorithm, transient bug) can rejoin the canonical view without losing exec progress and without operator intervention.

- **Detection splits the self-check verdict**:
  - *Divergence* (a name already canonical, own hash differs) → fatal, log loud.
  - *Minority* (own hash failed to reach quorum past a grace window while a different hash for the same name **did** reach canonical) → non-fatal, triggers staged adoption.
- **Staged download-and-replace**: compute the delta (canonical entries held at a non-canonical hash + canonical entries missing), fetch each by canonical info-hash into `<snapDir>/.staging-<canonical-gen>/`. Never overwrites live files.
- **Per-file validator chain runs on every staged file** before cutover. Any failure aborts the whole batch.
- **Atomic cutover** under one inventory-write lock:
  1. Quiesce block-reader opens for affected ranges
  2. `rename(2)` each staged file over its live counterpart
  3. Rewrite inventory hashes + reset `LifecycleState` to `Downloaded`
  4. Invalidate — drop old torrents, evict stale generations, drop per-peer manifest + segment caches
  5. Publish a fresh generation advertising only canonical hashes
- **In-flight consumers self-heal**: their torrents drop, their downloader retries the canonical info-hash from another peer, their validator rejects non-canonical bytes that arrive anyway.

### 5. Merge Transitions — Native Multi-Canonical ✓

Pre-merge file ranges (`X.0-1024 + X.1024-2048`) and merged ranges (`X.0-2048`) are different names with different hashes — they never collide.

- **Both stay canonical simultaneously**: each form accumulates its own quorum independently. A consumer can validate against either without arbitration.
- **Long-superseded forms GC out**: a form is dropped from the canonical view once zero verified publishers have advertised it for a configurable window (~24h default).
- **No mid-merge wedge**: a swarm mid-transition (some publishers pre-merge, some post-merge) is normal operating state, not an error condition the operator has to resolve.
- **PendingReplacement flag** marks jagged-step transitional files explicitly so the canonical-promotion filter excludes them while the new clean file builds.

### 6. Federated History-Network Substrate ◐

The forthcoming federated-history network across clients (Erigon, Reth, Geth, …) reuses this branch's BitTorrent + UCAN + manifest infrastructure as the distribution substrate.

- **ss2era** (shipped): Erigon snapshot → era format for cross-client distribution.
- **era2ss** + **ss2rpc** (proposed): the reverse path + a query gateway, completing the federation loop.
- **Wire-format SSZ migration matters here**: SSZ entries are hash-tree-rootable + Merkle-provable + consensus-bindable, which is what makes cross-client federation tractable (TOML's per-entry shape isn't).
- **The trust + quorum model carries across**: a federation member's authority is its trust-root membership, not its software stack.

---

## Erigon-specific features

These are how the Erigon binary integrates with and benefits from the network features above.

### 7. Block-Aligned Storage Model ✓

Storage steps align on atomic units (block for the execution layer, slot for the consensus layer) so fork and unwind become standard operations of the model, not separate features.

- **Atomic step boundaries**: a retire-produced file's end matches a block boundary by construction. No "straddled" files in the steady state.
- **Unified `storage.Provider.Unwind`**: snapshot trim + DB unwind + commitment-state-entry insertion happen in one path with consistent semantics. Fork-from CLI shares the same `WriteCommitmentEntryAtBlock` helper.
- **`--snap.block-aligned-boundaries` config flag**: per-chain opt-in during rollout. Legacy non-aligned chains keep working via the diff-replay path (see #8).
- **Consequences fall out by construction**: validator partial-block workarounds, manifest straddle classification, and the merge-straddle path become impossible — not "handled," just absent.

### 8. Admin SetHead (Mode B) + Fork-From ✓

Operators get arbitrary-block unwind without rebuilding from scratch, and a forked chain datadir can be produced from any cut point.

- **`debug_setHead(targetBlock)` works past the diffset horizon**. Mode A (within diffset) uses the existing pipeline unwind; Mode B (past diffset) engages the storage-layer admin path.
- **SD-less commitment recompute primitive**: `RecomputeAtTxNumWithoutSD` rebuilds the patricia trie at any toBlock from a file-side baseline + a history-replay of the touched keys. Bypasses SharedDomains' forward-execution-consistency guards that aren't compatible with mid-tx unwind state.
- **Works on non-aligned chains too**: a boundary-step diff-replay handles cuts that don't land on a file's step boundary. Legacy chains (hoodi as synced from peers with default retire) can be admin-unwound without re-syncing.
- **Transactional FS safety**: snapshot file deletions are deferred to post-tx-commit. A failed or rolled-back attempt leaves the datadir unchanged and retriable. (Caught a real bug pre-this-branch: a failed admin SetHead used to permanently damage the datadir because the FS deletions were irreversible.)
- **Engine-API SYNCING gate during unwind**: the EL flips an atomic flag while the unwind tx is in flight; `forkchoiceUpdated` + `newPayload` return `SyncingStatus` so the CL holds off pushing fresh FCUs. Without this, mode B's quiescence wait would time out under load.
- **Engine-API INVALID + `latestValidHash` after unwind**: when the CL pushes FCU for a head past the EL's new tip, the EL returns spec-compliant `InvalidStatus` with `latestValidHash` = the EL's actual current head. The CL re-anchors there and rebuilds forward. No bespoke Erigon-only signal — works against any spec-compliant CL.
- **`erigon snapshots fork-from <parent-datadir> <toBlock> <fork-datadir>`**: emits a runnable fork datadir at any cut block (aligned or non-aligned). The fork's `chain.Config` carries lineage fields; its V2 manifest carries a `[parent]` section linking back to the parent's `ParentManifestHash` chain.
- **Consumer-side fork enforcement**: `ValidateForkManifestPostCutOnly` refuses parent post-cut bytes on the fork side. Pre-cut bytes are still served from parent (with the parent's hashes preserved); post-cut bytes are fork-only.
- **Per-chain trust roots + `ValidParentTrustRoots` cascade**: a fork picks the parent trust roots it considers valid at creation; verification has a lite mode (fork trust root only) and a belt-and-braces mode (also independent parent UCAN verification).

### 9. State-Lifecycle Hardening (Phase 0) ✓

Nine validator-hardening fixes that closed the parked publisher validation tail before any spec feature went in.

- **Sweep reconciles merge-retired files** (`removeGoneFiles`): inventory state stops drifting away from filesystem reality during merges.
- **Receipt-root boundary-trim**: receipt validator skips partial-boundary blocks instead of failing them.
- **Commitment StateReady gate**: validator waits for state to be loaded before checking commitment — no more "loaded yet?" race.
- **Receipt skip under pruning modes** + **PruneMode wired from config**: the validators' `PruneMode` field was being captured unset; chains running under prune.minimal stopped failing receipt checks that should have been skipped.
- **Deterministic commitment Phase-B gate**: skip below prune horizon. No more "checking commitment for blocks we've intentionally dropped" errors.
- **Commitment merge-transition range-mismatch → pause not fail**: a mid-merge condition that's normal becomes a pause, not a hard error.
- **Disk-scan inventory promotion guard**: a file seen on disk without a `DownloadComplete` event is *unknown*, not `Local`. Closes the "file appeared from nowhere" bug class.
- **Result**: minimal-prune mainnet publisher reached tip with zero spurious validation failures, after a tail of ~450 per-cycle failures pre-fix.

### 10. Componentization ✓

Storage, downloader, manifest_exchange, and snapshotauth live in `node/components/` with **zero imports from `node/eth`**. The same providers can be composed into different binaries.

- **Decoupling is verified**: no import in `node/components/{storage, downloader, manifest_exchange, snapshotauth}/` references `node/eth`.
- **Enables alternate compositions**: history-network binaries (forthcoming) reuse the same providers without dragging in the full execution stack.
- **Backend reduces to wiring**: `node/eth/backend.go` is the integration point; the components themselves are independently testable and replaceable.

---

## Future work

Two near-term extensions that build on the substrate above.

### ✦ Sparse snapshots (virtualization)

Today, downloading or holding a file means downloading or holding its whole range. The "sparse" extension lets a node hold or fetch only the byte ranges it needs.

- **Building block in place**: `HeldRanges` on the storage component already tracks per-file partial-coverage.
- **What's missing**: consumer-side fetch-planning (which peer has which byte-range; how to coalesce range requests across peers).
- **Why it matters**: a node that only needs a specific block range (a fork-follower with a narrow window of interest, a light verifier, a historical-query node) can join the swarm without paying the full-archive disk cost. The trust + quorum model carries over unchanged — only the fetch-planning is new.

### ✦ Manifest-driven indexing — range-split index distribution

The manifest model already covers state-domain files (`.kv`, `.v`) and block files (`.seg`). The natural next step: extend it to **indexes** (account-history idx, storage-history idx, log indexes), which can also be range-split.

- **Why it's an extension**: the same quorum / advertisement / staged-adoption machinery applies. An index file is "just another typed file" in the manifest.
- **What's open**: the distribution model. Indexes have different access patterns from state — typically read-heavy with point lookups across the full chain. The question is whether the right partitioning is by block range (matching state files), by key-prefix range (matching access patterns), or by some hybrid. The distribution mechanism (which peers seed which range, how a consumer finds the range it needs) needs to be designed alongside the partitioning choice.
- **Why now is the right time**: with sparse snapshots landing in parallel, the byte-range + partition-range concerns can be designed together rather than re-litigated.

---

## Where to find this in the code

| Feature | Primary entry points |
|---|---|
| Two-UCAN auth | `node/components/snapshotauth/` |
| chain.v2 manifest | `db/downloader/chaintoml_v2*.go`, `p2p/enr/chain_toml.go` |
| Quorum + canonical view | `db/snapshotsync/canonical_view*.go`, `db/snapshotsync/validate_advertisement.go` |
| Minority healing | `db/snapshotsync/producer_self_check.go`, `node/components/storage/adoption.go` |
| Merge transitions | `db/downloader/chaintoml_v2.go` (PendingReplacement), canonical-view filtering |
| Block-aligned storage | `node/components/storage/provider.go` (`blockAlignedBoundaries`) |
| Admin SetHead mode B | `execution/execmodule/set_head_mode_b.go`, `node/components/storage/provider_unwind*.go`, `execution/commitment/commitmentdb/recompute_sdless.go` |
| Fork-from CLI | `cmd/snapshots/`, `node/components/storage/fork_*.go` |
| Engine-API coordination | `execution/engineapi/engine_server.go` (SYNCING + INVALID/`latestValidHash`) |
| Componentization | `node/components/` (no `node/eth` imports) |

The reference design docs live under [`docs/plans/`](../docs/plans/), notably:

- `20260520-chaintoml-ucan-flow-spec.md` — the chain.toml + UCAN distribution flow
- `20260524-chain-ssz-schema.md` — the SSZ wire-format migration
- `20260525-admin-sethead-unwind-design.md` — admin SetHead + CL coordination
- `20260528-linear-plan.md` — work-order trace for the active sprint
