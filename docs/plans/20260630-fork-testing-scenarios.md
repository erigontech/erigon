# Fork testing — scenario enumeration

**Status**: draft — initial enumeration, 2026-06-30. Ready for review + augmentation before any test rig is built.

**Companion to**: [`20260522-fork-identification-impl.md`](20260522-fork-identification-impl.md), [`20260527-sethead-external-cl-test-rig.md`](20260527-sethead-external-cl-test-rig.md), and the broader [snapshot-flow-status-and-roadmap](../snapshot-flow-status-and-roadmap.md) §3 (network layer) + §4 (recovery layer).

## Why this doc exists

The snapshot-flow branch has working fork **identification** (parent + cut + trust-root metadata in `ChainTomlV2`, fork-authority UCAN cascade, parent-manifest hash) — see `db/downloader/parentcut_*.go`, `node/components/snapshotauth/fork_authority*.go`. What's not yet pinned is a structured test plan that demonstrates the fork-handling code works against the same range of scenarios the unwind soak covers for non-fork chains. This doc enumerates those scenarios so we can:

1. Identify gaps in existing test coverage (unit, integration, harness, soak).
2. Decide which scenarios are blocking for PR integration vs. follow-ups.
3. Build a fork-soak driver paralleling `scripts/unwind-soak.sh`.

## Prerequisite: unwind soak signed off

This work assumes we're about to enter the long-running unwind-soak phase that validates the recovery layer (§4 of the [status doc](../snapshot-flow-status-and-roadmap.md)) end-to-end. Fork testing builds **on top of** working unwind because:

- Most fork-runtime scenarios (Category D — mode-A/A2/B on a fork) are unwind operations performed against a fork-shaped chain. If unwind itself is unstable on the root chain, fork-unwind variance can't be diagnosed.
- The fork-aware soak driver (gap §1.2 below) is a parameterisation of the existing `unwind-soak.sh` machinery: same iter loop, same liveness gates, same CSV output — just pointed at a fork-chain datadir and a fork-configured launcher.
- The soak harness's `inv_extras` / `inv_missing` drift signal is a reused invariant; we want it stable on the root chain before we trust it on the fork chain.

The implied gate for starting fork testing: a clean 5-iter unwind soak (depths up to 60k) passes on the root chain with iter-4 mode_b — currently the prior wedge zone — completing without `errors` or `inv_*` drift. That's the same target the active soak (PID 2852532 at time of writing) is being run against.

## Architectural reminder

A fork chain in this branch is defined by:

```go
chainConfig.Parent             string   // parent chain name (e.g. "mainnet", "sepolia")
chainConfig.CutBlock           uint64   // post-merge EL block where fork diverges
chainConfig.ParentManifestHash [20]byte // info-hash of parent's V2 manifest at cut time
chainConfig.ValidParentTrustRoots []ParentTrustRoot // accepted parent-chain trust roots
```

Plus a fork-authority UCAN that embeds `forked-from:<id>` capability tying the fork to a specific parent trust root.

The `ChainTomlV2.Parent *ParentSection` carries the lineage onto the manifest itself so consumers see it during fork-bootstrap. `RollingV2Publisher.SetForkCutBlock(cutBlock, stepToBlock)` filters pre-cut entries from the fork's manifest.

---

## Fork creation flavours

Before enumerating runtime scenarios, distinguish **how** a fork chain comes into existence. Three fundamentally different paths, each with its own creation mechanics, prerequisites, and failure modes. Post-creation runtime (Categories C / D / E / F below) is largely the same across flavours — but creation correctness (Category A) must test each flavour separately.

**Cross-references to existing design:**
- `memory/synthetic-fork-research-ethpandaops-2026-05-23.md` — maps our work onto ethpandaops' shadow-fork ecosystem; resolves six open questions on parent-cut capture, chain-ID handling, CL artefact scope, system contracts, validator set, and verification.
- `memory/fork-trust-root-model-2026-05-24.md` — per-chain trust roots, lite vs belt-and-braces verification, `ValidParentTrustRoots[]` capture at fork-from time.
- [`20260522-fork-identification-impl.md`](20260522-fork-identification-impl.md) — Phase 1 (identification) implementation plan, including the explicit design decisions §"Converged decisions" below.
- `cmd/snapshots/forkfrom/forkfrom.go` — the existing fork-from CLI, today supports Flavour 1.
- `/erigon/erigon-documents/ethereum/design/erigon-archive/fork-spec.md` — the canonical fork spec (Identification, Directory layout, Cut-point placement).

**Converged design decisions from 2026-05-23** that constrain the flavour catalogue:
1. **No sparse reads as a hard dep** — patch-form files implementable without #20587.
2. **No hardlinks** — fragile across backup/migration/non-POSIX tools.
3. **No multi-stream-on-disk** — each erigon process runs ONE chain config; multi-lineage hosting out of scope for v1.
4. **No in-process fork command** — operator workflow = write derived chain.Config + start erigon on a FRESH datadir; process never pivots identity at runtime.
5. **Chain.Config IS the fork pin** — `Parent`, `CutBlock`, `ParentManifestHash` fields; today's `chain.Config` has no implicit fork-command semantics.
6. **Fresh-datadir mandate** — on a datadir with post-cut parent data, erigon refuses to start. Avoids adapting an in-place running node into a fork.
7. **Post-merge only** — Erigon doesn't support PoW processing; cut blocks must be post-merge so CL-side anchoring works.

These decisions are why Flavour 3 below is flagged as a **reopening** of a previously-closed design question.

### Flavour 1 — Clone-from-source (datadir copy, source untouched)

**Use case**: an operator wants to fork from an existing running parent node WITHOUT affecting it. The source node continues serving the root chain; the new node uses a copy of the source's datadir as its starting state.

**This is what the existing `erigon snapshots fork-from` CLI implements today** ([cmd/snapshots/forkfrom/forkfrom.go](../../cmd/snapshots/forkfrom/forkfrom.go)).

**Mechanics**:
- Capture the parent's cut point (see sub-modes below).
- `BuildCopyPlan`: walk the source's `--parent-datadir`. Files entirely before the cut are copied; straddlers are excluded (the fork retires fresh — per converged decision 7, "straddle step = full self-contained"); post-cut files are skipped.
- `DeriveForkConfig`: from parent's `chain.json`. Keeps parent's `ChainID` for replay protection. Preserves activated forks through the cut; drops future activations. Populates `Parent`, `CutBlock`, `ParentManifestHash`, `ValidParentTrustRoots`.
- Write new datadir at `--new-datadir`. Refuses to clobber a non-empty existing dir.
- Boot a new erigon process with `--chain=<fork-name>` + the new datadir.
- New node's first publish writes a fork manifest naming the parent + cut.
- Source node never observes the fork.

**Two sub-modes for capturing the cut point** (each produces a `parent-cut.json` artefact):

- **1a. Live-RPC capture** (`--parent-rpc <url> --cut-block <N>`): JSON-RPC call to a running parent. Convenience mode — captures live, writes a `parent-cut.json` you can save with `--save-parent-cut`.
- **1b. Frozen-file capture** (`--parent-cut-file <path>`): consume a previously-captured `parent-cut.json`. Deterministic, replayable, offline-safe; this is the authoritative artefact downstream tooling consumes.

These sub-modes produce identical output downstream — they differ only in how the cut point was discovered.

**Test setup**: clone existing soak datadir (or a sub-range of mainnet snapshots) to a new path via `forkfrom`, boot with fork config, validate first manifest.

**Distinguishing characteristics**:
- No network sync required before fork creation (state is already there).
- Cut point can be set to any historical block the source had reached.
- Source node identity (ENR fingerprint) is NOT inherited — the fork node gets its own ENR + own Authority UCAN.
- Parent state is "what was on disk at copy time" — no parent-manifest validation needed locally.

**Risks specific to this flavour**:
- Datadir-copy timing: source must be at a consistent state (no in-flight retire / merge) at the moment of copy.
- Inventory entries inherited from source must be re-keyed under the fork's identity (the fork's chain.toml is not the source's chain.toml).
- The source's chain.toml on disk must be deleted or relocated, so the fork node's first publish doesn't accidentally inherit the source's V2 generation history.
- Straddle files at cut block: per converged decision 7, excluded from the copy plan; the fork retires fresh for the straddle step. Tested via `ValidateForkDatadir`.

### Flavour 2 — Bootstrap-to-fork-point (download-then-fork)

**Use case**: an operator wants to join (or originate) a fork from an existing parent chain without first running a full parent node. The fork is declared upfront via chain.Config; the node bootstraps by downloading parent files up to `CutBlock`, then switches to fork canonicity.

**This is the steady-state path for fork *followers* on the swarm — what most consumers will exercise.** Aligns with converged decision 6 (fresh-datadir mandate).

**Mechanics**:
- Operator authors a derived `chain.Config` (e.g. via `forkfrom` against a parent's chain.json or by hand) with `Parent`, `CutBlock`, `ParentManifestHash`, `ValidParentTrustRoots` set. The chain.toml `[parent]` section mirrors these.
- Boot erigon with `--chain=<fork>` config from an empty datadir.
- Storage component emits `ForkBootstrapRequired` event (see `db/downloader/parentcut_bootstrap.go:IsForkConfig`).
- `manifest_exchange` subscriber fetches parent's V2 manifest by `ParentManifestHash`. Validates per `fork-trust-root-model` lite mode (hash equality + fork-authority UCAN's `forked-from:<id>` is in operator's accept-set).
- Downloader fetches parent files for ranges `[genesis, CutBlock)` (via parent swarm) and fork files for `[CutBlock, tip)` (via fork swarm).
- Read-side routing (per `fork-trust-root-model`):
  - file range ≤ CutBlock → consult parent manifest, fetch via parent swarm
  - file range > CutBlock → consult fork manifest, fetch via fork swarm
  - straddle range → banned (`BuildCopyPlan` / `ValidateForkDatadir`)
- Once both halves are in place, execution proceeds normally.

**Test setup**: empty datadir + fork chain config. Verify the dual-download path; verify the canonical-view aggregator distinguishes parent-quorum (for pre-cut) from fork-quorum (for post-cut).

**Distinguishing characteristics**:
- Network-dependent — at least one parent publisher AND one fork publisher must be reachable.
- Largest creation-time download (entire history).
- Most rigorous validation surface — every category-B scenario exercises here.
- The bootstrapping node may never have seen the parent chain's tip; it only fetches files up to `CutBlock`.
- Per ethpandaops mapping: `ethereum_network_id` (post-cut) ≠ parent's network-id (so devp2p separates them), but `chainId` (EL) = parent's (so signed transactions retain replay protection against parent's history).

**Risks specific to this flavour**:
- B.3 (missing parent manifest) and B.5 (mid-history join) are central concerns here.
- The cut-boundary transition is the moment a consumer's "which trust root vets which file" logic must be correct.
- A parent publisher that has files for ranges past `CutBlock` (which are NOT canonical on the fork) must be ignored for those ranges.

### Flavour 3 — Unwind-to-fork via `debug_setFork` (in-place runtime transition)

**Design status — now feasible.** Decision 4 in `memory/fork-identification-design-pickup-2026-05-23.md` ruled out in-process fork pivoting in May 2026 because the unwind machinery wasn't reliable yet — the atomicity concern was real: *"a partial transition (unwind done but chain config not yet swapped) leaves the datadir in an unrecoverable state."* The current unwind soak (5-iter, depths up to 60k, see [status doc §10.1](../snapshot-flow-status-and-roadmap.md)) is the prerequisite that solves this. With unwind solid, the transition becomes a **single atomic RPC** (`debug_setFork`) rather than a multi-step CLI dance — the same approach `debug_setHead` already takes for unwinds.

**Use case**: an operator running a node on the root chain decides to convert it into a fork-following node WITHOUT stopping the process, without copying the datadir, without reboot. Distinct from Flavour 1 (which copies to a new datadir) and Flavour 2 (which starts fresh) — this keeps the same datadir, same ENR identity, same Inventory, same uptime.

**API placement**:

`debug_setHead` is a cross-client shared call (geth, reth, erigon all expose it with the same semantics: "rewind the local chain to block N"). Changing its shape — adding an optional fork-config parameter — breaks that cross-client contract: a tool that calls `debug_setHead(N)` on any client should keep working unchanged.

The right model is therefore:

- **`debug_setHead`** — unchanged, cross-client compatible. Unwind to N.
- **`debug_setFork`** — new, erigon-specific sibling method. Unwind to (cutBlock - 1) + swap chain identity.

**Co-location in code (not in API shape).** Both methods share the same underlying machinery (the SetHead lock, Mode-A/A2/B unwind path, snapshot trim+rebuild, the unwind-side Caplin anchor walk) — they differ only in what happens *after* the unwind commits:

| Stage | `debug_setHead` | `debug_setFork` | Notes |
|---|---|---|---|
| 1. Acquire SetHead lock | ✓ | ✓ | shared |
| 2. EL unwind (Mode-A/A2/B) to target | ✓ | ✓ | shared — exactly the soak's unwind machinery |
| 3. Caplin **anchor-slot** rewind (unwind side) | ✓ | ✓ | shared — anchorSlot walks down to match new EL head |
| 4. Swap EL chain.Config in-memory | — | ✓ | new |
| 5. Update kvcfg chain-name | — | ✓ | new |
| 6. **Caplin chain-identity swap** (fork side, additive) | — | ✓ | NEW — far more than an anchor reset (see below) |
| 7. Refresh manifest publisher identity | — | ✓ | new |
| 8. Rotate Authority UCAN | — | ✓ | new |
| 9. Release lock + return | ✓ | ✓ | shared |

Code structure:
- [`rpc/jsonrpc/debug_api.go:99`](../../rpc/jsonrpc/debug_api.go#L99) keeps `SetHead`. Add `SetFork` immediately adjacent.
- [`execution/execmodule/set_head_mode_b.go`](../../execution/execmodule/set_head_mode_b.go) stays focused on unwind machinery. Add `set_fork.go` in the same package; it calls into the same Mode-A/A2/B path (stages 1–3) and adds the stages-4–8 epilogue.
- The two RPC entry points share a small internal helper that does stages 1–3.

This pattern parallels how `eth_sendTransaction` and `eth_sendRawTransaction` are siblings — same underlying mempool/exec, different entry shapes.

**Caplin reset is additive — stage 3 vs stage 6.** These are two different things Caplin needs to do:

- **Stage 3 (anchor-slot rewind)** is what setHead already does today: same beacon chain, same config, same bootnodes — just walk `anchorSlot` down to match the new EL head. Implemented `cl/phase1/forward_sync.go:286` (memory pin: `mode-b-cl-rewind-mvi-shipped-2026-06-09`).

- **Stage 6 (chain-identity swap)** is fork-specific and far more extensive. A fork chain has a DIFFERENT beacon chain than the parent. From `memory/synthetic-fork-research-ethpandaops-2026-05-23`:
  - Fresh `GENESIS_FORK_VERSION` / `CONFIG_NAME` / `MIN_GENESIS_TIME` in `config.yaml`.
  - Fresh `genesis.ssz` with the parent's EL block embedded as `execution_payload` (per ethpandaops shadow-fork convention).
  - Different `genesis_validators_root` — drives the BLS domain for every signature on the fork.
  - Reset validator set (regenerated from a mnemonic, since fork operator doesn't have parent's validator keys).
  - Different CL bootnodes (the fork's own discv5 network), `bootstrap_nodes.txt` / `enodes.txt`.
  - Different `fork_version` schedule.
  - Different sentinel + gossip topic prefix (`/eth2/<fork-digest>/...`).

  All of this is what Phase 2c-CL was deferred to (the May plan). Until those artefacts land, `debug_setFork` cannot actually advance the fork past the cut block — the new Caplin won't be able to verify blocks against the new beacon chain.

**Implication for API shape and sequencing:**

The `debug_setFork` RPC needs to either:

- **(a)** Carry the CL artefacts inline (`clConfigYaml`, `genesisSsz`, `clBootnodes`, `validatorMnemonic`). Big payload, but atomic.
- **(b)** Reference a pre-staged directory on disk (`metadata/<fork-name>/`) containing `config.yaml` + `genesis.ssz` + `bootstrap_nodes.txt` + mnemonic — mirroring the ethpandaops `metadata/` convention. RPC just supplies the path. Smaller payload, requires operator pre-staging.

Option (b) reads cleaner and matches existing ecosystem patterns. The operator runs `erigon snapshots fork-from --out-metadata <path>` (extended to also emit CL artefacts under Phase 2c-CL) once, then any node can `debug_setFork` to transition by pointing at that metadata directory.

**Sequencing implication for the test plan**: Flavour 3 (the `debug_setFork` flavour) cannot ship before Phase 2c-CL lands. Today's `cmd/snapshots/forkfrom` is EL-only — there's no CL artefact pipeline yet. The dependency chain is therefore:

1. **Current**: unwind soak signs off (in progress) — proves the shared stages 1–3 are reliable.
2. **Then**: Phase 2c-CL ships — CL artefact pipeline + manifest carries `Parent.GenesisValidatorsRoot` + `Parent.ForkVersion` etc.
3. **Then**: `debug_setFork` is implementable — stages 4–8 land with stage 6 having concrete CL pieces to swap in.

Phase 2c-CL has been the load-bearing prerequisite for "fork that can actually run" since the May design. With unwind nearly signed off, it's now the path-critical work for Flavour 3.

**Proposed shape**:

```jsonrpc
debug_setFork({
    "chainName":          "mainnet-fork-23760000",   // becomes new --chain
    "parent":             "mainnet",                  // parent chain name
    "cutBlock":           23760000,                   // CutBlock
    "parentManifestHash": "0xab12...",                // 20-byte infohash hex
    "validParentTrustRoots": [...],
    "newChainConfig":     {...}                       // full chain.Config OR a path
})
```

Unwind target is derived as `cutBlock - 1` inside the method — the caller specifies the cut, not the unwind target (those would diverge only on off-by-one boundary mistakes, so deriving it removes that footgun).

Single atomic call. Internally:

1. Compute unwind target — typically `cutBlock - 1` (so post-transition the node's head IS at `cutBlock - 1`, ready to accept fork blocks from `cutBlock` onward).
2. Acquire the SetHead lock (same one `debug_setHead` uses — serialises against retire/merge/exec).
3. Run `setHead(target)` — uses the existing Mode-A/A2/B machinery. **If unwind fails, the whole RPC fails; nothing has been mutated yet.**
4. With unwind successful and the lock still held:
   - Swap the in-memory `chain.Config` to the new fork config.
   - Update `kvcfg.SnapP2PManifest` etc. for the new identity (the snap-mode flags carry over but `kvcfg` chain-name key updates).
   - Re-derive `GenesisFork`, `Forks []ForkActivation`, fork-ID filter from new config.
   - Update Authority UCAN scope (new chain name); operator must have a UCAN minter that can issue for the fork OR the RPC supplies the new Authority UCAN in the parameters.
   - Refresh manifest publisher with new identity (new `chain.v2.<enr-fp>.<seq>.toml` generation #1).
   - Rotate the V2 publisher's `lastV2InfoHash` (start new generation series).
5. Release lock. Return success.

After return, the node is now a fork-mode node: standard Flavour-2-style forward sync (from `cutBlock` onward) takes over via the existing manifest_exchange + downloader path. No restart, no new process.

**Why this works now (and didn't in May)**:

- Unwind is reliable (the current soak is its sign-off — depths up to 60k cleanly).
- `setHead` already returns a hard error on failure — no partial unwind state is committed (the soak run confirms this).
- The single-RPC scope means there's no "process restart" window where state could go inconsistent.
- The `kvcfg` change-detection mechanism (§4.4 of status doc) was designed for startup-time invariants; the runtime config-swap path bypasses it because it IS the authorised mutation (the persistence is updated INSIDE the RPC as part of the same transaction).

**Test setup**: a working root-chain soak datadir + the new `debug_setFork` RPC. Run transition. Verify the post-transition state matches what a Flavour-1 clone (at the same block) would produce. Verify the node continues running without restart.

**Distinguishing characteristics**:
- **Single RPC**, no restart. The atomicity argument from May is solved.
- Depends on a working unwind machinery — especially Mode-B if `CutBlock` is in snapshot territory.
- Lossy: the root-chain post-cut blocks the source had are discarded.
- Identity carries over: **same ENR fingerprint**, same Inventory entries (re-keyed under fork chain.toml). Different from Flavour 1 where identity is fresh. The Authority UCAN MUST rotate (new chain scope).
- Operator-facing UX is the simplest of the three — `curl -X POST … debug_setFork …` then keep using the node.
- This is what an `assertoor` / `kurtosis` playground would call after spinning up a parent-chain devnet — no second instance needed.

**Risks specific to this flavour**:
- All Category D scenarios apply at fork-creation time (the unwind step IS the fork creation).
- D.4 (Mode-B at-cut boundary) is the most relevant variant — `cutBlock - 1` is the natural unwind target.
- ENR/UCAN: the running node's Authority UCAN was for the parent chain. A fork-mode node needs a NEW Authority UCAN scoped to the fork chain. The RPC needs to either (a) require the operator to pass the new Authority UCAN in the request, or (b) issue a fork-specific Authority UCAN automatically (requires the running node's signing key has `forked-from` capability authority).
- Multi-component shutdown coordination: the in-process Caplin, the downloader's chain-identity, the publisher's generation counter all need to flip atomically. Single SetHead lock acquisition is the boundary.
- Persisted snap-mode flags carry over correctly; chain-name persistence is the only kvcfg field changing.

**Implementation footprint (estimate)**:
- New `rpc/jsonrpc/debug_api.go:SetFork(...)` handler. Wraps the existing `SetHead` path + a chain-config swap. Maybe 200-300 lines.
- New `execution/execmodule/set_fork.go` — the engine-side implementation, mirrors `set_head_mode_b.go` shape. Maybe 300-500 lines.
- Tests: the existing `unwind-soak.sh` machinery extends naturally — replace `debug_setHead` invocation with `debug_setFork` to a fork chain config. Same liveness gates, same CSV signal.

**Decision needed**: green-light `debug_setFork` design + implementation? The proposed shape above is one reading; alternatives include "make it an `admin_` RPC instead of `debug_`", "require an out-of-band UCAN mint vs inline", "make `chainName` a registered config vs an inline blob". Each is a small design choice but worth pinning before code lands.

### Flavour 4 — Fork-from-genesis (degenerate case)

**Use case**: a chain that diverges from another chain at genesis (i.e. `CutBlock == 0`). The fork shares genesis hash but has different chain.Config from block 1 onward. Most pre-merge testnets vs mainnet are technically this shape.

**Mechanics**: degenerate Flavour 2 — boot with `--chain=<fork>` on empty datadir, no parent file download required (no pre-cut range exists). All files are fork-local.

**Distinguishing characteristic**: this is the trivial case worth confirming the machinery handles cleanly. The `[parent]` section is present but covers an empty range. UCAN cascade still applies (the fork-authority UCAN still embeds a parent trust root, even if no parent files are consulted).

**Not separately catalogued in the scenario tables below** — treat as a "boundary value" instance of Flavour 2.

### Coverage implications

Each flavour requires its own Category-A correctness tests (A.6 expands into A.6.1 / A.6.2 / A.6.3). The runtime/security/operational categories (B–F) are largely shared post-creation — but a few scenarios are flavour-specific:

- B.5 (mid-history join) is most rigorous under Flavour 2.
- D.5 (mode-B before-cut) is the natural failure to test against under Flavour 3.
- F.1 (publisher restart identity) is more sensitive under Flavour 1 (because identity wasn't established via fresh ENR generation) than under Flavours 2/3.

These flavour-specific bindings are noted in the scenario rows below.

---

## Test scenario catalogue

Each scenario has: ID, name, what it exercises, fail signal, and current coverage status. Status legend:

- ✓ Covered (unit + integration / harness OR live-network)
- ◐ Partial (e.g. unit-only, lacks integration)
- ✗ Not covered
- 🤔 Open design question (test depends on a not-yet-decided behaviour)

### Category A — Fork creation correctness

Tests that the publisher side of a fork emits correct, verifiable manifests at fork creation.

| ID | Scenario | Exercises | Fail signal | Status |
|---|---|---|---|---|
| A.1 | **Aligned cut** | `CutBlock` lands on a 1k snapshot boundary. Resulting manifest has clean step coverage. No `PendingReplacement` entries. | Manifest contains pre-cut entries OR file ranges don't align cleanly to cut. | ◐ unit (likely in `db/downloader/parentcut_publish_filter_test.go`) |
| A.2 | **Non-aligned (jagged) cut** | `CutBlock` lands mid-1k-chunk. First retire produces transitional jagged-step file with `PendingReplacement=true`. Subsequent retire produces an aligned file that supersedes it. | Manifest never reaches a stable, aligned state OR `PendingReplacement` entry promoted to canonical view. | ◐ unit, no soak |
| A.3 | **Trust-root capture at fork-from time** | The operator's `--snapshot.trust-roots` value at fork-from time gets baked into `ValidParentTrustRoots[]`. Immutable after fork creation. | Trust roots silently reflect operator's current config, allowing post-fork rotation to invalidate the lineage. | ✓ `execution/chain/parent_trust_root_test.go` |
| A.4 | **Fork-authority UCAN binds to specific parent trust root** | The `forked-from:<id>` capability identifies which `ValidParentTrustRoot` vetted `ParentManifestHash`. A consumer can reject a fork whose UCAN claims a trust root not in its parent's authorised set. | UCAN missing `forked-from`, OR claims a trust root the parent never used. | ◐ `node/components/snapshotauth/fork_authority_cascade_test.go` covers cascade; the binding-at-fork-from path needs a focused fixture |
| A.5 | **Pre-cut file filter on publish** | Fork publisher MUST NOT advertise files whose range falls before `CutBlock` (those are parent canonicity). The `SetForkCutBlock` filter drops them. | Fork manifest lists pre-cut block / state file. | ✓ `db/downloader/parentcut_publish_filter_test.go` |
| A.6 | **Fork publisher cold-start** | Brand-new datadir + `--chain=<fork>` config + the operator's `cmd/snapshots/forkfrom` CLI output. First manifest is known-good (publisher-startup-preflight) AND fork-shape-correct. | Empty manifest OR mis-shaped (missing Parent section, wrong CutBlock). | ✗ no integration test |

### Category B — Fork consumer bootstrap

Tests that a consumer joining a fork chain validates the UCAN chain correctly and bootstraps via the parent's manifest.

| ID | Scenario | Exercises | Fail signal | Status |
|---|---|---|---|---|
| B.1 | **Cold-start follower validates UCAN chain** | A consumer with empty datadir, `--chain=<fork>` config, sees a fork peer's manifest. Validates the UCAN chain: fork content UCAN → fork authority UCAN → parent's trust root in `ValidParentTrustRoots`. | UCAN chain validates with a trust root NOT in `ValidParentTrustRoots[]`. OR rejects a valid chain. | ✓ `node/components/snapshotauth/fork_authority_cascade_test.go` |
| B.2 | **Parent-manifest hash mismatch** | A peer advertises a fork manifest whose `Parent.ParentManifestHash` doesn't match the consumer's expected hash. Manifest rejected. | Wrong-parent manifest accepted. | ◐ unit only; integration TBD |
| B.3 | **Missing parent manifest on swarm** | Fork consumer wants to fetch parent's V2 manifest by `ParentManifestHash` but no swarm peer serves it. Recovery: bootstrap-from-preverified OR fail fast with a clear error. | Hangs silently on missing parent. | 🤔 design question — what's the right fallback? |
| B.4 | **Parent trust-root rotation** | The parent chain's trust roots rotated after the fork was created. The fork's `ValidParentTrustRoots` still includes only the old root. A peer-advertised parent manifest signed under the new root should be REJECTED (the fork's lineage is anchored to its original parent root, not the parent's current roots). | Fork consumer accepts parent manifests signed by a root it never authorised. | 🤔 unit + design pin needed |
| B.5 | **Mid-history join** | Consumer joins after fork has been running for many blocks. Needs both parent files (up to `CutBlock`) and fork files (from `CutBlock` onward). | Consumer requests files from wrong chain or misses cut boundary. | ✗ no integration test |
| B.6 | **Restart after partial parent sync** | Consumer crashed/restarted while still downloading parent files. On restart, distinguishes "parent files in flight" from "fork files in flight" and resumes correctly. | Restart re-downloads everything OR drops parent files. | ✗ |

### Category C — Runtime sync (forward)

Tests the steady-state forward sync of a fork chain.

| ID | Scenario | Exercises | Fail signal | Status |
|---|---|---|---|---|
| C.1 | **Genesis-to-tip on fork** | Full sync of a fork chain from its genesis canonical v0 through cut block through current tip. End-to-end fresh-datadir test. | Sync stalls at cut boundary, OR drops fork-specific config (e.g. fork's CL genesis-validators-root). | ✗ |
| C.2 | **Multiple publishers on a fork (quorum convergence)** | Two+ publishers on the same fork chain. Consumer's canonical view converges via quorum from UCAN-authorised fork publishers only — parent-chain publishers are NOT counted. | Parent-chain publishers contribute to fork's quorum count (sybil-cascade bug). | ◐ unit, no live-multi-node test |
| C.3 | **Mixed parent + fork publisher set** | The same swarm has publishers for parent chain AND publishers for the fork. A consumer of the fork distinguishes the two — uses parent publishers for pre-cut files only, fork publishers for post-cut. | Consumer downloads post-cut files from a parent publisher (which doesn't have them) and stalls. | ✗ no integration test |
| C.4 | **Fork-of-a-fork** (depth ≥ 2) | A fork chain itself has a fork. The grandchild's `Parent` is the child; UCAN chain validates fork → fork → parent. | Cascade collapses or terminates early. | 🤔 explicitly deferred per fork-identification-impl.md? — confirm. |

### Category D — Unwind / recovery on a fork

The mode-A / mode-A2 / mode-B framework must work on fork chains. Some cases have parent-vs-fork crossings that are unique.

| ID | Scenario | Exercises | Fail signal | Status |
|---|---|---|---|---|
| D.1 | **Mode-A on fork** (within changeset) | `setHead(target)` where `target > CutBlock` AND `target ≥ minUnwindableBlock`. Standard mode-A path. | Same as for non-fork. | ✗ scenario not yet in soak |
| D.2 | **Mode-A2 on fork** (past changeset, within DB) | Same constraint: `target > CutBlock`. State-unwind via commitment recompute, no snapshot trim. | Same as for non-fork. | ✗ |
| D.3 | **Mode-B forward-of-cut** | `target > CutBlock` but `target ≤ frozenBlocksTip`. Snapshot trim within fork-local files only. Parent files untouched. | Trim touches parent files OR refuses to trim because it thinks the target is invalid. | ✗ |
| D.4 | **Mode-B at-cut boundary** | `target == CutBlock`. Boundary case — the last fork-local block is removed; consumer state matches parent's state at `CutBlock`. | Off-by-one — leaves a stray fork-local file OR removes one block too many. | ✗ |
| D.5 | **Mode-B before-cut** | `target < CutBlock`. The consumer is unwinding back into parent territory. Requires parent files to be present (or fetchable). Effectively a switch back to parent canonicity. | Unwind succeeds without parent files, leaving consumer with a gap. OR refuses to unwind across the cut without explicit operator confirmation. | 🤔 design question — is this allowed? |
| D.6 | **Mode-B with jagged-step cut** | A non-aligned fork (A.2). Unwind targets land near the `PendingReplacement` zone. The transitional file's range must be honoured by sweep. | Trim treats jagged file as canonical, leaving an inconsistent state. | ✗ |
| D.7 | **Successive unwinds across cut boundary** | A soak-iter that goes mode-B forward → mode-B at-cut → mode-B before-cut → forward-sync past cut again. Verifies the lineage transition is reversible. | Hysteresis: post-cycle state ≠ initial state. | ✗ |

### Category E — Adversarial / security

Tests that the fork machinery resists invalid input, both malicious and accidental.

| ID | Scenario | Exercises | Fail signal | Status |
|---|---|---|---|---|
| E.1 | **Fork-publisher emits pre-cut entries** | A buggy or malicious fork publisher advertises pre-cut files (claiming parent canonicity). Consumer's `SetForkCutBlock` filter MUST drop them OR consumer must reject the manifest entirely. | Pre-cut entry survives into the canonical view. | ✓ `parentcut_publish_filter_test.go` (publisher-side); consumer-side ✗ |
| E.2 | **Wrong parent declared** | Peer advertises a manifest with `Parent.Parent="mainnet"` but lists files clearly NOT from mainnet's lineage. Consumer must reject. | Accepted as valid fork of mainnet. | 🤔 — what's the check beyond ParentManifestHash mismatch (B.2)? |
| E.3 | **UCAN signed by a trust root NOT in ValidParentTrustRoots** | UCAN chain technically valid (signature + parent links) but the root pubkey isn't on the fork's accepted-parent list. Reject. | UCAN passes despite mismatched root. | ◐ unit (`fork_authority_cascade_test.go`); integration ✗ |
| E.4 | **Replay of expired Content UCAN** | A Content UCAN from a previous generation re-served as the current one. Consumer rejects on expiry / generation-mismatch. | Stale UCAN accepted. | ◐ unit |
| E.5 | **Fork-authority UCAN with malformed `forked-from`** | Capability string format wrong (missing prefix, malformed trust-root ID). Reject. | Malformed UCAN accepted. | ◐ |

### Category F — Operational / churn

Tests that fork-handling tolerates the messy realities of long-running deployments.

| ID | Scenario | Exercises | Fail signal | Status |
|---|---|---|---|---|
| F.1 | **Fork publisher restart preserves identity** | Restart with same `--chain=<fork>` config. Authority UCAN, Parent section, `ValidParentTrustRoots` all unchanged. First post-restart manifest is byte-identical to last pre-restart (or differs only in `<seq>`). | Identity drifts on restart. | ✗ |
| F.2 | **Long-running fork: Authority UCAN expiry / rotation** | The Authority UCAN expires mid-run. Operator rotates. Existing followers re-validate via the new UCAN; new followers join via the new UCAN. | Rotation breaks followers OR existing followers don't notice. | ✗ |
| F.3 | **Fork follower with stale parent state** | The fork chain has advanced; a long-paused follower restarts. Parent state is still at the original cut block. The follower correctly applies fork-local advances without re-fetching parent files. | Re-fetches everything OR can't proceed because of parent-state mismatch. | ✗ |
| F.4 | **Parent chain hard-forks (new continuous fork added to its schedule)** | The parent chain's `Forks []ForkActivation` gains a new entry. The fork's `Parent.Forks` snapshot at cut time stays fixed. Consumer correctly distinguishes "parent has a new fork" (informational, fork's lineage anchor unchanged) from "parent's lineage at cut block changed" (would invalidate the fork). | Mis-classifies parent-side schedule changes. | 🤔 |
| F.5 | **Two forks compete from same cut block** | Two operators independently create fork chains from the same parent + same CutBlock + same ParentManifestHash. Different post-cut histories. Their manifests are valid in isolation; a single consumer must run one or the other (different `--chain` configs). | Two-fork manifests confused by the consumer. | ✗ |

---

## What's testable today vs. needs new harness

**Existing unit/integration coverage** (run via `go test ./db/downloader/... ./node/components/snapshotauth/... ./execution/chain/...`):

- A.1, A.3, A.5: covered
- A.2, A.4: partial
- B.1, B.2: covered (cascade) + unit
- E.3, E.4, E.5: unit

**Gaps requiring new harness**:

1. **Fork bootstrap integration test** — a two-process test: publisher cold-starts on a fork; consumer cold-starts on the fork and validates the UCAN chain end-to-end. Maps to B.1, B.2, B.3, B.5, B.6 + A.6.
2. **Fork-aware soak** — extend `scripts/unwind-soak.sh` to drive the same iter cycle against a fork chain. Adds scenarios D.1–D.7. The soak datadir would need to be a fork rather than hoodi root.
3. **Multi-node fork harness** — extend the existing multi-node harness (`node/components/integration/snapshot/harness/`) with fork topology: parent publisher + fork publisher(s) + fork consumer(s). Covers C.2, C.3, F.1, F.5.

## Design questions to resolve before writing the harness

These are flagged 🤔 above; restating here so they don't get lost.

1. **B.3 — missing parent manifest fallback.** What does a fork follower do when no peer serves the parent's V2 manifest by `ParentManifestHash`? Options: (a) hard fail with a clear error, (b) fall back to bootstrap-from-preverified on the parent chain, (c) wait indefinitely. Each has a different operational contract.

2. **B.4 — parent trust-root rotation.** When a parent chain rotates its trust roots, are previously-created forks invalidated? Or do they continue to be valid because the rotation post-dates fork creation? My read of the model: fork creation captures `ValidParentTrustRoots` immutably, so the answer is "previously-created forks continue to be valid" — but we should explicitly state this and test it.

3. **C.4 — fork-of-a-fork.** The current fork-identification implementation plan explicitly says depth-1 only. Confirming: do we test that depth-≥2 is REJECTED (which would be a different test), or that depth-≥2 is allowed (which would need additional code)?

4. **D.5 — mode-B before-cut.** Is unwinding a fork back across its cut boundary allowed? The conservative answer is "no — switching back to parent canonicity is an operator decision (`erigon` with `--chain=parent`), not an in-process unwind." The permissive answer is "yes, and the unwind machinery quietly switches the consumer to parent canonicity." Need to pick one.

5. **E.2 — wrong-parent detection beyond hash mismatch.** Is the `ParentManifestHash` check (B.2) sufficient on its own? Or do we want additional cross-checks (genesis hash, fork ID derivation) before trusting a fork's lineage claim?

6. **F.4 — parent forwards-compatibility.** When a parent chain adds a new continuous fork to its schedule (Shanghai+1, Cancun+1, etc.), is the fork's lineage unaffected? Likely yes (the cut-block + ParentManifestHash anchor an immutable point) but worth pinning.

## Proposed sequencing

Build flavour-by-flavour. Each flavour exercises a strict superset of the previous one's surface — Flavour 1 isolates "does the fork shape work" from network behaviour; Flavour 2 layers in dual-download + manifest exchange; Flavour 3 adds runtime mutation + Caplin chain-identity swap. Bugs found in earlier flavours block later ones, so going in order keeps the diagnostic surface manageable.

### Phase F-0 — Foundation (parallel with current work)

Doesn't block on a flavour. Can start now.

- **Pin the design questions** (§"Design questions to resolve…"). Short doc updates or AskUserQuestion sessions. ~2 days.
- **Close partial unit/integration coverage**: A.2, A.4, B.4, E.1 (consumer side), E.2, E.3, E.5. Most of these are tests against existing code paths, not new code. ~1 week.

### Phase F-1 — Flavour 1 (clone-from-source) signs off

Smallest blast radius. The existing `cmd/snapshots/forkfrom` CLI already implements the EL side. Everything is explicit on disk before the fork node starts; no network discovery in the loop.

- **Fork-bootstrap integration test from a copied datadir** (covers A.6 via Flavour-1 path + scenario A.1, A.2, A.5). Two-process test: produce fork artefacts via `forkfrom` from a parent datadir, boot the fork node, validate first manifest. ~1 week.
- **Fork-aware unwind soak on a Flavour-1 datadir** (D.1, D.2, D.3, D.4, D.6). Same soak driver as today, pointed at a fork datadir produced by `forkfrom`. ~1 week. **This is the natural extension of the current unwind-soak machinery.**
- **Phase 2c-CL artefact pipeline lands** here as a prerequisite for fork nodes to advance past `CutBlock`. Without it, the fork is a static snapshot; with it, the fork can produce blocks via its own Caplin.

Phase exit gate: a Flavour-1 fork on hoodi-shadow can sync, can be unwound (modes A/A2/B), and the unwind soak iters pass cleanly with no fork-specific surprises.

### Phase F-2 — Flavour 2 (bootstrap-to-fork-point) signs off

Adds the network side: manifest exchange discovers parent + fork publishers, downloader fetches across the cut boundary, canonical view distinguishes parent-quorum from fork-quorum.

- **Cold-start follower integration test** (covers B.1, B.2, B.5, B.6 + C.1). Empty datadir + fork chain config, observe a fork peer's manifest, validate UCAN chain end-to-end, sync to tip. ~1-2 weeks.
- **Multi-node fork harness** for the cross-fork-publisher-set scenarios (C.2, C.3, F.1, F.5). Extends Phase 2g harness with fork topology. ~2 weeks.
- **Adversarial / security tests** (E.1 consumer side, E.2). Easiest to drive against a Flavour-2 setup where artefacts come from the network, not the local disk. ~1 week.

Phase exit gate: a Flavour-2 cold-start consumer on a multi-publisher fork swarm reaches tip + survives publisher churn without re-fetching parent files unnecessarily.

### Phase F-3 — Flavour 3 (`debug_setFork`) signs off

Most potential failure modes. Depends on Flavour 1 + Flavour 2 being solid (because a `debug_setFork` transition is essentially "run Flavour-1-style transition + Flavour-2-style forward sync, atomically"). Also depends on Phase 2c-CL being shipped for the CL chain-identity swap to work.

- **Design + implement `debug_setFork`** per the spec in Flavour 3 section. ~2-3 weeks (200-500 lines of RPC + engine code + the Caplin chain-swap plumbing).
- **Single-node `debug_setFork` test** (covers D.5, D.7 + the transition itself). Issues the RPC against a running root-chain node, verifies the node continues running as a fork node. ~1 week.
- **`debug_setFork` under churn** (F.2, F.3, F.4 fork-specific variants). The mid-life chain-identity swap is the FIRST authorised runtime mutation of a snap-mode-persisted flag — exercises an entirely new code path in the persistence machinery. ~1-2 weeks.

Phase exit gate: a running root-chain node can be transitioned to a fork via a single `debug_setFork` call, without restart, and continue operating cleanly for an unwind-soak-style iter cycle on the now-fork chain.

### Total

Roughly **6-9 weeks** of focused work end-to-end. Phase F-1 alone is ~3 weeks and validates ~half the scenario catalogue. Phase F-3 is the longest pole and slips if Phase 2c-CL slips. The build order is strict — Flavour 3 cannot ship before Flavour 1 + 2 are solid, because Flavour 3 IS a composition of their machinery.

## What this doc is NOT

- A list of bugs. None of the scenarios above are known bugs; they're testable behaviours that need confirmation.
- A test-implementation plan. Each scenario needs a separate "how do we set this up" decision (real testnet vs. devnet vs. simulator vs. unit fixture).
- A protocol spec. Fork identity / manifest format are specified elsewhere; this doc references them but doesn't redefine them.
- A finished enumeration. The categories + scenarios above are a starting framework. Real testing will reveal scenarios we didn't list, just like the unwind soak surfaced wedges no upfront enumeration anticipated.

---

## Methodology — what we learned from the unwind soak

The unwind soak signs off the recovery layer empirically, not by upfront proof. We enumerated scenarios (mode-A / mode-A2 / mode-B at five depths × five iters), ran them, and each iteration revealed something the enumeration didn't predict. Fork testing should expect the same shape: the catalogue above is the starting framework; the real coverage comes from running it and capturing the surprises.

Concrete patterns from the unwind work, all directly applicable to fork testing:

### 1. Liveness signals need many sources, not one

Head-only polling false-failed deep mode-B recoveries because head pins at target for tens of minutes while exec catches up. The fix took multiple iterations: head → +DL progress → +BlockCollector inserts → +Execution blk → +chain.toml regens → finally **log byte growth** as a universal "is erigon alive" signal. Plus a soft-wedge abort that catches the OTHER failure mode: log growing but with repeated error-pattern spam.

**Fork analogue**: a Flavour-3 `debug_setFork` transition will look like a deep mode-B recovery + identity swap. Head will pin at `cutBlock-1` for the whole window. Then there's a moment when the chain identity flips and the publisher starts emitting under the new identity. Naïve "is erigon at live tip yet" checks will mis-classify the whole transition window. Expect to extend the liveness gate to recognise "transition in progress" as alive.

### 2. Each surprise becomes a pinned fixture

The iter-5 narrow-straddler wedge, the 2026-06-25 v2.0-accounts union-cover wedge, the iter-4 Inventory write-through gap — none of these were anticipated. Once understood, each became a deterministic unit test (the 13 tests in `db/snapshotsync/fileset/rules_test.go`). The rules module's correctness is now verified not by argument but by every historical wedge being a fixture.

**Fork analogue**: when fork testing surfaces "the cut-boundary file straddle was handled differently on Flavour 1 vs Flavour 2" or "Caplin's anchor walked too far past the cut block" — those become fork-specific fixtures, not narrative postmortems. Build the fork test surface so adding a fixture is cheap.

### 3. "Inventory is the point of truth" (and the same for every dataset we publish)

Every disk-mutating path must update Inventory synchronously. Disk-scan reconciliation as a recovery mechanism is allowed (the sweep self-heal exists) but isn't the primary update path. The iter-4 wedge was Inventory drifting because the straddle rebuild wrote files via `seg.NewCompressor` without calling `Inventory.AddFile`.

**Fork analogue**: the fork's V2 manifest, the parent-manifest cache, the `parent-cut.json` artefact, the UCAN chain — each is a "published dataset" with the same invariant. If a code path mutates the on-disk artefact without updating the in-memory truth source, drift accumulates and downstream consumers read stale data. The same `*_test.go` shape that pins Inventory write-through should pin each of these.

### 4. Persistence is for change-detection, not omission

Three V2 mode flags are persisted in MDBX via `EnsureNotChanged`. Today's `debug_setFork` proposal needs to update one of those (chain-name) at runtime, which is the FIRST authorised mid-life mutation of a snap-mode flag. The persistence contract was designed assuming startup-only mutation; fork transitions break that assumption deliberately. Tests must confirm the runtime mutation path:

- Doesn't trip the change-detection guard at the next restart (the persisted value should match the in-memory value continuously, so the post-fork CLI value matches persisted).
- Is itself atomic — same SetHead lock that the unwind holds.
- Survives a crash mid-transition correctly (recovery on restart: did the new chain identity persist? If yes, replay forward. If no, treat as if transition never started.).

### 5. Multi-node + churn surfaces bugs that single-node misses

Memory pin: `feedback-multi-node-hardening`. The single-publisher / single-consumer soak we just ran caught the publish gap, the file-set drift, and the inventory write-through bug — but it CAN'T catch quorum-aggregation bugs, sybil-cascade bugs, or the cross-fork-publisher-set bugs (Category C). Those need a real swarm with N publishers + M consumers.

**Fork analogue**: Categories C, E, and F above largely cannot be exercised by a single-node soak. The Phase 2g harness (which currently has a 20-publisher target per `memory/feedback-quorum-test-harness`) is the right vehicle. Adding a fork-topology mode (parent publishers + fork publishers + consumers crossing the cut boundary) is the next harness extension, after fork-bootstrap integration works.

### 6. Real-network surprises encoded back as deterministic scenarios

Memory pin: `feedback-harness-testing-methodology`. "Simulate-first; real-network surprises encoded back as deterministic scenarios." Live testnet runs catch what fixtures don't (timing, peer diversity, sentry behaviour under load) — but every wedge from a live run should land as a fixture in the test surface so the same wedge can't sneak back.

**Fork analogue**: when fork testing gets to live testnets (hoodi shadow-fork, ethpandaops devnets), every wedge becomes an addition to this scenario catalogue + a fixture. Treat the live runs as exploratory; treat the fixtures as the binding contract.

### 7. Don't silently downgrade

Memory pin: `feedback-multi-node-hardening`. Today's session burned 2 hours on a soak that silently ran in non-V2 mode because the launcher omitted `--snap.p2p-manifest`. The publish path was being triggered but the fingerprint guard was rejecting every emit — visible only in error logs nobody was watching. **The fix tightened the gate so the publish path doesn't even ATTEMPT to run when the flag is off**.

**Fork analogue**: a fork node missing one of its CL artefacts (`config.yaml` present but `genesis.ssz` missing, or vice versa) should refuse to start, not silently fall back to parent canonicity. Every Flavour-3 transition step needs a hard-fail check, not a "warn + continue" path. The fork-bootstrap pre-flight (`memory/publisher-startup-preflight-2026-05-22`) is the model.

---

## How to apply this to fork testing

Concretely, before building the fork harness:

1. **Build the test scaffolding so adding a scenario is a one-liner.** The `rules_test.go` pattern (one named fixture per scenario) is the model. A fork-soak driver should accept a `--scenario <name>` flag that selects which fork shape + which operations to run.

2. **Wire the soak signal stack early.** Reuse `unwind-soak.sh`'s log-bytes-growth + soft-wedge gates. Add new signals as fork-specific wedges surface (e.g. "Caplin chain-identity swap progress" if Flavour 3 reveals a window where head + log are both quiet but the CL is mid-reset).

3. **Pin each surprise as a fixture immediately.** Don't let a surprise live only in a postmortem; the deterministic test is the only artefact that prevents it from coming back.

4. **Expect the same iteration shape as unwind**: enumerate → run → surprise → diagnose → fix → fixture → re-run. The wall-clock for this loop on a fork-soak iter will be similar to the unwind soak (single iter cycle minutes to an hour; full soak hours; surprise-investigation hours-to-days). Plan capacity accordingly.
