# PBT dual commitment: write hex and binary tries together, flip the canonical root at binaryTrieTime

## Overview

Let erigon join the PBT migration devnet — a chain that starts hex-committed and switches its canonical
state commitment to the EIP-8297 partitioned binary tree at a post-genesis `binaryTrieTime`.

Today `binaryTrieTime` must equal the genesis timestamp: which trie commits state is a process-global
property read from `COMMITMENT_BIN`, and `checkBinaryTrieSchedule` refuses any later schedule. That
makes the migration scenario unrunnable — erigon can be a bin-from-genesis node or a hex node, never a
node that carries both and flips.

This plan makes the trie a property of a **domain** and the canonical/shadow role a property of a
**block**. Two commitment domains are written from block 0; `Config.IsBinaryTrie` decides which root
goes in the header and which is recorded as a shadow root. After the fork the hex domain can be frozen
by an operator: files retained, no longer computed, not dropped.

**Scope is the devnet only.** Out of scope: the EIP-8347 snapshot artifact, the dual-check verifier,
BAL-replay catch-up, anchor finality, re-anchoring, the mainnet on-ramp, retargeting the offline
converter, and downloader/manifest awareness of the new domain.

## Context (from discovery)

Verified against this worktree (`binary-trie`), `CPerezz/go-ethereum@pbt`, and EIP-8297 / EIP-8347.
Source is cited by identifier and file, per `CLAUDE.md` — line anchors rot on the first edit this plan
makes.

**Already present and reusable:**
- `PBinPatriciaHashed` (`execution/commitment/pbin_patricia_hashed.go`), spec-conformant
- `beginWorkerRo` (`execution/commitment/commitmentdb/commitment_context.go`) — a per-worker RO txn
  pinned to the main tx's visible-file generation via `kv.TemporalFilesPin`
- `StateReader.Clone` / `StateReader.CloneForWorker` (`execution/commitment/commitmentdb/reader.go`),
  also on `main` — they rebind a reader onto a caller-supplied txn; nothing clones a txn
- `shadowCrossCheck` (`execution/stagedsync/committer.go`) — existing two-root compare plumbing

**Blocking properties:**
- one `kv.CommitmentDomain` (`db/kv/tables.go`, `DomainLen = 6`); the trie variant is a process global
  (`PickTrieVariant`, `db/state/execctx/domain_shared.go`)
- `checkBinaryTrieSchedule` (`execution/state/genesiswrite/genesis_write.go`) refuses `binaryTrieTime`
  after genesis
- `Updates` is variant-bound: `InitializeTrieAndUpdates` (`execution/commitment/commitment.go`) pairs
  hex with `KeyToHexNibbleHash` + `ModeUpdate` and bin with `pbinSelectedSum` + `ModeDirect`
- **the deferred-branch-update primitives are hex-only.** `BranchEncoder.setDeferUpdates` /
  `ApplyDeferredUpdates` (`execution/commitment/commitment.go`) are used by `HexPatriciaHashed`,
  `parallel_mount.go` and `streaming_deep_fold.go`. `PBinPatriciaHashed` carries a
  `pbinBranchEncoder` (`execution/commitment/pbin_branch.go`), a bare `{buf []byte}` with no deferral,
  and calls `pph.ctx.PutBranch` inline in four places. The bin arm therefore buffers at the **context**,
  not the encoder — see Task 13.

**Needs no change (verified):**
- **Fork ID.** Both gatherers are reflective over `*uint64` fields ending in `Time`, with no skip list,
  and both drop entries at or before the genesis timestamp — `GatherForks` (`p2p/forkid/forkid.go`) and
  geth's `gatherForks` (`core/forkid/forkid.go`). A post-genesis `binaryTrieTime` lands in both fork IDs
  identically.
- **Filename parsing of `commitment-bin`.** `ParseFileName` (`db/snaptype/files.go`) cuts at the *first*
  hyphen; its ext-trim loop is skipped because `filepath.Ext` yields `.0-1024`, which contains `-`;
  `IsStateFileV2` matches; the type string comes out `commitment-bin`. The `IsCorrectFileName` 3→4
  part-count flip is inert: its only caller (`db/downloader/util.go`) drops state files one line later.
- **Offline converter.** `commitment_convert.go` matches the suffix `-commitment.<from>-<to>` *with the
  trailing dot*, so bin files never match and the converter keeps seeing hex only.
- **State cache.** `NewStateCache` (`execution/cache/state_cache.go`) gives `kv.CommitmentDomain` a
  deliberate nil cache slot and the nil short-circuits every path; bin inherits that for free.
- **Unwind application.** `unwindExec3` (`execution/stagedsync/stage_execute.go`) merges per-domain
  generically and `unwindDomsToBlock` passes the whole `[kv.DomainLen]` array. Application is generic;
  diff *production* (Task 11) and on-disk *framing* (Task 2) are not.

## Development Approach

- **testing approach**: Regular (code first, then tests) — this refactors live consensus machinery, so
  each task's tests pin behaviour that already exists before the behaviour moves
- complete each task fully before moving to the next
- **every task must leave the tree building and `make test-short` green.** Several tasks change a
  signature; their Files blocks list every caller, test files included, because `make test-short`
  compiles the test tree and a missed `_test.go` caller is a red suite, not a warning
- **CRITICAL: every task MUST include new/updated tests** for code changes in that task
  - unit tests for new and for modified functions, both success and error scenarios
- **CRITICAL: all tests must pass before starting the next task** — no exceptions
- **CRITICAL: update this plan file when scope changes during implementation**
- maintain backward compatibility for a `hex`-only datadir: with no `binaryTrieTime` the node must
  behave bit-identically to today, including file layout, genesis header, and diffsets it can still read

**House conventions (enforced):**
- **no code comments** — a hook denies any edit that adds one to a code file; directives
  (`//go:`, `//nolint`), license headers and shebangs pass. Explanation goes in the commit message.
- new source files carry the **current year** in the licence header
- erigon naming: no `Factory`/`Provider`/`Manager`/`*Base`; registered function types are `*Func`
- commit subject ≤ 120 chars, subject line only; detail belongs in the PR body
- anything written into `docs/` cites erigon source by identifier and file, never `file.go:NNN`

## Testing Strategy

- **unit tests**: required for every task (see Development Approach)
- **integration test**: a `hex+bin` genesis fixture with `binaryTrieTime` a few blocks past genesis,
  driven through the flip (Task 26). This is the acceptance vehicle, not an add-on.
- **e2e**: the project has no UI e2e suite. The devnet run (`CPerezz/pbt-devnet` with erigon added as a
  participant) is the external equivalent and is listed under Post-Completion.
- project test commands: `make test-short` per task, `make test-all` at the acceptance gate

## Progress Tracking

- mark completed items with `[x]` immediately when done
- add newly discovered tasks with ➕ prefix
- document issues/blockers with ⚠️ prefix
- update the plan if implementation deviates from the original scope
- keep the plan in sync with the actual work done

## Solution Overview

**Two axes, kept independent.**

*Trie* is which algorithm owns a domain, fixed for the datadir's life. *Role* is canonical vs shadow,
decided per block from the header time.

| block range | `kv.CommitmentDomain` (hex) | `kv.CommitmentBinDomain` (bin) |
|---|---|---|
| `< binaryTrieTime` | canonical — header-checked | shadow — computed, recorded, never checked |
| `binaryTrieTime` → operator freezes hex | shadow — computed, recorded | canonical — header-checked |
| after the freeze | **frozen** — files retained, not computed | canonical, sole fold |

Nothing about the domains changes at the fork block; both are written on both sides of it. That makes
EIP-8347 §Reorg-across-the-swap free: rewinding to a pre-activation block rolls both domains through the
existing `[kv.DomainLen][]kv.DomainEntryDiff` changeset, and the predicate re-answers "hex is canonical"
on its own.

**Three datadir modes, fixed at genesis:**

| mode | config | domains registered |
|---|---|---|
| `hex` | no `binaryTrieTime` | `kv.CommitmentDomain` |
| `bin` | `binaryTrieTime == genesisTime` | `kv.CommitmentBinDomain` |
| `hex+bin` | `binaryTrieTime > genesisTime` | both, written from block 0 |

**Lockstep, computed concurrently.** Two folds per block over one touched plain-key set, joined before
publish; each arm takes its own RO txn from `beginWorkerRo`, pinned to the main tx's file generation.
Not a lagging follower: a lagging shadow reads accounts/storage at an older txNum through a view that
can be retired under it, so the pin only means something when both arms read at the same txNum. A
shadow fold error **stops** the shadow (marks the domain broken for the run); it never lags, and it
never fails the block.

**Key design decisions and rationale:**

1. **Domain per trie, not per role.** The post-fork flip then costs nothing, and a converted bin
   artifact drops in as files rather than needing a rewrite.
2. **Minimax selector.** `stateMinimaxTxNum` (`db/state/aggregator.go`) is `min(EndTxNum)` over
   `kv.StateDomains` and drives file building and merging **for every domain**. A shadow domain that is
   stopped, frozen, or not yet built would take the whole datadir down with it, so the set must contain
   the *canonical* commitment domain only. Note what this does **not** change: when the canonical
   commitment domain itself has no files the minimax still collapses to 0, and that is the guard which
   stops file building running past the commitment frontier on a fresh or rebuilding datadir.
3. **No `AddDependencyBtwnDomains` for bin.** That hard alignment (`Aggregator.AddDependencyBtwnDomains`,
   `db/state/aggregator.go`) exists only because referenced branch data carries file offsets into
   account/storage ranges (`replaceShortenedKeysInBranch`, `db/state/domain_committed.go`), and the
   merge transform is refs-gated. Bin runs `ReferencesInCommitmentBranches: false`, so there is no
   cross-file pointer and no dependency to enforce.
4. **Bin gets no branch-cache trunk.** The `trunk` type (`execution/commitment/branch_cache.go`) is
   `d1[16] / d2[256] / d3[4096] / d4[65536]` — 16^depth, nibble-indexed. A bit path indexes those slots
   as garbage. A second instance of the same structure does not help: at `trunkDepthFull = 4` a bin
   trunk covers 16 prefixes against hex's 65536, and equivalent coverage needs depth 16. Bin runs with a
   nil cache.
5. **The canonical selector is derived, never persisted.** It is a pure function of
   `Config.IsBinaryTrie` and the head header's time, so the aggregator resolves it at open from the head
   block and the committer re-resolves it per block. Persisting it would create a second source of truth
   that can disagree with the chain config.
6. **Frozen, not dropped, and operator-triggered.** `kv.CommitmentDomain`'s `HistCfg` is
   `HistoryDisabled: true`, so a commitment domain answers *latest* only. A frozen hex domain is
   therefore exactly one artifact — the MPT as of the freeze txNum — which serves pre-fork-anchored
   proofs and an un-flip without an O(state) rebuild. It cannot serve an MPT proof at an arbitrary
   historical block; it never could. The trigger is an explicit operator action: a finality-driven
   automatic freeze is anchor-finality machinery, which this plan puts out of scope.

## Technical Details

**New domain.** `kv.CommitmentBinDomain Domain = 6`, `DomainLen = 7`. `FilenameBase` and type string
`commitment-bin`; files `v1.0-commitment-bin.<from>-<to>.kv`. Its `DomainCfg` mirrors
`kv.CommitmentDomain`'s with `ReferencesInCommitmentBranches: false`, `Hist.SnapshotsDisabled` and
`Hist.HistoryDisabled` true, and its own `KVWriteVersion`.

The compile-time assertion `var _ [32 - 2*int(kv.DomainLen)]struct{}` (`db/kv/temporal/kv_temporal.go`)
caps domains at 16, so a 7th is allowed. Two `[kv.DomainLen]` literals are **keyed and incomplete**, so
the bin slot silently becomes a zero value: `mxsKVGet` (`db/state/metrics.go`), whose nil slice makes
`domainReadMetric` (`db/state/domain.go`) panic with index-out-of-range on the first bin file read under
`dbg.KVReadLevelledMetrics`.

**Diffset framing.** `deserializeKeys` (`db/state/changeset/state_changeset.go`) reads exactly
`kv.DomainLen` length-prefixed blocks in domain order, with no count header and no version byte;
`serializeKeys` writes `len(d.Diffs)` the same way; `ReadDiffSet` hands the buffer in with no length
validation. Bumping `DomainLen` 6→7 makes the seventh iteration call `binary.BigEndian.Uint32` on an
empty slice — `_ = b[3]`, an **index-out-of-range panic**, not a catchable error. Diffsets live in
chaindata, so any restart onto a rebuilt image over an existing datadir hits it on the first unwind; the
last devnet run logged 29 forced reorgs in 9h. The inner `DeserializeDiffSet` already has V0/V1
detection — the versioning stops one level too high.

**Gas schedule.** `NewRules` (`execution/vm/evmtypes/rules.go`) sets
`EIP8038Revised: c.EIP8038Revised || c.IsBinaryTrie(bc.Time)` — **time**-gated. geth ties the revised
8038 schedule to Amsterdam unconditionally (`params/protocol_params.go`, `core/vm/eips.go`) and its
`BinaryTrieTime` touches gas nowhere. With `binaryTrieTime == genesis` the two agree from block 0, which
is why the devnet passes today. Move `binaryTrieTime` past genesis and erigon charges the corpus-pinned
schedule across the whole `amsterdamTime` → `binaryTrieTime` window while geth charges revised:
divergence **opens at Amsterdam and closes at the flip**, covering exactly the shadow window the devnet
exercises. It reaches `execution/vm/gas_table.go`, `execution/vm/eips.go`, `execution/vm/interpreter.go`,
`TxPool.isEIP8038Revised` (`txnprovider/txpool/pool.go`) and the shutter pool.

Fix is config-level: `c.EIP8038Revised || c.BinaryTrieTime != nil`, matching geth's `IsPBT`
(`params/config.go`). It also removes the EIP-8347 §Backwards-Compatibility violation of bundling a
repricing into a commitment-only fork. geth additionally gates `IsBinaryTrie` on `IsLondon`; that is
**not** mirrored here — London is unconditionally active on any devnet this targets, so the gate is
inert and would force a block-number parameter through every caller, including the header-only call
sites this plan adds.

**Fold data flow.**

```
exec → touched plain keys
        ├── Updates(ModeUpdate, KeyToHexNibbleHash) → HexPatriciaHashed  → CommitmentDomain
        └── Updates(ModeDirect, pbinSelectedSum)    → PBinPatriciaHashed → CommitmentBinDomain
                  ↓ two arms, own beginWorkerRo txn each, join
        role = IsBinaryTrie(header.Time) → canonical root header-checked, shadow root recorded
```

The bin collector is built **once at fold time** from the hex `Updates` key set, not teed per touch:
`Updates.treeIdx` is already keyed by plain key, so `Updates.PlainKeys` only needs its missing
`ModeUpdate` branch. Bin runs `ModeDirect` — keys only, values re-read from the shared account/storage
domains — so no value is ever copied between the arms and they cannot disagree about the state.

**Where the fan-out goes.** The mainline block fold is `commitmentCalculator.compute`
(`execution/stagedsync/committer.go`), which installs `sdCtx` inline and calls `computeIsolated` or
`computeWithBlockAccumulator`, holding the header check itself. `computeRootFromUpdates` is a *second*
entry point, reached only from `computeRootFromBAL` and `shadowCrossCheck`. Both need arms; fanning out
only the latter leaves the mainline single-armed.

**Branch writes cannot go concurrent.** `DomainPutCommitmentDiff`
(`db/state/execctx/domain_shared.go`) hardcodes `kv.CommitmentDomain` and delegates to
`PutCommitmentBranchDiff` on `sd.mem` (`db/state/temporal_mem_batch.go`), which hardcodes the same
domain in several places and shares one `*kv.DomainDiff` accumulator. The bin arm therefore never writes
during the fold: it runs behind a buffering `PatriciaContext` (Task 13) and the calculator replays the
buffer after the join, on its own goroutine.

**Shadow roots** are recorded in a chaindata table keyed by `dbutils.BlockBodyKey(number, hash)` — the
way BALs already are (`db/rawdb/accessors_chain.go`) — and pruned with the block. Keyed by **hash, not
height**, per EIP-8347 §Reorg handling: two nodes on opposite sides of a reorg at the same height hold
different states and correctly report different roots.

**RPC surface**, shaped to match geth so the devnet checker reads both clients the same way:
`debug_shadowStateRoot(blockHash)` and a `debug_migrationProgress` equivalent.

## What Goes Where

- **Implementation Steps** (`[ ]`): everything achievable in this repo — code, tests, docs
- **Post-Completion** (no checkboxes): the devnet run itself, cross-client root comparison against geth,
  and the mainnet items this plan deliberately leaves out

## Implementation Steps

### Task 1: Decouple the EIP-8038 revised gas schedule from binaryTrieTime

**Files:**
- Modify: `execution/vm/evmtypes/rules.go`
- Modify: `execution/chain/chain_config.go`
- Modify: `txnprovider/txpool/pool.go`
- Modify: `txnprovider/shutter/pool.go`
- Modify: `execution/chain/binary_trie_test.go`
- Modify: `txnprovider/txpool/pool_test.go`

- [x] change `NewRules` to `EIP8038Revised: c.EIP8038Revised || c.BinaryTrieTime != nil`
- [x] add a `Config` predicate for "this chain schedules the PBT at all" and use it at the three call
      sites rather than repeating the nil check
- [x] change `TxPool.isEIP8038Revised` and the shutter pool's equivalent to the same config-level
      predicate, dropping the `isPostBinaryTrie` time latch
- [x] leave `Config.IsBinaryTrie` one-argument — do **not** add geth's London gate (see Technical
      Details); update the `BinaryTrieTime` doc comment, which no longer implies "genesis only"
- [x] write tests: a config with `binaryTrieTime` after `amsterdamTime` charges the revised schedule at
      a block *before* `binaryTrieTime`, and at one after; a config with no `binaryTrieTime` charges the
      corpus-pinned schedule at both
- [x] write tests: the txpool and the executor agree on `IsEIP8038Revised` at a pre-flip and a post-flip
      block time (the invariant `TxPool.isEIP8038Revised` documents)
- [x] run `make test-short` — must pass before task 2

### Task 2: Version the diffset domain framing

**Files:**
- Modify: `db/state/changeset/state_changeset.go`
- Modify: `db/state/changeset/state_changeset_test.go`

- [x] add a version byte and a domain count ahead of the per-domain blocks in `serializeKeys`
- [x] rewrite `deserializeKeys` to read the count from the record, place blocks by index into
      `[kv.DomainLen]`, and tolerate a count below `DomainLen` (missing domains stay nil)
- [x] keep reading the old unversioned format: detect it and read exactly the old domain count
- [x] add a length guard so a truncated record returns an error instead of panicking inside
      `binary.BigEndian.Uint32`
- [x] write tests: round-trip at the current domain count
- [x] write tests: a record whose count is below `kv.DomainLen` decodes with the trailing slots nil and
      no panic — this is the shape a pre-bump record takes after Task 5
- [x] write tests: an old-format fixture still decodes
- [x] write tests: a truncated record returns an error, not a panic
- [x] run `make test-short` — must pass before task 3

### Task 3: Make the datadir trie variant a three-state set

**Files:**
- Modify: `db/state/erigondb_settings.go`
- Modify: `db/state/erigondb_settings_test.go`

- [x] extend `TrieVariantName` and `reconcileTrieVariant` to a third value `hex+bin` alongside `hex` and
      `bin`, and expose a mode accessor the aggregator and `NewSharedDomains` can read
- [x] move the refs-vs-bin refusal from a whole-datadir rule to a per-`DomainCfg` one: under `hex+bin`
      the hex domain keeps `ReferencesInCommitmentBranches` and the bin domain does not
- [x] resolve the parallel-commitment refusal under `hex+bin`: the bin trie is sequential-only, so the
      flag must apply to the hex arm without banning the mode outright
- [x] keep `trie_hash` meaningful only when `bin` is in the set
- [x] write tests: each of the three values round-trips through the settings file
- [x] write tests: `hex+bin` with `--experimental.parallel-commitment` is accepted and applies the flag
      to the hex arm only
- [x] write tests: `hex` with `--experimental.bin-commitment` is still refused
- [x] run `make test-short` — must pass before task 4

### Task 4: Canonical-commitment selector plumbing (no new domain yet)

**Files:**
- Modify: `db/kv/tables.go`
- Modify: `db/state/aggregator.go`
- Modify: `db/state/merge.go`
- Modify: `db/state/domain_committed.go`
- Modify: `db/state/stats/agg_log_stats.go`
- Modify: `db/state/aggregator_align_test.go`
- Modify: `db/state/execctx/domain_shared_test.go`

- [x] turn `kv.StateDomains` from a package var into a function taking the canonical commitment domain
      and returning `{Accounts, Storage, Code, canonical}`
- [x] add a canonical-commitment accessor to `Aggregator`, **derived** at open from the head header's
      time via `Config.IsBinaryTrie`, defaulting to `kv.CommitmentDomain`
- [x] thread it through `stateMinimaxTxNum`, `replaceShortenedKeysInBranch`, the agg log stats, and the
      `LatestMergedRange` alignment; update the `findMergeRangeInFiles` doc comment
- [x] update the callers `kv.StateDomains` had as a var, test files included
- [x] write tests: with the selector on `kv.CommitmentDomain` the minimax equals today's value
- [x] write tests: a registered-but-empty **non-canonical** commitment domain does not enter the minimax
- [x] write tests: an empty **canonical** commitment domain still collapses the minimax to 0 — that
      guard must survive this change
- [x] run `make test-short` — must pass before task 5

### Task 5: Add kv.CommitmentBinDomain (enum, tables, schema, versions, metrics)

**Files:**
- Modify: `db/kv/tables.go`
- Modify: `db/state/statecfg/state_schema.go`
- Modify: `db/state/statecfg/version_schema.go`
- Modify: `db/state/statecfg/version_schema_gen.go`
- Modify: `db/state/statecfg/gen_version.go`
- Modify: `db/state/metrics.go`
- Modify: `db/state/statecfg/state_schema_test.go`
- Create: `db/kv/domain_name_test.go`

- [x] add `CommitmentBinDomain Domain = 6` and bump `DomainLen` to 7
- [x] add the `Domain.String` case `"commitment-bin"` and the matching `String2Domain` case
- [x] add `TblCommitmentBinVals` and the history/index tables, following the `TblCommitment*` shape
- [x] add the bin `DomainCfg` mirroring `kv.CommitmentDomain`'s with `ReferencesInCommitmentBranches:
      false`, `Hist.SnapshotsDisabled` / `Hist.HistoryDisabled` true, and its own `KVWriteVersion`; add
      its `version_schema` and `gen_version` entries
- [x] register the domain in `Configure` conditionally on the Task 3 datadir mode, and register **no**
      `AddDependencyBtwnDomains` for it
- [x] add the bin entry to the keyed `mxsKVGet` literal — a missing key leaves a nil slice and
      `domainReadMetric` panics on the first bin file read
- [x] audit every other keyed `[kv.DomainLen]` literal for the same hole
- [x] write tests: `Domain.String` / `String2Domain` round-trip for every domain including the new one
- [x] write tests: `domainReadMetric` returns a usable summary for every domain at every level
- [x] write tests: a `hex`-only datadir registers exactly the six domains it registers today
- [x] run `make test-short` — must pass before task 6

### Task 6: Teach the name tables and the snapshot type parser about commitment-bin

**Files:**
- Modify: `db/snaptype/type.go`
- Modify: `cmd/integration/commands/state_domains.go`
- Modify: `cmd/integration/commands/write_amplification.go`
- Modify: `cmd/utils/app/snapshots_cmd.go`
- Modify: `cmd/utils/app/domain_cmd.go`
- Modify: `db/snaptype/files_test.go`

- [x] add the `ParseFileType` / `ParseEnum` entry for `commitment-bin`
- [x] add `commitment-bin` to the hand-listed name tables in the integration and snapshot commands, and
      widen the `domain_cmd` bound check
- [x] write tests: `ParseFileName("v1.0-commitment-bin.0-1024.kv")` yields type string `commitment-bin`,
      from 0, to 1024, and is classified as a state file
- [x] write tests: `v1.0-commitment.0-1024.kv` still parses as `commitment`, unchanged
- [x] run `make test-short` — must pass before task 7

### Task 7: Make the branch cache per-domain and keep bin off the trunk

**Files:**
- Modify: `execution/commitment/branch_cache.go`
- Modify: `db/state/aggregator.go`
- Modify: `db/state/domain.go`
- Modify: `db/state/execctx/domain_shared.go`
- Modify: `execution/stagedsync/rawdbreset/reset_stages.go`
- Modify: `execution/exec/bal_commitment_warmup.go`
- Modify: `db/state/execctx/state_getter_test.go`
- Modify: `db/state/execctx/branch_cache_flush_test.go`
- Modify: `db/state/execctx/commitment_flag_test.go`
- Modify: `rpc/jsonrpc/rpc_branch_cache_test.go`
- Modify: `rpc/jsonrpc/eth_call_test.go`
- Modify: `rpc/jsonrpc/trace_view_consistency_test.go`

- [x] give `BranchCache` and `AdaptivePinController` a domain parameter **on the interface** as well as
      the concrete type — the provider is reached through a duck-typed assertion, so changing only one
      side compiles and silently disables the cache
- [x] change the cache constructor and teardown to walk the registered commitment domains rather than
      hardcoding `kv.CommitmentDomain`
- [x] leave the bin domain's cache nil — the trunk is 16^depth nibble-indexed and a bit path cannot use
      it
- [x] update every caller, test files included
- [x] write tests: the hex domain's cache is **non-nil**, takes a put, and serves it back — this is the
      assertion that fails if the duck-typed assertion stops matching
- [x] write tests: `BranchCache` for the bin domain is nil and every caller tolerates that
- [x] write tests: a branch write against the bin domain does not appear in the hex domain's cache
- [x] run `make test-short` — must pass before task 8

### Task 8: Parameterize the commitment context on its domain

**Files:**
- Modify: `db/state/aggregator.go`
- Modify: `execution/commitment/commitmentdb/commitment_context.go`
- Modify: `execution/commitment/commitmentdb/reader.go`
- Modify: `db/state/execctx/domain_shared.go`
- Modify: `db/state/execctx/pin_branch_resolver.go`
- Modify: `execution/stagedsync/committer.go`
- Modify: `execution/stagedsync/committer_step_boundary_test.go`
- Modify: `rpc/jsonrpc/debug_execution_witness.go`
- Modify: `rpc/jsonrpc/debug_execution_witness_test.go`
- Modify: `rpc/jsonrpc/eth_simulation.go`
- Modify: `execution/commitment/commitmentdb/commitment_context_test.go`
- Modify: `execution/commitment/commitmentdb/reader_test.go`
- Modify: `execution/commitment/commitmentdb/pbin_nocache_test.go`
- Modify: `execution/commitment/commitmentdb/pbin_state_header_test.go`

- [x] add the commitment domain as a field on `SharedDomainsCommitmentContext`, set through
      `NewSharedDomainsCommitmentContext`, and replace the hardcoded `kv.CommitmentDomain` on its read,
      write and state-blob paths
- [x] replace the `d == kv.CommitmentDomain` branches in the commitment readers with a check against the
      reader's own commitment domain
- [x] do the same in `asOfStateReader.Read`, which routes commitment reads to `GetLatest` and everything
      else to `GetAsOf`
- [x] cover the four remaining hardcodes on the same path: `flushPendingUpdates`' `putBranch`, the
      `useBranchCache := domain == kv.CommitmentDomain` gate, the adaptive-pin reader, and the pin
      branch resolver
- [x] update the constructor's callers, `pbin_nocache_test.go` and `pbin_state_header_test.go` included
- [x] write tests: a context bound to the bin domain reads and writes bin branches and never touches the
      hex tables
- [x] write tests: a context bound to the hex domain behaves exactly as today, cache gate included
- [x] run `make test-short` — must pass before task 9

### Task 9: SeekCommitment over both domains, with a torn-datadir refusal

**Files:**
- Modify: `execution/commitment/commitmentdb/commitment_context.go`
- Modify: `db/state/execctx/domain_shared.go`
- Modify: `execution/commitment/commitmentdb/commitment_context_test.go`

- [x] make `SeekCommitment` seek every registered commitment domain and restore each trie from its own
      `KeyCommitmentState`
- [x] require the restored `(blockNum, txNum)` to agree across domains; a disagreement is a torn datadir
      — return a distinct error and refuse to start rather than folding onto a mismatched pair
- [x] fix the `SyncStageProgress["Execution"]` fallback so "one domain has state, the other does not" is
      the torn case, not the fresh case
- [x] write tests: both domains at the same `(blockNum, txNum)` restores both tries
- [x] write tests: domains at different `(blockNum, txNum)` returns the torn error
- [x] write tests: neither domain has state and `SyncStageProgress` is absent → fresh, as today
- [x] write tests: one domain has state and the other does not → torn, not fresh
- [x] run `make test-short` — must pass before task 10

### Task 10: Build both trie contexts in NewSharedDomains

**Files:**
- Modify: `db/state/execctx/domain_shared.go`
- Modify: `db/state/execctx/options.go`
- Modify: `db/state/execctx/domain_shared_test.go`
- Modify: `db/state/aggregator.go`
- Modify: `db/state/erigondb_settings.go`
- Modify: `db/state/statecfg/state_schema.go`
- Modify: `execution/commitment/commitmentdb/commitment_context.go`
- Modify: `db/state/execctx/pbin_options_test.go`

- [x] under `hex+bin`, construct both a hex and a bin `SharedDomainsCommitmentContext`, each with its own
      `Trie` and `Updates` from `InitializeTrieAndUpdates`
- [x] replace the global `WithoutSharedBranchCache` side-effect with the per-domain cache selection from
      Task 7
- [x] keep `ErrBinCommitmentUnsupported` meaningful: under `hex+bin` a `WithHexCommitmentOnly` caller
      gets the hex context, and a caller needing a block-correct trie must ask for one by block (Task 21)
      rather than inheriting the process default
- [x] write tests: `hex` builds one context; `bin` builds one bin context; `hex+bin` builds both and they
      address different domains
- [x] write tests: `WithHexCommitmentOnly` under `bin` still returns `ErrBinCommitmentUnsupported`
- [x] run `make test-short` — must pass before task 11

### Task 11: Give the bin domain its own changeset diff slot

**Files:**
- Modify: `db/state/execctx/domain_shared.go`
- Modify: `db/state/temporal_mem_batch.go`
- Modify: `execution/blockreplay/witnessmembatch.go`
- Modify: `execution/stagedsync/committer.go`
- Modify: `db/state/execctx/domain_shared_test.go`

- [x] give `DomainPutCommitmentDiff` a domain parameter
- [x] give `PutCommitmentBranchDiff` and the `SetCommitmentDiff` / `CommitmentDiff` /
      `SetCommitmentDiffRaw` accessors on the mem batch a domain parameter, replacing their hardcoded
      `acc.Diffs[kv.CommitmentDomain]`; update the flush callback
- [x] update `execution/blockreplay/witnessmembatch.go`, which carries its own copy of the interface plus
      a forwarder
- [x] select `cs.Diffs[...]` / `live.Diffs[...]` by the arm's commitment domain in the committer instead
      of hardcoding `kv.CommitmentDomain`
- [x] write tests: a bin branch write lands in `Diffs[kv.CommitmentBinDomain]` and a hex write in
      `Diffs[kv.CommitmentDomain]`
- [x] write tests: an unwind over a block that wrote both domains restores both domains' branch values
- [x] run `make test-short` — must pass before task 12

### Task 12: Expose the touched plain-key set to the bin collector

**Files:**
- Modify: `execution/commitment/commitment.go`
- Modify: `execution/commitment/commitment_test.go`

- [x] add the `ModeUpdate` branch to `Updates.PlainKeys`, reading `treeIdx`
- [x] add a helper that builds a bin `Updates` from a plain-key set, so the committer does it once at
      fold time rather than teeing per touch
- [x] write tests: `PlainKeys` returns the same set for `ModeUpdate`, `ModeDirect` and `ModeParallel`
      given the same touches
- [x] write tests: the bin collector built from a hex `Updates` key set contains exactly those keys,
      deletions included
- [x] run `make test-short` — must pass before task 13

### Task 13: A buffering PatriciaContext for the bin arm

**Files:**
- Create: `execution/commitment/commitmentdb/buffered_context.go`
- Create: `execution/commitment/commitmentdb/buffered_context_test.go`

- [x] add a `PatriciaContext` wrapper that forwards reads and **collects** `PutBranch` calls in order
      instead of writing them, with an explicit replay entry point
- [x] this is required because the deferred-update primitives on `BranchEncoder` are hex-only:
      `PBinPatriciaHashed` uses `pbinBranchEncoder`, which has no deferral, and calls `ctx.PutBranch`
      inline
- [x] preserve `prev` values so the replayed writes produce the same diffs a direct write would
- [x] write tests: reads pass through; puts do not reach the underlying context until replay
- [x] write tests: replay produces the same branch values and the same `prev` pairing as a direct write
- [x] write tests: a fold that errors mid-way replays nothing
- [x] run `make test-short` — must pass before task 14

### Task 14: Fan the committer out to two concurrent folds

**Files:**
- Modify: `execution/stagedsync/committer.go`
- Modify: `execution/commitment/commitmentdb/commitment_context.go`
- Modify: `execution/stagedsync/committer_test.go`

- [x] fan `commitmentCalculator.compute` out to two arms and join before the header check — this is the
      mainline path, and it installs `sdCtx` inline rather than going through
      `computeRootFromUpdates`
- [x] fan `computeRootFromUpdates` out too, for the BAL compute-ahead and `shadowCrossCheck` paths
- [x] give each arm its own RO txn from `beginWorkerRo`, pinned to the main tx's visible-file generation
- [x] run the bin arm behind the Task 13 buffering context and replay the buffer after the join, on the
      calculator goroutine
- [x] keep the single-arm path untouched for `hex` and `bin` datadirs
- [x] write tests: both arms produce a root for the same block, and the hex root equals the root the
      single-arm path produces for the same input
- [x] write tests: no bin branch write reaches `sd` before the join — assert on the underlying context,
      not on a `-race` pass, which would be green whether or not the buffering exists
- [x] write tests: the BAL compute-ahead path also produces both roots
- [x] run `make test-short` — must pass before task 15

### Task 15: Split the role and the failure policy

**Files:**
- Modify: `execution/stagedsync/committer.go`
- Modify: `execution/stagedsync/exec3_parallel.go`
- Modify: `db/state/aggregator.go`
- Modify: `execution/stagedsync/committer_test.go`

- [x] decide the canonical arm per block from `Config.IsBinaryTrie` on the header time
- [x] canonical arm: header check, publish, and `cc.fail` on error, exactly as today
- [x] shadow arm: record the root, bump a metric, and on error mark the shadow domain broken for the run
      — never `cc.fail`
- [x] move the aggregator's canonical-commitment accessor when the committer crosses `binaryTrieTime`;
      it stays derived, so a restart re-resolves it from the head header rather than reading a stored
      value
- [x] write tests: pre-flip the header is checked against the hex root and the bin root is recorded;
      post-flip the reverse
- [x] write tests: a shadow fold error leaves the block valid, marks the shadow stopped, and does not
      unwind
- [x] write tests: a canonical fold mismatch still produces `ErrWrongTrieRoot`
- [x] write tests: restarting on either side of the flip re-derives the same canonical domain
- [x] run `make test-short` — must pass before task 16

### Task 16: Checkpoint step edges on both arms

**Files:**
- Modify: `db/state/execctx/domain_shared.go`
- Modify: `execution/stagedsync/committer.go`
- Modify: `execution/state/rw_v3.go`
- Modify: `execution/stagedsync/committer_test.go`

- [x] give `SharedDomains.IsUnfrozenStepEdge` a domain parameter — it currently ends in
      `StepsInFiles(kv.CommitmentDomain)`, so both arms would be gated on the hex frontier
- [x] drive `checkpointStepsFromBAL` and `computeStepBoundary` for every live commitment domain, not just
      the canonical one
- [x] write tests: a block straddling an unfrozen step edge leaves a commitment checkpoint in **both**
      domains at that edge
- [x] write tests: the **canonical** domain's `.kv` does not lag its accounts/storage `.kv` after such a
      block — the shadow domain is excluded from the minimax by design and may legitimately lag in file
      terms, so it is not part of this assertion
- [x] run `make test-short` — must pass before task 17

### Task 17: Point the canonical-domain consumers at the selector

**Files:**
- Modify: `execution/stagedsync/exec3.go`
- Modify: `execution/stagedsync/exec3_serial.go`
- Modify: `execution/stagedsync/stage_execute.go`
- Modify: `execution/stagedsync/stage_snapshots.go`
- Create: `execution/stagedsync/canonical_commitment_test.go`

- [x] the snapshot step-misalignment guard in `exec3.go` takes the canonical commitment domain
- [x] `lastFrozenStep` in `exec3.go` and `exec3_serial.go` takes the canonical domain
- [x] add the bin domain to `RetireCutoffs.PerDomain` with the commitment cutoff, so it does not fall to
      the history default and get retired on the history schedule
- [x] the snapshot stage reads `KeyCommitmentState` from the canonical domain
- [x] write tests: with hex frozen and bin canonical, the misalignment guard reads bin and does not fire
- [x] write tests: the bin domain's retire cutoff equals the commitment cutoff, not the history one
- [x] run `make test-short` — must pass before task 18

### Task 18: Genesis writes both commitments and accepts a post-genesis schedule

**Files:**
- Modify: `execution/state/genesiswrite/genesis_write.go`
- Modify: `execution/state/genesiswrite/genesis_test.go`
- Modify: `execution/state/genesiswrite/pbin_genesis_test.go`

- [x] lift the post-genesis refusal in `checkBinaryTrieSchedule` for `hex+bin`; keep the
      `amsterdamTime <= binaryTrieTime` check
- [x] rewrite `checkBinaryTrieCommitment` to require that the datadir carries a bin domain, replacing the
      `COMMITMENT_BIN=true` wording
- [x] make `ComputeGenesisCommitment` compute both roots at block 0 and write both domains; the header
      takes whichever `Config.IsBinaryTrie` names for the genesis timestamp
- [x] write tests: a genesis with `binaryTrieTime > timestamp` is accepted under `hex+bin` and refused
      under `hex` and under `bin`
- [x] write tests: a genesis with `binaryTrieTime < amsterdamTime` is still refused
- [x] write tests: block 0 writes a hex root to the header and a bin root to the bin domain, and the two
      differ
- [x] write tests: a `hex` datadir with no `binaryTrieTime` produces a byte-identical genesis header to
      today
- [x] run `make test-short` — must pass before task 19

### Task 19: Record and prune shadow roots

**Files:**
- Create: `db/rawdb/accessors_shadow_root.go`
- Create: `db/rawdb/accessors_shadow_root_test.go`
- Modify: `db/kv/tables.go`
- Modify: `db/rawdb/accessors_chain.go`
- Modify: `execution/stagedsync/stage_execute.go`
- Modify: `execution/stagedsync/stage_execute_prune_test.go`
- Modify: `execution/stagedsync/committer.go`
- Modify: `execution/state/genesiswrite/genesis_write.go`
- Modify: `execution/state/genesiswrite/pbin_genesis_test.go`

- [x] add a chaindata table for shadow roots keyed by `dbutils.BlockBodyKey(number, hash)`, following the
      BAL accessor shape
- [x] write the shadow root from the committer's shadow arm
- [x] wire pruning at the three places the BAL analogue is pruned: the `PruneTable` call in the execute
      stage and the two per-block sweeps in `accessors_chain.go` — the accessor alone does not prune
- [x] write block 0's shadow root at genesis; nothing else produces it
- [x] write tests: read-back by `(number, hash)`; two hashes at the same height store distinct roots
- [x] write tests: pruning a block through the **stage** path, not just the accessor, removes the record
- [x] write tests: genesis leaves a shadow root for block 0 under `hex+bin`
- [x] run `make test-short` — must pass before task 20

### Task 20: Expose the migration over the debug API

**Files:**
- Modify: `rpc/jsonrpc/debug_api.go`
- Create: `rpc/jsonrpc/debug_shadow_root_test.go`

- [x] add `debug_shadowStateRoot(blockHash)` returning the recorded root or null, matching geth's shape
- [x] add `debug_migrationProgress` reporting the datadir mode, the activation time, whether the flip has
      happened, and whether the shadow is stopped — keep it to fields with a consumer
- [x] write tests: a known block hash returns its recorded root; an unknown hash returns null
- [x] write tests: progress reports the right mode for `hex`, `bin` and `hex+bin`, and reports a stopped
      shadow after a shadow fold error
- [x] run `make test-short` — must pass before task 21

### Task 21: Select the trie by block on the RPC paths that need it

**Files:**
- Modify: `rpc/jsonrpc/debug_execution_witness.go`
- Modify: `rpc/rpchelper/commitment.go`
- Modify: `db/state/execctx/options.go`
- Modify: `rpc/jsonrpc/debug_execution_witness_test.go`

- [x] give `binCommitmentTrie` the block it is serving and answer from `Config.IsBinaryTrie` on that
      header's time instead of the process-global `PickTrieVariant`
- [x] make the genesis-commitment helper in `rpc/rpchelper` use the trie the genesis timestamp selects
- [x] make the block-correct selection an explicit `NewSharedDomains` option so a caller that needs it
      cannot silently inherit the process default
- [x] leave the non-folding RPC sites (`eth_call`, `eth_simulation`, the receipts generator) on
      `WithHexCommitmentOnly` — they compute no root
- [x] write tests: a witness request for a pre-flip block selects hex and for a post-flip block selects
      bin, on the same `hex+bin` datadir
- [x] write tests: a post-flip request can no longer silently receive a hex-derived answer
- [x] run `make test-short` — must pass before task 22

### Task 22: Stop the substring sweeps from eating bin files

**Files:**
- Modify: `cmd/utils/app/snapshots_cmd.go`
- Modify: `cmd/integration/commands/commitment.go`
- Create: `cmd/utils/app/snapshots_commitment_sweep_test.go`
- Modify: `cmd/integration/commands/commitment_report_test.go`

- [x] replace the `strings.Contains(..., "commitment")` file sweep in the snapshot command with an exact
      match against the parsed type string
- [x] do the same for the history/idx sweep in the same file
- [x] do the same for the `strings.Contains(name, kv.CommitmentDomain.String())` check in the integration
      commitment command
- [x] write tests: a commitment-removal pass over a directory holding both `v1.0-commitment.0-1024.kv`
      and `v1.0-commitment-bin.0-1024.kv` removes only the hex file
- [x] write tests: selecting `commitment-bin` removes only the bin file
- [x] run `make test-short` — must pass before task 23

### Task 23: Reset, BAL warmup and the rebuild command

**Files:**
- Modify: `execution/stagedsync/rawdbreset/reset_stages.go`
- Modify: `cmd/integration/commands/reset_state.go`
- Modify: `execution/exec/bal_commitment_warmup.go`
- Modify: `cmd/integration/commands/commitment.go`
- Modify: `execution/exec/bal_commitment_warmup_test.go`

- [x] add the bin domain to the hand-listed `DomainTables` calls in the reset stages
- [x] extend the commitment special case in `reset_state.go` to cover the bin domain
- [x] make the BAL commitment warmup domain-aware: warm the canonical domain, and never request the
      branch cache for a domain that has none
- [x] make the rebuild command's hex pins — domain progress, domain names, and the refs override —
      follow the domain being rebuilt
- [x] write tests: a reset on a `hex+bin` datadir clears both commitment tables
- [x] write tests: warmup against the bin domain performs no branch-cache put
- [x] run `make test-short` — must pass before task 24

### Task 24: Keep the two folds out of each other's metrics

**Files:**
- Modify: `execution/commitment/commitment.go`
- Modify: `execution/commitment/metrics.go`
- Modify: `execution/commitment/hex_patricia_hashed.go`
- Modify: `execution/commitment/parallel_mount.go`
- Modify: `execution/commitment/parallel_patricia_hashed.go`
- Modify: `execution/commitment/streaming_deep_fold.go`
- Modify: `execution/commitment/pbin_patricia_hashed.go`
- Modify: `execution/commitment/commitmentdb/commitment_context.go`
- Modify: `execution/stagedsync/committer.go`
- Modify: `execution/stagedsync/exec3_metrics.go`
- Modify: `execution/commitment/commitment_test.go`
- Create: `execution/stagedsync/exec3_metrics_test.go`

- [x] give the fold a metrics sink so the shadow arm does not write the package-level commitment
      counters (`domain_commitment_keys`, `domain_commitment_updates_applied`, `trie_state_skip_rate`,
      the per-level counters)
- [x] make the exec metrics read the canonical commitment domain's entry
- [x] write tests: a bin fold leaves the shared counters unchanged while a hex fold advances them
- [x] write tests: the exec metrics report the canonical domain before and after the flip
- [x] run `make test-short` — must pass before task 25

### Task 25: Freeze the hex domain on an explicit operator action

**Files:**
- Modify: `db/state/erigondb_settings.go`
- Modify: `db/state/aggregator2.go`
- Modify: `db/state/aggregator.go`
- Modify: `db/state/execctx/domain_shared.go`
- Modify: `execution/stagedsync/committer.go`
- Modify: `cmd/integration/commands/commitment.go`
- Modify: `db/state/erigondb_settings_test.go`
- Modify: `execution/stagedsync/committer_test.go`

- [x] add `frozen_at_txnum` per commitment domain to `erigondb.toml`, seeded at open
- [x] stop folding a frozen domain in the committer; refuse `DomainPut` against it; skip it in merge
- [x] refuse an unwind below `frozen_at_txnum` — that is a finality violation, not a recoverable state
- [x] expose the freeze as an explicit `integration commitment freeze --trie hex` action; no automatic
      finality trigger (see design decision 6)
- [x] write tests: after the freeze the hex domain stops advancing while accounts/storage continue, and
      the aggregator's minimax is unaffected
- [x] write tests: a `DomainPut` against a frozen domain errors; an unwind below the freeze point errors
- [x] write tests: the freeze survives a restart (read back from `erigondb.toml`, no resumed folding)
- [x] run `make test-short` — must pass before task 26

### Task 26: End-to-end hex+bin flip test

**Files:**
- Create: `execution/tests/pbt_dual_commitment_test.go`
- Modify: `execution/tests/testutil/` fixtures as needed

- [ ] build a `hex+bin` genesis fixture with `amsterdamTime` at genesis and `binaryTrieTime` a few blocks
      past it
- [ ] drive the chain through the flip and assert: hex root equals the header root before
      `binaryTrieTime`; bin root equals the header root from the flip block on
- [ ] assert both domains hold the flip block and its parent
- [ ] assert a reorg spanning the flip restores the correct canonical root from the predicate alone, with
      no migration-specific rollback code
- [ ] assert a shadow fold error leaves the block valid and marks the shadow stopped
- [ ] assert an unwind across the run does not panic in the diffset reader
- [ ] assert `debug_shadowStateRoot` returns the non-canonical root for a block on each side of the flip
- [ ] run `make test-short` — must pass before task 27

### Task 27: Verify acceptance criteria

- [ ] verify every requirement in Overview is implemented
- [ ] verify a `hex`-only datadir is unchanged: same file layout, same genesis header, and it still reads
      diffsets written before this change
- [ ] verify a `bin`-only datadir still runs the current devnet configuration
- [ ] run the full suite: `make test-all`
- [ ] run `go test -race` over `execution/commitment/...`, `db/state/...`, `execution/stagedsync/...`
- [ ] run the linter and confirm no new findings
- [ ] confirm no code comments were added (the hook denies them, but check the diff)
- [ ] confirm new files carry a 2026 copyright header

### Task 28: [Final] Update documentation

- [ ] add `docs/pbin-dual-commitment.md` describing the two-domain model, the three datadir modes, the
      freeze lifecycle and the debug API — citing source by identifier and file, never `file.go:NNN`
- [ ] update `docs/pbin-encoding.md` if the domain split changes anything it states
- [ ] update CLAUDE.md if new patterns emerged
- [ ] move this plan to `docs/plans/completed/`

## Post-Completion

*Items requiring manual intervention or external systems — no checkboxes, informational only*

**Devnet verification:**
- rebuild `erigon-pbt:local` and rerun `CPerezz/pbt-devnet` with a migration genesis (`binaryTrieTime`
  past genesis), 2 geth + 2 erigon, against the Docker memory ceiling noted in the devnet log
- compare shadow roots between erigon and geth per block across the pre-flip window via
  `debug_shadowStateRoot`; a divergence there is the signal the whole shadow period exists to produce
- confirm the canonical root matches across clients at the flip block and after
- rerun the reorg scenarios with an erigon node in the minority, spanning the flip
- note that `scripts/pbt.py status` omits erigon (it looks for the `rpc` port id while erigon publishes
  HTTP RPC as `ws-rpc`), so an "all clients agree" report from it excludes the node under test

**Upstream coordination:**
- the Task 1 change alters erigon's gas schedule for any chain carrying `binaryTrieTime`; confirm with
  the devnet operators that geth and besu charge the revised schedule from Amsterdam, and raise it on the
  EIP-8347 thread if they do not
- EIP-8347 leaves the shadow-root wire format to an unpublished companion EIP; the debug API here is
  erigon's local shape, matched to geth's, not a standard

**Deliberately out of scope, for a later plan:**
- EIP-8347 snapshot artifact export/import and the dual-check verifier
- BAL-replay catch-up and re-anchoring, for a node joining mid-window
- anchor-block finality check on the offline converter, and with it a finality-driven automatic freeze
- retargeting `integration commitment rebuild` to write the bin domain, which is what makes the existing
  430 GB mainnet artifact droppable into a `hex+bin` datadir
- downloader/manifest awareness of the `commitment-bin` files
