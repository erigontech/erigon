# Genesis + Chain Config Writing Consolidation

## Overview

Consolidate the fragmented paths that write a genesis block and its associated chain-DB metadata in Erigon. Today there are four+ overlapping entry points (`CommitGenesisBlock`, `CommitGenesisBlockWithOverride`, `WriteGenesisBlock`, `MustCommitGenesis`, `WriteGenesisBesideState`) with inconsistent semantics. The most serious problem: `MustCommitGenesis` and direct `WriteGenesisBesideState` callers never call `rawdb.WriteGenesisIfNotExist`, so DBs initialized through those paths lack the `ConfigTable[GenesisKey]` entry that `rawdb.ReadGenesis` looks for. `node/eth/backend.go:346` silently falls back when the read returns nil, masking the inconsistency.

This refactor introduces a two-layer split:

- **Layer 1 (rawdb):** a `GenesisBundle` type and `WriteGenesisBundle` / `ReadGenesisBundle` functions that own "the bytes that make a valid chain DB at block 0." Pure persistence — no policy, no state computation, no logger.
- **Layer 2 (genesiswrite):** a single `CommitGenesis` / `CommitGenesisTx` entry point with an `Options` struct. Owns the policy: stored-vs-fresh branching, config compatibility, overrides, chain-name fallback, state root computation, and the "repair missing chain config" path.

Scope is **medium** (scope B from brainstorm). Explicit non-goals: BorJSON asymmetry, `DatabaseInfo` version stamping, stages progress seeding, snapshot download marker, `chainspec.*GenesisBlock()` constructor duplication. Those are separate refactors.

## Context (from discovery)

**Files involved:**
- `db/rawdb/accessors_metadata.go` — contains today's `ReadChainConfig`, `WriteChainConfig`, `WriteGenesisIfNotExist`, `ReadGenesis` primitives
- `execution/state/genesiswrite/genesis_write.go` — current policy layer with all 5 overlapping entry points
- `execution/state/genesiswrite/genesis_test.go` — ~10 call sites using the old API
- `node/eth/backend.go:342-373` — manual "read genesis → check canonical → pass nil if exists" dance that duplicates policy
- `cmd/utils/app/init_cmd.go:109`, `cmd/integration/commands/stages.go:1149`, `cmd/evm/runner.go:181` — CLI callers
- `execution/execmodule/execmoduletester/exec_module_tester.go:485`, `execution/engineapi/engineapitester/engine_api_tester.go:204` — test harness callers
- `execution/protocol/rules/aura/aura_test.go:168` — the only direct caller of `WriteGenesisBesideState`
- `p2p/sentry/sentry_grpc_server_test.go:602,603,691` — `MustCommitGenesis` test callers

**Related patterns:**
- Existing `rawdb` primitives (`WriteBlock`, `WriteTd`, `WriteCanonicalHash`, `WriteHeadBlockHash`, `WriteHeadHeaderHash`) are composed into the new `WriteGenesisBundle`.
- `rawdbv3.TxNums.Append` is a separate subsystem and stays at the genesiswrite layer via a private helper.

**Dependencies:**
- `rawdb` must not gain a dependency on `genesiswrite` (no cycle risk in this direction).
- `rawdb` does NOT need to import `rawdbv3` — TxNums handling is deliberately excluded from the bundle.

**Observed bugs to fix in passing:**
- `WriteGenesisBlock:127` calls `WriteGenesisIfNotExist` before the `genesis.Config == nil` check on line 132. New flow validates before persisting.
- Fresh-DB path writes chain config twice (once inside `WriteGenesisBesideState:309`, once at `WriteGenesisBlock:227`) — both with the same hash, so no wrong bytes, just wasted writes.

## Development Approach

- **testing approach:** Regular (code first, then tests per task — brainstorm defined tests as separate checklist items per task)
- complete each task fully before moving to the next
- make small, focused changes
- **CRITICAL: every task MUST include new/updated tests** for code changes in that task
- **CRITICAL: all tests must pass before starting next task** — no exceptions
- **CRITICAL: update this plan file when scope changes during implementation**
- run `make lint` after each task — the linter is non-deterministic per CLAUDE.md, run repeatedly until clean
- maintain backward compatibility — no schema change, no migration, existing DBs load identically

## Testing Strategy

- **unit tests:** required for every task
- **regression net:** Task 2 introduces a fresh-DB key-presence assertion that checks every required KV entry exists after `CommitGenesis`. This is the primary safeguard against any future refactor omitting a mandatory field.
- **round-trip test:** Task 1 adds a rawdb-level test that writes a bundle and reads it back, asserting byte-equality. No state computation involved — isolated from the policy layer.
- **repair path test:** Task 2 seeds a DB with a block but no chain config, calls `CommitGenesis`, asserts config is written.
- **no e2e tests:** Erigon doesn't have UI-driven e2e tests. Acceptance is `make lint && make erigon integration` + full test suite.

## Progress Tracking

- mark completed items with `[x]` immediately when done
- add newly discovered tasks with ➕ prefix
- document issues/blockers with ⚠️ prefix
- update plan if implementation deviates from original scope
- keep plan in sync with actual work done

## What Goes Where

- **Implementation Steps** (`[ ]` checkboxes): all Go code changes and tests — these are achievable inside the erigon repo
- **Post-Completion** (no checkboxes): PR description talking points, manual smoke test guidance

## Implementation Steps

### Task 1: Add `rawdb.GenesisBundle` type and Write/Read functions

**Files:**
- Modify: `db/rawdb/accessors_metadata.go`
- Create: `db/rawdb/genesis_bundle_test.go`

- [x] add `GenesisBundle` struct type (fields: `Genesis *types.Genesis`, `Config *chain.Config`, `Block *types.Block`, `TD *big.Int`) to `accessors_metadata.go`
- [x] add `WriteGenesisBundleOpts` struct with `FreshDB bool`
- [x] implement `WriteGenesisBundle(tx kv.RwTx, b *GenesisBundle, opts WriteGenesisBundleOpts) error`:
      Fresh-DB path (opts.FreshDB == true): `WriteGenesisIfNotExist(tx, b.Genesis)` → `WriteBlock(tx, b.Block)` → `WriteTd(tx, hash, 0, b.TD)` → `WriteCanonicalHash(tx, hash, 0)` → `WriteHeadBlockHash(tx, hash)` → `WriteHeadHeaderHash(tx, hash)` → `WriteChainConfig(tx, hash, b.Config)`. Fail-fast on any error; caller's tx handles rollback.
      Config-only path (opts.FreshDB == false): only `WriteChainConfig(tx, b.Block.Hash(), b.Config)`.
- [x] implement `ReadGenesisBundle(tx kv.Getter) (*GenesisBundle, error)`:
      returns a bundle where any field may be nil if not persisted. Logic: read `ReadGenesis(tx)` → `ReadCanonicalHash(tx, 0)` → if canonical hash present, `ReadBlockWithSenders(tx, hash, 0)`, `ReadTd(tx, hash, 0)`, `ReadChainConfig(tx, hash)`.
- [x] create `db/rawdb/genesis_bundle_test.go` with a bundle round-trip test: build a minimal `GenesisBundle`, `WriteGenesisBundle(tx, b, {FreshDB: true})`, `ReadGenesisBundle(tx)`, assert all 4 fields equal byte-for-byte (use `reflect.DeepEqual` or per-field checks for pointers)
- [x] write a second test for the `FreshDB: false` path: seed bundle via fresh write, then rewrite with `FreshDB: false` using a different chain config, read back, assert only the config changed
- [x] run `go test ./db/rawdb/...` — must pass before task 2
- [x] run `make lint` — fix any issues

### Task 2: Add `genesiswrite.CommitGenesis` / `CommitGenesisTx` with `Options`

**Files:**
- Modify: `execution/state/genesiswrite/genesis_write.go`
- Create: `execution/state/genesiswrite/commit_genesis_test.go`

- [x] add `Options` struct with fields: `Genesis *types.Genesis`, `ChainName string`, `OverrideOsakaTime *uint64`, `OverrideAmsterdamTime *uint64`, `KeepStoredChainConfig bool`, `Dirs datadir.Dirs`, `Logger log.Logger`
- [x] add unexported helper `writeFreshGenesisDB(tx kv.RwTx, b *rawdb.GenesisBundle) error`:
      calls `rawdb.WriteGenesisBundle(tx, b, rawdb.WriteGenesisBundleOpts{FreshDB: true})` then `rawdbv3.TxNums.Append(tx, 0, uint64(b.Block.Transactions().Len()+1))`. This is the ONLY path CommitGenesis* uses for fresh-DB writes so TxNums can't be forgotten.
- [x] implement `CommitGenesisTx(tx kv.RwTx, opts Options) (*chain.Config, *types.Block, error)`:
      1. `stored, err := rawdb.ReadGenesisBundle(tx)` (reuse Task 1 primitive)
      2. if `stored.Block == nil` (fresh DB):
         - pick `g := opts.Genesis`; if nil, `g = chainspec.MainnetGenesisBlock()` and log "Writing main-net genesis block"
         - if `g.Config == nil` return `types.ErrGenesisNoConfig` (this fixes the ordering bug — validate BEFORE persisting)
         - apply overrides via local helper (same logic as current `applyOverrides`)
         - `if err := g.Config.CheckConfigForkOrder(); err != nil { return nil, nil, err }`
         - `block, _, err := GenesisToBlock(nil, g, opts.Dirs, opts.Logger)`
         - `err := writeFreshGenesisDB(tx, &rawdb.GenesisBundle{Genesis: g, Config: g.Config, Block: block, TD: g.Difficulty.ToBig()})`
         - if custom genesis (opts.Genesis != nil), log "Writing custom genesis block"
         - return `g.Config, block, nil`
      3. else (existing DB):
         - if `opts.Genesis != nil`, derive its block hash via `GenesisToBlock`; if != `stored.Block.Hash()`, return `&GenesisMismatchError{...}`
         - `newCfg := configOrDefault(opts.Genesis, opts.ChainName, stored.Block.Hash())`
         - apply overrides
         - `if err := newCfg.CheckConfigForkOrder(); err != nil { return newCfg, nil, err }`
         - if `stored.Config == nil` (repair branch): log "Found genesis block without chain config", `rawdb.WriteGenesisBundle(tx, &rawdb.GenesisBundle{Block: stored.Block, Config: newCfg}, {FreshDB: false})`, return
         - existing-config special-case: if `opts.Genesis == nil && !opts.KeepStoredChainConfig`, check whether stored matches a known spec by name (same logic as today lines 210-216); flip `KeepStoredChainConfig = true` if unknown, then `newCfg = storedCfg` + apply overrides
         - compat check against head height (same logic as today lines 220-226); if incompatible error, return it with `stored.Block`
         - `rawdb.WriteGenesisBundle(tx, &rawdb.GenesisBundle{Block: stored.Block, Config: newCfg}, {FreshDB: false})`
         - return `newCfg, stored.Block, nil`
- [x] implement `CommitGenesis(ctx context.Context, db kv.RwDB, opts Options) (*chain.Config, *types.Block, error)` as trivial wrapper: `BeginRw` → `CommitGenesisTx` → `Commit` (or `Rollback` on error)
- [x] implement `CommitGenesisTxWithPrecomputedBlock(tx kv.RwTx, opts Options, block *types.Block) (*chain.Config, *types.Block, error)`:
      same as `CommitGenesisTx` fresh-DB branch, but skip `GenesisToBlock` and use the caller-provided block. Config must come from `opts.Genesis.Config`. Used only by `aura_test.go` where the block is computed separately so its IBS can be wired into the real temporal domains.
- [x] create `execution/state/genesiswrite/commit_genesis_test.go` with:
      (a) **fresh-DB key-presence regression test** (THE regression net): after `CommitGenesis` on empty DB, assert every required KV entry exists: `ConfigTable[GenesisKey]`, `ConfigTable[blockHash]` (chain config), `Headers[blockHash]`, `BlockBody[blockHash]`, `HeaderTD[blockHash]`, `HeaderCanonical[0]`, `HeadBlockHash`, `HeadHeaderHash`, `TxNums[0]`. Each missing key is a distinct subtest failure.
      (b) **repair path test**: seed DB with block but no chain config (simulate stored_cfg == nil branch), call `CommitGenesis`, assert `ReadChainConfig` now returns non-nil.
      (c) **ordering bug fix test**: pass `Options{Genesis: &types.Genesis{Config: nil}}`, assert returns `ErrGenesisNoConfig` AND asserts `ReadGenesis(tx)` returns nil (the genesis JSON was NOT persisted — old behavior would have persisted it before validating).
      (d) **mismatch test**: seed DB with mainnet, call `CommitGenesis` with sepolia spec, assert `*GenesisMismatchError`.
- [x] run `go test ./execution/state/genesiswrite/...` — must pass before task 3
- [x] run `make lint` — fix any issues

### Task 3: Add `genesistest` test helper package with `MustCommitGenesis`

**Files:**
- Create: `execution/state/genesiswrite/genesistest/genesistest.go`

- [x] create package `genesistest` in `execution/state/genesiswrite/genesistest/`
- [x] implement `MustCommitGenesis(tb testing.TB, g *types.Genesis, db kv.RwDB, dirs datadir.Dirs, logger log.Logger) *types.Block`:
      wraps `genesiswrite.CommitGenesis(context.Background(), db, genesiswrite.Options{Genesis: g, Dirs: dirs, Logger: logger})`, calls `tb.Fatal(err)` on error, returns the block. `tb` may be nil for non-test callers (cmd/evm/runner-style) — in that case, panic on error.
- [x] no separate test file needed — this is a thin wrapper; coverage comes from the tests in downstream tasks that use it
- [x] run `go build ./...` — must compile before task 4
- [x] run `make lint` — fix any issues

### Task 4: Migrate `node/eth/backend.go` to `CommitGenesisTx`

**Files:**
- Modify: `node/eth/backend.go`

- [x] locate the genesis-writing block at `node/eth/backend.go:342-373`
- [x] delete the manual `rawdb.ReadGenesis` + `rawdb.ReadCanonicalHash` + "pass nil if canonical exists" dance
- [x] replace with: `bundle, err := rawdb.ReadGenesisBundle(tx); if err != nil { return err }; if bundle.Genesis != nil { config.Genesis = bundle.Genesis }`
- [x] then call `chainConfig, genesis, genesisErr = genesiswrite.CommitGenesisTx(tx, genesiswrite.Options{Genesis: config.Genesis, ChainName: config.Snapshot.ChainName, OverrideOsakaTime: config.OverrideOsakaTime, OverrideAmsterdamTime: config.OverrideAmsterdamTime, KeepStoredChainConfig: config.KeepStoredChainConfig, Dirs: dirs, Logger: logger})`
- [x] keep the existing `*chain.ConfigCompatError` check unchanged
- [x] ⚠️ verify nothing downstream relies on the old timing of `config.Genesis` being rewritten — grep `config.Genesis` in backend.go and callsites (only downstream use at ~line 842 passes config.Genesis into block builder; still populated from bundle.Genesis before that)
- [x] add/update unit tests for backend.go genesis-init path if existing tests exist (check `node/eth/*_test.go`); if no tests, document this as a gap without adding new — the regression net from Task 2 covers the underlying CommitGenesisTx logic (backend_test.go only tests RemoveContents — no genesis-init coverage gap documented here, covered by Task 2's regression net)
- [x] run `go build ./node/eth/...` and `go test ./node/eth/...` — must pass before task 5
- [x] run `make lint`

### Task 5: Migrate `cmd/` callers

**Files:**
- Modify: `cmd/utils/app/init_cmd.go`
- Modify: `cmd/integration/commands/stages.go`
- Modify: `cmd/evm/runner.go`

- [ ] `init_cmd.go:109`: replace `genesiswrite.CommitGenesisBlock(chaindb, genesis, cliCtx.String(utils.ChainFlag.Name), datadir.New(...), logger)` with `genesiswrite.CommitGenesis(cliCtx.Context, chaindb, genesiswrite.Options{Genesis: genesis, ChainName: cliCtx.String(utils.ChainFlag.Name), Dirs: datadir.New(...), Logger: logger})`. **BorJSON manual decode on lines 82-90 stays** — explicit non-goal.
- [ ] `stages.go:1149`: replace `genesiswrite.CommitGenesisBlock(db, genesis, chain, dirs, logger)` with `genesiswrite.CommitGenesis(ctx, db, genesiswrite.Options{Genesis: genesis, ChainName: chain, Dirs: dirs, Logger: logger})`
- [ ] `cmd/evm/runner.go:181`: replace `genesiswrite.MustCommitGenesis(gen, db, datadir.New(tmpDir), log.Root())` with:
      ```go
      if _, _, err := genesiswrite.CommitGenesis(context.Background(), db, genesiswrite.Options{Genesis: gen, Dirs: datadir.New(tmpDir), Logger: log.Root()}); err != nil {
          panic(err)
      }
      ```
- [ ] update any existing tests in `cmd/utils/app/`, `cmd/integration/commands/`, `cmd/evm/` that exercise these call sites
- [ ] if no tests exist for these entry points, rely on the regression net from Task 2 and add a comment noting the coverage gap
- [ ] run `go build ./cmd/...` — must pass before task 6
- [ ] run `make lint`

### Task 6: Migrate test harnesses (`engineapitester`, `execmoduletester`)

**Files:**
- Modify: `execution/engineapi/engineapitester/engine_api_tester.go`
- Modify: `execution/execmodule/execmoduletester/exec_module_tester.go`

- [ ] `engine_api_tester.go:204`: replace `genesiswrite.CommitGenesisBlock(chainDB, genesis, networkname.Mainnet, ethNode.Config().Dirs, logger)` with `genesiswrite.CommitGenesis(ctx, chainDB, genesiswrite.Options{Genesis: genesis, ChainName: networkname.Mainnet, Dirs: ethNode.Config().Dirs, Logger: logger})`
- [ ] `exec_module_tester.go:485`: replace `genesiswrite.CommitGenesisBlock(mock.DB, gspec, "", datadir.New(tmpdir), mock.Log)` with `genesiswrite.CommitGenesis(context.Background(), mock.DB, genesiswrite.Options{Genesis: gspec, Dirs: datadir.New(tmpdir), Logger: mock.Log})`
- [ ] run the downstream tests that use these harnesses: `go test ./execution/engineapi/...` and `go test ./execution/execmodule/...` — must pass before task 7
- [ ] run `make lint`

### Task 7: Migrate `aura_test.go` to `CommitGenesisTxWithPrecomputedBlock`

**Files:**
- Modify: `execution/protocol/rules/aura/aura_test.go`

- [ ] locate the `WriteGenesisBesideState` call at line 168
- [ ] the test keeps its manual `GenesisToBlock` call (line 161) and `MakeWriteSet` to domains (lines 163-165) unchanged — the point is that the block's IBS goes into the real temporal domains, not the disposable one
- [ ] replace `genesiswrite.WriteGenesisBesideState(genesisBlock, tx, genesis)` with:
      ```go
      _, _, err = genesiswrite.CommitGenesisTxWithPrecomputedBlock(tx, genesiswrite.Options{Genesis: genesis, Dirs: dirs, Logger: logger}, genesisBlock)
      require.NoError(err)
      ```
- [ ] ⚠️ verify: `CommitGenesisTxWithPrecomputedBlock` must NOT call `GenesisToBlock` internally (that would create a fresh disposable temporal DB and defeat the purpose of pre-computing). Re-read Task 2 implementation.
- [ ] run `go test ./execution/protocol/rules/aura/...` — must pass before task 8
- [ ] run `make lint`

### Task 8: Migrate `p2p/sentry` tests to `genesistest.MustCommitGenesis`

**Files:**
- Modify: `p2p/sentry/sentry_grpc_server_test.go`

- [ ] replace the 3 occurrences of `genesiswrite.MustCommitGenesis(gspec, db, datadir.New(t.TempDir()), log.Root())` at lines 602, 603, 691 with `genesistest.MustCommitGenesis(t, gspec, db, datadir.New(t.TempDir()), log.Root())`
- [ ] add import for the new `genesistest` package
- [ ] run `go test ./p2p/sentry/...` — must pass before task 9
- [ ] run `make lint`

### Task 9: Rewrite `execution/state/genesiswrite/genesis_test.go`

**Files:**
- Modify: `execution/state/genesiswrite/genesis_test.go`

- [ ] this is the biggest mechanical diff — ~10 call sites
- [ ] replace each `genesiswrite.WriteGenesisBlock(tx, spec.Genesis, network, nil, nil, false, ...)` with `genesiswrite.CommitGenesisTx(tx, genesiswrite.Options{Genesis: spec.Genesis, ChainName: network, Dirs: ..., Logger: logger})`
- [ ] replace each `genesiswrite.CommitGenesisBlock(db, genesis, chainName, dirs, logger)` with `genesiswrite.CommitGenesis(context.Background(), db, genesiswrite.Options{Genesis: genesis, ChainName: chainName, Dirs: dirs, Logger: logger})`
- [ ] replace each `genesiswrite.MustCommitGenesis(&customg, db, ..., logger)` with `genesistest.MustCommitGenesis(t, &customg, db, ..., logger)`
- [ ] verify each test still exercises the same semantic case it did before (double-pass, mismatch, config compat, repair, etc.)
- [ ] add any missing test coverage revealed during the rewrite
- [ ] run `go test ./execution/state/genesiswrite/...` — must pass before task 10
- [ ] run `make lint`

### Task 10: Delete dead code and unexport `WriteGenesisBesideState`

**Files:**
- Modify: `execution/state/genesiswrite/genesis_write.go`

- [ ] ⚠️ before deleting: `grep -rn "genesiswrite\\.\\(CommitGenesisBlock\\|CommitGenesisBlockWithOverride\\|WriteGenesisBlock\\|MustCommitGenesis\\|WriteGenesisBesideState\\)" /home/agent/erigon` — confirm only migrated callers exist (or none). If hidden callers surface, migrate them in this task before deletion.
- [ ] delete `CommitGenesisBlock`
- [ ] delete `CommitGenesisBlockWithOverride`
- [ ] delete `WriteGenesisBlock`
- [ ] delete `MustCommitGenesis`
- [ ] delete private `write()` helper (inlined into the fresh-DB branch of `CommitGenesisTx` in Task 2, so already orphaned)
- [ ] rename `WriteGenesisBesideState` → `writeBundleToTx` and unexport it. It is now called only internally by `CommitGenesisTx` / `CommitGenesisTxWithPrecomputedBlock`. If it turns out the internal paths no longer need it (because Task 2 composed directly from `writeFreshGenesisDB`), delete it entirely.
- [ ] re-verify nothing else imports `WriteGenesisBesideState`: `grep -rn "WriteGenesisBesideState" /home/agent/erigon` should return nothing after rename
- [ ] confirm `WriteGenesisState`, `GenesisToBlock`, `GenesisWithoutStateToBlock`, `ComputeGenesisCommitment` remain exported — those are used elsewhere
- [ ] run `go build ./...` — full repo must compile
- [ ] run `go test ./execution/state/genesiswrite/...` — all tests must still pass
- [ ] run `make lint` — multiple times until clean

### Task 11: Verify acceptance criteria

- [ ] `grep -rn "CommitGenesisBlock\\|CommitGenesisBlockWithOverride\\|MustCommitGenesis\\|WriteGenesisBlock\\|WriteGenesisBesideState" /home/agent/erigon` returns zero matches (outside the test helper in `genesistest` package, which has its own `MustCommitGenesis` — grep for full qualified names like `genesiswrite.MustCommitGenesis` to distinguish)
- [ ] `make erigon integration` — both binaries build
- [ ] `make test-short` passes
- [ ] `make test-all` passes (full suite)
- [ ] `make lint` clean on first run (run a second time since lint is non-deterministic per CLAUDE.md)
- [ ] spot check: start `./build/bin/erigon --datadir=dev --chain=dev --mine` on a fresh datadir, confirm genesis logs appear and node boots
- [ ] spot check: restart on the same datadir, confirm `ReadGenesisBundle`-backed path loads without "Found genesis block without chain config" warning
- [ ] verify the new regression test from Task 2 would catch a regression: temporarily comment out the `WriteChainConfig` call inside `WriteGenesisBundle`, run the fresh-DB key-presence test, confirm it fails with a clear message, revert

### Task 12: Final — documentation & close-out

- [ ] no README or CLAUDE.md update needed (this is an internal refactor with no user-visible API change)
- [ ] add a brief comment on `rawdb.GenesisBundle` explaining the "bundle" contract and what is deliberately excluded (TxNums, DatabaseInfo, stages progress)
- [ ] add a brief comment on `genesiswrite.Options` explaining the fresh-vs-existing branching
- [ ] move this plan to `docs/plans/completed/`

## Technical Details

### Data flow (fresh DB)

```
caller (cmd/init_cmd.go)
  └─ genesiswrite.CommitGenesis(ctx, db, Options{Genesis, ChainName, ..., Dirs, Logger})
       ├─ BeginRw
       └─ CommitGenesisTx(tx, opts)
            ├─ rawdb.ReadGenesisBundle(tx) → {Block: nil, ...}  (fresh)
            ├─ pick genesis (opts.Genesis or MainnetGenesisBlock)
            ├─ validate: g.Config != nil, CheckConfigForkOrder
            ├─ GenesisToBlock(nil, g, dirs, logger) → block (computes state root)
            └─ writeFreshGenesisDB(tx, &rawdb.GenesisBundle{Genesis, Config, Block, TD})
                 ├─ rawdb.WriteGenesisBundle(tx, b, {FreshDB: true})
                 │    ├─ WriteGenesisIfNotExist(tx, Genesis)  ← ConfigTable[GenesisKey]
                 │    ├─ WriteBlock(tx, Block)
                 │    ├─ WriteTd(tx, hash, 0, TD)
                 │    ├─ WriteCanonicalHash(tx, hash, 0)
                 │    ├─ WriteHeadBlockHash(tx, hash)
                 │    ├─ WriteHeadHeaderHash(tx, hash)
                 │    └─ WriteChainConfig(tx, hash, Config)  ← ConfigTable[hash]
                 └─ rawdbv3.TxNums.Append(tx, 0, tx_count)
```

### Data flow (existing DB, config upgrade)

```
caller
  └─ CommitGenesisTx(tx, opts)
       ├─ rawdb.ReadGenesisBundle(tx) → {Block: storedBlock, Config: storedCfg, ...}
       ├─ if opts.Genesis != nil: verify hash match or return GenesisMismatchError
       ├─ newCfg = configOrDefault(opts.Genesis, opts.ChainName, storedHash)
       ├─ apply overrides, CheckConfigForkOrder
       ├─ compat check against head height
       └─ rawdb.WriteGenesisBundle(tx, &rawdb.GenesisBundle{Block: storedBlock, Config: newCfg}, {FreshDB: false})
            └─ WriteChainConfig(tx, storedBlock.Hash(), newCfg)  ← only this
```

### Key signatures

```go
// package rawdb
type GenesisBundle struct {
    Genesis *types.Genesis
    Config  *chain.Config
    Block   *types.Block
    TD      *big.Int
}
type WriteGenesisBundleOpts struct {
    FreshDB bool
}
func WriteGenesisBundle(tx kv.RwTx, b *GenesisBundle, opts WriteGenesisBundleOpts) error
func ReadGenesisBundle(tx kv.Getter) (*GenesisBundle, error)

// package genesiswrite
type Options struct {
    Genesis               *types.Genesis
    ChainName             string
    OverrideOsakaTime     *uint64
    OverrideAmsterdamTime *uint64
    KeepStoredChainConfig bool
    Dirs                  datadir.Dirs
    Logger                log.Logger
}
func CommitGenesis(ctx context.Context, db kv.RwDB, opts Options) (*chain.Config, *types.Block, error)
func CommitGenesisTx(tx kv.RwTx, opts Options) (*chain.Config, *types.Block, error)
func CommitGenesisTxWithPrecomputedBlock(tx kv.RwTx, opts Options, block *types.Block) (*chain.Config, *types.Block, error)

// package genesistest
func MustCommitGenesis(tb testing.TB, g *types.Genesis, db kv.RwDB, dirs datadir.Dirs, logger log.Logger) *types.Block
```

### Deletions

| Symbol | Action |
|---|---|
| `genesiswrite.CommitGenesisBlock` | delete |
| `genesiswrite.CommitGenesisBlockWithOverride` | delete |
| `genesiswrite.WriteGenesisBlock` | delete |
| `genesiswrite.MustCommitGenesis` | delete (replaced by `genesistest.MustCommitGenesis`) |
| `genesiswrite.WriteGenesisBesideState` | unexport → `writeBundleToTx`, or delete if no internal caller needs it |
| `genesiswrite.write` (private) | delete (inlined) |

### Backward compatibility

No schema change. No migration. Every key this refactor writes is a key the old code also wrote. An existing datadir on disk loads identically via the new `ReadGenesisBundle`.

## Post-Completion

**PR description talking points:**
- Fixes a silent inconsistency where `MustCommitGenesis` paths created DBs missing the `ConfigTable[GenesisKey]` entry, causing `rawdb.ReadGenesis` to return nil on reload.
- Fixes an ordering bug where `WriteGenesisBlock` persisted the genesis JSON before validating `genesis.Config != nil`.
- Consolidates 5 overlapping entry points into one `CommitGenesis` / `CommitGenesisTx` with an `Options` struct.
- No schema change, no migration — fully backward-compatible for existing datadirs.
- Out of scope: BorJSON asymmetry cleanup, `DatabaseInfo` versioning, chainspec constructor consolidation (tracked separately).

**Manual verification (after merge):**
- Smoke test on a fresh `--chain=dev` datadir: start miner, confirm genesis logs, restart on same datadir, confirm no "Found genesis block without chain config" warning.
- Smoke test on a `--chain=mainnet` rebuild against a copy of a known-good datadir: confirm no `GenesisMismatchError` and the chain config update path (existing-DB branch) works.
- CI integration tests on all chains (mainnet, sepolia, hoodi, gnosis, chiado, bor-mainnet, amoy) — the `make test-all` target should cover this but watch for any chain-specific failures.

**External system updates:** none — internal refactor only.
