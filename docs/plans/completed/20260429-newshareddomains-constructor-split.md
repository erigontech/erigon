# NewSharedDomains Constructor Split

## Overview
- Remove `TrieConfig` from the default `NewSharedDomains` constructor signature
- Add `NewSharedDomainsWithTrieConfig` for the few sites that need explicit trie configuration
- Eliminate ~70 unnecessary `commitment.DefaultTrieConfig()` arguments scattered across the codebase
- Preserve all existing behavior with the smallest clean change

## Context (from discovery)
- Constructor lives in `db/state/execctx/domain_shared.go:123`
- Current signature: `NewSharedDomains(ctx, tx, logger, cfg ...commitment.TrieConfig)`
- ~70 call sites pass `commitment.DefaultTrieConfig()` for no behavioral difference
- ~17 call sites pass explicit non-default configs (RPC, builder, squeeze, integrity, backtester)
- ~10 call sites already use `NewSharedDomains(ctx, tx, logger)` with no config arg
- Config types defined in `execution/commitment/config.go`

## Development Approach
- **testing approach**: Regular (code change is mechanical, existing tests validate correctness)
- Complete each task fully before moving to the next
- Make small, focused changes
- Run `make lint && make erigon integration` after each task to verify compilation
- No new tests needed — this is a pure signature refactor; existing tests cover behavior

## Solution Overview
Split into three functions:
1. `newSharedDomains(ctx, tx, logger, cfg)` — private shared initializer (current body)
2. `NewSharedDomains(ctx, tx, logger)` — public, uses DefaultTrieConfig + ExperimentalConcurrentCommitment override
3. `NewSharedDomainsWithTrieConfig(ctx, tx, logger, cfg)` — public, explicit config

The private helper contains all initialization logic. Both public constructors delegate to it.

## Implementation Steps

### Task 1: Split constructor in domain_shared.go

**Files:**
- Modify: `db/state/execctx/domain_shared.go`

- [x] Rename current `NewSharedDomains` to `newSharedDomains` (private, takes `cfg commitment.TrieConfig` non-variadic)
- [x] Create new `NewSharedDomains(ctx context.Context, tx kv.TemporalTx, logger log.Logger) (*SharedDomains, error)` that builds default config with ExperimentalConcurrentCommitment override and calls `newSharedDomains`
- [x] Create `NewSharedDomainsWithTrieConfig(ctx context.Context, tx kv.TemporalTx, logger log.Logger, cfg commitment.TrieConfig) (*SharedDomains, error)` that delegates to `newSharedDomains`
- [x] Verify `OpenSharedDomains` internal call at line 80 still compiles (already uses no-config signature)
- [x] Run `go build ./db/state/execctx/...` to confirm package compiles (callers will break — expected)

### Task 2: Migrate Group B — explicit-config call sites to NewSharedDomainsWithTrieConfig

**Files:**
- Modify: `rpc/jsonrpc/receipts/receipts_generator.go`
- Modify: `rpc/jsonrpc/eth_call.go`
- Modify: `rpc/jsonrpc/debug_execution_witness.go`
- Modify: `rpc/jsonrpc/eth_simulation.go`
- Modify: `rpc/rpchelper/commitment.go`
- Modify: `execution/builder/builder.go`
- Modify: `execution/builder/exec.go`
- Modify: `execution/engineapi/testing_api.go`
- Modify: `db/integrity/commitment_integrity.go`
- Modify: `db/state/squeeze.go`
- Modify: `execution/commitment/backtester/backtester.go`

- [x] Change all `NewSharedDomains(..., cfg)` calls in above files to `NewSharedDomainsWithTrieConfig(..., cfg)`
- [x] Run `go build ./rpc/... ./execution/... ./db/...` on modified packages to verify

### Task 3: Migrate Group A — remove DefaultTrieConfig() from all call sites

**Files:**
- Modify: `db/test/*.go` (multiple test files)
- Modify: `db/state/*_test.go` (multiple test files)
- Modify: `db/state/squeeze.go` (line 331 only)
- Modify: `db/kv/temporal/kv_temporal_test.go`
- Modify: `db/kv/kvcache/cache_test.go`
- Modify: `db/rawdb/rawtemporaldb/accessors_receipt_test.go`
- Modify: `db/rawdb/accessors_chain_test.go`
- Modify: `db/integrity/commitment_integrity.go` (line 205)
- Modify: `db/integrity/commitment_state_verify_test.go`
- Modify: `db/state/execctx/domain_shared_test.go`
- Modify: `db/state/execctx/flush_unwind_error_test.go`
- Modify: `execution/stagedsync/exec3_parallel_test.go`
- Modify: `execution/stagedsync/exec3_2cache_test.go`
- Modify: `execution/state/state_test.go`
- Modify: `execution/state/genesiswrite/genesis_write.go`
- Modify: `execution/vm/gas_table_test.go`
- Modify: `execution/vm/runtime/runtime.go`
- Modify: `execution/tests/testutil/state_test_util.go`
- Modify: `execution/tests/testutil/temporal_db.go`
- Modify: `execution/tests/blockgen/chain_makers.go`
- Modify: `execution/commitment/backtester/backtester.go` (if applicable)
- Modify: `txnprovider/txpool/pool_test.go`
- Modify: `cmd/integration/commands/commitment.go`
- Modify: `cmd/integration/commands/state_domains.go`
- Modify: `cmd/integration/commands/state_stages.go`
- Modify: `cmd/integration/commands/stages.go`
- Modify: `cmd/evm/runner.go`
- Modify: `cmd/evm/internal/t8ntool/transition.go`
- Modify: `rpc/jsonrpc/eth_simulation_intrablock_test.go`
- Modify: `db/state/squeeze_concurrent_rebuild_test.go`
- Modify: `db/state/trie_reader_integration_test.go`
- Modify: `db/state/aggregator_bench_test.go`
- Modify: `db/state/aggregator_fuzz_test.go`
- Modify: `db/test/domains_restart_test.go`
- Modify: `db/test/lifecycle_bench_test.go`
- Modify: `db/test/domain_shared_bench_test.go`
- Modify: `db/test/aggregator_ext_test.go`

- [x] Remove `, commitment.DefaultTrieConfig()` argument from all call sites listed above
- [x] Remove unused `commitment` imports where no other reference remains
- [x] Run `make erigon integration` to confirm full build

### Task 4: Final verification

- [x] Run `make lint` (repeat until clean — linter is non-deterministic)
- [x] Run `make test-short` to verify no regressions
- [x] Grep for any remaining `NewSharedDomains.*DefaultTrieConfig` calls that were missed
- [x] Grep for any remaining variadic `cfg ...commitment.TrieConfig` in the signature
- [x] Move this plan to `docs/plans/completed/`

## Post-Completion

**Manual verification:**
- Run full test suite (`make test-all`) before merging
- Verify PR diff is clean — no unrelated changes mixed in
