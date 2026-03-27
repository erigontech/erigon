# Commitment Rebuild Data Generator Test

## Overview
Create a Go integration test `TestGenerateCommitmentRebuildData` that generates a large-scale dataset (10M unique keys across accounts/storage/code domains, 59 steps) with valid commitment roots. The resulting datadir can be used for manual testing of `integration commitment rebuild` with concurrent commitment enabled.

This test exercises the full SharedDomains write + ComputeCommitment + BuildFiles pipeline at realistic scale, producing segment files suitable for `ERIGON_REBUILD_CONCURRENT_COMMITMENT=true ./build/bin/integration commitment rebuild --datadir <path> --chain hoodi --pprof`.

## Context (from discovery)

### Files/components involved:
- `db/state/squeeze_test.go` — target file, contains existing `TestAggregator_RebuildCommitmentBasedOnFiles` and helpers
- `db/state/squeeze.go` — `RebuildCommitmentFiles` (what the integration binary calls)
- `db/state/aggregator.go` — `BuildFiles`, `NewTest`, `ForTestReplaceKeysInValues`
- `db/kv/mdbx/kv_mdbx.go` — MDBX database creation with MapSize config
- `execution/commitment/commitmentdb/` — commitment context, `KeyCommitmentState`
- `execution/types/accounts/` — `SerialiseV3` for account encoding

### Related patterns found:
- `testDbAndAggregatorv3()` — creates temporal DB + aggregator with custom step size (InMem, 2GB MapSize)
- `testDbAggregatorWithNoFiles()` — writes account data, computes commitment at step boundaries, no file building
- `testDbAggregatorWithFiles()` — extends NoFiles with `agg.BuildFiles()`
- `ForTestReplaceKeysInValues(kv.CommitmentDomain, bool)` — must be set BEFORE writing data
- `domains.ComputeCommitment(ctx, tx, true, blockNum, txNum, "", nil)` — commitment computation
- `domains.Flush(ctx, tx)` should be called at step boundaries to manage memory

### Dependencies identified:
- `crypto/sha256` for deterministic key generation with uniform nibble distribution
- MDBX MapSize must be increased to ~16GB for 10M keys (default 2GB in squeeze tests is too small)
- For persistent datadir (`TEST_DATADIR` set): on-disk MDBX instead of InMem

## Development Approach
- **Testing approach**: Regular (the test IS the deliverable — it generates data)
- Complete each task fully before moving to the next
- Make small, focused changes
- **CRITICAL: all tests must pass before starting next task** — run `go test -run TestGenerateCommitmentRebuildData -count=1 -timeout 30m ./db/state/...` with small key counts first
- **CRITICAL: update this plan file when scope changes during implementation**

## Testing Strategy
- **Smoke test**: Run with small parameters first (1K keys, 3 steps) to verify correctness
- **Full run**: Run with full 10M keys, 59 steps only when smoke test passes
- **Validation**: Final root hash is non-empty and not the empty trie root

## Progress Tracking
- Mark completed items with `[x]` immediately when done
- Add newly discovered tasks with ➕ prefix
- Document issues/blockers with ⚠️ prefix

## Implementation Steps

### Task 1: Add deterministic key generation helpers

**Files:**
- Modify: `db/state/squeeze_test.go`

- [x] Add `makeAccountAddr(idx uint64) []byte` — uses sha256(seed + idx) truncated to 20 bytes for uniform first-nibble distribution
- [x] Add `makeStorageKey(addrIdx, slotIdx uint64) []byte` — composite(accountAddr, sha256(seed2 + addrIdx*maxSlots + slotIdx)[:32])
- [x] Add `makeCodeValue(idx uint64, rnd *rndGen) []byte` — generates deterministic bytecode (32-256 bytes)
- [x] Verify nibble distribution: quick sanity check that first nibbles of 1000 generated keys cover all 16 values
- [x] Run existing squeeze tests to ensure no regressions: `go test -run TestAggregator -count=1 ./db/state/...`

### Task 2: Add test DB helper for large datasets

**Files:**
- Modify: `db/state/squeeze_test.go`

- [x] Add `testDbAndAggregatorForLargeData(tb testing.TB, aggStep uint64, persistentDir string) (kv.TemporalRwDB, *state.Aggregator, datadir.Dirs)` helper
- [x] When `persistentDir != ""`: use on-disk MDBX at that path with 16GB MapSize (for integration binary compatibility)
- [x] When `persistentDir == ""`: use `t.TempDir()` with InMem MDBX, 16GB MapSize
- [x] Set `GrowthStep(64 * datasize.MB)` for better large-write performance
- [x] Return `dirs` so caller knows the output path
- [x] Run existing tests to verify no regressions

### Task 3: Implement TestGenerateCommitmentRebuildData

**Files:**
- Modify: `db/state/squeeze_test.go`

- [x] Add `TestGenerateCommitmentRebuildData` gated behind `!testing.Short()`
- [x] Read `TEST_DATADIR` env var for persistent directory
- [x] Configure: stepSize=100, totalSteps=59, totalTxs=5900
- [x] Configure: numAccounts=3_000_000, numStorageSlots=2 (per account = 6M total), numCodeAccounts=1_000_000
- [x] Call `ForTestReplaceKeysInValues(kv.CommitmentDomain, false)` before writes (matching rebuild default)
- [x] Main write loop: for each txNum, write batch of accounts + storage + code using deterministic key generators
  - ~508 accounts per tx, ~1017 storage per tx, ~170 code per tx
  - All use `domains.DomainPut(domain, tx, key, value, txNum, nil)` with nil prev (first write)
- [x] At every step boundary (`(txNum+1) % stepSize == 0`): call `domains.ComputeCommitment(ctx, tx, true, blockNum, txNum, "", nil)` and log root
- [x] At every step boundary: call `domains.Flush(ctx, tx)` to manage memory
- [x] After all writes: final `Flush` + `tx.Commit()` + `agg.BuildFiles(totalTxs)`
- [x] Assert final root is non-empty and != `empty.RootHash.Bytes()`
- [x] Log the output datadir path for manual use
- [x] Run with small parameters first: `TEST_SMALL=true` mode with numAccounts=1000, stepSize=10, totalSteps=3

### Task 4: Smoke test and parameter validation

**Files:**
- Modify: `db/state/squeeze_test.go`

- [x] Run `TestGenerateCommitmentRebuildData` with small params (1K accounts, 3 steps, stepSize=10)
- [x] Verify all 3 commitment computations succeed with non-empty roots
- [x] Verify files are built in the snapshots directory
- [x] Verify test completes in reasonable time (<30s for small params)
- [x] Run `make lint` to check for linter issues

### Task 5: Verify acceptance criteria

- [ ] Test is self-contained in `db/state/squeeze_test.go`
- [ ] Test is skipped with `-short` flag
- [ ] `TEST_DATADIR` env var controls persistent output directory
- [ ] Key generation produces uniform first-nibble distribution (all 16 nibbles)
- [ ] 3M accounts + 6M storage + 1M code = 10M total unique keys
- [ ] 59 steps with stepSize=100 produces correct merge tree (32+16+8+2+1)
- [ ] All commitment computations succeed
- [ ] Files are built on disk
- [ ] Run full test suite: `go test -count=1 ./db/state/...`

## Technical Details

### Parameters
| Parameter | Value | Rationale |
|-----------|-------|-----------|
| stepSize | 100 | Small enough for fast steps, 59*100=5900 txNums |
| totalSteps | 59 | Produces merge tree: 32+16+8+2+1 |
| numAccounts | 3,000,000 | Realistic account:storage ratio |
| storageSlots/account | 2 | 6M total storage keys |
| numCodeAccounts | 1,000,000 | First 1M accounts get code |
| MDBX MapSize | 16GB | ~10M keys × ~100 bytes avg + overhead |
| RNG seed | fixed (42) | Deterministic, reproducible |

### Key Generation (uniform nibble distribution)
```
accountAddr(i) = sha256(0xAC || i)[:20]
storageKey(i,j) = composite(accountAddr(i), sha256(0x57 || i*2+j)[:32])
codeAddr(i) = accountAddr(i)  // first 1M accounts
```

### Data Flow
```
Pre-generate keys (deterministic, on-the-fly)
  → For each txNum 0..5899:
      → DomainPut accounts batch
      → DomainPut storage batch
      → DomainPut code batch
      → Every 100 txNums: ComputeCommitment + Flush
  → tx.Commit()
  → agg.BuildFiles(5900)
  → Log datadir path
```

### Account Value Format
```go
acc := accounts.Account{
    Nonce:    txNum,
    Balance:  *uint256.NewInt(txNum * 1000),
    CodeHash: accounts.EmptyCodeHash, // or real hash for code accounts
    Incarnation: 0,
}
buf := accounts.SerialiseV3(&acc)
```

## Post-Completion

**Manual verification:**
- Run with persistent datadir: `TEST_DATADIR=/tmp/commitment-rebuild-test go test -run TestGenerateCommitmentRebuildData -count=1 -timeout 2h ./db/state/...`
- Build integration binary: `make integration`
- Run rebuild: `ERIGON_REBUILD_CONCURRENT_COMMITMENT=true ./build/bin/integration commitment rebuild --datadir /tmp/commitment-rebuild-test --chain hoodi --pprof`
- Compare rebuilt commitment root with reference root logged by the test
