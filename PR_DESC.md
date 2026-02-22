## Summary

Add early return for zero-value transfers in `Transfer()` function to avoid unnecessary state operations on non-Aura chains.

PR #18846 ("fix Chiado re-exec from genesis") removed the zero-value check from `SubBalance()` to fix a Gnosis chain issue. However, this caused a significant performance regression for EVM execution, as every CALL with value=0 now triggers full balance change logic including:
- State object lookups via `GetOrNewStateObject()`
- Journal entries for balance changes (`balanceChange` structs)
- Snapshot revert processing

This fix adds the zero-check at the `Transfer()` function level, but **only for non-Aura chains**:
1. Restores performance by skipping both `SubBalance()` and `AddBalance()` for zero-value transfers
2. Preserves Gnosis/Aura compatibility by checking `rules.IsAura` - Aura chains still go through the full path to touch accounts for state root compatibility
3. Is safe because the EIP-7708 log emission is already guarded by `!amount.IsZero()`

## Benchmark Results

| Benchmark | Before | After | Improvement |
|-----------|--------|-------|-------------|
| call-identity-100M | 176ms, 1.3M allocs | 128ms, 3 allocs | **27% faster, 99.9% fewer allocs** |
| call-EOA-100M | 194ms, 1.5M allocs | 144ms, 0 allocs | **26% faster** |
| call-reverting-100M | 237ms, 1.4M allocs | 192ms, 0 allocs | **19% faster** |

## Test plan

- [x] `BenchmarkSimpleLoop` shows restored performance
- [ ] Verify Chiado re-execution from genesis still works
- [ ] Run full test suite
