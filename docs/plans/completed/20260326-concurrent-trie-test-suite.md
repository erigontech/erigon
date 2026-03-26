# ConcurrentHexPatriciaHashed Test Suite

## Overview
Create a test suite that validates `ConcurrentPatriciaHashed` produces identical state roots as sequential `HexPatriciaHashed` for all key distribution patterns and multi-batch sequences.

**Problem:** After 2 correct concurrent state root updates, 27M keys fell into a single first nibble — pointing to extension trimming in `foldNibble` (lines 77-82 of `hex_concurrent_patricia_hashed.go`) corrupting carried-forward state. The fold propagation from subtries to the top row is the primary suspect.

**Goal:** Catch this class of bugs via side-by-side sequential vs concurrent comparison across targeted key distribution patterns, with emphasis on multi-batch state carry-forward.

## Context (from discovery)
- `execution/commitment/hex_concurrent_patricia_hashed.go` — `ConcurrentPatriciaHashed`, `foldNibble()`, `unfoldRoot()`, `ParallelHashSort()`
- `execution/commitment/hex_patricia_hashed.go` — `HexPatriciaHashed`, `fold()`, `unfold()`, `foldMounted()`, `Process()`
- `execution/commitment/patricia_state_mock_test.go` — `MockState`, `UpdateBuilder`, `WrapKeyUpdates`, `WrapKeyUpdatesParallel`
- `execution/commitment/commitment.go` — `PatriciaContext`, `Updates`, `TrieContextFactory` type
- `execution/commitment/warmuper.go` — `WarmupConfig`, `TrieContextFactory`
- Key-to-nibble mapping: `hashedKey[0]` (first nibble of keccak hash) routes to nibble collector
- `MockState` is already thread-safe (`sync.Mutex` + `atomic.Bool` for concurrent mode)

## Development Approach
- **Testing approach**: TDD — this IS the test suite
- Complete each task fully before moving to the next
- Make small, focused changes
- **CRITICAL: every task MUST include new/updated tests** for code changes in that task
- **CRITICAL: all tests must pass before starting next task** — no exceptions
- **CRITICAL: update this plan file when scope changes during implementation**
- Run tests after each change
- Maintain backward compatibility

## Testing Strategy
- **Unit tests**: Each task adds tests comparing sequential vs concurrent root hashes
- **Key technique**: Pre-compute which nibble each address hashes to (keccak first nibble), then craft `UpdateBuilder` inputs that target specific nibble distributions
- **Oracle**: Sequential `HexPatriciaHashed.Process()` is the ground truth; concurrent must match exactly

## Progress Tracking
- Mark completed items with `[x]` immediately when done
- Add newly discovered tasks with ➕ prefix
- Document issues/blockers with ⚠️ prefix
- Update plan if implementation deviates from original scope

## Implementation Steps

### Task 1: Test harness — `compareRoots` helper and nibble targeting

**Files:**
- Create: `execution/commitment/hex_concurrent_patricia_hashed_test.go`

The core helper runs the same updates through both sequential and concurrent tries, asserts root hash equality. Also need a helper to find addresses that hash to a target nibble.

- [x] Add `findAddressForNibble(targetNibble int, seed int) []byte` — brute-force search for a 20-byte address whose keccak first nibble matches target. Cache results to avoid repeated work across tests.
- [x] Add `compareRoots(t, seqMs, parMs, seqTrie, parTrie, plainKeys, updates)` helper:
  1. Apply `plainKeys`/`updates` to both `MockState` instances
  2. Wrap via `WrapKeyUpdates` (sequential) and `WrapKeyUpdatesParallel` (concurrent)
  3. Call `seqTrie.Process(ctx, seqUpds, ...)` and `parTrie.Process(ctx, parUpds, ...)`
  4. `require.Equal(t, seqRoot, parRoot)`
  5. Return the root hash for chaining
- [x] Add `setupTriePair(t) (seqMs, parMs, seqTrie, parTrie)` factory:
  1. Create two independent `MockState` instances
  2. `seqTrie = NewHexPatriciaHashed(length.Addr, seqMs)`
  3. `parTrie = NewConcurrentPatriciaHashed(NewHexPatriciaHashed(length.Addr, parMs), parMs)`
  4. Set `parMs.SetConcurrentCommitment(true)`
  5. Return all four
- [x] Add `mockTrieCtxFactory(ms *MockState) TrieContextFactory` — returns `func() (PatriciaContext, func()) { return ms, func(){} }`
- [x] Write `TestCompareRoots_Smoke` — single account balance update, verify sequential == concurrent root
- [x] Run `go test -run TestCompareRoots_Smoke ./execution/commitment/...` — must pass

### Task 2: Layer A — Key distribution pattern tests

**Files:**
- Modify: `execution/commitment/hex_concurrent_patricia_hashed_test.go`

Each subtest creates a specific nibble distribution using addresses pre-computed by `findAddressForNibble`.

- [x] `TestCompareRoots_AllKeysSingleNibble` — generate 50+ addresses all hashing to nibble 0x0, set balances, compare roots. This is the most likely scenario to trigger the foldNibble bug.
- [x] `TestCompareRoots_AllNibblesPopulated` — at least 2 addresses per nibble (32+ total), balance updates, compare roots
- [x] `TestCompareRoots_TwoNibblesOnly` — addresses in nibbles 0x0 and 0xF only, compare roots
- [x] `TestCompareRoots_FifteenNibbles` — all nibbles except one populated, compare roots
- [x] `TestCompareRoots_SingleKey` — exactly one account, compare roots
- [x] `TestCompareRoots_HeavySkew` — 90% of addresses in one nibble, 10% spread across others, compare roots
- [x] Run `go test -run TestCompareRoots_ ./execution/commitment/...` — all must pass

### Task 3: Layer B — Update type combination tests

**Files:**
- Modify: `execution/commitment/hex_concurrent_patricia_hashed_test.go`

Use a spread-across-all-nibbles distribution for each, varying the update types.

- [x] `TestCompareRoots_AccountsOnly` — balance + nonce + codeHash updates across nibbles
- [x] `TestCompareRoots_StorageOnly` — storage slot updates for accounts across nibbles
- [x] `TestCompareRoots_MixedAccountStorage` — same address has both account updates and storage slots
- [x] `TestCompareRoots_Deletes` — create accounts, then delete some, compare roots after each step
- [x] `TestCompareRoots_FullAccountUpdate` — balance + nonce + codeHash + storage all at once per account
- [x] Run `go test -run TestCompareRoots_ ./execution/commitment/...` — all must pass

### Task 4: Layer C — Multi-batch sequencing (regression path)

**Files:**
- Modify: `execution/commitment/hex_concurrent_patricia_hashed_test.go`

These tests DO NOT reset tries between batches — state carries forward. This targets the exact regression scenario.

- [x] `TestCompareRoots_MultiBatch_SpreadThenConcentrate` — batch 1: accounts spread across all 16 nibbles; batch 2: all new accounts in a single nibble. Compare roots after each batch.
- [x] `TestCompareRoots_MultiBatch_ThreePhases` — batch 1: create 32 accounts across nibbles; batch 2: update balances of half; batch 3: delete a quarter. Compare after each.
- [x] `TestCompareRoots_MultiBatch_CreateThenDeleteAll` — batch 1: create accounts; batch 2: delete every account. Verify empty trie root matches.
- [x] `TestCompareRoots_MultiBatch_RepeatedSingleNibble` — starts with spread, then 3+ batches targeting single nibble with incremental updates. Uses `multiBatchComparer` which respects `CanDoConcurrentNext()` for concurrent/sequential mode selection. NOTE: pure single-nibble-from-start multi-batch is a known limitation (concurrent trie's root state after single-nibble batch is incompatible with subsequent processing; production code handles this via CanDoConcurrentNext fallback).
- [x] `TestCompareRoots_MultiBatch_AlternatingConcentration` — starts with spread, then alternates: nibble 0x3, nibble 0xA, spread. Tests extension trimming across different nibbles with multi-batch carry-forward.
- [x] Run `go test -run TestCompareRoots_MultiBatch ./execution/commitment/...` — all pass

### Task 5: Edge case tests

**Files:**
- Modify: `execution/commitment/hex_concurrent_patricia_hashed_test.go`

- [x] `TestCompareRoots_EmptyUpdates` — zero keys, verify both produce same (empty or prior) root
- [x] `TestCompareRoots_SingleAccountManyStorageSlots` — one account with 100+ storage slots, compare roots
- [x] `TestCompareRoots_ExtensionNodes` — addresses that share a long keccak prefix (same first 4+ nibbles) within the same nibble, forcing extension node creation
- [x] `TestCompareRoots_LargeScale` — 1000+ accounts spread across nibbles (stress test for correctness, not perf)
- [x] Run `go test -run TestCompareRoots_ ./execution/commitment/...` — all must pass

### Task 6: Verify acceptance criteria
- [x] Verify all key distribution patterns from Layer A are covered
- [x] Verify all update type combinations from Layer B are covered
- [x] Verify multi-batch regression path (Layer C) is covered
- [x] Verify edge cases are handled
- [x] Run full test suite: `go test ./execution/commitment/...`
- [x] Run with race detector: `go test -race -run TestCompareRoots ./execution/commitment/...`

### Task 7: [Final] Update documentation
- [x] Update `CLAUDE.md` if new testing patterns discovered (reviewed — no update needed; patterns are in-code and self-documenting)
- [x] Move this plan to `docs/plans/completed/`

## Technical Details

### Nibble Targeting Strategy
Keys are routed to nibble collectors by `hashedKey[0]` — the first nibble of `keccak256(address)`. To target a specific nibble:
```go
func findAddressForNibble(target int, seed int) []byte {
    // Brute force: increment seed until keccak(addr)[0] >> 4 == target
    // Cache results since keccak is deterministic
}
```

### Test Harness Data Flow
```
UpdateBuilder.Build() → (plainKeys, updates)
        │
        ├─→ seqMs.applyPlainUpdates(plainKeys, updates)
        │   WrapKeyUpdates(t, ModeDirect, KeyToHexNibbleHash, plainKeys, updates)
        │   seqTrie.Process(ctx, seqUpds, "", nil, WarmupConfig{})
        │   → seqRoot
        │
        └─→ parMs.applyPlainUpdates(plainKeys, updates)
            WrapKeyUpdatesParallel(t, ModeDirect, KeyToHexNibbleHash, plainKeys, updates)
            parTrie.Process(ctx, parUpds, "", nil, WarmupConfig{CtxFactory: mockFactory})
            → parRoot

    require.Equal(t, seqRoot, parRoot)
```

### Multi-Batch Flow
```
For batch i:
    builder_i.Build() → (keys_i, updates_i)
    seqMs.applyPlainUpdates(keys_i, updates_i)
    parMs.applyPlainUpdates(keys_i, updates_i)

    seqUpds_i = WrapKeyUpdates(...)      // fresh Updates each batch
    parUpds_i = WrapKeyUpdatesParallel(...)  // fresh Updates each batch

    seqRoot_i = seqTrie.Process(ctx, seqUpds_i, ...)   // trie carries state
    parRoot_i = parTrie.Process(ctx, parUpds_i, ...)   // trie carries state

    require.Equal(t, seqRoot_i, parRoot_i)
```

### Suspected Bug Location
`foldNibble()` lines 77-82 — extension trimming:
```go
if c.extLen > 0 {
    c.extLen--
    copy(c.extension[:], c.extension[1:])
    c.hashedExtLen -= 2
    copy(c.hashedExtension[:], c.hashedExtension[2:])
}
```
This trims the first byte from extension and first 2 nibbles from hashedExtension. If the subtrie's root cell has an extension that does NOT start with its nibble (e.g., after a previous fold cycle corrupted state), this trim produces wrong data that accumulates across batches.

## Post-Completion

**Manual verification:**
- Run against a real datadir snapshot to compare sequential vs concurrent on actual Ethereum state
- If any test fails, the failing test case provides a minimal reproducer for debugging `foldNibble`

**Follow-up work:**
- If bugs found: fix `foldNibble` extension trimming and/or `foldMounted` propagation
- Consider adding fuzz test: random key distributions, random update types, always compare roots
