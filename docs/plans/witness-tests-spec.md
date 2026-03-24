# Spec: Witness Trie Test Expansion

## Location

All new subtests go inside the existing `Test_WitnessTrie_GenerateWitness` function in:
`execution/commitment/hex_patricia_hashed_test.go` (after line 2543, before the closing `}`)

## Existing Infrastructure to Reuse

| Helper | Purpose |
|--------|---------|
| `buildTrieAndWitness(t, builder, keysToWitness, keyExists)` | Single-round: Build → Process → GenerateWitness → validate root + key lookups |
| `NewUpdateBuilder()` | Accumulates Balance/Nonce/CodeHash/Storage/Delete/DeleteStorage |
| `generatePlainKeysWithSameHashPrefix(tb, prefix, keyLen, prefixLen, count)` | Generate `count` random keys whose keccak hash shares `prefixLen` nibbles |
| `generateKeyWithHashedPrefix(prefix, keyLen)` | Single random key with given hashed prefix |
| `NewMockState(t)` / `NewHexPatriciaHashed(length.Addr, ms)` | In-memory state + trie |
| `WrapKeyUpdates(t, mode, hasher, keys, updates)` | Wrap plain keys/updates into `*Updates` |

## New Helper Needed

A `buildTrieMultiRoundAndWitness` helper for tests that need multiple `Process()` rounds (non-empty starting state). This helper:
1. Accepts a slice of `*UpdateBuilder` (one per round).
2. For each round: `Build()` → `applyPlainUpdates()` → `WrapKeyUpdates()` → `Process()`.
3. After all rounds, generates witness for the given keys and validates root hash + key existence.
4. Lives inside `Test_WitnessTrie_GenerateWitness` alongside `buildTrieAndWitness` (closure, not exported).

## New Test Cases

### Category 1: Multi-Round Process (Non-Empty Starting State)

#### 1.1 `MultiRound_AccountBalanceUpdate`
- **Round 1**: Create 5 accounts with balances.
- **Round 2**: Update balance of 2 existing accounts.
- **Witness**: One of the updated accounts → `keyExists: true`.
- **Why**: Tests witness generation after trie has been modified (not built from scratch). Exercises the fold/unfold path over pre-existing branch nodes.

#### 1.2 `MultiRound_AddAccountsToExistingTrie`
- **Round 1**: Create 3 accounts (prefix 0x3).
- **Round 2**: Add 3 new accounts (prefix 0x7) — different branch of the trie.
- **Witness**: One account from round 1 AND one from round 2 → both `keyExists: true`.
- **Why**: Ensures trie nodes from earlier rounds are correctly preserved and accessible.

#### 1.3 `MultiRound_StorageUpdateOnExistingAccount`
- **Round 1**: Create 2 accounts, one with 3 storage slots.
- **Round 2**: Update value of one existing storage slot, add 1 new storage slot.
- **Witness**: Updated storage slot + new storage slot → both `keyExists: true`.
- **Why**: Storage subtrie modifications on an existing account are the most common real-world pattern.

### Category 2: Deletions

#### 2.1 `DeletedAccount_NonExistentProof`
- Create 5 accounts.
- Delete 1 account.
- Process with both the creates and the delete in a single round.
- **Witness**: The deleted account → `keyExists: false`.
- **Why**: Account deletion changes the trie shape (branch collapse). Verifies witness correctly reports absence.

#### 2.2 `MultiRound_DeleteAccount_ThenWitness`
- **Round 1**: Create 5 accounts.
- **Round 2**: Delete 1 account.
- **Witness**: Deleted account → `keyExists: false`; surviving account → `keyExists: true`.
- **Why**: Deletion in a subsequent round tests trie compaction over a pre-existing structure.

#### 2.3 `MultiRound_DeleteStorage_ThenWitness`
- **Round 1**: Create 1 account with 5 storage slots.
- **Round 2**: Delete 2 storage slots.
- **Witness**: One deleted slot → `keyExists: false`; one surviving slot → `keyExists: true`.
- **Why**: Storage deletion changes the storage subtrie shape.

### Category 3: Accounts With Code

#### 3.1 `AccountWithCodeHash`
- Create 3 accounts, 1 with `CodeHash` set (non-empty code hash).
- **Witness**: The account with code → `keyExists: true`.
- **Why**: Existing tests never set `CodeHash`. This exercises the `CodeUpdate` flag path in `Build()` and the account node encoding in `toWitnessTrie()`.

#### 3.2 `AccountWithCodeHash_AndStorage`
- Create 1 account with `CodeHash` + 3 storage slots.
- **Witness**: Account key + 1 storage slot key → both `keyExists: true`.
- **Why**: Contract accounts (code + storage) are the norm in production; tests should cover the combination.

### Category 4: Mixed Existing + Non-Existing Keys

#### 4.1 `MixedAccountProof_ExistingAndNonExisting`
- Create 10 accounts.
- **Witness**: 2 existing account keys + 2 non-existing account keys → `[true, true, false, false]`.
- **Why**: Real `eth_getWitness` calls often request multiple addresses, some of which may not exist. Current tests only mix existing/non-existing for storage, not accounts.

#### 4.2 `MixedStorageProof_ExistingAndNonExisting`
- Create 1 account with 5 storage slots.
- **Witness**: 2 existing storage keys + 2 non-existing storage keys (same account) → `[true, true, false, false]`.
- **Why**: Similar to 4.1 but in the storage subtrie. Ensures `MergeTries` correctly handles mixed presence within a single account's storage.

### Category 5: Deep/Large Tries

#### 5.1 `DeepStorageTrie_ManySlots`
- Create 1 account with 50 storage slots (random prefixes to ensure a deep trie).
- **Witness**: 1 storage slot from deep in the trie → `keyExists: true`.
- **Why**: Exercises deep traversal in `toWitnessTrie()`. Production contracts like ERC-20 have hundreds of slots.

#### 5.2 `LargeAccountTrie_100Accounts`
- Create 100 accounts with random keys.
- **Witness**: 3 accounts spread across the trie → all `keyExists: true`.
- **Why**: Stresses branch node conversion with a larger trie. The existing "Many" test uses only 25 accounts and witnesses only 1 key.

### Category 6: Edge Cases

#### 6.1 `AccountWithZeroBalanceNonZeroNonce`
- Create 1 account with `Balance(0)` + `Nonce(42)`.
- **Witness**: That account → `keyExists: true`.
- **Why**: Zero balance is a valid state (e.g., contract with no ETH). Ensures the account node encodes correctly when balance is zero.

#### 6.2 `AccountsWithLongCommonPrefix`
- Generate 5 accounts whose hashed keys share a 6-nibble common prefix.
- **Witness**: 1 account → `keyExists: true`.
- **Why**: Long shared prefixes create deep extension nodes. Tests the extension-splitting logic in `toWitnessTrie()`.

#### 6.3 `MultipleAccountsWithStorage_WitnessStorageAcrossAccounts`
- Create 3 accounts, each with 3 storage slots.
- **Witness**: 1 storage slot from each of the 3 accounts → all `keyExists: true`.
- **Why**: Multi-account storage witness exercises `MergeTries` merging tries that each contain storage subtries from different accounts.

## Implementation Notes

1. **`buildTrieMultiRoundAndWitness` helper**: Defined as a closure inside `Test_WitnessTrie_GenerateWitness`, right after the existing `buildTrieAndWitness` closure. It shares the same `MockState` and `HexPatriciaHashed` across rounds.

2. **CodeHash values**: Use a fixed non-empty hash like `crypto.Keccak256Hash([]byte("contract-code"))` for test code hashes. Do NOT use `common.Hash{}` (empty hash means no code).

3. **Key generation**: Reuse `generatePlainKeysWithSameHashPrefix` and `generateKeyWithHashedPrefix` for all key generation. For "random" accounts (no prefix constraint), pass `nil` for `constPrefixNibbles` and `0` for `prefixLen`.

4. **Storage key format**: Full storage key = `addr (20 bytes) + storageSlot (32 bytes)` = 52 bytes. Use `length.Addr + length.Hash` for the assertion.

5. **Subtest naming**: Follow the existing pattern — `t.Run("CategoryName_SpecificCase", ...)`.

6. **No `fmt.Printf`**: Existing tests have `fmt.Printf` debug prints. New tests should use `t.Logf` instead (or omit logging entirely) to keep output clean unless `-v` is passed.

## Estimated Scope

- ~1 new helper function (`buildTrieMultiRoundAndWitness`): ~40 lines
- ~15 new subtests: ~20-40 lines each
- Total: ~400-600 lines of new test code
- All in a single file: `hex_patricia_hashed_test.go`

## Tasks

### Task 1: Multi-Round Helper + Category 1 (Multi-Round Process)
- [x] Implement `buildTrieMultiRoundAndWitness` closure helper
- [x] Implement subtest `MultiRound_AccountBalanceUpdate` (1.1)
- [x] Implement subtest `MultiRound_AddAccountsToExistingTrie` (1.2)
- [x] Implement subtest `MultiRound_StorageUpdateOnExistingAccount` (1.3)
- [x] Tests pass and lint clean

### Task 2: Category 2 (Deletions)
- [x] Implement subtest `DeletedAccount_NonExistentProof` (2.1)
- [x] Implement subtest `MultiRound_DeleteAccount_ThenWitness` (2.2)
- [x] Implement subtest `MultiRound_DeleteStorage_ThenWitness` (2.3)
- [x] Tests pass and lint clean

### Task 3: Category 3 (Accounts With Code)
- [x] Implement subtest `AccountWithCodeHash` (3.1)
- [x] Implement subtest `AccountWithCodeHash_AndStorage` (3.2)
- [x] Tests pass and lint clean

### Task 4: Category 4 (Mixed Existing + Non-Existing Keys)
- [ ] Implement subtest `MixedAccountProof_ExistingAndNonExisting` (4.1)
- [ ] Implement subtest `MixedStorageProof_ExistingAndNonExisting` (4.2)
- [ ] Tests pass and lint clean

### Task 5: Category 5 (Deep/Large Tries)
- [ ] Implement subtest `DeepStorageTrie_ManySlots` (5.1)
- [ ] Implement subtest `LargeAccountTrie_100Accounts` (5.2)
- [ ] Tests pass and lint clean

### Task 6: Category 6 (Edge Cases)
- [ ] Implement subtest `AccountWithZeroBalanceNonZeroNonce` (6.1)
- [ ] Implement subtest `AccountsWithLongCommonPrefix` (6.2)
- [ ] Implement subtest `MultipleAccountsWithStorage_WitnessStorageAcrossAccounts` (6.3)
- [ ] Tests pass and lint clean
