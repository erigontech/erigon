# Objective: Expand Witness Trie Unit Test Coverage

## Goal

Harden `HexPatriciaHashed.GenerateWitness()` and `toWitnessTrie()` against production failures by adding unit tests for trie configurations not covered by the existing 17 subtests in `Test_WitnessTrie_GenerateWitness`.

## Problem

`eth_getWitness` occasionally fails in production on certain trie configurations. The existing tests always start from an empty trie (single `Process()` call), never exercise deletions, never set `CodeHash`, and don't cover mixed existing/non-existing keys in a single witness request for accounts. These are all scenarios that occur in real blocks.

## Success Criteria

1. All new subtests pass: `go test -run Test_WitnessTrie_GenerateWitness -count=1 ./execution/commitment/...`
2. No changes to production code — tests only.
3. New tests reuse the existing `buildTrieAndWitness` helper or a minimal extension of it (for multi-round Process).
4. Each new subtest targets a distinct trie shape or lifecycle scenario not covered by the existing 17 cases.
5. `make lint` passes with no new warnings.

## Non-Goals

- Benchmarking or performance testing.
- Modifying `toWitnessTrie()`, `GenerateWitness()`, or any production code.
- Adding integration tests against a real database.
- Changing the existing 17 subtests.
