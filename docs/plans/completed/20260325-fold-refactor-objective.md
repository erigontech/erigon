# Objective: Refactor HexPatriciaHashed.fold() (Zabaniya)

## Problem

`HexPatriciaHashed.fold()` is a 258-line function (lines 2037–2293 of `hex_patricia_hashed.go`) that handles three fundamentally different update paths (delete, propagate, branch) inside a single switch statement. The branch case alone is ~140 lines and contains a fork between deferred and non-deferred encoding paths that share ~60% of their logic but are fully duplicated. Trace logging, metrics collection, memoization state management, and trie structure updates are all interleaved.

This makes fold() hard to understand, hard to modify safely, and hard to test in isolation. Recent work (trie trace recording, deferred branch encoding, memoization) has added complexity without the function being restructured to absorb it cleanly.

## Goal

Break fold() into smaller, individually testable functions without changing its external behavior or regressing performance. After refactoring:

- fold() itself should be ~40–50 lines: setup, switch dispatch, common epilogue
- Each update path (delete, propagate, branch) is a separate method
- The deferred and non-deferred branch encoding paths share their cell preparation and keccak2-feeding logic
- New contributors can understand a single update path without reading the other two

## Success Criteria

1. fold() body is ≤60 lines (currently 258)
2. Each extracted method is ≤80 lines
3. All existing tests pass unchanged: `go test ./execution/commitment/... -count=1`
4. No measurable performance regression: `go test ./execution/commitment/ -bench=. -benchmem -count=5` shows no statistically significant increase in ns/op or allocs/op (use benchstat)
5. `make lint` passes
6. Zero new heap allocations in fold() hot path (verify with `-benchmem`)
7. No new exported API — all extracted methods are unexported

## Non-Goals

- Changing fold()'s algorithm or correctness semantics
- Refactoring createCellGetter (separate initiative — Option B already selected)
- Modifying the BranchEncoder/CollectUpdate/EncodeBranch interface
- Removing or restructuring trace/debug logging (we extract it, not delete it)
- Optimizing fold() performance (this is a structure-only refactoring)
- Changing the deferred vs non-deferred decision logic
