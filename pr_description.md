# execution/rlp: wrong trie root regression from stale listLimit re-read (already fixed)

## What was failing

The `stage-exec-test` CI jobs (from-0 serial, from-0 parallel, resume-nonchaintip serial) all failed on 2026-06-17 with `invalid block: wrong trie root` errors at different blocks:

- **from-0, serial**: wrong trie root at block 513814
- **from-0, parallel**: wrong trie root at block 263641
- **resume-nonchaintip, serial**: wrong trie root at block 25314648

CI run: https://github.com/erigontech/erigon/actions/runs/27668420051  
Head SHA: `cecf3add9a9983927cfb2619cc0a8e0ae6b7ae48`

## Root cause

PR #21839 (`cecf3add`) added a `listLimit` re-read inside `Stream.Kind()` in `execution/rlp/decode.go`:

```go
// Re-read listLimit because readKind consumed header bytes via
// willRead, which decremented the enclosing list's remaining size.
if inList {
    _, listLimit = s.listLimit()
}
```

This was incorrect. After `readKind()` calls `willRead()` to consume the 1-byte type header, `listLimit` drops by 1. The re-read then compares the declared element `s.size` against `(original - 1 byte)`, causing the `ErrElemTooLarge` guard to fire spuriously for elements that exactly fill the remaining list space. This aborted block execution with corrupted state and produced wrong trie roots.

The original code correctly reads `listLimit` **before** `readKind()` and uses that value for the `ErrElemTooLarge` check, since the guard compares the *declared* element size against the *available* space before any consumption.

## Fix

Already merged as PR #21867 (commit `e72f961e`), which reverted the stale `listLimit` re-read block (6 lines removed from `execution/rlp/decode.go`). The fuzz corpus entry (`29859ba08ac1b7a2`) was kept for future investigation.

The current `main` branch at `a2d3bba` includes this fix. No additional code changes are needed.

## Verification

- `go test -count=3 -race ./execution/rlp/...` passes
- `go test -count=3 -race ./execution/types/...` passes
- `go build ./execution/rlp/...` succeeds
- The buggy `listLimit` re-read block is confirmed absent from `execution/rlp/decode.go:1094`
