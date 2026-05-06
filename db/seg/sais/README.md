This package wraps Go's stdlib `index/suffixarray` SA-IS implementation,
providing a `Sais(data []byte, sa []int32, buf *[]int32)` API for callers that need
a caller-provided `[]int32` suffix array buffer. The `buf` parameter is reusable
scratch space that is grown as needed; callers should preserve it across calls
to amortize allocations.
