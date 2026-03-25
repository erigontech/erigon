This package wraps Go's stdlib `index/suffixarray` SA-IS implementation,
providing a `Sais(data, sa []int32)` API for callers that need
a caller-provided `[]int32` suffix array buffer.
