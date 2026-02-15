Pure Go SA-IS (Suffix Array by Induced Sorting) implementation.

Adapted from Go standard library `index/suffixarray` (BSD-3-clause, Copyright 2019 The Go Authors).
The stdlib implementation is itself inspired by sais-lite-2.4.1 by Yuta Mori.

Previously this package was a CGo wrapper around sais-lite C code. It was replaced with
pure Go to eliminate CGo call overhead and enable Go compiler optimizations.
