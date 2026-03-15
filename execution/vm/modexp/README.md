# vm/modexp — Standalone Modular Exponentiation for EVM

This package implements the EVM MODEXP precompile (EIP-198) as a standalone
pure-Go package with optional ADX/BMI2 assembly for the inner loop.

## Status: Experimental (branch `modexp-asm`)

This work is preserved for future reference. It is **not merged to main**.
The main branch continues to use `go-bigmodexpfix` (a patched `math/big`).

## What Was Done

### Implementation

- **Word-by-Word Montgomery multiplication** (CIOS form) operating on raw
  `[]uint` limb slices, with a single slab allocation for all working storage.
- **ADX/BMI2 assembly** (`addmul_amd64.s`) for `addMulVVW` — the inner
  multiply-accumulate loop. Uses MULXQ + ADCXQ + ADOXQ dual carry chains,
  8x unrolled with MULQ fallback (4x unrolled) for older CPUs.
- **4-bit windowed exponentiation** with lazy table building and leading
  zero skip.
- **Even moduli** routed to `big.Int.Exp` (rare in practice).
- **Small exponents** (<=8 bytes) routed to `big.Int.Exp` (avoids Montgomery
  setup overhead for cases like e=3 or e=65537).

### Allocation Reduction

Reduced heap allocations from ~31 to ~7 per `Exp()` call:
- Raw `[]uint` slice Montgomery (eliminated nat pointer allocs for table entries)
- Merged limb buffer: modulus + base parsed into one `make()`
- `sync.Pool` for `big.Int` in `computeRR`
- Single slab allocation for all Montgomery workspace (T, one, out, table, tmp, rr)

### Test Suite

- ~750+ test vectors covering EVM precompile vectors, word boundary sizes,
  even moduli, small values, large exponents, carry propagation patterns
- Property-based oracle testing against `math/big`
- ADX/BMI2 assembly vs pure-Go cross-validation

## Performance

Measured via engine_x blockchain tests (100M gas blocks of MODEXP calls):

| Implementation | Time | vs Main |
|---|---|---|
| Main (`go-bigmodexpfix`) | 410s | baseline |
| Standard library (`big.Int.Exp`) | 526s | +28% |
| This package (v1, before alloc work) | 545s | +33% |
| This package (final, reduced allocs) | ~545s | +33% |

The allocation reduction work confirmed that the remaining gap is **not**
from heap allocations — it's in the computational core.

## Why go-bigmodexpfix Is Faster

`go-bigmodexpfix` patches `math/big` internals and benefits from:

1. **Custom stack allocator** (`stk.nat()`) — effectively zero heap allocs
   by reusing a pre-allocated scratch buffer for all intermediate results.
2. **Go's internal assembly-optimized `addMulVVW`** — the standard library's
   version uses the same ADX/BMI2 instructions but with tighter register
   scheduling and integration with the compiler's calling convention.
3. **2-bit window** for small exponents — fewer table entries to precompute.

Our standalone package cannot access Go's internal assembly (it's in
`crypto/internal/fips140/bigmod`, unexported). Matching go-bigmodexpfix
requires either:
- A full assembly-level Montgomery multiplication (not just `addMulVVW`)
  that avoids function call overhead on the inner loop
- Or a custom allocator pattern similar to `stk.nat()`

## Practical Significance

MODEXP performance is **more of an attack vector than a day-to-day concern**.
In normal Ethereum operation, MODEXP calls are rare and small. The performance
difference only matters for adversarial blocks that fill gas limits with
worst-case MODEXP inputs (large moduli, large exponents). EIP-2565 and
EIP-7883 repricing largely mitigate this by making such attacks expensive.

The broader optimization plan recognizes that while modexp is a measurable
factor in worst-case benchmarks, getting substantially better than the current
`go-bigmodexpfix` baseline requires significant assembly-level work that has
diminishing practical returns. There are other independent reasons for working
on assembly-level Montgomery multiplication (e.g., other cryptographic
primitives), and when that work is done, this package can be revisited to
benefit from it.

## Future Work

When assembly-level Montgomery multiplication becomes available (for other
reasons), revisit this package:

1. **Full assembly Montgomery**: Implement the entire Montgomery multiplication
   loop in assembly (not just `addMulVVW`), eliminating Go function call
   overhead on each limb iteration.
2. **Per-size unrolled variants**: Like Go's `crypto/internal/fips140/bigmod`,
   generate size-specialized routines for common modulus sizes (256-bit,
   2048-bit, 4096-bit).
3. **AVX2/AVX512-IFMA**: VPMULUDQ 4-wide (AVX2) or VPMADD52LUQ 8-wide
   (AVX512-IFMA) for larger moduli.
4. **Custom allocator**: Stack-based scratch buffer to eliminate remaining
   heap allocations.

## Files

| File | Purpose |
|---|---|
| `modexp.go` | Public API, routing, limb helpers |
| `montgomery.go` | Montgomery multiplication (raw slice) |
| `nat.go` | Minimal nat type (tests), `minusInverseModW`, `bigIntPool` |
| `crt.go` | Even moduli via `big.Int.Exp` |
| `addmul_amd64.s` | ADX/BMI2 + MULQ assembly for `addMulVVW` |
| `addmul_asm.go` | Assembly declaration + CPU feature detection |
| `addmul_noasm.go` | Pure Go fallback for non-amd64 |
| `modexp_test.go` | Unit + property-based + oracle tests |
| `bench_test.go` | Comparative benchmarks |
