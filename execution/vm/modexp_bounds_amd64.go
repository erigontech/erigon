// Copyright 2025 The Erigon Authors
// SPDX-License-Identifier: LGPL-3.0

//go:build amd64

package vm

import "golang.org/x/sys/cpu"

// Modulus widths, in bytes, from which math/big beats evmone, as measured by
// BenchmarkModexpBackends. math/big's Montgomery inner loop uses the dual carry
// chain (ADCX/ADOX) here, which evmone's portable C++ has no answer to, so it
// takes over as soon as the modulus outgrows the uint256 path.
var (
	modexpBigIntMinModLenNarrowExp uint64 = 128
	// The band just above one word is unmeasured here, so it keeps the routing
	// this target had before: evmone, at every width EIP-7823 allows.
	modexpBigIntMinModLenMidExp  uint64 = modexpBigIntNever
	modexpBigIntMinModLenWideExp uint64 = 64
)

// modexpBigIntNever is past EIP-7823's 1024-byte operand cap, so a bound set to
// it never selects math/big.
const modexpBigIntNever uint64 = 1 << 20

// Without ADX math/big drops to its MULQ inner loop, so the measured crossover
// no longer applies and the release baseline still covers such CPUs. Fall back
// to the unmeasured-target bounds, which keep evmone until the modulus is wide.
func init() {
	if !cpu.X86.HasADX || !cpu.X86.HasBMI2 {
		modexpBigIntMinModLenNarrowExp = modexpBigIntMinModLenNoADX
		modexpBigIntMinModLenWideExp = modexpBigIntMinModLenNoADX
	}
}

const modexpBigIntMinModLenNoADX uint64 = 256
