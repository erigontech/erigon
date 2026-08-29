// Copyright 2025 The Erigon Authors
// SPDX-License-Identifier: LGPL-3.0

//go:build amd64

package vm

// Modulus widths, in bytes, from which math/big beats evmone, as measured by
// BenchmarkModexpBackends. math/big's Montgomery inner loop uses the dual carry
// chain (ADCX/ADOX) here, which evmone's portable C++ has no answer to, so it
// takes over as soon as the modulus outgrows the uint256 path.
const (
	modexpBigIntMinModLenWideExp   = 64
	modexpBigIntMinModLenNarrowExp = 128
)
