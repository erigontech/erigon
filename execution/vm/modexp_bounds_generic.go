// Copyright 2025 The Erigon Authors
// SPDX-License-Identifier: LGPL-3.0

//go:build !amd64

package vm

// Modulus widths, in bytes, from which math/big beats evmone, as measured by
// BenchmarkModexpBackends on arm64 and kept for every target without its own
// measurement. math/big's Montgomery assembly has no dual carry chain outside
// amd64, so evmone stays ahead until the modulus is very wide — and longest of
// all for an exponent just over one word, where math/big pays for a window it
// barely uses. EIP-7823 caps an operand at 1024 bytes, so that band only reaches
// math/big at the very top of the range.
const (
	modexpBigIntMinModLenNarrowExp uint64 = 256
	modexpBigIntMinModLenMidExp    uint64 = 1024
	modexpBigIntMinModLenWideExp   uint64 = 512
)
