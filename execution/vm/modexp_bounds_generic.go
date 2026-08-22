// Copyright 2025 The Erigon Authors
// SPDX-License-Identifier: LGPL-3.0

//go:build !amd64

package vm

// Modulus widths, in bytes, from which math/big beats evmone, as measured by
// BenchmarkModexpBackends on arm64 and kept for every target without its own
// measurement. math/big's Montgomery assembly has no dual carry chain outside
// amd64, so evmone stays ahead until the modulus is very wide.
const (
	modexpBigIntMinModLenWideExp   = 256
	modexpBigIntMinModLenNarrowExp = 256
)
