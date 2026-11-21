// Copyright 2013 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Adapted from: https://golang.org/src/crypto/cipher/xor_test.go

package bitutil

import (
	"testing"
)

// Benchmarks the potentially optimized AND performance.
func BenchmarkFastAND1KB(b *testing.B) { benchmarkFastAND(b, 1024) }

func BenchmarkFastAND2KB(b *testing.B) { benchmarkFastAND(b, 2048) }

func BenchmarkFastAND4KB(b *testing.B) { benchmarkFastAND(b, 4096) }

// Benchmarks the baseline AND performance.
func BenchmarkBaseAND1KB(b *testing.B) { benchmarkBaseAND(b, 1024) }

func BenchmarkBaseAND2KB(b *testing.B) { benchmarkBaseAND(b, 2048) }

func BenchmarkBaseAND4KB(b *testing.B) { benchmarkBaseAND(b, 4096) }

// Benchmarks the potentially optimized OR performance.
func BenchmarkFastOR1KB(b *testing.B) { benchmarkFastOR(b, 1024) }

func BenchmarkFastOR2KB(b *testing.B) { benchmarkFastOR(b, 2048) }

func BenchmarkFastOR4KB(b *testing.B) { benchmarkFastOR(b, 4096) }

// Benchmarks the baseline OR performance.
func BenchmarkBaseOR1KB(b *testing.B) { benchmarkBaseOR(b, 1024) }

func BenchmarkBaseOR2KB(b *testing.B) { benchmarkBaseOR(b, 2048) }

func BenchmarkBaseOR4KB(b *testing.B) { benchmarkBaseOR(b, 4096) }

// Benchmarks the potentially optimized bit testing performance.
func BenchmarkFastTest1KB(b *testing.B) { benchmarkFastTest(b, 1024) }

func BenchmarkFastTest2KB(b *testing.B) { benchmarkFastTest(b, 2048) }

func BenchmarkFastTest4KB(b *testing.B) { benchmarkFastTest(b, 4096) }

// Benchmarks the baseline bit testing performance.
func BenchmarkBaseTest1KB(b *testing.B) { benchmarkBaseTest(b, 1024) }

func BenchmarkBaseTest2KB(b *testing.B) { benchmarkBaseTest(b, 2048) }

func BenchmarkBaseTest4KB(b *testing.B) { benchmarkBaseTest(b, 4096) }

func benchmarkFastAND(b *testing.B, size int) {
	b.Helper()
	p, q := make([]byte, size), make([]byte, size)

	for b.Loop() {
		ANDBytes(p, p, q)
	}
}

func benchmarkBaseAND(b *testing.B, size int) {
	b.Helper()
	p, q := make([]byte, size), make([]byte, size)

	for b.Loop() {
		safeANDBytes(p, p, q)
	}
}

func benchmarkFastOR(b *testing.B, size int) {
	b.Helper()
	p, q := make([]byte, size), make([]byte, size)

	for b.Loop() {
		ORBytes(p, p, q)
	}
}

func benchmarkBaseOR(b *testing.B, size int) {
	b.Helper()
	p, q := make([]byte, size), make([]byte, size)

	for b.Loop() {
		safeORBytes(p, p, q)
	}
}

func benchmarkFastTest(b *testing.B, size int) {
	b.Helper()
	p := make([]byte, size)
	a := false
	for b.Loop() {
		a = a != TestBytes(p)
	}
	GloBool = a // Use of benchmark "result" to prevent total dead code elimination.
}

func benchmarkBaseTest(b *testing.B, size int) {
	b.Helper()
	p := make([]byte, size)
	a := false
	for b.Loop() {
		a = a != safeTestBytes(p)
	}
	GloBool = a // Use of benchmark "result" to prevent total dead code elimination.
}
