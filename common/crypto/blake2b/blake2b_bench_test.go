// Copyright 2016 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package blake2b

import (
	"testing"
)

func BenchmarkWrite128Generic(b *testing.B) { benchmarkWrite(b, 128, false, false, false) }

func BenchmarkWrite1KGeneric(b *testing.B) { benchmarkWrite(b, 1024, false, false, false) }

func BenchmarkWrite128SSE4(b *testing.B) { benchmarkWrite(b, 128, true, false, false) }

func BenchmarkWrite1KSSE4(b *testing.B) { benchmarkWrite(b, 1024, true, false, false) }

func BenchmarkWrite128AVX(b *testing.B) { benchmarkWrite(b, 128, false, true, false) }

func BenchmarkWrite1KAVX(b *testing.B) { benchmarkWrite(b, 1024, false, true, false) }

func BenchmarkWrite128AVX2(b *testing.B) { benchmarkWrite(b, 128, false, false, true) }

func BenchmarkWrite1KAVX2(b *testing.B) { benchmarkWrite(b, 1024, false, false, true) }

func BenchmarkSum128Generic(b *testing.B) { benchmarkSum(b, 128, false, false, false) }

func BenchmarkSum1KGeneric(b *testing.B) { benchmarkSum(b, 1024, false, false, false) }

func BenchmarkSum128SSE4(b *testing.B) { benchmarkSum(b, 128, true, false, false) }

func BenchmarkSum1KSSE4(b *testing.B) { benchmarkSum(b, 1024, true, false, false) }

func BenchmarkSum128AVX(b *testing.B) { benchmarkSum(b, 128, false, true, false) }

func BenchmarkSum1KAVX(b *testing.B) { benchmarkSum(b, 1024, false, true, false) }

func BenchmarkSum128AVX2(b *testing.B) { benchmarkSum(b, 128, false, false, true) }

func BenchmarkSum1KAVX2(b *testing.B) { benchmarkSum(b, 1024, false, false, true) }

func benchmarkSum(b *testing.B, size int, sse4, avx, avx2 bool) {
	b.Helper()
	// Enable the correct set of instructions
	defer func(sse4, avx, avx2 bool) {
		useSSE4, useAVX, useAVX2 = sse4, avx, avx2
	}(useSSE4, useAVX, useAVX2)
	useSSE4, useAVX, useAVX2 = sse4, avx, avx2

	data := make([]byte, size)
	b.SetBytes(int64(size))
	for b.Loop() {
		Sum512(data)
	}
}

func benchmarkWrite(b *testing.B, size int, sse4, avx, avx2 bool) {
	b.Helper()
	// Enable the correct set of instructions
	defer func(sse4, avx, avx2 bool) {
		useSSE4, useAVX, useAVX2 = sse4, avx, avx2
	}(useSSE4, useAVX, useAVX2)
	useSSE4, useAVX, useAVX2 = sse4, avx, avx2

	data := make([]byte, size)
	h, _ := New512(nil)
	b.SetBytes(int64(size))
	for b.Loop() {
		h.Write(data)
	}
}
