// Copyright 2017 The go-ethereum Authors
// (original work)
// Copyright 2024 The Erigon Authors
// (modifications)
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package bitutil

import (
	"math/rand"
	"testing"

	"github.com/erigontech/erigon/common/log/v3"
)

// Crude benchmark for compressing random slices of bytes.
func BenchmarkEncoding1KBVerySparse(b *testing.B) { benchmarkEncoding(b, 1024, 0.0001) }

func BenchmarkEncoding2KBVerySparse(b *testing.B) { benchmarkEncoding(b, 2048, 0.0001) }

func BenchmarkEncoding4KBVerySparse(b *testing.B) { benchmarkEncoding(b, 4096, 0.0001) }

func BenchmarkEncoding1KBSparse(b *testing.B) { benchmarkEncoding(b, 1024, 0.001) }

func BenchmarkEncoding2KBSparse(b *testing.B) { benchmarkEncoding(b, 2048, 0.001) }

func BenchmarkEncoding4KBSparse(b *testing.B) { benchmarkEncoding(b, 4096, 0.001) }

func BenchmarkEncoding1KBDense(b *testing.B) { benchmarkEncoding(b, 1024, 0.1) }

func BenchmarkEncoding2KBDense(b *testing.B) { benchmarkEncoding(b, 2048, 0.1) }

func BenchmarkEncoding4KBDense(b *testing.B) { benchmarkEncoding(b, 4096, 0.1) }

func BenchmarkEncoding1KBSaturated(b *testing.B) { benchmarkEncoding(b, 1024, 0.5) }

func BenchmarkEncoding2KBSaturated(b *testing.B) { benchmarkEncoding(b, 2048, 0.5) }

func BenchmarkEncoding4KBSaturated(b *testing.B) { benchmarkEncoding(b, 4096, 0.5) }

func benchmarkEncoding(b *testing.B, bytes int, fill float64) {
	b.Helper()
	// Generate a random slice of bytes to compress
	random := rand.NewSource(0) // reproducible and comparable

	data := make([]byte, bytes)
	bits := int(float64(bytes) * 8 * fill)

	for range bits {
		idx := random.Int63() % int64(len(data))
		bit := uint(random.Int63() % 8)
		data[idx] |= 1 << bit
	}
	// Reset the benchmark and measure encoding/decoding
	b.ResetTimer()
	b.ReportAllocs()
	for b.Loop() {
		_, decodeErr := bitsetDecodeBytes(bitsetEncodeBytes(data), len(data))
		if decodeErr != nil {
			log.Warn("Failed to decode bitset bytes", "err", decodeErr)
		}
	}
}
