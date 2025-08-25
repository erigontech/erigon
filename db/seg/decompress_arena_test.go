// Copyright 2024 The Erigon Authors
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

package seg

import (
	"encoding/binary"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/edsrzf/mmap-go"
	"github.com/stretchr/testify/require"
)

// MockCompressedReader simulates a compressed segment reader
// that has decompression overhead
type MockCompressedReader struct {
	data               map[string][]byte
	decompressionDelay time.Duration
}

func NewMockCompressedReader(decompressionDelay time.Duration) *MockCompressedReader {
	return &MockCompressedReader{
		data:               make(map[string][]byte),
		decompressionDelay: decompressionDelay,
	}
}

func (r *MockCompressedReader) AddKeyValue(key, value []byte) {
	r.data[string(key)] = value
}

func (r *MockCompressedReader) Get(key []byte) []byte {
	// Simulate decompression delay
	time.Sleep(r.decompressionDelay)
	return r.data[string(key)]
}

// BenchmarkDecompressionCache_vs_Direct compares cached vs direct decompression
func BenchmarkDecompressionCache_vs_Direct(b *testing.B) {
	decompressionDelay := 10 * time.Microsecond // 10μs per decompression

	// Setup test data
	const numKeys = 1000
	keys := make([][]byte, numKeys)
	values := make([][]byte, numKeys)

	for i := 0; i < numKeys; i++ {
		keys[i] = []byte(fmt.Sprintf("k: %x", i))
		values[i] = []byte(fmt.Sprintf("v: %x", i))
	}

	// Setup mock reader
	reader := NewMockCompressedReader(decompressionDelay)
	for i := 0; i < numKeys; i++ {
		reader.AddKeyValue(keys[i], values[i])
	}

	// Setup cache
	cache, err := NewDecompressArena(64 * 1024 * 1024) // 64MB
	if err != nil {
		b.Fatal(err)
	}
	defer cache.Close()

	b.Run("Direct_Decompression", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			key := keys[i%numKeys]
			_ = reader.Get(key)
		}
	})

	b.Run("Cached_Decompression", func(b *testing.B) {
		cache, err := NewDecompressArena(64 * 1024 * 1024) // 64MB
		if err != nil {
			b.Fatal(err)
		}
		defer cache.Close()

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if cache.NoSpaceLeft() {
				cache.Close()
				cache, err = NewDecompressArena(64 * 1024 * 1024) // 64MB
				if err != nil {
					b.Fatal(err)
				}
			}
			cache.Allocate(20)
		}
	})
}

func TestDecompressArena(t *testing.T) {
	cache, err := NewDecompressArena(1024) // 1KB
	if err != nil {
		t.Fatal(err)
	}
	defer cache.Close()

	size := 8
	k1 := cache.Allocate(size)
	binary.BigEndian.PutUint64(k1, 1)
	k2 := cache.Allocate(size)
	binary.BigEndian.PutUint64(k2, 2)
	// Mem-Safety: invariant! Capacity and Len must equal to request space! Then golang's compiler bounds-checks will work on arena-allocated slice
	//  - Invariant1: `cap(k1) == len(k1)`
	//require.Equal(t, 8, cap(k1))

	k3 := append(k1, []byte{9, 9}...) // should panic if cap/len invariant is broken
	_ = k3
	//fmt.Printf("k1:%x, k2: %x, k3: %x, %d, %d\n", k1, k2, k3, cap(k1), len(k1))
}

func BenchmarkDecompressArena(b *testing.B) {
	var size int = 64
	var arenaSize = 64 * 1024
	var iters = 100_000
	b.Run("make", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			for j := 0; j < iters; j++ {
				size++
				size--
				k := make([]byte, size)
				_ = k
			}
			//runtime.GC()
		}
	})

	b.Run("arena", func(b *testing.B) {
		b.ReportAllocs()
		arenaSize := 64 * 1024 * 1024
		for i := 0; i < b.N; i++ {
			a, err := NewDecompressArena(arenaSize)
			require.NoError(b, err)
			for j := 0; j < iters; j++ {
				size++
				size--
				k := a.Allocate(size)
				_ = k
			}
			a.Close()
		}
	})
	b.Run("slice.64", func(b *testing.B) {
		b.ReportAllocs()
		//arenaSize := 128 * 1024
		arenaSize := 64 * 1024 * 1024

		for i := 0; i < b.N; i++ {
			a, err := NewDecompressArenaSlice(arenaSize)
			require.NoError(b, err)
			for j := 0; j < iters; j++ {
				size++
				size--
				k := a.Allocate(size)
				_ = k
			}
			a.Close()
		}
	})
	b.Run("pool", func(b *testing.B) {
		b.ReportAllocs()
		//arenaSize := 128 * 1024x
		var bufPool = sync.Pool{
			New: func() interface{} {
				return make([]byte, arenaSize)
			},
		}

		for i := 0; i < b.N; i++ {

			//a, err := NewDecompressArenaSlice(arenaSize)
			//require.NoError(b, err)
			for j := 0; j < iters; j++ {
				size++
				size--
				k := bufPool.Get().([]byte)
				bufPool.Put(k)
				//k := a.Allocate(size)
				_ = k
			}
			//a.Close()
		}
	})

	_ = arenaSize
}

func BenchmarkMmap(b *testing.B) {
	b.ReportAllocs()
	size := 64 * 1024
	b.Run("mmap", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			mmapData, err := mmap.MapRegion(nil, int(size), mmap.RDWR, mmap.ANON, 0)
			require.NoError(b, err)
			for j := 0; j < size/64; j++ {
				mmapData[j] = 1
			}
			mmapData.Unmap()
		}
	})
	b.Run("make", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			size++
			size--
			size++
			size--
			mmapData := make([]byte, size)
			for j := 0; j < size/64; j++ {
				mmapData[j] = 1
			}
			mmapData = nil
		}
	})
}

// BenchmarkMemoryAllocation compares allocation patterns
//func BenchmarkMemoryAllocation(b *testing.B) {
//	cache, err := NewDecompressArena(64 * 1024 * 1024)
//	if err != nil {
//		b.Fatal(err)
//	}
//	defer cache.Close()
//
//	key := make([]byte, 32)
//	value := make([]byte, 256)
//	rand.Read(key)
//	rand.Read(value)
//
//	b.Run("Cache_Put", func(b *testing.B) {
//		b.ResetTimer()
//		for i := 0; i < b.N; i++ {
//			cache.Reset() // Reset to allow reallocation
//			cache.Put(key, value)
//		}
//	})
//
//	b.Run("Cache_Get", func(b *testing.B) {
//		cache.Put(key, value) // Pre-populate
//
//		b.ResetTimer()
//		for i := 0; i < b.N; i++ {
//			cache.Get(key)
//		}
//	})
//
//	b.Run("Slice_Allocation", func(b *testing.B) {
//		b.ResetTimer()
//		for i := 0; i < b.N; i++ {
//			// Simulate traditional approach - allocate new slice each time
//			result := make([]byte, len(value))
//			copy(result, value)
//			_ = result
//		}
//	})
//}
//
//// BenchmarkTransactionScenario simulates real transaction processing
//func BenchmarkTransactionScenario(b *testing.B) {
//	decompressionDelay := 5 * time.Microsecond // 5μs per decompression
//
//	reader := NewMockCompressedReader(decompressionDelay)
//	cache, err := NewDecompressArena(32 * 1024 * 1024) // 32MB cache
//	if err != nil {
//		b.Fatal(err)
//	}
//	defer cache.Close()
//
//	// Setup data for transactions
//	const keysPerTransaction = 100
//	const numTransactions = 100
//	const keyOverlapRatio = 0.3 // 30% of keys overlap between transactions
//
//	transactions := make([][][]byte, numTransactions)
//	allValues := make(map[string][]byte)
//
//	// Generate transaction data with overlapping keys
//	baseKeys := make([][]byte, int(float64(keysPerTransaction)*(1-keyOverlapRatio)))
//	for i := range baseKeys {
//		baseKeys[i] = make([]byte, 32)
//		rand.Read(baseKeys[i])
//	}
//
//	for tx := 0; tx < numTransactions; tx++ {
//		transactions[tx] = make([][]byte, keysPerTransaction)
//
//		// Add base keys (overlapping)
//		overlapCount := int(float64(keysPerTransaction) * keyOverlapRatio)
//		for i := 0; i < overlapCount; i++ {
//			transactions[tx][i] = baseKeys[i%len(baseKeys)]
//		}
//
//		// Add unique keys for this transaction
//		for i := overlapCount; i < keysPerTransaction; i++ {
//			transactions[tx][i] = make([]byte, 32)
//			rand.Read(transactions[tx][i])
//		}
//
//		// Generate values for all keys
//		for _, key := range transactions[tx] {
//			if _, exists := allValues[string(key)]; !exists {
//				value := make([]byte, 256)
//				rand.Read(value)
//				allValues[string(key)] = value
//				reader.AddKeyValue(key, value)
//			}
//		}
//	}
//
//	b.Run("Without_Cache", func(b *testing.B) {
//		b.ResetTimer()
//		for i := 0; i < b.N; i++ {
//			tx := transactions[i%numTransactions]
//			for _, key := range tx {
//				_ = reader.Get(key)
//			}
//		}
//	})
//
//	b.Run("With_Cache", func(b *testing.B) {
//		b.ResetTimer()
//		for i := 0; i < b.N; i++ {
//			// Reset cache for new transaction
//			cache.Reset()
//
//			tx := transactions[i%numTransactions]
//			for _, key := range tx {
//				cached := cache.Get(key)
//				if cached == nil {
//					// Cache miss - decompress and cache
//					value := reader.Get(key)
//					cache.Put(key, value)
//				}
//			}
//		}
//	})
//
//	b.Run("With_Cache_Warmed", func(b *testing.B) {
//		// Pre-warm cache with base keys
//		for _, key := range baseKeys {
//			value := allValues[string(key)]
//			cache.Put(key, value)
//		}
//
//		b.ResetTimer()
//		for i := 0; i < b.N; i++ {
//			tx := transactions[i%numTransactions]
//			for _, key := range tx {
//				cached := cache.Get(key)
//				if cached == nil {
//					// Cache miss - decompress and cache
//					value := reader.Get(key)
//					cache.Put(key, value)
//				}
//			}
//
//			// Don't reset cache to maintain warm state
//		}
//	})
//}
