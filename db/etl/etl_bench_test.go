// Copyright 2021 The Erigon Authors
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

package etl

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"testing"

	"github.com/c2h5oh/datasize"

	"github.com/erigontech/erigon/common/dir"
	"github.com/erigontech/erigon/common/log/v3"
)

func BenchmarkFileDataProviderNext(b *testing.B) {
	const keySize = 32
	for _, valSize := range []int{32, 128, 1024} {
		name := fmt.Sprintf("key%d_val%d", keySize, valSize)
		buf := makeSortedBuffer(keySize, valSize, 10_000)

		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				b.StopTimer()
				tmpdir, _ := os.MkdirTemp("", "bench-fdp-")
				provider, err := FlushToDisk("bench", buf, tmpdir, log.LvlInfo)
				if err != nil {
					b.Fatal(err)
				}
				b.StartTimer()

				for {
					_, _, err := provider.Next()
					if errors.Is(err, io.EOF) {
						break
					}
					if err != nil {
						b.Fatal(err)
					}
				}

				b.StopTimer()
				provider.Dispose()
				_ = dir.RemoveAll(tmpdir)
				b.StartTimer()
			}
		})
	}
}

func BenchmarkCollect(b *testing.B) {
	logger := log.New()
	const keyLen = 32
	const valLen = 128

	for _, tc := range []struct {
		name    string
		count   int
		bufSize datasize.ByteSize
	}{
		{"10k_smallbuf", 10_000, 64 * datasize.KB},
		{"10k_largebuf", 10_000, 256 * datasize.MB},
		{"100k_smallbuf", 100_000, 256 * datasize.KB},
		{"100k_largebuf", 100_000, 256 * datasize.MB},
	} {
		// Pre-generate deterministic keys/values
		keys := make([][]byte, tc.count)
		vals := make([][]byte, tc.count)
		for i := range tc.count {
			k := make([]byte, keyLen)
			binary.BigEndian.PutUint64(k, uint64(i)*6364136223846793005)
			keys[i] = k
			v := make([]byte, valLen)
			binary.BigEndian.PutUint64(v, uint64(i))
			vals[i] = v
		}

		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			tmpdir := b.TempDir()
			for b.Loop() {
				c := NewCollector("bench", tmpdir, NewSortableBuffer(tc.bufSize), logger)
				for i := range tc.count {
					if err := c.Collect(keys[i], vals[i]); err != nil {
						b.Fatal(err)
					}
				}
				c.Close()
			}
		})
	}
}

func BenchmarkMergeSortFiles(b *testing.B) {
	logger := log.New()
	const keyLen = 32
	const valLen = 128

	for _, tc := range []struct {
		name         string
		count        int
		bufSize      datasize.ByteSize
		expectOnDisk bool // true when bufSize is small enough to force file providers
	}{
		{"mem_only_10k", 10_000, 256 * datasize.MB, false},
		{"file_only_10k", 10_000, 64 * datasize.KB, true},
		{"file_only_100k", 100_000, 256 * datasize.KB, true},
	} {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			tmpdir := b.TempDir()

			// Pre-generate deterministic keys/values
			keys := make([][]byte, tc.count)
			vals := make([][]byte, tc.count)
			for i := range tc.count {
				k := make([]byte, keyLen)
				binary.BigEndian.PutUint64(k, uint64(i)*6364136223846793005)
				keys[i] = k
				v := make([]byte, valLen)
				binary.BigEndian.PutUint64(v, uint64(i))
				vals[i] = v
			}

			for b.Loop() {
				c := NewCollector("bench", tmpdir, NewSortableBuffer(tc.bufSize), logger)
				for i := range tc.count {
					if err := c.Collect(keys[i], vals[i]); err != nil {
						b.Fatal(err)
					}
				}
				if err := c.Load(nil, "", func(k, v []byte, _ CurrentTableReader, next LoadNextFunc) error {
					return nil
				}, TransformArgs{}); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkSortableBufferSort(b *testing.B) {
	const keyLen = 32
	const valLen = 64

	makeBuffer := func(n int, sorted bool) *sortableBuffer {
		buf := NewSortableBuffer(256 * 1024 * 1024)
		buf.Prealloc(n, n*(keyLen+valLen))
		key := make([]byte, keyLen)
		val := make([]byte, valLen)
		for i := range n {
			if sorted {
				binary.BigEndian.PutUint64(key, uint64(i))
			} else {
				// deterministic pseudo-random: mix the index
				x := uint64(i) * 6364136223846793005
				binary.BigEndian.PutUint64(key, x)
				binary.BigEndian.PutUint64(key[8:], x^0xdeadbeef)
			}
			binary.BigEndian.PutUint64(val, uint64(i))
			buf.Put(key, val)
		}
		return buf
	}

	for _, tc := range []struct {
		name   string
		count  int
		sorted bool
	}{
		{"random_100k", 100_000, false},
		{"random_500k", 500_000, false},
		{"sorted_100k", 100_000, true},
		{"sorted_500k", 500_000, true},
	} {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				b.StopTimer()
				ref := makeBuffer(tc.count, tc.sorted)
				b.StartTimer()
				ref.Sort()
			}
		})
	}
}

func BenchmarkSortableBufferPutSort(b *testing.B) {
	const keyLen = 32
	const valLen = 64

	for _, tc := range []struct {
		name   string
		count  int
		sorted bool
	}{
		{"random_100k", 100_000, false},
		{"random_500k", 500_000, false},
		{"sorted_100k", 100_000, true},
		{"sorted_500k", 500_000, true},
	} {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			key := make([]byte, keyLen)
			val := make([]byte, valLen)
			buf := NewSortableBuffer(256 * 1024 * 1024)
			buf.Prealloc(tc.count, tc.count*(keyLen+valLen))
			for b.Loop() {
				buf.Reset()
				for i := range tc.count {
					if tc.sorted {
						binary.BigEndian.PutUint64(key, uint64(i))
					} else {
						x := uint64(i) * 6364136223846793005
						binary.BigEndian.PutUint64(key, x)
						binary.BigEndian.PutUint64(key[8:], x^0xdeadbeef)
					}
					binary.BigEndian.PutUint64(val, uint64(i))
					buf.Put(key, val)
				}
				buf.Sort()
			}
		})
	}
}

func BenchmarkSortableBufferPutSortLoad(b *testing.B) {
	const keyLen = 32
	const valLen = 64

	for _, tc := range []struct {
		name   string
		count  int
		sorted bool
	}{
		{"random_100k", 100_000, false},
		{"random_500k", 500_000, false},
		{"sorted_100k", 100_000, true},
		{"sorted_500k", 500_000, true},
	} {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			key := make([]byte, keyLen)
			val := make([]byte, valLen)
			buf := NewSortableBuffer(256 * 1024 * 1024)
			buf.Prealloc(tc.count, tc.count*(keyLen+valLen))
			for b.Loop() {
				buf.Reset()
				for i := range tc.count {
					if tc.sorted {
						binary.BigEndian.PutUint64(key, uint64(i))
					} else {
						x := uint64(i) * 6364136223846793005
						binary.BigEndian.PutUint64(key, x)
						binary.BigEndian.PutUint64(key[8:], x^0xdeadbeef)
					}
					binary.BigEndian.PutUint64(val, uint64(i))
					buf.Put(key, val)
				}
				buf.Sort()
				// Load phase: iterate sorted buffer like ETL load does
				for _, _, ok := buf.Next(); ok; _, _, ok = buf.Next() {
				}
			}
		})
	}
}

func BenchmarkSortableBufferPutOnly(b *testing.B) {
	const keyLen = 32
	const valLen = 64

	for _, tc := range []struct {
		name   string
		count  int
		sorted bool
	}{
		{"random_100k", 100_000, false},
		{"random_500k", 500_000, false},
		{"sorted_100k", 100_000, true},
		{"sorted_500k", 500_000, true},
	} {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			key := make([]byte, keyLen)
			val := make([]byte, valLen)
			buf := NewSortableBuffer(256 * 1024 * 1024)
			buf.Prealloc(tc.count, tc.count*(keyLen+valLen))
			for b.Loop() {
				buf.Reset()
				for i := range tc.count {
					if tc.sorted {
						binary.BigEndian.PutUint64(key, uint64(i))
					} else {
						x := uint64(i) * 6364136223846793005
						binary.BigEndian.PutUint64(key, x)
						binary.BigEndian.PutUint64(key[8:], x^0xdeadbeef)
					}
					binary.BigEndian.PutUint64(val, uint64(i))
					buf.Put(key, val)
				}
			}
		})
	}
}

// BenchmarkSortableBufferRead reads a sorted buffer end to end. The sort runs
// once before the loop, so an iteration is the read itself - it re-reads one
// buffer, where a collector reads one only once.
func BenchmarkSortableBufferRead(b *testing.B) {
	const keyLen = 32
	const valLen = 64

	for _, tc := range []struct {
		name   string
		count  int
		sorted bool
	}{
		{"random_100k", 100_000, false},
		{"random_500k", 500_000, false},
		{"sorted_100k", 100_000, true},
		{"sorted_500k", 500_000, true},
	} {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			key := make([]byte, keyLen)
			val := make([]byte, valLen)
			buf := NewSortableBuffer(256 * 1024 * 1024)
			buf.Prealloc(tc.count, tc.count*(keyLen+valLen))
			for i := range tc.count {
				if tc.sorted {
					binary.BigEndian.PutUint64(key, uint64(i))
				} else {
					x := uint64(i) * 6364136223846793005
					binary.BigEndian.PutUint64(key, x)
					binary.BigEndian.PutUint64(key[8:], x^0xdeadbeef)
				}
				binary.BigEndian.PutUint64(val, uint64(i))
				buf.Put(key, val)
			}
			buf.Sort()
			b.ResetTimer()
			for b.Loop() {
				buf.at = 0 // rewind; Sort would re-check sortedness first
				for _, _, ok := buf.Next(); ok; _, _, ok = buf.Next() {
				}
			}
		})
	}
}

func BenchmarkSortableBufferLoadOnly(b *testing.B) {
	const keyLen = 32
	const valLen = 64
	logger := log.New()

	// bufSize is chosen to produce ~5 disk providers per run:
	// 100k entries × 96 bytes ≈ 9.6 MB → bufSize = 2 MB → ~5 flushes
	// 500k entries × 96 bytes ≈ 48 MB → bufSize = 10 MB → ~5 flushes
	for _, tc := range []struct {
		name    string
		count   int
		sorted  bool
		bufSize datasize.ByteSize
	}{
		{"random_100k", 100_000, false, 2 * datasize.MB},
		{"random_500k", 500_000, false, 10 * datasize.MB},
		{"sorted_100k", 100_000, true, 2 * datasize.MB},
		{"sorted_500k", 500_000, true, 10 * datasize.MB},
	} {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			key := make([]byte, keyLen)
			val := make([]byte, valLen)
			tmpdir := b.TempDir()
			for b.Loop() {
				b.StopTimer()
				c := NewCollector(b.Name(), tmpdir, NewSortableBuffer(tc.bufSize), logger)
				for i := range tc.count {
					if tc.sorted {
						binary.BigEndian.PutUint64(key, uint64(i))
					} else {
						x := uint64(i) * 6364136223846793005
						binary.BigEndian.PutUint64(key, x)
						binary.BigEndian.PutUint64(key[8:], x^0xdeadbeef)
					}
					binary.BigEndian.PutUint64(val, uint64(i))
					if err := c.Collect(key, val); err != nil {
						b.Fatal(err)
					}
				}
				c.buf.Sort()
				b.StartTimer()
				if err := c.Load(nil, "", func(k, v []byte, _ CurrentTableReader, next LoadNextFunc) error {
					return nil
					//return next(k, k, v)
				}, TransformArgs{}); err != nil {
					b.Fatal(err)
				}
				b.StopTimer()
				c.Close()
				b.StartTimer()
			}
		})
	}
}

func BenchmarkMemoryDataProviderNext(b *testing.B) {
	for _, keySize := range []int{20, 32, 64} {
		for _, valSize := range []int{32, 128, 256, 1024} {
			name := fmt.Sprintf("key%d_val%d", keySize, valSize)
			buf := makeSortedBuffer(keySize, valSize, 10_000)

			b.Run(name+"/Next", func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					buf.at = 0
					p := &memoryDataProvider{buffer: buf}
					for {
						_, _, err := p.Next()
						if errors.Is(err, io.EOF) {
							break
						}
						if err != nil {
							b.Fatal(err)
						}
					}
				}
			})

			b.Run(name+"/Buffer", func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					buf.at = 0
					for _, _, ok := buf.Next(); ok; _, _, ok = buf.Next() {
					}
				}
			})
		}
	}
}

// BenchmarkSortableBufferPutOnlyCold fills a fresh buffer without Prealloc.
// Only the first iteration misses the chunk pool - Reset hands the chunks
// straight back - so this measures a new buffer against a warm pool, not a
// cold-start cost.
func BenchmarkSortableBufferPutOnlyCold(b *testing.B) {
	const keyLen = 32
	const valLen = 64

	for _, count := range []int{100_000, 500_000, 1_000_000} {
		b.Run(fmt.Sprintf("random_%dk", count/1000), func(b *testing.B) {
			b.ReportAllocs()
			key := make([]byte, keyLen)
			val := make([]byte, valLen)
			for b.Loop() {
				buf := NewSortableBuffer(256 * 1024 * 1024)
				for i := range count {
					x := uint64(i) * 6364136223846793005
					binary.BigEndian.PutUint64(key, x)
					binary.BigEndian.PutUint64(key[8:], x^0xdeadbeef)
					binary.BigEndian.PutUint64(val, uint64(i)) //nolint:gosec
					buf.Put(key, val)
				}
				buf.Reset() // give the chunks back, as the collector does
			}
		})
	}
}

// BenchmarkSortableBufferWrite is the flush path minus the file.
func BenchmarkSortableBufferWrite(b *testing.B) {
	const keyLen = 32

	for _, tc := range []struct {
		name   string
		count  int
		valLen int
		sorted bool
	}{
		{"random_100k_val64", 100_000, 64, false},
		{"random_100k_val1024", 100_000, 1024, false},
		{"sorted_100k_val64", 100_000, 64, true},
	} {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			key := make([]byte, keyLen)
			val := make([]byte, tc.valLen)
			buf := NewSortableBuffer(256 * 1024 * 1024)
			for b.Loop() {
				b.StopTimer()
				buf.Reset()
				for i := range tc.count {
					if tc.sorted {
						binary.BigEndian.PutUint64(key, uint64(i))
					} else {
						x := uint64(i) * 6364136223846793005
						binary.BigEndian.PutUint64(key, x)
						binary.BigEndian.PutUint64(key[8:], x^0xdeadbeef)
					}
					buf.Put(key, val)
				}
				b.StartTimer()
				buf.Sort()
				if err := buf.Write(io.Discard); err != nil {
					b.Fatal(err)
				}
			}
			buf.Reset()
		})
	}
}

func BenchmarkCollectorRefillFromEmptyPool(b *testing.B) {
	const entries = 300_000
	allocator := NewAllocator(&sync.Pool{New: func() any { return NewSortableBuffer(etlSmallBufRAM) }})
	tmpdir := b.TempDir()
	key := make([]byte, 40)
	val := make([]byte, 24)
	b.ReportAllocs()
	for b.Loop() {
		b.StopTimer()
		purgePool()
		b.StartTimer()
		c := NewCollectorWithAllocator(b.Name(), tmpdir, allocator, log.New())
		for i := range entries {
			binary.BigEndian.PutUint32(key[36:], uint32(i))
			if err := c.Collect(key, val); err != nil {
				b.Fatal(err)
			}
		}
		if err := c.Load(nil, "", discardLoad, TransformArgs{}); err != nil {
			b.Fatal(err)
		}
		c.Close()
	}
}
