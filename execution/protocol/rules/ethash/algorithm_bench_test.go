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

package ethash

import (
	"bytes"
	"encoding/binary"
	"io"
	"testing"

	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/common/length"
)

// Benchmarks the cache generation performance.
func BenchmarkCacheGeneration(b *testing.B) {
	for b.Loop() {
		cache := make([]uint32, cacheSize(1)/4)
		generateCache(cache, 0, make([]byte, 32))
	}
}

// Benchmarks the dataset (small) generation performance.
func BenchmarkSmallDatasetGeneration(b *testing.B) {
	cache := make([]uint32, 65536/4)
	generateCache(cache, 0, make([]byte, 32))

	for b.Loop() {
		dataset := make([]uint32, 32*65536/4)
		generateDataset(dataset, 0, cache)
	}
}

// Benchmarks the light verification performance.
func BenchmarkHashimotoLight(b *testing.B) {
	cache := make([]uint32, cacheSize(1)/4)
	generateCache(cache, 0, make([]byte, 32))

	hash := hexutil.MustDecode("0xc9149cc0386e689d789a1c2f3d5d169a61a6218ed30e74414dc736e442ef3d1f")

	for b.Loop() {
		hashimotoLight(datasetSize(1), cache, hash, 0)
	}
}

// Benchmarks the full (small) verification performance.
func BenchmarkHashimotoFullSmall(b *testing.B) {
	cache := make([]uint32, 65536/4)
	generateCache(cache, 0, make([]byte, 32))

	dataset := make([]uint32, 32*65536/4)
	generateDataset(dataset, 0, cache)

	hash := hexutil.MustDecode("0xc9149cc0386e689d789a1c2f3d5d169a61a6218ed30e74414dc736e442ef3d1f")

	for b.Loop() {
		hashimotoFull(dataset, hash, 0)
	}
}

// Benchmarks the full verification performance for mmap
func BenchmarkHashimotoFullMmap(b *testing.B) {
	benchmarkHashimotoFullMmap(b, "WithLock", true)
	benchmarkHashimotoFullMmap(b, "WithoutLock", false)
}

func BenchmarkSeedHash(b *testing.B) {
	var res []byte
	for i := uint64(0); b.Loop(); i++ {
		res = seedHash(i*epochLength + 1)
	}

	_, err := io.Copy(io.Discard, bytes.NewBuffer(res))
	if err != nil {
		b.Error(err)
	}
}

func BenchmarkSeedHashOld(b *testing.B) {
	var res []byte
	for i := uint64(0); b.Loop(); i++ {
		res = seedHashOld(i*epochLength + 1)
	}

	_, err := io.Copy(io.Discard, bytes.NewBuffer(res))
	if err != nil {
		b.Error(err)
	}
}

func benchmarkHashimotoFullMmap(b *testing.B, name string, lock bool) {
	b.Run(name, func(b *testing.B) {
		tmpdir := b.TempDir()
		d := &dataset{epoch: 0}
		d.generate(tmpdir, 1, lock, testing.Short())
		var hash [length.Hash]byte
		b.ResetTimer()
		for i := 0; b.Loop(); i++ {
			binary.PutVarint(hash[:], int64(i))
			hashimotoFull(d.dataset, hash[:], 0)
		}
	})
}
