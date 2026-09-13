// Copyright 2026 The Erigon Authors
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

package posidx

import (
	"fmt"
	"math/rand"
	"path/filepath"
	"testing"
)

func benchIndex(b *testing.B, runs, perRun, pageSize uint64) *Index {
	b.Helper()
	items := runs * perRun
	pages := (items + pageSize - 1) / pageSize
	path := filepath.Join(b.TempDir(), "bench.vi")
	w, err := NewWriter(path, b.TempDir(), pageSize, runs, items, pages*64)
	if err != nil {
		b.Fatal(err)
	}
	defer w.Close()
	w.NoFsync()
	for range runs {
		w.AddRun(perRun)
	}
	for p := range pages {
		w.AddPage(p * 64)
	}
	if err := w.Build(); err != nil {
		b.Fatal(err)
	}
	idx, err := Open(path)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(idx.Close)
	return idx
}

func BenchmarkGet(b *testing.B) {
	for _, pageSize := range []uint64{1, 64} {
		b.Run(fmt.Sprintf("page%d", pageSize), func(b *testing.B) {
			const runs, perRun = 1 << 16, 8
			idx := benchIndex(b, runs, perRun, pageSize)
			r := rand.New(rand.NewSource(1))
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				if _, ok := idx.Get(uint64(r.Intn(runs)), uint64(r.Intn(perRun))); !ok {
					b.Fatal("miss")
				}
			}
		})
	}
}

func BenchmarkOpen(b *testing.B) {
	const runs, perRun, pageSize = 1 << 20, 8, 1
	items := uint64(runs * perRun)
	pages := items / pageSize
	path := filepath.Join(b.TempDir(), "big.vi")
	w, err := NewWriter(path, b.TempDir(), pageSize, runs, items, pages*64)
	if err != nil {
		b.Fatal(err)
	}
	w.NoFsync()
	for range uint64(runs) {
		w.AddRun(perRun)
	}
	for p := range pages {
		w.AddPage(p * 64)
	}
	if err := w.Build(); err != nil {
		b.Fatal(err)
	}
	w.Close()
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		idx, err := Open(path)
		if err != nil {
			b.Fatal(err)
		}
		idx.Close()
	}
}
