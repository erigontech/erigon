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

package commitment

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math/rand"
	"slices"
	"testing"

	"github.com/erigontech/erigon/common/length"
)

func orderBenchAccountKeys(n int, seed int64) [][]byte {
	rnd := rand.New(rand.NewSource(seed))
	out := make([][]byte, n)
	addr := make([]byte, length.Addr)
	for i := range out {
		binary.BigEndian.PutUint64(addr, rnd.Uint64())
		binary.BigEndian.PutUint64(addr[8:], rnd.Uint64())
		out[i] = KeyToHexNibbleHash(addr)
	}
	return out
}

func orderBenchStorageKeys(n, slotsPerAddr int, seed int64) [][]byte {
	rnd := rand.New(rand.NewSource(seed))
	out := make([][]byte, 0, n)
	key := make([]byte, length.Addr+length.Hash)
	for len(out) < n {
		binary.BigEndian.PutUint64(key, rnd.Uint64())
		binary.BigEndian.PutUint64(key[8:], rnd.Uint64())
		for s := 0; s < slotsPerAddr && len(out) < n; s++ {
			binary.BigEndian.PutUint64(key[length.Addr:], rnd.Uint64())
			binary.BigEndian.PutUint64(key[length.Addr+8:], rnd.Uint64())
			out = append(out, KeyToHexNibbleHash(key))
		}
	}
	return out
}

func insertAll(tr *prefixTrie, keys [][]byte) {
	for _, k := range keys {
		tr.Insert(k, k, nil)
	}
}

func BenchmarkPrefixTrieInsertOrder(b *testing.B) {
	const n = touchChunkKeys

	for _, c := range []struct {
		name string
		keys [][]byte
	}{
		{"accounts", orderBenchAccountKeys(n, 11)},
		{"storage-clustered", orderBenchStorageKeys(n, 500, 22)},
	} {
		sorted := slices.Clone(c.keys)
		slices.SortFunc(sorted, bytes.Compare)

		b.Run(fmt.Sprintf("%s/arrival", c.name), func(b *testing.B) {
			tr := newPrefixTrie()
			b.ReportAllocs()
			for b.Loop() {
				insertAll(tr, c.keys)
				b.StopTimer()
				tr.Reset()
				b.StartTimer()
			}
		})

		b.Run(fmt.Sprintf("%s/presorted", c.name), func(b *testing.B) {
			tr := newPrefixTrie()
			b.ReportAllocs()
			for b.Loop() {
				insertAll(tr, sorted)
				b.StopTimer()
				tr.Reset()
				b.StartTimer()
			}
		})

		b.Run(fmt.Sprintf("%s/sort+insert", c.name), func(b *testing.B) {
			tr := newPrefixTrie()
			scratch := make([][]byte, n)
			b.ReportAllocs()
			for b.Loop() {
				copy(scratch, c.keys)
				slices.SortFunc(scratch, bytes.Compare)
				insertAll(tr, scratch)
				b.StopTimer()
				tr.Reset()
				b.StartTimer()
			}
		})
	}
}
