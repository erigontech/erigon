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

package cache

import (
	"encoding/binary"
	"sync/atomic"
	"testing"
	"unsafe"

	"github.com/c2h5oh/datasize"

	"github.com/erigontech/erigon/common/crypto"
)

func BenchmarkKeccakCacheLRU(b *testing.B) {
	keys := make([][]byte, 1<<20)
	for i := range keys {
		keys[i] = make([]byte, 64)
		binary.BigEndian.PutUint64(keys[i][24:], uint64(i))
		binary.BigEndian.PutUint64(keys[i][56:], uint64(i)*7919)
	}
	entryBytes := int64(unsafe.Sizeof(crypto.KeccakCacheEntry{})) + ByteLRUEntryOverheadBytes
	crypto.SetKeccakLRU(NewByteLRU[*crypto.KeccakCacheEntry](datasize.ByteSize(entryBytes)<<17, func(uint64, *crypto.KeccakCacheEntry) int64 { return entryBytes }))
	defer crypto.SetKeccakLRU(nil)
	for _, sc := range []struct {
		name string
		span int
	}{{"hit", 1024}, {"miss", len(keys)}} {
		b.Run("lru/"+sc.name, func(b *testing.B) {
			i := 0
			for b.Loop() {
				crypto.Keccak256Hash(keys[i%sc.span])
				i++
			}
		})
		b.Run("lru/"+sc.name+"/parallel", func(b *testing.B) {
			var seed atomic.Uint64
			b.RunParallel(func(pb *testing.PB) {
				i := int(seed.Add(7777))
				for pb.Next() {
					crypto.Keccak256Hash(keys[i%sc.span])
					i++
				}
			})
		})
	}
}
