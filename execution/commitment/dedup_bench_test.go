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
	"encoding/binary"
	"math/rand"
	"testing"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/length"
)

func dedupBenchKeys(n, keyLen int) []string {
	rnd := rand.New(rand.NewSource(20260922))
	keys := make([]string, n)
	for i := range keys {
		k := make([]byte, keyLen)
		for j := 0; j+8 <= keyLen; j += 8 {
			binary.BigEndian.PutUint64(k[j:], rnd.Uint64())
		}
		keys[i] = string(k)
	}
	return keys
}

func BenchmarkDedupCost(b *testing.B) {
	const n = 50_000

	for _, c := range []struct {
		name   string
		keyLen int
	}{
		{"account", length.Addr},
		{"storage", length.Addr + length.Hash},
	} {
		keys := dedupBenchKeys(n, c.keyLen)

		b.Run(c.name+"/mapHit", func(b *testing.B) {
			seen := make(map[string]struct{}, n)
			for _, k := range keys {
				seen[k] = struct{}{}
			}
			b.ReportAllocs()
			i := 0
			for b.Loop() {
				if _, ok := seen[keys[i]]; !ok {
					b.Fatal("key must be present")
				}
				i = (i + 1) % n
			}
		})

		b.Run(c.name+"/hash", func(b *testing.B) {
			b.ReportAllocs()
			i := 0
			for b.Loop() {
				sinkHashedKey = KeyToHexNibbleHash(common.ToBytesZeroCopy(keys[i]))
				i = (i + 1) % n
			}
		})

		b.Run(c.name+"/hashCached", func(b *testing.B) {
			b.ReportAllocs()
			var cache addrHashCache
			i := 0
			for b.Loop() {
				sinkHashedKey = keyToHexNibbleHashCached(common.ToBytesZeroCopy(keys[i]), &cache)
				i = (i + 1) % n
			}
		})

		b.Run(c.name+"/trieHit", func(b *testing.B) {
			hashed := make([][]byte, n)
			tr := newPrefixTrie()
			for i, k := range keys {
				hashed[i] = KeyToHexNibbleHash(common.ToBytesZeroCopy(k))
				tr.Insert(hashed[i], common.ToBytesZeroCopy(k), nil)
			}
			b.ReportAllocs()
			i := 0
			for b.Loop() {
				if tr.Insert(hashed[i], common.ToBytesZeroCopy(keys[i]), nil) {
					b.Fatal("key must already be in the trie")
				}
				i = (i + 1) % n
			}
		})
	}
}

var sinkHashedKey []byte
