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

package eip8297

import (
	"encoding/binary"
	"fmt"
	"testing"
)

func BenchmarkParallelStreamRootSkewed(b *testing.B) {
	entries := parallelStreamRootSkewedEntries(100_000)
	for _, workers := range []int{1, 4, 16} {
		b.Run(fmt.Sprintf("workers=%d", workers), func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				parallel, err := NewParallelStreamRoot(streamRootHash(HashKeccak), workers)
				if err != nil {
					b.Fatal(err)
				}
				for _, entry := range entries {
					if err := parallel.AddContext(b.Context(), entry.Key, entry.Value); err != nil {
						b.Fatal(err)
					}
				}
				if _, err := parallel.RootHash(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkParallelStreamRootHoodiMix(b *testing.B) {
	entries := parallelStreamRootHoodiEntries(100_000)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		parallel, err := NewParallelStreamRoot(streamRootHash(HashKeccak), 16)
		if err != nil {
			b.Fatal(err)
		}
		for _, entry := range entries {
			if err := parallel.AddContext(b.Context(), entry.Key, entry.Value); err != nil {
				b.Fatal(err)
			}
		}
		if _, err := parallel.RootHash(); err != nil {
			b.Fatal(err)
		}
	}
}

func parallelStreamRootHoodiEntries(count int) []Entry {
	entries := make([]Entry, count)
	for i := range entries {
		stem := uint64(i / 256)
		subIndex := byte(i)
		var zone byte
		positionLength := 32
		switch i % 10 {
		case 0, 1:
			zone = AccountZone
		case 2:
			zone = CodeZone
		default:
			zone = StorageZone
			positionLength = 64
		}
		position := make([]byte, positionLength)
		position[0] = byte(stem)
		binary.BigEndian.PutUint64(position[positionLength-8:], stem)
		value := streamRootValue(byte(i))
		value[0] = 1
		entries[i] = Entry{Key: TreeKey(zone, position, subIndex), Value: value}
	}
	return sortStreamEntries(entries)
}
