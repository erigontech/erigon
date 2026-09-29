// Copyright 2022 The go-ethereum Authors
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

package lru

import (
	crand "crypto/rand"
	"fmt"
	"io"
	"math/rand"
	"testing"
)

func BenchmarkLRU(b *testing.B) {
	var (
		capacity = 1000
		indexes  = make([]int, capacity*20)
		keys     = make([]string, capacity)
		values   = make([][]byte, capacity)
	)
	for i := range indexes {
		indexes[i] = rand.Intn(capacity)
	}
	for i := range keys {
		b := make([]byte, 32)
		crand.Read(b)
		keys[i] = string(b)
		crand.Read(b)
		values[i] = b
	}

	var sink []byte

	b.Run("Add/BasicLRU", func(b *testing.B) {
		cache := NewBasicLRU[int, int](capacity)
		for i := 0; b.Loop(); i++ {
			cache.Add(i, i)
		}
	})
	b.Run("Get/BasicLRU", func(b *testing.B) {
		cache := NewBasicLRU[string, []byte](capacity)
		for i := range capacity {
			index := indexes[i]
			cache.Add(keys[index], values[index])
		}

		b.ResetTimer()
		for i := 0; b.Loop(); i++ {
			k := keys[indexes[i%len(indexes)]]
			v, ok := cache.Get(k)
			if ok {
				sink = v
			}
		}
	})

	fmt.Fprintln(io.Discard, sink)
}
