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

package etl

import (
	"encoding/binary"
	"fmt"
	"runtime"
	"sync"
	"testing"

	"github.com/erigontech/erigon/common/log/v3"
)

func discardLoad(_, _ []byte, _ CurrentTableReader, _ LoadNextFunc) error { return nil }

func BenchmarkCollectorRefillFromEmptyPool(b *testing.B) {
	for _, entries := range []int{10_000, 300_000} {
		b.Run(fmt.Sprintf("entries_%d", entries), func(b *testing.B) {
			allocator := NewAllocator(&sync.Pool{New: func() any { return NewSortableBuffer(etlSmallBufRAM) }})
			tmpdir := b.TempDir()
			key := make([]byte, 40)
			val := make([]byte, 24)
			b.ReportAllocs()
			for b.Loop() {
				b.StopTimer()
				runtime.GC()
				runtime.GC()
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
		})
	}
}
