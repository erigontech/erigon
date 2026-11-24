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

package accounts

import (
	"cmp"
	"encoding/binary"
	"runtime"
	"slices"
	"testing"
	"unsafe"

	"github.com/erigontech/erigon/common"
)

func BenchmarkCanonMigrationOrder(b *testing.B) {
	const keys = 1 << 20
	byAddress := func(a, b *common.Hash) int {
		return cmp.Compare(uintptr(unsafe.Pointer(a)), uintptr(unsafe.Pointer(b)))
	}
	orders := []struct {
		name string
		sort func([]*common.Hash)
	}{
		{"range", func([]*common.Hash) {}},
		{"ascending", func(p []*common.Hash) { slices.SortFunc(p, byAddress) }},
		{"descending", func(p []*common.Hash) { slices.SortFunc(p, func(a, b *common.Hash) int { return byAddress(b, a) }) }},
	}
	for _, o := range orders {
		b.Run(o.name, func(b *testing.B) {
			for b.Loop() {
				b.StopTimer()
				c := newCanonTable[common.Hash](keys + 1)
				for i := range keys {
					var k common.Hash
					binary.BigEndian.PutUint64(k[:], uint64(i))
					c.intern(k)
				}
				old := c.swap()
				runtime.GC()
				b.StartTimer()
				ptrs := make([]*common.Hash, 0, old.Size())
				old.Range(func(_ common.Hash, p *common.Hash) bool {
					ptrs = append(ptrs, p)
					return true
				})
				o.sort(ptrs)
				c.addWeak(ptrs)
				c.prev.Store(nil) // the cleanups capture c, so a set prev would keep this generation alive across iterations
			}
		})
	}
}
