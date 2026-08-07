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

package state

// The cap bounds the working set, not coverage: slots are handed out in
// sequence, so an arena that outgrows L2 costs more in cache misses than the
// allocations it saves. 2048 slots is 574 KB. Past the cap the caller
// allocates, which is what every object did before the arena.
const (
	arenaSlabSize   = 64
	arenaMaxSlabs   = 32
	arenaMaxObjects = arenaSlabSize * arenaMaxSlabs
)

// stateObjectArena is a slab allocator for stateObjects that are never cached
// and so die with the transaction that created them. Slabs are append-only, so
// a pointer stays valid until reset.
type stateObjectArena struct {
	slabs []*[arenaSlabSize]stateObject
	slab  int
	idx   int
}

func (a *stateObjectArena) alloc() *stateObject {
	if a.slab == len(a.slabs) && !a.grow() {
		return nil
	}
	so := &a.slabs[a.slab][a.idx]
	a.idx++
	if a.idx == arenaSlabSize {
		a.slab++
		a.idx = 0
	}
	return so
}

func (a *stateObjectArena) grow() bool {
	if len(a.slabs) == arenaMaxSlabs {
		return false
	}
	slab := new([arenaSlabSize]stateObject)
	for i := range slab {
		slab[i].arena = true
	}
	a.slabs = append(a.slabs, slab)
	return true
}

// reset makes every slot handed out since the last reset available again.
// Slots are cleared here rather than on hand-out so a slot that is not reused
// stops retaining the account and code it last held.
func (a *stateObjectArena) reset() {
	for s := 0; s <= a.slab && s < len(a.slabs); s++ {
		used := a.slabs[s][:]
		if s == a.slab {
			used = used[:a.idx]
		}
		for i := range used {
			used[i].reset()
		}
	}
	a.slab, a.idx = 0, 0
}

func (a *stateObjectArena) release() {
	a.slabs, a.slab, a.idx = nil, 0, 0
}
