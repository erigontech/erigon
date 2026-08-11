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
	// One store establishes every field, so what a caller gets never depends on
	// the rewind having cleared the slot first. The arena tag rides along: it is
	// slot identity, and release() must not hand a live slot to the shared pool.
	*so = stateObject{arena: true}
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
	a.slabs = append(a.slabs, new([arenaSlabSize]stateObject))
	return true
}

// reset makes every slot handed out since the last reset available again. Slots
// are zeroed as well as rewound so a slot that is not reused stops retaining the
// account and code it last held; alloc re-establishes the slot, so correctness
// does not depend on this pass.
func (a *stateObjectArena) reset() {
	for s := 0; s <= a.slab && s < len(a.slabs); s++ {
		used := a.slabs[s][:]
		if s == a.slab {
			used = used[:a.idx]
		}
		clear(used)
	}
	a.slab, a.idx = 0, 0
}

func (a *stateObjectArena) release() {
	a.slabs, a.slab, a.idx = nil, 0, 0
}
