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

// The cap bounds the working set to fit L2 (2048 slots = 574 KB); past it the
// caller falls back to a normal allocation, as before the arena existed.
const (
	arenaSlabSize   = 64
	arenaMaxSlabs   = 32
	arenaMaxObjects = arenaSlabSize * arenaMaxSlabs
)

// stateObjectArena is a slab allocator for stateObjects that die with the tx.
// Slabs are append-only, so a pointer stays valid until reset.
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
	// The full-struct store means a slot never depends on the prior rewind; the arena tag rides along so release() won't pool a live slot.
	*so = stateObject{arena: true}
	a.idx++
	if a.idx == arenaSlabSize {
		a.slab++
		a.idx = 0
	}
	return so
}

func (a *stateObjectArena) empty() bool { return a.slab == 0 && a.idx == 0 }

func (a *stateObjectArena) grow() bool {
	if len(a.slabs) == arenaMaxSlabs {
		return false
	}
	a.slabs = append(a.slabs, new([arenaSlabSize]stateObject))
	return true
}

// Zeroes every handed-out slot so it stops pinning old account/code data; alloc's full overwrite means correctness never depends on this cleanup.
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
