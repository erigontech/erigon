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

// maxSlabs is what one transaction may draw, residentSlabs what the arena keeps
// between them: an outlier is served from the arena rather than the heap without
// parking its slabs for the life of the worker.
const (
	arenaSlabSize      = 64
	arenaResidentSlabs = 128
	arenaMaxSlabs      = 512
	arenaMaxObjects    = arenaSlabSize * arenaMaxSlabs
)

// stateObjectArena is a slab allocator for stateObjects that are never cached
// and so die with the transaction that created them. Slabs are append-only, so
// a pointer stays valid until rewind.
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

// rewind makes every slot handed out since the last rewind available again.
// Slots are reset here rather than on hand-out so a slot that is not reused
// stops retaining the account and code it last held.
func (a *stateObjectArena) rewind() {
	for s := 0; s <= a.slab && s < len(a.slabs) && s < arenaResidentSlabs; s++ {
		used := a.slabs[s][:]
		if s == a.slab {
			used = used[:a.idx]
		}
		for i := range used {
			used[i].reset()
		}
	}
	for s := arenaResidentSlabs; s < len(a.slabs); s++ {
		a.slabs[s] = nil
	}
	if len(a.slabs) > arenaResidentSlabs {
		a.slabs = a.slabs[:arenaResidentSlabs]
	}
	a.slab, a.idx = 0, 0
}

func (a *stateObjectArena) free() {
	a.slabs, a.slab, a.idx = nil, 0, 0
}
