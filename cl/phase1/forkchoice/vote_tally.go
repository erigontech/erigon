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

package forkchoice

import (
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/common"
)

type voteTally struct {
	state      *checkpointState
	checkpoint solid.Checkpoint
	ids        map[common.Hash]uint32
	weights    []uint64
	applied    []uint32
	dirty      map[uint64]struct{}
}

func (t *voteTally) markDirty(validatorIndex uint64) {
	if t.state == nil {
		return
	}
	t.dirty[validatorIndex] = struct{}{}
	if len(t.dirty) > len(t.applied)/2 {
		*t = voteTally{}
	}
}

func (t *voteTally) votes(f *ForkChoiceStore, justified solid.Checkpoint, cs *checkpointState) map[common.Hash]uint64 {
	if t.state != cs || t.checkpoint != justified {
		t.rebuild(f, justified, cs)
	} else {
		for vi := range t.dirty {
			t.apply(f, int(vi))
		}
		clear(t.dirty)
	}
	votes := make(map[common.Hash]uint64, len(t.ids))
	for root, id := range t.ids {
		if w := t.weights[id]; w > 0 {
			votes[root] = w
		}
	}
	return votes
}

func (t *voteTally) rebuild(f *ForkChoiceStore, justified solid.Checkpoint, cs *checkpointState) {
	if t.ids == nil {
		t.ids = make(map[common.Hash]uint32)
		t.dirty = make(map[uint64]struct{})
	}
	t.state, t.checkpoint = cs, justified
	clear(t.ids)
	clear(t.dirty)
	t.weights = append(t.weights[:0], 0)
	t.applied = make([]uint32, f.latestMessages.latestMessagesCount())
	for vi := range t.applied {
		t.apply(f, vi)
	}
}

func (t *voteTally) apply(f *ForkChoiceStore, vi int) {
	if vi >= len(t.applied) {
		t.applied = append(t.applied, make([]uint32, vi+1-len(t.applied))...)
	}
	if id := t.applied[vi]; id != 0 {
		t.weights[id] -= t.state.balances[vi]
		t.applied[vi] = 0
	}
	message, balance, ok := f.countableVote(t.state, vi)
	if !ok {
		return
	}
	id, ok := t.ids[message.Root]
	if !ok {
		id = uint32(len(t.weights))
		t.ids[message.Root] = id
		t.weights = append(t.weights, 0)
	}
	t.weights[id] += balance
	t.applied[vi] = id
}
