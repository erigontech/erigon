// Copyright 2024 The Erigon Authors
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

// Package coherence holds the (epoch, floor) unwind-coherence primitive shared
// by the state, code and commitment-branch caches. It is a leaf package (only
// sync/atomic + math) so both execution/cache and execution/commitment can
// embed it without an import cycle.
package coherence

import (
	"math"
	"sync/atomic"
)

// gen is the immutable (epoch, floor) pair. Held behind a single atomic.Pointer
// so a reader never observes a torn epoch/floor mid-Unwind.
type gen struct {
	epoch uint32
	floor uint64
}

// Gen tracks unwind coherence for a cache: every entry is stamped (txNum, epoch),
// and is stale iff it was written in a superseded epoch AND its txNum is at or
// above the unwind floor (the first rolled-back txNum). Unwind bumps the epoch
// and lowers the floor — O(1), scan-free; stale entries drop lazily on their
// next read. The floor only ever decreases, so a shallower later unwind can't
// resurrect entries a deeper one invalidated.
//
// The zero value is not usable — call Init (constructors and Clear).
type Gen struct {
	state atomic.Pointer[gen]
}

// Init sets the pre-unwind state: epoch 0, floor = MaxUint64 (every entry
// predates the nonexistent floor, so all reads are valid until the first unwind).
func (g *Gen) Init() {
	g.state.Store(&gen{epoch: 0, floor: math.MaxUint64})
}

// Epoch returns the current epoch, for stamping freshly written entries.
func (g *Gen) Epoch() uint32 {
	return g.load().epoch
}

func (g *Gen) load() *gen {
	if s := g.state.Load(); s != nil {
		return s
	}
	// Defensive: an un-Init'd Gen reads as pre-unwind rather than panicking.
	return &gen{epoch: 0, floor: math.MaxUint64}
}

// IsStale reports whether an entry stamped (txNum, epoch) reflects dead-fork
// state after an unwind.
func (g *Gen) IsStale(txNum uint64, epoch uint32) bool {
	s := g.load()
	return epoch != s.epoch && txNum >= s.floor
}

// Unwind bumps the epoch and lowers the floor to unwindToTxN (the first
// rolled-back txNum, e.g. Min(UnwindPoint+1)). Atomic against concurrent readers
// and other Unwinds: the new (epoch+1, min(floor, unwindToTxN)) pair is published
// in one CAS, so IsStale/Epoch never see a half-applied update.
func (g *Gen) Unwind(unwindToTxN uint64) {
	for {
		cur := g.state.Load() // raw (may be nil); CAS must compare the stored pointer
		epoch := uint32(0)
		floor := uint64(math.MaxUint64)
		if cur != nil {
			epoch, floor = cur.epoch, cur.floor
		}
		if unwindToTxN < floor {
			floor = unwindToTxN
		}
		if g.state.CompareAndSwap(cur, &gen{epoch: epoch + 1, floor: floor}) {
			return
		}
	}
}
