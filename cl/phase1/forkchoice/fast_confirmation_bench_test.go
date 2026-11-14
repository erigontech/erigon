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
	"encoding/binary"
	"testing"

	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/common"
)

func BenchmarkFastConfirmationMillionValidators(b *testing.B) {
	const count = 1_000_000
	balances := &fcrBalances{balances: make([]uint64, count), active: make([]bool, count), slashed: make([]bool, count), total: count * 32_000_000_000}
	roots := make([]common.Hash, 64)
	for i := range roots {
		binary.LittleEndian.PutUint64(roots[i][:], uint64(i+1))
	}
	justified := solid.Checkpoint{Root: roots[32], Epoch: 1}
	view := &fcrView{slot: 64, slotsPerEpoch: 32, balanceIncrement: 1_000_000_000, proposerBoost: 40, head: roots[63], headBalances: balances, finalized: solid.Checkpoint{Root: roots[0]}, unrealized: justified, blocks: make(map[common.Hash]fcrBlock), committees: make(map[uint64][]uint64), votes: make([]fcrVote, count)}
	for i := range roots {
		block := fcrBlock{slot: uint64(i), valid: true, unrealized: justified, votingSource: justified}
		if i > 0 {
			block.parent = roots[i-1]
		}
		view.blocks[roots[i]] = block
	}
	for i := range count {
		balances.balances[i] = 32_000_000_000
		balances.active[i] = true
		view.votes[i] = fcrVote{root: roots[63], epoch: 1}
		slot := uint64(i%32) + 32
		view.committees[slot] = append(view.committees[slot], uint64(i))
	}
	rule := newFastConfirmation(view.finalized)
	rule.confirmedRoot = roots[32]
	rule.currentObserved = justified
	rule.previousSlotHead = roots[63]
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if got := rule.latestConfirmed(view, balances, balances); got != roots[63] {
			b.Fatalf("confirmed %x", got)
		}
	}
}
