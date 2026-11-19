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
	"math/rand"
	"testing"
	"time"

	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/common"
)

func BenchmarkVoteTally(b *testing.B) {
	const validators = 1_100_000
	const changedPerSlot = validators / 32
	rng := rand.New(rand.NewSource(1))
	cs := &checkpointState{
		balances:         make([]uint64, validators),
		actives:          make([]byte, (validators+7)/8),
		slasheds:         make([]byte, (validators+7)/8),
		validatorSetSize: validators,
	}
	for i := range validators {
		cs.balances[i] = 32_000_000_000
		writeToBitset(cs.actives, i, true)
	}
	roots := make([]common.Hash, 64)
	for i := range roots {
		roots[i][0] = byte(i + 1)
	}
	f := &ForkChoiceStore{latestMessages: newLatestMessagesStore(validators)}
	for vi := range validators {
		f.setLatestMessage(uint64(vi), LatestMessage{Epoch: 1, Root: roots[rng.Intn(len(roots))]})
	}
	checkpoint := solid.Checkpoint{Epoch: 1}

	b.Run("recount", func(b *testing.B) {
		for b.Loop() {
			recountVotes(f, cs)
		}
	})
	b.Run("slot-delta", func(b *testing.B) {
		f.voteTally.votes(f, checkpoint, cs)
		epoch := uint64(2)
		var spent time.Duration
		for b.Loop() {
			for range changedPerSlot {
				f.setLatestMessage(uint64(rng.Intn(validators)), LatestMessage{Epoch: epoch, Root: roots[rng.Intn(len(roots))]})
			}
			epoch++
			start := time.Now()
			f.voteTally.votes(f, checkpoint, cs)
			spent += time.Since(start)
		}
		b.ReportMetric(float64(spent.Nanoseconds())/float64(b.N), "votes-ns/op")
	})
}
