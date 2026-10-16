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

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/common"
)

func newVoteTallyTestState(rng *rand.Rand, validators int) *checkpointState {
	cs := &checkpointState{
		balances:         make([]uint64, validators),
		actives:          make([]byte, (validators+7)/8),
		slasheds:         make([]byte, (validators+7)/8),
		validatorSetSize: validators,
	}
	for i := range validators {
		cs.balances[i] = uint64(rng.Intn(4)) * 8_000_000_000
		writeToBitset(cs.actives, i, rng.Intn(10) != 0)
		writeToBitset(cs.slasheds, i, rng.Intn(20) == 0)
	}
	return cs
}

func recountVotes(f *ForkChoiceStore, cs *checkpointState) map[common.Hash]uint64 {
	votes := make(map[common.Hash]uint64)
	for vi := range f.latestMessages.latestMessagesCount() {
		if message, balance, ok := f.countableVote(cs, vi); ok {
			votes[message.Root] += balance
		}
	}
	return votes
}

func TestVoteTallyMatchesFullRecount(t *testing.T) {
	const validators = 400
	rng := rand.New(rand.NewSource(1))
	roots := make([]common.Hash, 6)
	for i := range roots {
		roots[i][0] = byte(i + 1)
	}
	f := &ForkChoiceStore{latestMessages: newLatestMessagesStore(validators)}
	cs := newVoteTallyTestState(rng, validators)
	checkpoint := solid.Checkpoint{Epoch: 1}
	vote := func(vi int) {
		f.setLatestMessage(uint64(vi), LatestMessage{Epoch: uint64(rng.Intn(1 << 20)), Root: roots[rng.Intn(len(roots))]})
	}
	compared := 0
	for step := range 5000 {
		switch r := rng.Intn(100); {
		case r < 75:
			vote(rng.Intn(validators + 40))
		case r < 80:
			f.setUnequivocating(uint64(rng.Intn(validators + 40)))
		case r < 82:
			cs = newVoteTallyTestState(rng, validators+rng.Intn(40))
		case r < 83:
			checkpoint.Epoch++
		case r < 84:
			for range validators * 3 / 4 {
				vote(rng.Intn(validators))
			}
		default:
			got := f.voteTally.votes(f, checkpoint, cs)
			require.Equal(t, recountVotes(f, cs), got, "step %d", step)
			for root := range got {
				got[root] = 0
			}
			compared++
		}
	}
	require.Greater(t, compared, 500)
}
