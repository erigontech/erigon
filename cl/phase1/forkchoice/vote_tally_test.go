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

// Once the tally has a baseline, every recorded vote or equivocation must already be counted,
// so get_head has no per-validator work left.
func TestVoteTallyAppliesVotesWhenRecorded(t *testing.T) {
	const validators = 400
	rng := rand.New(rand.NewSource(2))
	roots := make([]common.Hash, 6)
	for i := range roots {
		roots[i][0] = byte(i + 1)
	}
	f := &ForkChoiceStore{latestMessages: newLatestMessagesStore(validators)}
	cs := newVoteTallyTestState(rng, validators)
	f.voteTally.votes(f, solid.Checkpoint{Epoch: 1}, cs)
	for step := range 2000 {
		vi := uint64(rng.Intn(validators + 40))
		if rng.Intn(10) == 0 {
			f.setUnequivocating(vi)
		} else {
			f.setLatestMessage(vi, LatestMessage{Epoch: uint64(step), Root: roots[rng.Intn(len(roots))]})
		}
		require.Equal(t, recountVotes(f, cs), f.voteTally.weightsByRoot(), "step %d", step)
	}
}

func TestVoteTallyReusesSlotsAndOmitsZeroWeightRoots(t *testing.T) {
	const validators = 4
	cs := &checkpointState{
		balances:         make([]uint64, validators),
		actives:          make([]byte, 1),
		slasheds:         make([]byte, 1),
		validatorSetSize: validators,
	}
	for i := range validators {
		cs.balances[i] = 32_000_000_000
		writeToBitset(cs.actives, i, true)
	}
	f := &ForkChoiceStore{latestMessages: newLatestMessagesStore(validators)}
	checkpoint := solid.Checkpoint{Epoch: 1}
	roots := []common.Hash{{1}, {2}}
	for epoch := range 8 {
		root := roots[epoch%len(roots)]
		f.setLatestMessage(0, LatestMessage{Epoch: uint64(epoch), Root: root})
		require.Equal(t, map[common.Hash]uint64{root: 32_000_000_000}, f.voteTally.votes(f, checkpoint, cs), "epoch %d", epoch)
		require.LessOrEqual(t, len(f.voteTally.weights), len(roots)+1, "epoch %d", epoch)
	}
}

func TestVoteTallyResetsOnlyOnGloasAttestation(t *testing.T) {
	for _, gloasForkEpoch := range []uint64{0, 1} {
		f, _ := newHeadSnapshotTestStore()
		f.beaconCfg.GloasForkEpoch = gloasForkEpoch
		f.beaconCfg.InitializeForkSchedule()
		f.voteTally.votes(f, solid.Checkpoint{Epoch: 1}, newVoteTallyTestState(rand.New(rand.NewSource(1)), 16))
		require.NotNil(t, f.voteTally.state)

		f.ProcessAttestingIndicies(&solid.Attestation{Data: &solid.AttestationData{
			Slot:            1,
			BeaconBlockRoot: common.HexToHash("0xaaaa"),
		}}, []uint64{1})

		if gloasForkEpoch == 0 {
			require.Zero(t, f.voteTally)
		} else {
			require.NotNil(t, f.voteTally.state)
		}
	}
}
