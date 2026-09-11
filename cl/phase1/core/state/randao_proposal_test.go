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

package state_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/common"
)

// THE RANDAO A PROPOSER PUTS IN A PAYLOAD IS THE STATE'S, NOT THE CLOCK'S.
//
// ProcessExecutionPayload verifies `payload.prev_randao == get_randao_mix(state, get_current_epoch(state))`
// against the state at the block's own slot. A proposer that sourced the mix from the WALL CLOCK's epoch
// instead agreed with that only while the chain was keeping up with its clock. Let the clock run ahead of
// the head — a 2s-slot L2 whose proposer misses a slot, which is ordinary — and it reads a LATER entry of
// the randao ring, one still holding its genesis value on the first pass round.
//
// The rejection that follows is self-reinforcing and fatal: the head stops, so the clock drifts further, so
// the gap widens, so every block after it is rejected too. Measured on a three-chain dev node — the 12s
// chains kept up and were fine, the 2s venue chain wedged at block 5 and never produced another.
func TestRandaoMixForProposal_ComesFromTheStateEpochNotTheClock(t *testing.T) {
	cfg := &clparams.MainnetBeaconConfig
	s := state.New(cfg)

	// A chain that has run a while: epoch 0's entry has been mixed by every block so far, while the
	// entries further round the ring have never been written and still hold genesis.
	evolved := common.HexToHash("0x15e07f6585635d77d6f063c610b9de4c5986b356e30f1c67df7cf3744836008b")
	genesisMix := common.HexToHash("0x2ca85eb8c57e0281879dec8cc28aae07a32e72d480d5c41aa34c3346d3f85580")
	for i := 0; i < int(cfg.EpochsPerHistoricalVector); i++ {
		s.SetRandaoMixAt(i, genesisMix)
	}
	s.SetRandaoMixAt(0, evolved)

	// The state is at slot 5 — epoch 0 — which is the slot being proposed.
	s.SetSlot(5)
	require.Equal(t, uint64(0), state.Epoch(s), "slot 5 is in epoch 0")

	// What the verifier will compute for this block.
	expected := s.GetRandaoMixes(state.Epoch(s))
	require.Equal(t, evolved, common.Hash(expected), "the verifier reads the state's own epoch")

	// ⚠ What a clock-sourced proposer computed once the wall clock had run on. The ring is indexed
	// modulo its length, so a later epoch is a DIFFERENT entry — and on the first pass round, an
	// entry no block has written yet: genesis.
	const clockEpochAfterDrift = 34
	drifted := s.GetRandaoMixes(clockEpochAfterDrift)
	require.Equal(t, genesisMix, common.Hash(drifted),
		"a drifted clock reads an entry the chain has never written")
	require.NotEqual(t, common.Hash(expected), common.Hash(drifted),
		"clock epoch and state epoch disagree the moment the clock runs ahead — this is the wedge")
}

// Within one epoch the two agree, which is exactly why this survived until a chain with 2s slots ran
// ahead of itself: on a chain that keeps up, there is no difference to see.
func TestRandaoMixForProposal_AgreesWhileTheClockKeepsUp(t *testing.T) {
	cfg := &clparams.MainnetBeaconConfig
	s := state.New(cfg)

	mix := common.HexToHash("0xcb134cbcdeac9cccb46716ee5485bb1e05537f4a15d4e59fe90e00cd287db514")
	s.SetRandaoMixAt(0, mix)
	s.SetSlot(5)

	// Clock epoch == state epoch: the old expression and the correct one return the same thing.
	require.Equal(t, uint64(0), state.Epoch(s))
	require.Equal(t, s.GetRandaoMixes(0), s.GetRandaoMixes(state.Epoch(s)))
}
