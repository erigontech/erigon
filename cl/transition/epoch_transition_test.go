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

package transition_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/antiquary/tests"
	"github.com/erigontech/erigon/cl/transition"
	"github.com/erigontech/erigon/cl/transition/impl/eth2"
	"github.com/erigontech/erigon/common"
)

func TestTransitionStateFromPrecomputedEpochStateMatchesNormalTransition(t *testing.T) {
	blocks, parentState, _ := tests.GetBellatrixRandom()
	config := parentState.BeaconConfig()

	boundaries := 0
	for _, block := range blocks {
		if block.Block.Slot/config.SlotsPerEpoch > parentState.Slot()/config.SlotsPerEpoch {
			normalState, err := parentState.Copy()
			require.NoError(t, err)
			normalRewards := &eth2.BlockRewardsCollector{}
			require.NoError(t, transition.TransitionState(normalState, block, normalRewards, true))

			advancedState, err := parentState.Copy()
			require.NoError(t, err)
			epochStart := config.RoundSlotToEpoch(block.Block.Slot)
			require.NoError(t, transition.DefaultMachine.ProcessSlots(advancedState, epochStart))
			_, err = advancedState.HashSSZ()
			require.NoError(t, err)
			advancedRewards := &eth2.BlockRewardsCollector{}
			require.NoError(t, transition.TransitionStateFromPrecomputedEpochState(advancedState, block, advancedRewards, true))
			require.Equal(t, normalRewards, advancedRewards)

			normalBytes, err := normalState.EncodeSSZ(nil)
			require.NoError(t, err)
			advancedBytes, err := advancedState.EncodeSSZ(nil)
			require.NoError(t, err)
			require.Equal(t, normalBytes, advancedBytes)

			advancedRoot, err := advancedState.HashSSZ()
			require.NoError(t, err)
			require.Equal(t, common.Hash(block.Block.StateRoot), common.Hash(advancedRoot))
			boundaries++
			parentState = normalState
			continue
		}

		require.NoError(t, transition.TransitionState(parentState, block, nil, false))
	}
	require.NotZero(t, boundaries)
}

func TestTransitionStateRejectsBlockAtStateSlot(t *testing.T) {
	blocks, parentState, _ := tests.GetBellatrixRandom()
	block := blocks[0]
	require.NoError(t, transition.DefaultMachine.ProcessSlots(parentState, block.Block.Slot))

	err := transition.TransitionState(parentState, block, nil, false)
	require.ErrorContains(t, err, "not greater than current slot")
}

func TestTransitionStateFromPrecomputedEpochStateRejectsStateAheadOfBlock(t *testing.T) {
	blocks, state, _ := tests.GetBellatrixRandom()
	block := blocks[0]
	require.NoError(t, transition.DefaultMachine.ProcessSlots(state, block.Block.Slot+1))

	err := transition.TransitionStateFromPrecomputedEpochState(state, block, nil, false)
	require.ErrorContains(t, err, "not greater than current slot")
}
