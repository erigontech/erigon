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

package handler

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/transition"
	"github.com/erigontech/erigon/common/log/v3"
)

func TestGetDutiesProposerHistoricalEpochFirstSlotMissed(t *testing.T) {
	_, blocks, _, _, postState, handler, _, manager, fcu, _ := setupTestingHandler(t, clparams.Phase0Version, log.Root(), true)
	cfg := handler.beaconChainCfg

	epoch := state.Epoch(postState)
	firstSlot := epoch * cfg.SlotsPerEpoch
	for _, block := range blocks {
		require.NotEqual(t, firstSlot, block.Block.Slot)
	}
	require.Greater(t, postState.Slot(), firstSlot)

	headRoot, err := blocks[len(blocks)-1].Block.HashSSZ()
	require.NoError(t, err)
	fcu.HeadVal = headRoot
	fcu.HeadSlotVal = postState.Slot()
	fcu.FinalizedCheckpointVal = solid.Checkpoint{Epoch: epoch, Root: headRoot}
	fcu.FinalizedSlotVal = (epoch+1)*cfg.SlotsPerEpoch - 1
	fcu.LowestAvailableSlotVal = &firstSlot

	expected, err := postState.GetBeaconProposerIndices(epoch)
	require.NoError(t, err)
	head, err := postState.Copy()
	require.NoError(t, err)
	require.NoError(t, transition.DefaultMachine.ProcessSlots(head, (epoch+1)*cfg.SlotsPerEpoch))
	require.NoError(t, manager.OnHeadStateWithBlockRoot(head, headRoot))

	response := getProposerDutiesForEpoch(t, handler, epoch)
	require.Len(t, response.Data, int(cfg.SlotsPerEpoch))
	for i, duty := range response.Data {
		require.Equal(t, firstSlot+uint64(i), duty.Slot)
		require.Equal(t, expected[i], duty.ValidatorIndex, "slot %d", duty.Slot)
	}
}
