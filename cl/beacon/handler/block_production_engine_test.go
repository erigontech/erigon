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

	"github.com/erigontech/erigon/cl/antiquary/tests"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/consensus"
	"github.com/erigontech/erigon/cl/consensus/rollup"
	forkchoice_mock "github.com/erigontech/erigon/cl/phase1/forkchoice/mock_services"
	"github.com/erigontech/erigon/cl/transition"
	"github.com/erigontech/erigon/cl/transition/machine"
)

type rollupDevForkChoice struct {
	*forkchoice_mock.ForkChoiceStorageMock
}

func (rollupDevForkChoice) ConsensusEngine() consensus.Engine {
	return rollup.NewDevEngine()
}

func TestProducedBlockStateRootFollowsConsensusEngine(t *testing.T) {
	blocks, s, _ := tests.GetCapellaRandom()
	block := blocks[0].Block
	require.NoError(t, transition.DefaultMachine.ProcessSlots(s, block.Slot))

	expectedState, err := s.Copy()
	require.NoError(t, err)
	engine := rollup.NewDevEngine()
	require.NoError(t, machine.ProcessBlock(engine.Machine(false, nil), expectedState, block))
	expectedRoot, err := expectedState.HashSSZ()
	require.NoError(t, err)

	handler := &ApiHandler{forkchoiceStore: rollupDevForkChoice{forkchoice_mock.NewForkChoiceStorageMock(t)}}
	producedState, _, err := handler.processProducedBlock(s, &cltypes.BlindOrExecutionBeaconBlock{
		Slot:          block.Slot,
		ProposerIndex: block.ProposerIndex,
		ParentRoot:    block.ParentRoot,
		BeaconBody:    block.Body,
		Cfg:           s.BeaconConfig(),
	})
	require.NoError(t, err)
	producedRoot, err := producedState.HashSSZ()
	require.NoError(t, err)

	require.Equal(t, expectedRoot, producedRoot)
}
