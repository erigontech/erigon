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

package eth2_test

import (
	_ "embed"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/transition"
	"github.com/erigontech/erigon/cl/transition/machine"
	"github.com/erigontech/erigon/cl/utils"
	"github.com/erigontech/erigon/common"
)

//go:embed statechange/test_data/block_processing/capella_block.ssz_snappy
var capellaBlock []byte

//go:embed statechange/test_data/block_processing/capella_state.ssz_snappy
var capellaState []byte

func loadCapellaFixture(t *testing.T) (*state.CachingBeaconState, *cltypes.SignedBeaconBlock) {
	t.Helper()
	s := state.New(&clparams.MainnetBeaconConfig)
	require.NoError(t, utils.DecodeSSZSnappy(s, capellaState, int(clparams.CapellaVersion)))
	block := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.CapellaVersion)
	require.NoError(t, utils.DecodeSSZSnappy(block, capellaBlock, int(clparams.CapellaVersion)))
	return s, block
}

func TestBlockProcessing(t *testing.T) {
	s, block := loadCapellaFixture(t)
	require.NoError(t, transition.TransitionState(s, block, nil, true)) // All checks already made in transition state
}

func TestProcessBlockRejectsUnexpectedPayloadWithdrawal(t *testing.T) {
	s, block := loadCapellaFixture(t)
	block.Block.Body.ExecutionPayload.Withdrawals.Append(&cltypes.Withdrawal{Amount: 1})

	require.NoError(t, transition.ValidatingMachine.ProcessSlots(s, block.Block.Slot))
	require.ErrorContains(t, machine.ProcessBlock(transition.ValidatingMachine, s, block.Block), "withdrawals root mismatch")
}

func TestProcessBlockRejectsUnexpectedBlindedWithdrawalsRoot(t *testing.T) {
	s, block := loadCapellaFixture(t)
	blinded, err := block.Block.Blinded()
	require.NoError(t, err)
	blinded.Body.ExecutionPayload.WithdrawalsRoot = common.Hash{1}

	require.NoError(t, transition.ValidatingMachine.ProcessSlots(s, block.Block.Slot))
	require.ErrorContains(t, machine.ProcessBlock(transition.ValidatingMachine, s, blinded), "withdrawals root mismatch")
}

func TestProcessSyncAggregateRejectsUnresolvableDomain(t *testing.T) {
	s, block := loadCapellaFixture(t)
	require.NoError(t, transition.ValidatingMachine.ProcessSlots(s, block.Block.Slot))
	s.SetFork(nil)
	require.ErrorContains(t, transition.ValidatingMachine.ProcessSyncAggregate(s, block.Block.Body.SyncAggregate), "nil fork")
}
