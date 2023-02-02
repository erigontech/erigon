package transition_test

import (
	"testing"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cmd/erigon-cl/core/state"
	"github.com/ledgerwatch/erigon/cmd/erigon-cl/core/transition"
	"github.com/stretchr/testify/require"
)

func getJustificationAndFinalizationState() *state.BeaconState {
	cfg := clparams.MainnetBeaconConfig
	epoch := cfg.FarFutureEpoch
	bal := cfg.MaxEffectiveBalance
	state := state.GetEmptyBeaconState()
	blockRoots := make([][32]byte, cfg.SlotsPerEpoch*2+1)
	for i := 0; i < len(blockRoots); i++ {
		blockRoots[i][0] = byte(i)
		state.SetBlockRootAt(i, blockRoots[i])
	}
	state.SetSlot(cfg.SlotsPerEpoch*2 + 1)
	bits := cltypes.JustificationBits{}
	bits.FromByte(0x3)
	state.SetJustificationBits(bits)
	state.SetValidators([]*cltypes.Validator{
		{ExitEpoch: epoch}, {ExitEpoch: epoch}, {ExitEpoch: epoch}, {ExitEpoch: epoch},
	})
	state.SetBalances([]uint64{bal, bal, bal, bal})
	state.SetCurrentEpochParticipation(cltypes.ParticipationFlagsList{0b01, 0b01, 0b01, 0b01})
	state.SetPreviousEpochParticipation(cltypes.ParticipationFlagsList{0b01, 0b01, 0b01, 0b01})

	return state
}

func TestProcessJustificationAndFinalizationJustifyCurrentEpoch(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	testState := getJustificationAndFinalizationState()
	transitioner := transition.New(testState, &cfg, nil, false)
	transitioner.ProcessJustificationBitsAndFinality()
	rt := libcommon.Hash{byte(64)}
	require.Equal(t, rt, testState.CurrentJustifiedCheckpoint().Root, "Unexpected current justified root")
	require.Equal(t, uint64(2), testState.CurrentJustifiedCheckpoint().Epoch, "Unexpected justified epoch")
	require.Equal(t, uint64(0), testState.PreviousJustifiedCheckpoint().Epoch, "Unexpected previous justified epoch")
	require.Equal(t, libcommon.Hash{}, testState.FinalizedCheckpoint().Root)
	require.Equal(t, uint64(0), testState.FinalizedCheckpoint().Epoch, "Unexpected finalized epoch")
}
