package state_test

import (
	"testing"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cmd/erigon-cl/core/state"
	"github.com/stretchr/testify/require"
)

func TestActiveValidatorIndices(t *testing.T) {
	epoch := uint64(2)
	testState := state.GetEmptyBeaconState()
	// Not Active validator
	testState.AddValidator(&cltypes.Validator{
		ActivationEpoch:  3,
		ExitEpoch:        9,
		EffectiveBalance: 2e9,
	})
	// Active Validator
	testState.AddValidator(&cltypes.Validator{
		ActivationEpoch:  1,
		ExitEpoch:        9,
		EffectiveBalance: 2e9,
	})
	testState.SetSlot(epoch * 32) // Epoch
	testFlags := cltypes.ParticipationFlagsListFromBytes([]byte{1, 1})
	testState.SetCurrentEpochParticipation(testFlags)
	// Only validator at index 1 (second validator) is active.
	require.Equal(t, testState.GetActiveValidatorsIndices(epoch), []uint64{1})
	set, err := testState.GetUnslashedParticipatingIndices(0x00, epoch)
	require.NoError(t, err)
	require.Equal(t, set, []uint64{1})
	// Check if balances are retrieved correctly
	totalBalance, err := testState.GetTotalActiveBalance()
	require.NoError(t, err)
	require.Equal(t, totalBalance, uint64(2e9))
}

func TestGetBlockRoot(t *testing.T) {
	epoch := uint64(2)
	testState := state.GetEmptyBeaconState()
	root := common.HexToHash("ff")
	testState.SetSlot(100)
	testState.SetBlockRootAt(int(epoch*32), root)
	retrieved, err := testState.GetBlockRoot(epoch)
	require.NoError(t, err)
	require.Equal(t, retrieved, root)
}
