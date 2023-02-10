package transition_test

import (
	"testing"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cmd/erigon-cl/core/state"
	"github.com/ledgerwatch/erigon/cmd/erigon-cl/core/transition"
	"github.com/stretchr/testify/require"
)

func TestProcessEth1DataReset(t *testing.T) {
	testState := state.GetEmptyBeaconState()
	testState.SetSlot(63 * clparams.MainnetBeaconConfig.SlotsPerEpoch)
	testState.AddEth1DataVote(&cltypes.Eth1Data{})
	transitioner := transition.New(testState, &clparams.MainnetBeaconConfig, nil, false)
	transitioner.ProcessEth1DataReset()
	require.Zero(t, len(testState.Eth1DataVotes()))
}

func TestProcessSlashingsReset(t *testing.T) {
	testState := state.GetEmptyBeaconState()
	testState.SetSlashingSegmentAt(1, 9)
	testState.SetSlot(0) // Epoch 0
	transitioner := transition.New(testState, &clparams.MainnetBeaconConfig, nil, false)
	transitioner.ProcessSlashingsReset()
	require.Zero(t, testState.SlashingSegmentAt(1))
}

func TestProcessRandaoMixesReset(t *testing.T) {
	testState := state.GetEmptyBeaconState()
	testState.SetRandaoMixAt(1, common.HexToHash("a"))
	testState.SetSlot(0) // Epoch 0
	transitioner := transition.New(testState, &clparams.MainnetBeaconConfig, nil, false)
	transitioner.ProcessRandaoMixesReset()
	require.Equal(t, testState.GetRandaoMixes(1), [32]byte{})
}
