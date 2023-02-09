package transition_test

import (
	_ "embed"
	"testing"

	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/utils"
	"github.com/ledgerwatch/erigon/cmd/erigon-cl/core/state"
	"github.com/ledgerwatch/erigon/cmd/erigon-cl/core/transition"
	"github.com/stretchr/testify/require"
)

//go:embed test_data/rewards_penalty_test_expected.ssz_snappy
var expectedRewardsPenaltyState []byte

//go:embed test_data/rewards_penalty_test_state.ssz_snappy
var startingRewardsPenaltyState []byte

//go:embed test_data/registry_updates_test_expected.ssz_snappy
var expectedRegistryUpdatesState []byte

//go:embed test_data/registry_updates_test_state.ssz_snappy
var startingRegistryUpdatesState []byte

func TestProcessRewardsAndPenalties(t *testing.T) {
	// Load test states.
	testState := state.New(&clparams.MainnetBeaconConfig)
	require.NoError(t, utils.DecodeSSZSnappyWithVersion(testState, startingRewardsPenaltyState, int(clparams.BellatrixVersion)))
	expected := state.New(&clparams.MainnetBeaconConfig)
	require.NoError(t, utils.DecodeSSZSnappyWithVersion(expected, expectedRewardsPenaltyState, int(clparams.BellatrixVersion)))
	// Make up state transistor
	s := transition.New(testState, &clparams.MainnetBeaconConfig, nil, false)
	// Do processing
	require.NoError(t, s.ProcessRewardsAndPenalties())
	// Now compare if the two states are the same by taking their root and comparing.
	haveRoot, err := testState.HashSSZ()
	require.NoError(t, err)
	expectedRoot, err := testState.HashSSZ()
	require.NoError(t, err)
	// Lastly compare
	require.Equal(t, expectedRoot, haveRoot)
}

func TestProcessRegistryUpdates(t *testing.T) {
	// Load test states.
	testState := state.New(&clparams.MainnetBeaconConfig)
	require.NoError(t, utils.DecodeSSZSnappyWithVersion(testState, startingRegistryUpdatesState, int(clparams.BellatrixVersion)))
	expected := state.New(&clparams.MainnetBeaconConfig)
	require.NoError(t, utils.DecodeSSZSnappyWithVersion(expected, expectedRegistryUpdatesState, int(clparams.BellatrixVersion)))
	// Make up state transistor
	s := transition.New(testState, &clparams.MainnetBeaconConfig, nil, false)
	// Do processing
	require.NoError(t, s.ProcessRegistryUpdates())
	// Now compare if the two states are the same by taking their root and comparing.
	haveRoot, err := testState.HashSSZ()
	require.NoError(t, err)
	expectedRoot, err := testState.HashSSZ()
	require.NoError(t, err)
	// Lastly compare
	require.Equal(t, expectedRoot, haveRoot)
}
