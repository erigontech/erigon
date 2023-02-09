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

//go:embed test_data/rewards_finality_test_expected.ssz_snappy
var expectedState []byte

//go:embed test_data/rewards_finality_test_state.ssz_snappy
var startingState []byte

func TestProcessRewardsAndPenalties(t *testing.T) {
	// Load test states.
	testState := state.New(&clparams.MainnetBeaconConfig)
	require.NoError(t, utils.DecodeSSZSnappyWithVersion(testState, startingState, int(clparams.BellatrixVersion)))
	expected := state.New(&clparams.MainnetBeaconConfig)
	require.NoError(t, utils.DecodeSSZSnappyWithVersion(expected, expectedState, int(clparams.BellatrixVersion)))
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
