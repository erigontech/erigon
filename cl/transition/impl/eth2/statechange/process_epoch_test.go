package statechange

import (
	_ "embed"
	"github.com/ledgerwatch/erigon/cl/abstract"
	"testing"

	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/phase1/core/state"
	"github.com/ledgerwatch/erigon/cl/utils"
	"github.com/stretchr/testify/require"
)

type processFunc func(s abstract.BeaconState) error

func runEpochTransitionConsensusTest(t *testing.T, sszSnappyTest, sszSnappyExpected []byte, f processFunc) {
	testState := state.New(&clparams.MainnetBeaconConfig)
	require.NoError(t, utils.DecodeSSZSnappy(testState, sszSnappyTest, int(clparams.BellatrixVersion)))
	expectedState := state.New(&clparams.MainnetBeaconConfig)
	require.NoError(t, utils.DecodeSSZSnappy(expectedState, sszSnappyExpected, int(clparams.BellatrixVersion)))
	// Make up state transistor
	require.NoError(t, f(testState))
	haveRoot, err := testState.HashSSZ()
	require.NoError(t, err)
	expectedRoot, err := expectedState.HashSSZ()
	require.NoError(t, err)
	// Lastly compare
	require.Equal(t, expectedRoot, haveRoot)
}

//go:embed test_data/epoch_processing/rewards_penalty_test_expected.ssz_snappy
var expectedRewardsPenaltyState []byte

//go:embed test_data/epoch_processing/rewards_penalty_test_state.ssz_snappy
var startingRewardsPenaltyState []byte

//go:embed test_data/epoch_processing/registry_updates_test_expected.ssz_snappy
var expectedRegistryUpdatesState []byte

//go:embed test_data/epoch_processing/registry_updates_test_state.ssz_snappy
var startingRegistryUpdatesState []byte

//go:embed test_data/epoch_processing/effective_balances_expected.ssz_snappy
var expectedEffectiveBalancesState []byte

//go:embed test_data/epoch_processing/effective_balances_test_state.ssz_snappy
var startingEffectiveBalancesState []byte

//go:embed test_data/epoch_processing/historical_roots_expected_test.ssz_snappy
var expectedHistoricalRootsState []byte

//go:embed test_data/epoch_processing/historical_roots_state_test.ssz_snappy
var startingHistoricalRootsState []byte

//go:embed test_data/epoch_processing/participation_flag_updates_expected_test.ssz_snappy
var expectedParticipationFlagState []byte

//go:embed test_data/epoch_processing/participation_flag_updates_state_test.ssz_snappy
var startingParticipationFlagState []byte

//go:embed test_data/epoch_processing/slashings_expected_test.ssz_snappy
var expectedSlashingsState []byte

//go:embed test_data/epoch_processing/slashings_state_test.ssz_snappy
var startingSlashingsState []byte

//go:embed test_data/epoch_processing/justification_and_finality_expected_test.ssz_snappy
var expectedJustificationAndFinalityState []byte

//go:embed test_data/epoch_processing/justification_and_finality_state_test.ssz_snappy
var startingJustificationAndFinalityState []byte

//go:embed test_data/epoch_processing/eth1_data_reset_expected_test.ssz_snappy
var expectedEth1DataResetState []byte

//go:embed test_data/epoch_processing/eth1_data_reset_state_test.ssz_snappy
var startingEth1DataResetState []byte

//go:embed test_data/epoch_processing/randao_mixes_reset_expected_test.ssz_snappy
var expectedRandaoMixesResetState []byte

//go:embed test_data/epoch_processing/randao_mixes_reset_state_test.ssz_snappy
var startingRandaoMixesResetState []byte

//go:embed test_data/epoch_processing/slashings_reset_expected_test.ssz_snappy
var expectedSlashingsResetState []byte

//go:embed test_data/epoch_processing/slashings_reset_state_test.ssz_snappy
var startingSlashingsResetState []byte

func TestProcessRewardsAndPenalties(t *testing.T) {
	runEpochTransitionConsensusTest(t, startingRewardsPenaltyState, expectedRewardsPenaltyState, func(s abstract.BeaconState) error {
		return ProcessRewardsAndPenalties(s)
	})
}

func TestProcessRegistryUpdates(t *testing.T) {
	runEpochTransitionConsensusTest(t, startingRegistryUpdatesState, expectedRegistryUpdatesState, func(s abstract.BeaconState) error {
		return ProcessRegistryUpdates(s)
	})
}

func TestProcessEffectiveBalances(t *testing.T) {
	runEpochTransitionConsensusTest(t, startingEffectiveBalancesState, expectedEffectiveBalancesState, func(s abstract.BeaconState) error {
		return ProcessEffectiveBalanceUpdates(s)
	})
}

func TestProcessHistoricalRoots(t *testing.T) {
	runEpochTransitionConsensusTest(t, startingHistoricalRootsState, expectedHistoricalRootsState, func(s abstract.BeaconState) error {
		return ProcessHistoricalRootsUpdate(s)
	})
}

func TestProcessParticipationFlagUpdates(t *testing.T) {
	runEpochTransitionConsensusTest(t, startingParticipationFlagState, expectedParticipationFlagState, func(s abstract.BeaconState) error {
		ProcessParticipationFlagUpdates(s)
		return nil
	})
}

func TestProcessSlashings(t *testing.T) {
	runEpochTransitionConsensusTest(t, startingSlashingsState, expectedSlashingsState, func(s abstract.BeaconState) error {
		return ProcessSlashings(s)
	})
}

func TestProcessJustificationAndFinality(t *testing.T) {
	runEpochTransitionConsensusTest(t, startingJustificationAndFinalityState, expectedJustificationAndFinalityState, func(s abstract.BeaconState) error {
		return ProcessJustificationBitsAndFinality(s)
	})
}

func TestEth1DataReset(t *testing.T) {
	runEpochTransitionConsensusTest(t, startingEth1DataResetState, expectedEth1DataResetState, func(s abstract.BeaconState) error {
		ProcessEth1DataReset(s)
		return nil
	})
}

func TestRandaoMixesReset(t *testing.T) {
	runEpochTransitionConsensusTest(t, startingRandaoMixesResetState, expectedRandaoMixesResetState, func(s abstract.BeaconState) error {
		ProcessRandaoMixesReset(s)
		return nil
	})
}

func TestSlashingsReset(t *testing.T) {
	runEpochTransitionConsensusTest(t, startingSlashingsResetState, expectedSlashingsResetState, func(s abstract.BeaconState) error {
		ProcessSlashingsReset(s)
		return nil
	})
}

//go:embed test_data/epoch_processing/inactivity_scores_expected_test.ssz_snappy
var expectedInactivityScoresState []byte

//go:embed test_data/epoch_processing/inactivity_scores_state_test.ssz_snappy
var startingInactivityScoresState []byte

func TestInactivityScores(t *testing.T) {
	runEpochTransitionConsensusTest(t, startingInactivityScoresState, expectedInactivityScoresState, func(s abstract.BeaconState) error {
		return ProcessInactivityScores(s)
	})
}
