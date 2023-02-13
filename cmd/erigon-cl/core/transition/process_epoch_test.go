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

type processFunc func(s *transition.StateTransistor) error

func runEpochTransitionConsensusTest(t *testing.T, sszSnappyTest, sszSnappyExpected []byte, f processFunc) {
	testState := state.New(&clparams.MainnetBeaconConfig)
	require.NoError(t, utils.DecodeSSZSnappyWithVersion(testState, sszSnappyTest, int(clparams.BellatrixVersion)))
	expectedState := state.New(&clparams.MainnetBeaconConfig)
	require.NoError(t, utils.DecodeSSZSnappyWithVersion(expectedState, sszSnappyExpected, int(clparams.BellatrixVersion)))
	// Make up state transistor
	s := transition.New(testState, &clparams.MainnetBeaconConfig, nil, true)
	require.NoError(t, f(s))
	haveRoot, err := testState.HashSSZ()
	require.NoError(t, err)
	expectedRoot, err := expectedState.HashSSZ()
	require.NoError(t, err)
	// Lastly compare
	require.Equal(t, expectedRoot, haveRoot)
}

//go:embed test_data/rewards_penalty_test_expected.ssz_snappy
var expectedRewardsPenaltyState []byte

//go:embed test_data/rewards_penalty_test_state.ssz_snappy
var startingRewardsPenaltyState []byte

//go:embed test_data/registry_updates_test_expected.ssz_snappy
var expectedRegistryUpdatesState []byte

//go:embed test_data/registry_updates_test_state.ssz_snappy
var startingRegistryUpdatesState []byte

//go:embed test_data/effective_balances_expected.ssz_snappy
var expectedEffectiveBalancesState []byte

//go:embed test_data/effective_balances_test_state.ssz_snappy
var startingEffectiveBalancesState []byte

//go:embed test_data/historical_roots_expected_test.ssz_snappy
var expectedHistoricalRootsState []byte

//go:embed test_data/historical_roots_state_test.ssz_snappy
var startingHistoricalRootsState []byte

//go:embed test_data/participation_flag_updates_expected_test.ssz_snappy
var expectedParticipationFlagState []byte

//go:embed test_data/participation_flag_updates_state_test.ssz_snappy
var startingParticipationFlagState []byte

//go:embed test_data/slashings_expected_test.ssz_snappy
var expectedSlashingsState []byte

//go:embed test_data/slashings_state_test.ssz_snappy
var startingSlashingsState []byte

//go:embed test_data/justification_and_finality_expected_test.ssz_snappy
var expectedJustificationAndFinalityState []byte

//go:embed test_data/justification_and_finality_state_test.ssz_snappy
var startingJustificationAndFinalityState []byte

//go:embed test_data/eth1_data_reset_expected_test.ssz_snappy
var expectedEth1DataResetState []byte

//go:embed test_data/eth1_data_reset_state_test.ssz_snappy
var startingEth1DataResetState []byte

//go:embed test_data/randao_mixes_reset_expected_test.ssz_snappy
var expectedRandaoMixesResetState []byte

//go:embed test_data/randao_mixes_reset_state_test.ssz_snappy
var startingRandaoMixesResetState []byte

//go:embed test_data/slashings_reset_expected_test.ssz_snappy
var expectedSlashingsResetState []byte

//go:embed test_data/slashings_reset_state_test.ssz_snappy
var startingSlashingsResetState []byte

func TestProcessRewardsAndPenalties(t *testing.T) {
	runEpochTransitionConsensusTest(t, startingRewardsPenaltyState, expectedRewardsPenaltyState, func(s *transition.StateTransistor) error {
		return s.ProcessRewardsAndPenalties()
	})
}

func TestProcessRegistryUpdates(t *testing.T) {
	runEpochTransitionConsensusTest(t, startingRegistryUpdatesState, expectedRegistryUpdatesState, func(s *transition.StateTransistor) error {
		return s.ProcessRegistryUpdates()
	})
}

func TestProcessEffectiveBalances(t *testing.T) {
	runEpochTransitionConsensusTest(t, startingEffectiveBalancesState, expectedEffectiveBalancesState, func(s *transition.StateTransistor) error {
		return s.ProcessEffectiveBalanceUpdates()
	})
}

func TestProcessHistoricalRoots(t *testing.T) {
	runEpochTransitionConsensusTest(t, startingHistoricalRootsState, expectedHistoricalRootsState, func(s *transition.StateTransistor) error {
		return s.ProcessHistoricalRootsUpdate()
	})
}

func TestProcessParticipationFlagUpdates(t *testing.T) {
	runEpochTransitionConsensusTest(t, startingParticipationFlagState, expectedParticipationFlagState, func(s *transition.StateTransistor) error {
		s.ProcessParticipationFlagUpdates()
		return nil
	})
}

func TestProcessSlashings(t *testing.T) {
	runEpochTransitionConsensusTest(t, startingSlashingsState, expectedSlashingsState, func(s *transition.StateTransistor) error {
		return s.ProcessSlashings()
	})
}

func TestProcessJustificationAndFinality(t *testing.T) {
	runEpochTransitionConsensusTest(t, startingJustificationAndFinalityState, expectedJustificationAndFinalityState, func(s *transition.StateTransistor) error {
		return s.ProcessJustificationBitsAndFinality()
	})
}

func TestEth1DataReset(t *testing.T) {
	runEpochTransitionConsensusTest(t, startingEth1DataResetState, expectedEth1DataResetState, func(s *transition.StateTransistor) error {
		s.ProcessEth1DataReset()
		return nil
	})
}

func TestRandaoMixesReset(t *testing.T) {
	runEpochTransitionConsensusTest(t, startingRandaoMixesResetState, expectedRandaoMixesResetState, func(s *transition.StateTransistor) error {
		s.ProcessRandaoMixesReset()
		return nil
	})
}

func TestSlashingsReset(t *testing.T) {
	runEpochTransitionConsensusTest(t, startingSlashingsResetState, expectedSlashingsResetState, func(s *transition.StateTransistor) error {
		s.ProcessSlashingsReset()
		return nil
	})
}

//go:embed test_data/inactivity_scores_expected_test.ssz_snappy
var expectedInactivityScoresState []byte

//go:embed test_data/inactivity_scores_state_test.ssz_snappy
var startingInactivityScoresState []byte

func TestInactivityScores(t *testing.T) {
	runEpochTransitionConsensusTest(t, startingInactivityScoresState, expectedInactivityScoresState, func(s *transition.StateTransistor) error {
		return s.ProcessInactivityScores()
	})
}
