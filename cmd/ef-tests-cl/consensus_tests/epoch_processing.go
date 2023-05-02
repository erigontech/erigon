package consensus_tests

import (
	"io/fs"
	"os"
	"testing"

	"github.com/ledgerwatch/erigon/cmd/ef-tests-cl/spectest"
	"github.com/ledgerwatch/erigon/cmd/erigon-cl/core/state"
	"github.com/ledgerwatch/erigon/cmd/erigon-cl/core/transition"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type EpochProcessing struct {
	Fn func(s *state.BeaconState) error
}

func NewEpochProcessing(fn func(s *state.BeaconState) error) *EpochProcessing {
	return &EpochProcessing{
		Fn: fn,
	}
}

func (b *EpochProcessing) Run(t *testing.T, root fs.FS, c spectest.TestCase) (err error) {
	testState, err := spectest.ReadBeaconState(root, c.Version(), spectest.PreSsz)
	require.NoError(t, err)

	var expectedError bool
	expectedState, err := spectest.ReadBeaconState(root, c.Version(), spectest.PostSsz)
	if os.IsNotExist(err) {
		expectedError = true
		err = nil
	}
	require.NoError(t, err)
	if err := b.Fn(testState); err != nil {
		if expectedError {
			return nil
		}
		return err
	}

	haveRoot, err := testState.HashSSZ()
	require.NoError(t, err)

	expectedRoot, err := expectedState.HashSSZ()
	require.NoError(t, err)

	assert.EqualValues(t, expectedRoot, haveRoot)
	return nil
}

var effectiveBalancesUpdateTest = NewEpochProcessing(func(s *state.BeaconState) error {
	return transition.ProcessEffectiveBalanceUpdates(s)
})

var eth1DataResetTest = NewEpochProcessing(func(s *state.BeaconState) error {
	transition.ProcessEth1DataReset(s)
	return nil
})

var historicalRootsUpdateTest = NewEpochProcessing(func(s *state.BeaconState) error {
	transition.ProcessHistoricalRootsUpdate(s)
	return nil
})

var inactivityUpdateTest = NewEpochProcessing(func(s *state.BeaconState) error {
	return transition.ProcessInactivityScores(s)
})

var justificationFinalizationTest = NewEpochProcessing(func(s *state.BeaconState) error {
	return transition.ProcessJustificationBitsAndFinality(s)
})

var participationFlagUpdatesTest = NewEpochProcessing(func(s *state.BeaconState) error {
	transition.ProcessParticipationFlagUpdates(s)
	return nil
})
var participationRecordUpdatesTest = NewEpochProcessing(func(s *state.BeaconState) error {
	return transition.ProcessParticipationRecordUpdates(s)
})

var randaoMixesTest = NewEpochProcessing(func(s *state.BeaconState) error {
	transition.ProcessRandaoMixesReset(s)
	return nil
})

var registryUpdatesTest = NewEpochProcessing(func(s *state.BeaconState) error {
	return transition.ProcessRegistryUpdates(s)
})

var rewardsAndPenaltiesTest = NewEpochProcessing(func(s *state.BeaconState) error {
	return transition.ProcessRewardsAndPenalties(s)
})

var slashingsTest = NewEpochProcessing(func(s *state.BeaconState) error {
	return transition.ProcessSlashings(s)
})

var slashingsResetTest = NewEpochProcessing(func(s *state.BeaconState) error {
	transition.ProcessSlashingsReset(s)
	return nil
})

var recordsResetTest = NewEpochProcessing(func(s *state.BeaconState) error {
	transition.ProcessParticipationRecordUpdates(s)
	return nil
})
