package consensus_tests

import (
	"io/fs"
	"os"
	"testing"

	"github.com/ledgerwatch/erigon/cl/phase1/core/state"
	transition2 "github.com/ledgerwatch/erigon/cl/phase1/core/transition"

	"github.com/ledgerwatch/erigon/spectest"
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
	return transition2.ProcessEffectiveBalanceUpdates(s)
})

var eth1DataResetTest = NewEpochProcessing(func(s *state.BeaconState) error {
	transition2.ProcessEth1DataReset(s)
	return nil
})

var historicalRootsUpdateTest = NewEpochProcessing(func(s *state.BeaconState) error {
	transition2.ProcessHistoricalRootsUpdate(s)
	return nil
})

var inactivityUpdateTest = NewEpochProcessing(func(s *state.BeaconState) error {
	return transition2.ProcessInactivityScores(s)
})

var justificationFinalizationTest = NewEpochProcessing(func(s *state.BeaconState) error {
	return transition2.ProcessJustificationBitsAndFinality(s)
})

var participationFlagUpdatesTest = NewEpochProcessing(func(s *state.BeaconState) error {
	transition2.ProcessParticipationFlagUpdates(s)
	return nil
})
var participationRecordUpdatesTest = NewEpochProcessing(func(s *state.BeaconState) error {
	return transition2.ProcessParticipationRecordUpdates(s)
})

var randaoMixesTest = NewEpochProcessing(func(s *state.BeaconState) error {
	transition2.ProcessRandaoMixesReset(s)
	return nil
})

var registryUpdatesTest = NewEpochProcessing(func(s *state.BeaconState) error {
	return transition2.ProcessRegistryUpdates(s)
})

var rewardsAndPenaltiesTest = NewEpochProcessing(func(s *state.BeaconState) error {
	return transition2.ProcessRewardsAndPenalties(s)
})

var slashingsTest = NewEpochProcessing(func(s *state.BeaconState) error {
	return transition2.ProcessSlashings(s)
})

var slashingsResetTest = NewEpochProcessing(func(s *state.BeaconState) error {
	transition2.ProcessSlashingsReset(s)
	return nil
})

var recordsResetTest = NewEpochProcessing(func(s *state.BeaconState) error {
	transition2.ProcessParticipationRecordUpdates(s)
	return nil
})
