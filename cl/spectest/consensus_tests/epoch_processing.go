package consensus_tests

import (
	"github.com/ledgerwatch/erigon/spectest"
	"io/fs"
	"os"
	"testing"

	"github.com/ledgerwatch/erigon/cl/abstract"
	"github.com/ledgerwatch/erigon/cl/phase1/core/state"
	"github.com/ledgerwatch/erigon/cl/transition/impl/eth2/statechange"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type EpochProcessing struct {
	Fn func(s abstract.BeaconState) error
}

func NewEpochProcessing(fn func(s abstract.BeaconState) error) *EpochProcessing {
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

var effectiveBalancesUpdateTest = NewEpochProcessing(func(s abstract.BeaconState) error {
	return statechange.ProcessEffectiveBalanceUpdates(s)
})

var eth1DataResetTest = NewEpochProcessing(func(s abstract.BeaconState) error {
	statechange.ProcessEth1DataReset(s)
	return nil
})

var historicalRootsUpdateTest = NewEpochProcessing(func(s abstract.BeaconState) error {
	return statechange.ProcessHistoricalRootsUpdate(s)
})

var inactivityUpdateTest = NewEpochProcessing(func(s abstract.BeaconState) error {
	return statechange.ProcessInactivityScores(s, state.EligibleValidatorsIndicies(s), statechange.GetUnslashedIndiciesSet(s))
})

var justificationFinalizationTest = NewEpochProcessing(func(s abstract.BeaconState) error {
	return statechange.ProcessJustificationBitsAndFinality(s, nil)
})

var participationFlagUpdatesTest = NewEpochProcessing(func(s abstract.BeaconState) error {
	statechange.ProcessParticipationFlagUpdates(s)
	return nil
})
var participationRecordUpdatesTest = NewEpochProcessing(func(s abstract.BeaconState) error {
	return statechange.ProcessParticipationRecordUpdates(s)
})

var randaoMixesTest = NewEpochProcessing(func(s abstract.BeaconState) error {
	statechange.ProcessRandaoMixesReset(s)
	return nil
})

var registryUpdatesTest = NewEpochProcessing(func(s abstract.BeaconState) error {
	return statechange.ProcessRegistryUpdates(s)
})

var rewardsAndPenaltiesTest = NewEpochProcessing(func(s abstract.BeaconState) error {
	return statechange.ProcessRewardsAndPenalties(s, state.EligibleValidatorsIndicies(s), statechange.GetUnslashedIndiciesSet(s))
})

var slashingsTest = NewEpochProcessing(func(s abstract.BeaconState) error {
	return statechange.ProcessSlashings(s)
})

var slashingsResetTest = NewEpochProcessing(func(s abstract.BeaconState) error {
	statechange.ProcessSlashingsReset(s)
	return nil
})

var recordsResetTest = NewEpochProcessing(func(s abstract.BeaconState) error {
	statechange.ProcessParticipationRecordUpdates(s)
	return nil
})
