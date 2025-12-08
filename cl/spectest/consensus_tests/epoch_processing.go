// Copyright 2024 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package consensus_tests

import (
	"io/fs"
	"os"
	"testing"

	"github.com/erigontech/erigon/spectest"

	"github.com/erigontech/erigon/cl/abstract"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/transition/impl/eth2/statechange"

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

	assert.Equal(t, expectedRoot, haveRoot)
	return nil
}

var effectiveBalancesUpdateTest = NewEpochProcessing(statechange.ProcessEffectiveBalanceUpdates)

var eth1DataResetTest = NewEpochProcessing(func(s abstract.BeaconState) error {
	statechange.ProcessEth1DataReset(s)
	return nil
})

var historicalRootsUpdateTest = NewEpochProcessing(statechange.ProcessHistoricalRootsUpdate)

var inactivityUpdateTest = NewEpochProcessing(func(s abstract.BeaconState) error {
	var unslashedIndiciesSet [][]bool
	if s.Version() >= clparams.AltairVersion {
		unslashedIndiciesSet = statechange.GetUnslashedIndiciesSet(s.BeaconConfig(), state.PreviousEpoch(s), s.ValidatorSet(), s.PreviousEpochParticipation())
	}
	return statechange.ProcessInactivityScores(s, state.EligibleValidatorsIndicies(s), unslashedIndiciesSet)
})

var justificationFinalizationTest = NewEpochProcessing(func(s abstract.BeaconState) error {
	return statechange.ProcessJustificationBitsAndFinality(s, nil)
})

var participationFlagUpdatesTest = NewEpochProcessing(func(s abstract.BeaconState) error {
	statechange.ProcessParticipationFlagUpdates(s)
	return nil
})
var participationRecordUpdatesTest = NewEpochProcessing(statechange.ProcessParticipationRecordUpdates)

var randaoMixesTest = NewEpochProcessing(func(s abstract.BeaconState) error {
	statechange.ProcessRandaoMixesReset(s)
	return nil
})

var registryUpdatesTest = NewEpochProcessing(statechange.ProcessRegistryUpdates)

var rewardsAndPenaltiesTest = NewEpochProcessing(func(s abstract.BeaconState) error {
	var unslashedIndiciesSet [][]bool
	if s.Version() >= clparams.AltairVersion {
		unslashedIndiciesSet = statechange.GetUnslashedIndiciesSet(s.BeaconConfig(), state.PreviousEpoch(s), s.ValidatorSet(), s.PreviousEpochParticipation())
	}
	return statechange.ProcessRewardsAndPenalties(s, state.EligibleValidatorsIndicies(s), unslashedIndiciesSet)
})

var slashingsTest = NewEpochProcessing(statechange.ProcessSlashings)

var slashingsResetTest = NewEpochProcessing(func(s abstract.BeaconState) error {
	statechange.ProcessSlashingsReset(s)
	return nil
})

var recordsResetTest = NewEpochProcessing(func(s abstract.BeaconState) error {
	statechange.ProcessParticipationRecordUpdates(s)
	return nil
})

var pendingDepositTest = NewEpochProcessing(func(s abstract.BeaconState) error {
	statechange.ProcessPendingDeposits(s)
	return nil
})

var PendingConsolidationTest = NewEpochProcessing(func(s abstract.BeaconState) error {
	statechange.ProcessPendingConsolidations(s)
	return nil
})

var ProposerLookaheadTest = NewEpochProcessing(func(s abstract.BeaconState) error {
	statechange.ProcessProposerLookahead(s)
	return nil
})
