package consensustests

import (
	"errors"
	"fmt"
	"os"

	"github.com/ledgerwatch/erigon/cmd/erigon-cl/core/state"
	"github.com/ledgerwatch/erigon/cmd/erigon-cl/core/transition"
)

func getTestEpochProcessing(f func(s *state.BeaconState) error) testFunc {
	return func(context testContext) (err error) {
		defer func() {
			// recover from panic if one occured. Set err to nil otherwise.
			if recovered := recover(); recovered != nil {
				err = fmt.Errorf("panic: %s", recovered)
			}
		}()
		if err != nil {
			return err
		}
		// Read post and
		testState, err := decodeStateFromFile(context, "pre.ssz_snappy")
		if err != nil {
			return err
		}
		var isErrExpected bool
		expectedState, err := decodeStateFromFile(context, "post.ssz_snappy")
		if os.IsNotExist(err) {
			isErrExpected = true
		} else {
			return err
		}

		// Make up state transistor
		if err := f(testState); err != nil {
			if isErrExpected {
				return nil
			}
			return err
		}

		if isErrExpected && err == nil {
			return fmt.Errorf("expected an error got none")
		}
		haveRoot, err := testState.HashSSZ()
		if err != nil {
			return err
		}
		expectedRoot, err := expectedState.HashSSZ()
		if err != nil {
			return err
		}
		if expectedRoot != haveRoot {
			return errors.New("mismatching roots")
		}
		return nil
	}
}

var effectiveBalancesUpdateTest = getTestEpochProcessing(func(s *state.BeaconState) error {
	return transition.ProcessEffectiveBalanceUpdates(s)
})

var eth1DataResetTest = getTestEpochProcessing(func(s *state.BeaconState) error {
	transition.ProcessEth1DataReset(s)
	return nil
})

var historicalRootsUpdateTest = getTestEpochProcessing(func(s *state.BeaconState) error {
	transition.ProcessHistoricalRootsUpdate(s)
	return nil
})

var inactivityUpdateTest = getTestEpochProcessing(func(s *state.BeaconState) error {
	return transition.ProcessInactivityScores(s)
})

var justificationFinalizationTest = getTestEpochProcessing(func(s *state.BeaconState) error {
	return transition.ProcessJustificationBitsAndFinality(s)
})

var participationFlagUpdatesTest = getTestEpochProcessing(func(s *state.BeaconState) error {
	transition.ProcessParticipationFlagUpdates(s)
	return nil
})

var randaoMixesTest = getTestEpochProcessing(func(s *state.BeaconState) error {
	transition.ProcessRandaoMixesReset(s)
	return nil
})

var registryUpdatesTest = getTestEpochProcessing(func(s *state.BeaconState) error {
	return transition.ProcessRegistryUpdates(s)
})

var rewardsAndPenaltiesTest = getTestEpochProcessing(func(s *state.BeaconState) error {
	return transition.ProcessRewardsAndPenalties(s)
})

var slashingsTest = getTestEpochProcessing(func(s *state.BeaconState) error {
	return transition.ProcessSlashings(s)
})

var slashingsResetTest = getTestEpochProcessing(func(s *state.BeaconState) error {
	transition.ProcessSlashingsReset(s)
	return nil
})
