package main

import (
	"errors"
	"fmt"
	"os"

	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/utils"
	"github.com/ledgerwatch/erigon/cmd/erigon-cl/core/state"
	"github.com/ledgerwatch/erigon/cmd/erigon-cl/core/transition"
)

func getTestEpochProcessing(f func(s *transition.StateTransistor) error) testFunc {
	return func() (err error) {
		defer func() {
			// recover from panic if one occured. Set err to nil otherwise.
			if recovered := recover(); recovered != nil {
				err = fmt.Errorf("panic: %s", recovered)
			}
		}()
		sszSnappyTest, err := os.ReadFile("pre.ssz_snappy")
		if err != nil {
			return err
		}
		sszSnappyExpected, err := os.ReadFile("post.ssz_snappy")
		isErrExpected := os.IsNotExist(err)
		if isErrExpected {
			err = nil
		}
		if err != nil {
			return err
		}
		// Read post and
		testState := state.New(&clparams.MainnetBeaconConfig)
		if err := utils.DecodeSSZSnappyWithVersion(testState, sszSnappyTest, int(testVersion)); err != nil {
			return err
		}
		var expectedState *state.BeaconState
		if !isErrExpected {
			expectedState = state.New(&clparams.MainnetBeaconConfig)
			if err := utils.DecodeSSZSnappyWithVersion(expectedState, sszSnappyExpected, int(testVersion)); err != nil {
				return err
			}
		}

		// Make up state transistor
		s := transition.New(testState, &clparams.MainnetBeaconConfig, nil, false)
		if err := f(s); err != nil {
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

var effectiveBalancesUpdateTest = getTestEpochProcessing(func(s *transition.StateTransistor) error {
	return s.ProcessEffectiveBalanceUpdates()
})

var eth1DataResetTest = getTestEpochProcessing(func(s *transition.StateTransistor) error {
	s.ProcessEth1DataReset()
	return nil
})

var historicalRootsUpdateTest = getTestEpochProcessing(func(s *transition.StateTransistor) error {
	s.ProcessHistoricalRootsUpdate()
	return nil
})

var inactivityUpdateTest = getTestEpochProcessing(func(s *transition.StateTransistor) error {
	return s.ProcessInactivityScores()
})

var justificationFinalizationTest = getTestEpochProcessing(func(s *transition.StateTransistor) error {
	return s.ProcessJustificationBitsAndFinality()
})

var participationFlagUpdatesTest = getTestEpochProcessing(func(s *transition.StateTransistor) error {
	s.ProcessParticipationFlagUpdates()
	return nil
})

var randaoMixesTest = getTestEpochProcessing(func(s *transition.StateTransistor) error {
	s.ProcessRandaoMixesReset()
	return nil
})

var registryUpdatesTest = getTestEpochProcessing(func(s *transition.StateTransistor) error {
	return s.ProcessRegistryUpdates()
})

var rewardsAndPenaltiesTest = getTestEpochProcessing(func(s *transition.StateTransistor) error {
	return s.ProcessRewardsAndPenalties()
})

var slashingsTest = getTestEpochProcessing(func(s *transition.StateTransistor) error {
	return s.ProcessSlashings()
})

var slashingsResetTest = getTestEpochProcessing(func(s *transition.StateTransistor) error {
	s.ProcessSlashingsReset()
	return nil
})
