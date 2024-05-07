package statechange

import (
	"github.com/ledgerwatch/erigon/cl/abstract"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes/solid"
	"github.com/ledgerwatch/erigon/cl/phase1/core/state"
)

func GetUnslashedIndiciesSet(cfg *clparams.BeaconChainConfig, previousEpoch uint64, validatorSet *solid.ValidatorSet, previousEpochPartecipation *solid.BitList) [][]bool {
	weights := cfg.ParticipationWeights()
	flagsUnslashedIndiciesSet := make([][]bool, len(weights))
	for i := range weights {
		flagsUnslashedIndiciesSet[i] = make([]bool, validatorSet.Length())
	}

	validatorSet.Range(func(validatorIndex int, validator solid.Validator, total int) bool {
		for i := range weights {
			flagsUnslashedIndiciesSet[i][validatorIndex] = state.IsUnslashedParticipatingIndex(validatorSet, previousEpochPartecipation, previousEpoch, uint64(validatorIndex), i)
		}
		return true
	})
	return flagsUnslashedIndiciesSet
}

// ProcessEpoch process epoch transition.
func ProcessEpoch(s abstract.BeaconState) error {
	eligibleValidators := state.EligibleValidatorsIndicies(s)
	// start := time.Now()
	var unslashedIndiciesSet [][]bool
	if s.Version() >= clparams.AltairVersion {
		unslashedIndiciesSet = GetUnslashedIndiciesSet(s.BeaconConfig(), state.PreviousEpoch(s), s.ValidatorSet(), s.PreviousEpochParticipation())
	}
	if err := ProcessJustificationBitsAndFinality(s, unslashedIndiciesSet); err != nil {
		return err
	}
	// fmt.Println("ProcessJustificationBitsAndFinality", time.Since(start))
	// start = time.Now()

	if s.Version() >= clparams.AltairVersion {
		if err := ProcessInactivityScores(s, eligibleValidators, unslashedIndiciesSet); err != nil {
			return err
		}
	}
	// fmt.Println("ProcessInactivityScores", time.Since(start))
	// start = time.Now()
	if err := ProcessRewardsAndPenalties(s, eligibleValidators, unslashedIndiciesSet); err != nil {
		return err
	}
	// fmt.Println("ProcessRewardsAndPenalties", time.Since(start))
	// start = time.Now()
	if err := ProcessRegistryUpdates(s); err != nil {
		return err
	}
	// fmt.Println("ProcessRegistryUpdates", time.Since(start))
	// start = time.Now()
	if err := ProcessSlashings(s); err != nil {
		return err
	}
	// fmt.Println("ProcessSlashings", time.Since(start))
	ProcessEth1DataReset(s)
	// start = time.Now()
	if err := ProcessEffectiveBalanceUpdates(s); err != nil {
		return err
	}
	// fmt.Println("ProcessEffectiveBalanceUpdates", time.Since(start))
	ProcessSlashingsReset(s)
	ProcessRandaoMixesReset(s)
	if err := ProcessHistoricalRootsUpdate(s); err != nil {
		return err
	}
	if s.Version() == clparams.Phase0Version {
		if err := ProcessParticipationRecordUpdates(s); err != nil {
			return err
		}
	}

	if s.Version() >= clparams.AltairVersion {
		ProcessParticipationFlagUpdates(s)
		if err := ProcessSyncCommitteeUpdate(s); err != nil {
			return err
		}
	}
	return nil
}
