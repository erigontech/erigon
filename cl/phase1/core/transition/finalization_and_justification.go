package transition

import (
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/cltypes/solid"
	state2 "github.com/ledgerwatch/erigon/cl/phase1/core/state"
)

// weighJustificationAndFinalization checks justification and finality of epochs and adds records to the state as needed.
func weighJustificationAndFinalization(s *state2.BeaconState, previousEpochTargetBalance, currentEpochTargetBalance uint64) error {
	totalActiveBalance := s.GetTotalActiveBalance()
	currentEpoch := state2.Epoch(s.BeaconState)
	previousEpoch := state2.PreviousEpoch(s.BeaconState)
	oldPreviousJustifiedCheckpoint := s.PreviousJustifiedCheckpoint()
	oldCurrentJustifiedCheckpoint := s.CurrentJustifiedCheckpoint()
	justificationBits := s.JustificationBits()
	// Process justification
	s.SetPreviousJustifiedCheckpoint(oldCurrentJustifiedCheckpoint)
	// Discard oldest bit
	copy(justificationBits[1:], justificationBits[:3])
	// Turn off current justification bit
	justificationBits[0] = false
	// Update justified checkpoint if super majority is reached on previous epoch
	if previousEpochTargetBalance*3 >= totalActiveBalance*2 {
		checkPointRoot, err := state2.GetBlockRoot(s.BeaconState, previousEpoch)
		if err != nil {
			return err
		}

		s.SetCurrentJustifiedCheckpoint(solid.NewCheckpointFromParameters(checkPointRoot, previousEpoch))
		justificationBits[1] = true
	}
	if currentEpochTargetBalance*3 >= totalActiveBalance*2 {
		checkPointRoot, err := state2.GetBlockRoot(s.BeaconState, currentEpoch)
		if err != nil {
			return err
		}

		s.SetCurrentJustifiedCheckpoint(solid.NewCheckpointFromParameters(checkPointRoot, currentEpoch))
		justificationBits[0] = true
	}
	// Process finalization
	// The 2nd/3rd/4th most recent epochs are justified, the 2nd using the 4th as source
	// The 2nd/3rd most recent epochs are justified, the 2nd using the 3rd as source
	if (justificationBits.CheckRange(1, 4) && oldPreviousJustifiedCheckpoint.Epoch()+3 == currentEpoch) ||
		(justificationBits.CheckRange(1, 3) && oldPreviousJustifiedCheckpoint.Epoch()+2 == currentEpoch) {
		s.SetFinalizedCheckpoint(oldPreviousJustifiedCheckpoint)
	}
	// The 1st/2nd/3rd most recent epochs are justified, the 1st using the 3rd as source
	// The 1st/2nd most recent epochs are justified, the 1st using the 2nd as source
	if (justificationBits.CheckRange(0, 3) && oldCurrentJustifiedCheckpoint.Epoch()+2 == currentEpoch) ||
		(justificationBits.CheckRange(0, 2) && oldCurrentJustifiedCheckpoint.Epoch()+1 == currentEpoch) {
		s.SetFinalizedCheckpoint(oldCurrentJustifiedCheckpoint)
	}
	// Write justification bits
	s.SetJustificationBits(justificationBits)
	return nil
}

func ProcessJustificationBitsAndFinality(s *state2.BeaconState) error {
	currentEpoch := state2.Epoch(s.BeaconState)
	previousEpoch := state2.PreviousEpoch(s.BeaconState)
	beaconConfig := s.BeaconConfig()
	// Skip for first 2 epochs
	if currentEpoch <= beaconConfig.GenesisEpoch+1 {
		return nil
	}
	var previousTargetBalance, currentTargetBalance uint64

	if s.Version() == clparams.Phase0Version {
		var err error
		s.ForEachValidator(func(validator solid.Validator, idx, total int) bool {
			if validator.Slashed() {
				return true
			}
			isCurrentMatchingTargetAttester, err2 := s.ValidatorIsCurrentMatchingTargetAttester(idx)
			if err2 != nil {
				err = err2
				return false
			}
			isPreviousMatchingTargetAttester, err2 := s.ValidatorIsPreviousMatchingTargetAttester(idx)
			if err2 != nil {
				err = err2
				return false
			}

			if isCurrentMatchingTargetAttester {
				currentTargetBalance += validator.EffectiveBalance()
			}
			if isPreviousMatchingTargetAttester {
				previousTargetBalance += validator.EffectiveBalance()
			}
			return true
		})
		if err != nil {
			return err
		}
	} else {
		// Use bitlists to determine finality.
		previousParticipation, currentParticipation := s.EpochParticipation(false), s.EpochParticipation(true)
		s.ForEachValidator(func(validator solid.Validator, i, total int) bool {
			if validator.Slashed() {
				return true
			}
			if validator.Active(previousEpoch) &&
				cltypes.ParticipationFlags(previousParticipation.Get(i)).HasFlag(int(beaconConfig.TimelyTargetFlagIndex)) {
				previousTargetBalance += validator.EffectiveBalance()
			}
			if validator.Active(currentEpoch) &&
				cltypes.ParticipationFlags(currentParticipation.Get(i)).HasFlag(int(beaconConfig.TimelyTargetFlagIndex)) {
				currentTargetBalance += validator.EffectiveBalance()
			}
			return true
		})
	}

	return weighJustificationAndFinalization(s, previousTargetBalance, currentTargetBalance)
}
