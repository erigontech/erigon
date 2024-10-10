package transition

import (
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cmd/erigon-cl/core/state"
)

// weighJustificationAndFinalization checks justification and finality of epochs and adds records to the state as needed.
func weighJustificationAndFinalization(state *state.BeaconState, previousEpochTargetBalance, currentEpochTargetBalance uint64) error {
	totalActiveBalance := state.GetTotalActiveBalance()
	currentEpoch := state.Epoch()
	previousEpoch := state.PreviousEpoch()
	oldPreviousJustifiedCheckpoint := state.PreviousJustifiedCheckpoint()
	oldCurrentJustifiedCheckpoint := state.CurrentJustifiedCheckpoint()
	justificationBits := state.JustificationBits()
	// Process justification
	state.SetPreviousJustifiedCheckpoint(oldCurrentJustifiedCheckpoint)
	// Discard oldest bit
	copy(justificationBits[1:], justificationBits[:3])
	// Turn off current justification bit
	justificationBits[0] = false
	// Update justified checkpoint if super majority is reached on previous epoch
	if previousEpochTargetBalance*3 >= totalActiveBalance*2 {
		checkPointRoot, err := state.GetBlockRoot(previousEpoch)
		if err != nil {
			return err
		}
		state.SetCurrentJustifiedCheckpoint(&cltypes.Checkpoint{
			Epoch: previousEpoch,
			Root:  checkPointRoot,
		})
		justificationBits[1] = true
	}
	if currentEpochTargetBalance*3 >= totalActiveBalance*2 {
		checkPointRoot, err := state.GetBlockRoot(currentEpoch)
		if err != nil {
			return err
		}
		state.SetCurrentJustifiedCheckpoint(&cltypes.Checkpoint{
			Epoch: currentEpoch,
			Root:  checkPointRoot,
		})
		justificationBits[0] = true
	}
	// Process finalization
	// The 2nd/3rd/4th most recent epochs are justified, the 2nd using the 4th as source
	// The 2nd/3rd most recent epochs are justified, the 2nd using the 3rd as source
	if (justificationBits.CheckRange(1, 4) && oldPreviousJustifiedCheckpoint.Epoch+3 == currentEpoch) ||
		(justificationBits.CheckRange(1, 3) && oldPreviousJustifiedCheckpoint.Epoch+2 == currentEpoch) {
		state.SetFinalizedCheckpoint(oldPreviousJustifiedCheckpoint)
	}
	// The 1st/2nd/3rd most recent epochs are justified, the 1st using the 3rd as source
	// The 1st/2nd most recent epochs are justified, the 1st using the 2nd as source
	if (justificationBits.CheckRange(0, 3) && oldCurrentJustifiedCheckpoint.Epoch+2 == currentEpoch) ||
		(justificationBits.CheckRange(0, 2) && oldCurrentJustifiedCheckpoint.Epoch+1 == currentEpoch) {
		state.SetFinalizedCheckpoint(oldCurrentJustifiedCheckpoint)
	}
	// Write justification bits
	state.SetJustificationBits(justificationBits)
	return nil
}

func ProcessJustificationBitsAndFinality(state *state.BeaconState) error {
	currentEpoch := state.Epoch()
	previousEpoch := state.PreviousEpoch()
	beaconConfig := state.BeaconConfig()
	// Skip for first 2 epochs
	if currentEpoch <= beaconConfig.GenesisEpoch+1 {
		return nil
	}
	var previousTargetBalance, currentTargetBalance uint64
	if state.Version() == clparams.Phase0Version {
		for _, validator := range state.Validators() {
			if validator.Slashed {
				continue
			}
			if validator.IsCurrentMatchingTargetAttester {
				currentTargetBalance += validator.EffectiveBalance
			}
			if validator.IsPreviousMatchingTargetAttester {
				previousTargetBalance += validator.EffectiveBalance
			}
		}
	} else {
		// Use bitlists to determine finality.
		previousParticipation, currentParticipation := state.EpochParticipation(false), state.EpochParticipation(true)
		for i, validator := range state.Validators() {
			if validator.Slashed {
				continue
			}
			if validator.Active(previousEpoch) &&
				previousParticipation[i].HasFlag(int(beaconConfig.TimelyTargetFlagIndex)) {
				previousTargetBalance += validator.EffectiveBalance
			}
			if validator.Active(currentEpoch) &&
				currentParticipation[i].HasFlag(int(beaconConfig.TimelyTargetFlagIndex)) {
				currentTargetBalance += validator.EffectiveBalance
			}
		}
	}

	return weighJustificationAndFinalization(state, previousTargetBalance, currentTargetBalance)
}
