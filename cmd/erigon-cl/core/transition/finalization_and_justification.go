package transition

import (
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes"
)

// weighJustificationAndFinalization checks justification and finality of epochs and adds records to the state as needed.
func (s *StateTransistor) weighJustificationAndFinalization(totalActiveBalance, previousEpochTargetBalance, currentEpochTargetBalance uint64) error {
	currentEpoch := s.state.Epoch()
	previousEpoch := s.state.PreviousEpoch()
	oldPreviousJustifiedCheckpoint := s.state.PreviousJustifiedCheckpoint()
	oldCurrentJustifiedCheckpoint := s.state.CurrentJustifiedCheckpoint()
	justificationBits := s.state.JustificationBits()
	// Process justification
	s.state.SetPreviousJustifiedCheckpoint(oldCurrentJustifiedCheckpoint)
	// Discard oldest bit
	copy(justificationBits[1:], justificationBits[:3])
	// Turn off current justification bit
	justificationBits[0] = false
	// Update justified checkpoint if super majority is reached on previous epoch
	if previousEpochTargetBalance*3 >= totalActiveBalance*2 {
		checkPointRoot, err := s.state.GetBlockRoot(previousEpoch)
		if err != nil {
			return err
		}
		s.state.SetCurrentJustifiedCheckpoint(&cltypes.Checkpoint{
			Epoch: previousEpoch,
			Root:  checkPointRoot,
		})
		justificationBits[1] = true
	}
	if currentEpochTargetBalance*3 >= totalActiveBalance*2 {
		checkPointRoot, err := s.state.GetBlockRoot(currentEpoch)
		if err != nil {
			return err
		}
		s.state.SetCurrentJustifiedCheckpoint(&cltypes.Checkpoint{
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
		s.state.SetFinalizedCheckpoint(oldPreviousJustifiedCheckpoint)
	}
	// The 1st/2nd/3rd most recent epochs are justified, the 1st using the 3rd as source
	// The 1st/2nd most recent epochs are justified, the 1st using the 2nd as source
	if (justificationBits.CheckRange(0, 3) && oldCurrentJustifiedCheckpoint.Epoch+2 == currentEpoch) ||
		(justificationBits.CheckRange(0, 2) && oldCurrentJustifiedCheckpoint.Epoch+1 == currentEpoch) {
		s.state.SetFinalizedCheckpoint(oldCurrentJustifiedCheckpoint)
	}
	// Write justification bits
	s.state.SetJustificationBits(justificationBits)
	return nil
}

func (s *StateTransistor) ProcessJustificationBitsAndFinality() error {
	if s.state.Version() == clparams.Phase0Version {
		return s.processJustificationBitsAndFinalityPreAltair()
	}
	return s.processJustificationBitsAndFinalityAltair()
}

func (s *StateTransistor) processJustificationBitsAndFinalityPreAltair() error {
	panic("NOT IMPLEMENTED. STOOOOOP")
}

func (s *StateTransistor) processJustificationBitsAndFinalityAltair() error {
	currentEpoch := s.state.Epoch()
	previousEpoch := s.state.PreviousEpoch()
	// Skip for first 2 epochs
	if currentEpoch <= s.beaconConfig.GenesisEpoch+1 {
		return nil
	}
	previousParticipation, currentParticipation := s.state.PreviousEpochParticipation(), s.state.CurrentEpochParticipation()
	var previousTargetBalance, currentTargetBalance uint64
	for i, validator := range s.state.Validators() {
		if validator.Slashed {
			continue
		}
		if validator.Active(previousEpoch) &&
			previousParticipation[i].HasFlag(int(s.beaconConfig.TimelyTargetFlagIndex)) {
			previousTargetBalance += validator.EffectiveBalance
		}
		if validator.Active(currentEpoch) &&
			currentParticipation[i].HasFlag(int(s.beaconConfig.TimelyTargetFlagIndex)) {
			currentTargetBalance += validator.EffectiveBalance
		}
	}

	return s.weighJustificationAndFinalization(s.state.GetTotalActiveBalance(), previousTargetBalance, currentTargetBalance)
}
