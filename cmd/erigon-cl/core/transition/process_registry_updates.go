package transition

import (
	"sort"
)

const preAllocatedSizeActivationQueue = 8192

// computeActivationExitEpoch is Implementation of compute_activation_exit_epoch. Defined in https://github.com/ethereum/consensus-specs/blob/dev/specs/phase0/beacon-chain.md#compute_activation_exit_epoch.
func (s *StateTransistor) computeActivationExitEpoch(epoch uint64) uint64 {
	return epoch + 1 + s.beaconConfig.MaxSeedLookahead
}

// ProcessRegistyUpdates updates every epoch the activation status of validators. Specs at: https://github.com/ethereum/consensus-specs/blob/dev/specs/phase0/beacon-chain.md#registry-updates.
func (s *StateTransistor) ProcessRegistryUpdates() error {
	currentEpoch := s.state.Epoch()
	// start also initializing the activation queue.
	activationQueue := make([]uint64, 0)
	validators := s.state.Validators()
	// Process activation eligibility and ejections.
	for validatorIndex, validator := range validators {
		if s.state.IsValidatorEligibleForActivationQueue(validator) {
			validator.ActivationEligibilityEpoch = currentEpoch + 1
			if err := s.state.SetValidatorAt(validatorIndex, validator); err != nil {
				return err
			}
		}
		if validator.Active(currentEpoch) && validator.EffectiveBalance <= s.beaconConfig.EjectionBalance {
			if err := s.state.InitiateValidatorExit(uint64(validatorIndex)); err != nil {
				return err
			}
		}
		// Insert in the activation queue in case.
		if s.state.IsValidatorEligibleForActivation(validator) {
			activationQueue = append(activationQueue, uint64(validatorIndex))
		}
	}
	// order the queue accordingly.
	sort.Slice(activationQueue, func(i, j int) bool {
		//  Order by the sequence of activation_eligibility_epoch setting and then index.
		if validators[activationQueue[i]].ActivationEligibilityEpoch != validators[activationQueue[j]].ActivationEligibilityEpoch {
			return validators[activationQueue[i]].ActivationEligibilityEpoch < validators[activationQueue[j]].ActivationEligibilityEpoch
		}
		return activationQueue[i] < activationQueue[j]
	})
	activationQueueLength := s.state.GetValidatorChurnLimit()
	if len(activationQueue) > int(activationQueueLength) {
		activationQueue = activationQueue[:activationQueueLength]
	}
	// Only process up to epoch limit.
	for _, validatorIndex := range activationQueue {
		validator, err := s.state.ValidatorAt(int(validatorIndex))
		if err != nil {
			return err
		}
		validator.ActivationEpoch = s.computeActivationExitEpoch(currentEpoch)
		if err := s.state.SetValidatorAt(int(validatorIndex), &validator); err != nil {
			return err
		}
	}
	return nil
}
