package statechange

import (
	"sort"

	"github.com/ledgerwatch/erigon/cl/cltypes/solid"
	"github.com/ledgerwatch/erigon/cl/phase1/core/state"

	"github.com/ledgerwatch/erigon/cl/clparams"
)

// computeActivationExitEpoch is Implementation of compute_activation_exit_epoch. Defined in https://github.com/ethereum/consensus-specs/blob/dev/specs/phase0/beacon-chain.md#compute_activation_exit_epoch.
func computeActivationExitEpoch(beaconConfig *clparams.BeaconChainConfig, epoch uint64) uint64 {
	return epoch + 1 + beaconConfig.MaxSeedLookahead
}

// ProcessRegistyUpdates updates every epoch the activation status of validators. Specs at: https://github.com/ethereum/consensus-specs/blob/dev/specs/phase0/beacon-chain.md#registry-updates.
func ProcessRegistryUpdates(s *state.BeaconState) error {
	beaconConfig := s.BeaconConfig()
	currentEpoch := state.Epoch(s.BeaconState)
	// start also initializing the activation queue.
	activationQueue := make([]uint64, 0)
	// Process activation eligibility and ejections.
	var err error
	s.ForEachValidator(func(validator solid.Validator, validatorIndex, total int) bool {
		if state.IsValidatorEligibleForActivationQueue(s.BeaconState, validator) {
			s.SetActivationEligibilityEpochForValidatorAtIndex(validatorIndex, currentEpoch+1)
		}
		if validator.Active(currentEpoch) && validator.EffectiveBalance() <= beaconConfig.EjectionBalance {
			if err = s.InitiateValidatorExit(uint64(validatorIndex)); err != nil {
				return false
			}
		}
		// Insert in the activation queue in case.
		if state.IsValidatorEligibleForActivation(s.BeaconState, validator) {
			activationQueue = append(activationQueue, uint64(validatorIndex))
		}
		return true
	})
	if err != nil {
		return err
	}
	// order the queue accordingly.
	sort.Slice(activationQueue, func(i, j int) bool {
		//  Order by the sequence of activation_eligibility_epoch setting and then index.
		validatori, _ := s.ValidatorForValidatorIndex(int(activationQueue[i]))
		validatorj, _ := s.ValidatorForValidatorIndex(int(activationQueue[j]))
		if validatori.ActivationEligibilityEpoch() != validatorj.ActivationEligibilityEpoch() {
			return validatori.ActivationEligibilityEpoch() < validatorj.ActivationEligibilityEpoch()
		}
		return activationQueue[i] < activationQueue[j]
	})
	activationQueueLength := s.GetValidatorChurnLimit()
	if len(activationQueue) > int(activationQueueLength) {
		activationQueue = activationQueue[:activationQueueLength]
	}
	// Only process up to epoch limit.
	for _, validatorIndex := range activationQueue {
		s.SetActivationEpochForValidatorAtIndex(int(validatorIndex), computeActivationExitEpoch(beaconConfig, currentEpoch))
	}
	return nil
}
