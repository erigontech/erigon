package transition

import (
	"sort"

	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cmd/erigon-cl/core/state"
)

// computeActivationExitEpoch is Implementation of compute_activation_exit_epoch. Defined in https://github.com/ethereum/consensus-specs/blob/dev/specs/phase0/beacon-chain.md#compute_activation_exit_epoch.
func computeActivationExitEpoch(beaconConfig *clparams.BeaconChainConfig, epoch uint64) uint64 {
	return epoch + 1 + beaconConfig.MaxSeedLookahead
}

// ProcessRegistyUpdates updates every epoch the activation status of validators. Specs at: https://github.com/ethereum/consensus-specs/blob/dev/specs/phase0/beacon-chain.md#registry-updates.
func ProcessRegistryUpdates(state *state.BeaconState) error {
	beaconConfig := state.BeaconConfig()
	currentEpoch := state.Epoch()
	// start also initializing the activation queue.
	activationQueue := make([]uint64, 0)
	validators := state.Validators()
	// Process activation eligibility and ejections.
	for validatorIndex, validator := range validators {
		if state.IsValidatorEligibleForActivationQueue(validator) {
			validator.ActivationEligibilityEpoch = currentEpoch + 1
			if err := state.SetValidatorAt(validatorIndex, validator); err != nil {
				return err
			}
		}
		if validator.Active(currentEpoch) && validator.EffectiveBalance <= beaconConfig.EjectionBalance {
			if err := state.InitiateValidatorExit(uint64(validatorIndex)); err != nil {
				return err
			}
		}
		// Insert in the activation queue in case.
		if state.IsValidatorEligibleForActivation(validator) {
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
	activationQueueLength := state.GetValidatorChurnLimit()
	if len(activationQueue) > int(activationQueueLength) {
		activationQueue = activationQueue[:activationQueueLength]
	}
	// Only process up to epoch limit.
	for _, validatorIndex := range activationQueue {
		validator, err := state.ValidatorAt(int(validatorIndex))
		if err != nil {
			return err
		}
		validator.ActivationEpoch = computeActivationExitEpoch(beaconConfig, currentEpoch)
		if err := state.SetValidatorAt(int(validatorIndex), validator); err != nil {
			return err
		}
	}
	return nil
}
