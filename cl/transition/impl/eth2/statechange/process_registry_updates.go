package statechange

import (
	"sort"

	"github.com/ledgerwatch/erigon/cl/abstract"

	"github.com/ledgerwatch/erigon/cl/cltypes/solid"
	"github.com/ledgerwatch/erigon/cl/phase1/core/state"

	"github.com/ledgerwatch/erigon/cl/clparams"
)

// computeActivationExitEpoch is Implementation of compute_activation_exit_epoch. Defined in https://github.com/ethereum/consensus-specs/blob/dev/specs/phase0/beacon-chain.md#compute_activation_exit_epoch.
func computeActivationExitEpoch(beaconConfig *clparams.BeaconChainConfig, epoch uint64) uint64 {
	return epoch + 1 + beaconConfig.MaxSeedLookahead
}

type minimizeQueuedValidator struct {
	validatorIndex             uint64
	activationEligibilityEpoch uint64
}

// ProcessRegistyUpdates updates every epoch the activation status of validators. Specs at: https://github.com/ethereum/consensus-specs/blob/dev/specs/phase0/beacon-chain.md#registry-updates.
func ProcessRegistryUpdates(s abstract.BeaconState) error {
	beaconConfig := s.BeaconConfig()
	currentEpoch := state.Epoch(s)
	// start also initializing the activation queue.
	activationQueue := make([]minimizeQueuedValidator, 0)
	// Process activation eligibility and ejections.
	var err error
	s.ForEachValidator(func(validator solid.Validator, validatorIndex, total int) bool {
		activationEligibilityEpoch := validator.ActivationEligibilityEpoch()
		effectivaBalance := validator.EffectiveBalance()
		if activationEligibilityEpoch == s.BeaconConfig().FarFutureEpoch &&
			validator.EffectiveBalance() == s.BeaconConfig().MaxEffectiveBalance {
			s.SetActivationEligibilityEpochForValidatorAtIndex(validatorIndex, currentEpoch+1)
		}
		if validator.Active(currentEpoch) && effectivaBalance <= beaconConfig.EjectionBalance {
			if err = s.InitiateValidatorExit(uint64(validatorIndex)); err != nil {
				return false
			}
		}
		// Insert in the activation queue in case.
		if activationEligibilityEpoch <= s.FinalizedCheckpoint().Epoch() &&
			validator.ActivationEpoch() == s.BeaconConfig().FarFutureEpoch {
			activationQueue = append(activationQueue, minimizeQueuedValidator{
				validatorIndex:             uint64(validatorIndex),
				activationEligibilityEpoch: activationEligibilityEpoch,
			})
		}
		return true
	})
	if err != nil {
		return err
	}
	// order the queue accordingly.
	sort.Slice(activationQueue, func(i, j int) bool {
		//  Order by the sequence of activation_eligibility_epoch setting and then index.
		if activationQueue[i].activationEligibilityEpoch != activationQueue[j].activationEligibilityEpoch {
			return activationQueue[i].activationEligibilityEpoch < activationQueue[j].activationEligibilityEpoch
		}
		return activationQueue[i].validatorIndex < activationQueue[j].validatorIndex
	})
	activationQueueLength := s.GetValidatorChurnLimit()
	if len(activationQueue) > int(activationQueueLength) {
		activationQueue = activationQueue[:activationQueueLength]
	}
	// Only process up to epoch limit.
	for _, entry := range activationQueue {
		s.SetActivationEpochForValidatorAtIndex(int(entry.validatorIndex), computeActivationExitEpoch(beaconConfig, currentEpoch))
	}
	return nil
}
