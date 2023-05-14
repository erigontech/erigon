package state

import (
	"fmt"

	"github.com/ledgerwatch/erigon/cl/phase1/core/state/raw"

	"github.com/Giulio2002/bls"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/cltypes/solid"
	"github.com/ledgerwatch/erigon/cl/fork"
	"github.com/ledgerwatch/erigon/cl/utils"
	"github.com/ledgerwatch/erigon/core/types"
)

const PreAllocatedRewardsAndPenalties = 8192

// these are view functions that should only getters, but are here as common utilities for packages to use

// GetEpochAtSlot gives the epoch for a certain slot
func GetEpochAtSlot(config *clparams.BeaconChainConfig, slot uint64) uint64 {
	return slot / config.SlotsPerEpoch
}

// Epoch returns current epoch.
func Epoch(b *raw.BeaconState) uint64 {
	return GetEpochAtSlot(b.BeaconConfig(), b.Slot())
}

// GetTotalBalance return the sum of all balances within the given validator set.
func GetTotalBalance(b *raw.BeaconState, validatorSet []uint64) (uint64, error) {
	var (
		total uint64
	)
	for _, validatorIndex := range validatorSet {
		// Should be in bounds.
		delta, err := b.ValidatorEffectiveBalance(int(validatorIndex))
		if err != nil {
			return 0, err
		}
		total += delta
	}
	// Always minimum set to EffectiveBalanceIncrement
	if total < b.BeaconConfig().EffectiveBalanceIncrement {
		total = b.BeaconConfig().EffectiveBalanceIncrement
	}
	return total, nil
}

// GetTotalSlashingAmount return the sum of all slashings.
func GetTotalSlashingAmount(b *raw.BeaconState) (t uint64) {
	b.ForEachSlashingSegment(func(v uint64, idx, total int) bool {
		t += v
		return true
	})
	return
}

// PreviousEpoch returns previous epoch.
func PreviousEpoch(b *raw.BeaconState) uint64 {
	epoch := Epoch(b)
	if epoch == 0 {
		return epoch
	}
	return epoch - 1
}

// GetBlockRoot returns blook root at start of a given epoch
func GetBlockRoot(b *raw.BeaconState, epoch uint64) (libcommon.Hash, error) {
	return b.GetBlockRootAtSlot(epoch * b.BeaconConfig().SlotsPerEpoch)
}

// FinalityDelay determines by how many epochs we are late on finality.
func FinalityDelay(b *raw.BeaconState) uint64 {
	return PreviousEpoch(b) - b.FinalizedCheckpoint().Epoch()
}

// Implementation of is_in_inactivity_leak. tells us if network is in danger pretty much. defined in ETH 2.0 specs.
func InactivityLeaking(b *raw.BeaconState) bool {
	return FinalityDelay(b) > b.BeaconConfig().MinEpochsToInactivityPenalty
}

// IsUnslashedParticipatingIndex
func IsUnslashedParticipatingIndex(b *raw.BeaconState, epoch, index uint64, flagIdx int) bool {
	validator, err := b.ValidatorForValidatorIndex(int(index))
	if err != nil {
		return false
	}
	return validator.Active(epoch) && cltypes.ParticipationFlags(b.EpochParticipation(false).Get(int(index))).HasFlag(flagIdx) && !validator.Slashed()
}

// EligibleValidatorsIndicies Implementation of get_eligible_validator_indices as defined in the eth 2.0 specs.
func EligibleValidatorsIndicies(b *raw.BeaconState) (eligibleValidators []uint64) {
	eligibleValidators = make([]uint64, 0, b.ValidatorLength())
	previousEpoch := PreviousEpoch(b)

	b.ForEachValidator(func(validator *cltypes.Validator, i, total int) bool {
		if validator.Active(previousEpoch) || (validator.Slashed() && previousEpoch+1 < validator.WithdrawableEpoch()) {
			eligibleValidators = append(eligibleValidators, uint64(i))
		}
		return true
	})
	return
}

func IsValidIndexedAttestation(b *raw.BeaconState, att *cltypes.IndexedAttestation) (bool, error) {
	inds := att.AttestingIndices
	if len(inds) == 0 || !utils.IsSliceSortedSet(inds) {
		return false, fmt.Errorf("isValidIndexedAttestation: attesting indices are not sorted or are null")
	}

	pks := [][]byte{}
	for _, v := range inds {
		val, err := b.ValidatorForValidatorIndex(int(v))
		if err != nil {
			return false, err
		}
		pk := val.PublicKey()
		pks = append(pks, pk[:])
	}

	domain, err := b.GetDomain(b.BeaconConfig().DomainBeaconAttester, att.Data.Target().Epoch())
	if err != nil {
		return false, fmt.Errorf("unable to get the domain: %v", err)
	}

	signingRoot, err := fork.ComputeSigningRoot(att.Data, domain)
	if err != nil {
		return false, fmt.Errorf("unable to get signing root: %v", err)
	}

	valid, err := bls.VerifyAggregate(att.Signature[:], signingRoot[:], pks)
	if err != nil {
		return false, fmt.Errorf("error while validating signature: %v", err)
	}
	if !valid {
		return false, fmt.Errorf("invalid aggregate signature")
	}
	return true, nil
}

// getUnslashedParticipatingIndices returns set of currently unslashed participating indexes
func GetUnslashedParticipatingIndices(b *raw.BeaconState, flagIndex int, epoch uint64) (validatorSet []uint64, err error) {
	var participation solid.BitList
	// Must be either previous or current epoch
	switch epoch {
	case Epoch(b):
		participation = b.EpochParticipation(true)
	case PreviousEpoch(b):
		participation = b.EpochParticipation(false)
	default:
		return nil, fmt.Errorf("getUnslashedParticipatingIndices: only epoch and previous epoch can be used")
	}
	// Iterate over all validators and include the active ones that have flag_index enabled and are not slashed.
	b.ForEachValidator(func(validator *cltypes.Validator, i, total int) bool {
		if !validator.Active(epoch) ||
			!cltypes.ParticipationFlags(participation.Get(i)).HasFlag(flagIndex) ||
			validator.Slashed() {
			return true
		}
		validatorSet = append(validatorSet, uint64(i))
		return true
	})
	return
}

// Implementation of is_eligible_for_activation_queue. Specs at: https://github.com/ethereum/consensus-specs/blob/dev/specs/phase0/beacon-chain.md#is_eligible_for_activation_queue
func IsValidatorEligibleForActivationQueue(b *raw.BeaconState, validator *cltypes.Validator) bool {
	return validator.ActivationEligibilityEpoch() == b.BeaconConfig().FarFutureEpoch &&
		validator.EffectiveBalance() == b.BeaconConfig().MaxEffectiveBalance
}

// Implementation of is_eligible_for_activation. Specs at: https://github.com/ethereum/consensus-specs/blob/dev/specs/phase0/beacon-chain.md#is_eligible_for_activation
func IsValidatorEligibleForActivation(b *raw.BeaconState, validator *cltypes.Validator) bool {
	return validator.ActivationEligibilityEpoch() <= b.FinalizedCheckpoint().Epoch() &&
		validator.ActivationEpoch() == b.BeaconConfig().FarFutureEpoch
}

// Check whether a merge transition is complete by verifying the presence of a valid execution payload header.
func IsMergeTransitionComplete(b *raw.BeaconState) bool {
	return !b.LatestExecutionPayloadHeader().IsZero()
}

// Compute the Unix timestamp at the specified slot number.
func ComputeTimestampAtSlot(b *raw.BeaconState, slot uint64) uint64 {
	return b.GenesisTime() + (slot-b.BeaconConfig().GenesisSlot)*b.BeaconConfig().SecondsPerSlot
}

// ExpectedWithdrawals calculates the expected withdrawals that can be made by validators in the current epoch
func ExpectedWithdrawals(b *raw.BeaconState) []*types.Withdrawal {
	// Get the current epoch, the next withdrawal index, and the next withdrawal validator index
	currentEpoch := Epoch(b)
	nextWithdrawalIndex := b.NextWithdrawalIndex()
	nextWithdrawalValidatorIndex := b.NextWithdrawalValidatorIndex()

	// Determine the upper bound for the loop and initialize the withdrawals slice with a capacity of bound
	maxValidators := uint64(b.ValidatorLength())
	maxValidatorsPerWithdrawalsSweep := b.BeaconConfig().MaxValidatorsPerWithdrawalsSweep
	bound := utils.Min64(maxValidators, maxValidatorsPerWithdrawalsSweep)
	withdrawals := make([]*types.Withdrawal, 0, bound)

	// Loop through the validators to calculate expected withdrawals
	for validatorCount := uint64(0); validatorCount < bound && len(withdrawals) != int(b.BeaconConfig().MaxWithdrawalsPerPayload); validatorCount++ {
		// Get the validator and balance for the current validator index
		// supposedly this operation is safe because we checked the validator length about
		currentValidator, _ := b.ValidatorForValidatorIndex(int(nextWithdrawalValidatorIndex))
		currentBalance, _ := b.ValidatorBalance(int(nextWithdrawalValidatorIndex))
		wd := currentValidator.WithdrawalCredentials()
		// Check if the validator is fully withdrawable
		if isFullyWithdrawableValidator(b.BeaconConfig(), currentValidator, currentBalance, currentEpoch) {
			// Add a new withdrawal with the validator's withdrawal credentials and balance
			newWithdrawal := &types.Withdrawal{
				Index:     nextWithdrawalIndex,
				Validator: nextWithdrawalValidatorIndex,
				Address:   libcommon.BytesToAddress(wd[12:]),
				Amount:    currentBalance,
			}
			withdrawals = append(withdrawals, newWithdrawal)
			nextWithdrawalIndex++
		} else if isPartiallyWithdrawableValidator(b.BeaconConfig(), currentValidator, currentBalance) { // Check if the validator is partially withdrawable
			// Add a new withdrawal with the validator's withdrawal credentials and balance minus the maximum effective balance
			newWithdrawal := &types.Withdrawal{
				Index:     nextWithdrawalIndex,
				Validator: nextWithdrawalValidatorIndex,
				Address:   libcommon.BytesToAddress(wd[12:]),
				Amount:    currentBalance - b.BeaconConfig().MaxEffectiveBalance,
			}
			withdrawals = append(withdrawals, newWithdrawal)
			nextWithdrawalIndex++
		}

		// Increment the validator index, looping back to 0 if necessary
		nextWithdrawalValidatorIndex = (nextWithdrawalValidatorIndex + 1) % maxValidators
	}

	// Return the withdrawals slice
	return withdrawals
}
