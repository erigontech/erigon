// Copyright 2024 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package state

import (
	"encoding/binary"
	"errors"
	"fmt"
	"runtime"

	"github.com/erigontech/erigon/cl/phase1/core/state/shuffling"
	"github.com/erigontech/erigon/cl/utils/bls"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"

	"github.com/erigontech/erigon/cl/abstract"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/fork"
	"github.com/erigontech/erigon/cl/utils"
	"github.com/erigontech/erigon/cl/utils/threading"
)

const PreAllocatedRewardsAndPenalties = 8192

// these are view functions that should only getters, but are here as common utilities for packages to use

// GetEpochAtSlot gives the epoch for a certain slot
func GetEpochAtSlot(config *clparams.BeaconChainConfig, slot uint64) uint64 {
	return slot / config.SlotsPerEpoch
}

// Epoch returns current epoch.
func Epoch(b abstract.BeaconStateBasic) uint64 {
	return GetEpochAtSlot(b.BeaconConfig(), b.Slot())
}

func IsAggregator(cfg *clparams.BeaconChainConfig, committeeLength, committeeIndex uint64, slotSignature common.Bytes96) bool {
	modulo := max(1, committeeLength/cfg.TargetAggregatorsPerCommittee)
	hashSlotSignatue := utils.Sha256(slotSignature[:])
	return binary.LittleEndian.Uint64(hashSlotSignatue[:8])%modulo == 0
}

// GetTotalBalance return the sum of all balances within the given validator set.
func GetTotalBalance(b abstract.BeaconStateBasic, validatorSet []uint64) (uint64, error) {
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
func GetTotalSlashingAmount(b abstract.BeaconState) (t uint64) {
	b.ForEachSlashingSegment(func(idx int, v uint64, total int) bool {
		t += v
		return true
	})
	return
}

// PreviousEpoch returns previous epoch.
func PreviousEpoch(b abstract.BeaconState) uint64 {
	epoch := Epoch(b)
	if epoch == 0 {
		return epoch
	}
	return epoch - 1
}

// GetBlockRoot returns blook root at start of a given epoch
func GetBlockRoot(b abstract.BeaconState, epoch uint64) (common.Hash, error) {
	return b.GetBlockRootAtSlot(epoch * b.BeaconConfig().SlotsPerEpoch)
}

// FinalityDelay determines by how many epochs we are late on finality.
func FinalityDelay(b abstract.BeaconState) uint64 {
	return PreviousEpoch(b) - b.FinalizedCheckpoint().Epoch
}

// InactivityLeaking returns whether epochs are in inactivity penalty.
// Implementation of is_in_inactivity_leak as defined in the ETH 2.0 specs.
func InactivityLeaking(b abstract.BeaconState) bool {
	return FinalityDelay(b) > b.BeaconConfig().MinEpochsToInactivityPenalty
}

// IsUnslashedParticipatingIndex
func IsUnslashedParticipatingIndex(validatorSet *solid.ValidatorSet, previousEpochParticipation *solid.ParticipationBitList, epoch, index uint64, flagIdx int) bool {
	validator := validatorSet.Get(int(index))
	return validator.Active(epoch) && cltypes.ParticipationFlags(previousEpochParticipation.Get(int(index))).HasFlag(flagIdx) && !validator.Slashed()
}

// EligibleValidatorsIndicies Implementation of get_eligible_validator_indices as defined in the eth 2.0 specs.
func EligibleValidatorsIndicies(b abstract.BeaconState) (eligibleValidators []uint64) {
	/* This is a parallel implementation of get_eligible_validator_indices*/

	// We divide computation into multiple threads to speed up the process.
	numThreads := runtime.NumCPU()
	wp := threading.NewParallelExecutor()
	eligibleValidatorsShards := make([][]uint64, numThreads)
	shardSize := b.ValidatorLength() / numThreads
	for i := range eligibleValidatorsShards {
		eligibleValidatorsShards[i] = make([]uint64, 0, shardSize)
	}
	previousEpoch := PreviousEpoch(b)
	// Iterate over all validators and include the active ones that have flag_index enabled and are not slashed.
	for i := 0; i < numThreads; i++ {
		workerID := i
		wp.AddWork(func() error {
			from := workerID * shardSize
			to := (workerID + 1) * shardSize
			if workerID == numThreads-1 {
				to = b.ValidatorLength()
			}
			for j := from; j < to; j++ {
				validator, err := b.ValidatorForValidatorIndex(j)
				if err != nil {
					panic(err)
				}
				if validator.Active(previousEpoch) || (validator.Slashed() && previousEpoch+1 < validator.WithdrawableEpoch()) {
					eligibleValidatorsShards[workerID] = append(eligibleValidatorsShards[workerID], uint64(j))
				}
			}
			return nil
		})
	}
	wp.Execute()
	// Merge the results from all threads.
	for i := range eligibleValidatorsShards {
		eligibleValidators = append(eligibleValidators, eligibleValidatorsShards[i]...)
	}

	return
}

func IsValidIndexedAttestation(b abstract.BeaconStateBasic, att *cltypes.IndexedAttestation) (bool, error) {
	inds := att.AttestingIndices
	if inds.Length() == 0 || !solid.IsUint64SortedSet(inds) {
		return false, errors.New("isValidIndexedAttestation: attesting indices are not sorted or are null")
	}

	pks := make([][]byte, 0, inds.Length())
	if err := solid.RangeErr[uint64](inds, func(_ int, v uint64, _ int) error {
		val, err := b.ValidatorForValidatorIndex(int(v))
		if err != nil {
			return err
		}
		pk := val.PublicKeyBytes()
		pks = append(pks, pk)
		return nil
	}); err != nil {
		return false, err
	}

	domain, err := b.GetDomain(b.BeaconConfig().DomainBeaconAttester, att.Data.Target.Epoch)
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
		return false, errors.New("invalid aggregate signature")
	}
	return true, nil
}

// GetUnslashedParticipatingIndices returns set of currently unslashed participating indexes.
func GetUnslashedParticipatingIndices(b abstract.BeaconState, flagIndex int, epoch uint64) (validatorSet []uint64, err error) {
	var participation *solid.ParticipationBitList
	// Must be either previous or current epoch
	switch epoch {
	case Epoch(b):
		participation = b.EpochParticipation(true)
	case PreviousEpoch(b):
		participation = b.EpochParticipation(false)
	default:
		return nil, errors.New("getUnslashedParticipatingIndices: only epoch and previous epoch can be used")
	}
	// Iterate over all validators and include the active ones that have flag_index enabled and are not slashed.
	b.ForEachValidator(func(validator solid.Validator, i, total int) bool {
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

// IsValidatorEligibleForActivationQueue returns whether the validator is eligible to be placed into the activation queue.
// Implementation of is_eligible_for_activation_queue.
// Specs at: https://github.com/ethereum/consensus-specs/blob/dev/specs/phase0/beacon-chain.md#is_eligible_for_activation_queue
// updated for Electra: https://github.com/ethereum/consensus-specs/blob/dev/specs/electra/beacon-chain.md#modified-is_eligible_for_activation_queue
func IsValidatorEligibleForActivationQueue(b abstract.BeaconState, validator solid.Validator) bool {
	if b.Version() <= clparams.DenebVersion {
		return validator.ActivationEligibilityEpoch() == b.BeaconConfig().FarFutureEpoch &&
			validator.EffectiveBalance() == b.BeaconConfig().MaxEffectiveBalance
	}
	// Electra and after
	return validator.ActivationEligibilityEpoch() == b.BeaconConfig().FarFutureEpoch &&
		validator.EffectiveBalance() >= b.BeaconConfig().MinActivationBalance
}

// IsValidatorEligibleForActivation returns whether the validator is eligible for activation.
// Implementation of is_eligible_for_activation.
// Specs at: https://github.com/ethereum/consensus-specs/blob/dev/specs/phase0/beacon-chain.md#is_eligible_for_activation
func IsValidatorEligibleForActivation(b abstract.BeaconState, validator solid.Validator) bool {
	return validator.ActivationEligibilityEpoch() <= b.FinalizedCheckpoint().Epoch &&
		validator.ActivationEpoch() == b.BeaconConfig().FarFutureEpoch
}

// IsMergeTransitionComplete returns whether a merge transition is complete by verifying the presence of a valid execution payload header.
func IsMergeTransitionComplete(b abstract.BeaconState) bool {
	if b.Version() < clparams.BellatrixVersion {
		return false
	}
	if b.Version() > clparams.BellatrixVersion {
		return true
	}
	return !b.LatestExecutionPayloadHeader().IsZero()
}

// ComputeTimestampAtSlot computes the Unix timestamp at the specified slot number.
func ComputeTimestampAtSlot(b abstract.BeaconState, slot uint64) uint64 {
	return b.GenesisTime() + (slot-b.BeaconConfig().GenesisSlot)*b.BeaconConfig().SecondsPerSlot
}

// GetPendingPartialWithdrawals processes pending partial withdrawals for validators,
// respecting the combined limit of prior withdrawals + MAX_PENDING_PARTIALS_PER_WITHDRAWALS_SWEEP
// and MAX_WITHDRAWALS_PER_PAYLOAD - 1.
// Returns the new withdrawals, updated withdrawal index, and the number of processed entries.
func GetPendingPartialWithdrawals(b abstract.BeaconState, withdrawalIndex uint64, priorWithdrawals []*cltypes.Withdrawal) ([]*cltypes.Withdrawal, uint64, uint64) {
	epoch := Epoch(b)
	cfg := b.BeaconConfig()

	withdrawalsLimit := min(
		len(priorWithdrawals)+int(cfg.MaxPendingPartialsPerWithdrawalsSweep),
		int(cfg.MaxWithdrawalsPerPayload)-1,
	)

	var processedCount uint64
	var withdrawals []*cltypes.Withdrawal

	b.GetPendingPartialWithdrawals().Range(func(_ int, w *solid.PendingPartialWithdrawal, _ int) bool {
		isWithdrawable := w.WithdrawableEpoch <= epoch
		hasReachedLimit := len(priorWithdrawals)+len(withdrawals) >= withdrawalsLimit
		if !isWithdrawable || hasReachedLimit {
			return false
		}

		validator := b.ValidatorSet().Get(int(w.ValidatorIndex))
		balance := getBalanceAfterWithdrawals(b, w.ValidatorIndex, priorWithdrawals, withdrawals)

		if validator.ExitEpoch() == cfg.FarFutureEpoch &&
			validator.EffectiveBalance() >= cfg.MinActivationBalance &&
			balance > cfg.MinActivationBalance {

			withdrawalAmount := min(balance-cfg.MinActivationBalance, w.Amount)
			wd := validator.WithdrawalCredentials()
			withdrawals = append(withdrawals, &cltypes.Withdrawal{
				Index:     withdrawalIndex,
				Validator: w.ValidatorIndex,
				Address:   common.BytesToAddress(wd[12:]),
				Amount:    withdrawalAmount,
			})
			withdrawalIndex++
		}

		processedCount++
		return true
	})

	return withdrawals, withdrawalIndex, processedCount
}

// getBalanceAfterWithdrawals returns a validator's balance minus the total amount
// already withdrawn across all provided withdrawal slices.
func getBalanceAfterWithdrawals(b abstract.BeaconState, validatorIndex uint64, withdrawalSets ...[]*cltypes.Withdrawal) uint64 {
	balance, err := b.ValidatorBalance(int(validatorIndex))
	if err != nil {
		log.Warn("Failed to get validator balance", "index", validatorIndex, "error", err)
		return 0
	}
	var totalWithdrawn uint64
	for _, set := range withdrawalSets {
		for _, w := range set {
			if w.Validator == validatorIndex {
				totalWithdrawn += w.Amount
			}
		}
	}
	if balance > totalWithdrawn {
		return balance - totalWithdrawn
	}
	return 0
}

// GetExpectedWithdrawals calculates the expected withdrawals that can be made by validators in the current epoch
func GetExpectedWithdrawals(b abstract.BeaconState, currentEpoch uint64) *cltypes.ExpectedWithdrawals {
	nextWithdrawalIndex := b.NextWithdrawalIndex()
	expWithdrawals := &cltypes.ExpectedWithdrawals{
		Withdrawals: []*cltypes.Withdrawal{},
	}

	// [New in Gloas:EIP7732] Get builder withdrawals
	if b.Version() >= clparams.GloasVersion {
		builderWithdrawals, nextIdx, processedBuilderWithdrawalsCount := GetBuilderWithdrawals(b, nextWithdrawalIndex, expWithdrawals.Withdrawals)
		expWithdrawals.Withdrawals = append(expWithdrawals.Withdrawals, builderWithdrawals...)
		nextWithdrawalIndex = nextIdx
		expWithdrawals.ProcessedBuilderWithdrawalsCount = processedBuilderWithdrawalsCount
	}

	// [New in Electra:EIP7251] Consume pending partial withdrawals
	if b.Version() >= clparams.ElectraVersion {
		partialWithdrawals, nextIdx, count := GetPendingPartialWithdrawals(b, nextWithdrawalIndex, expWithdrawals.Withdrawals)
		expWithdrawals.Withdrawals = append(expWithdrawals.Withdrawals, partialWithdrawals...)
		nextWithdrawalIndex = nextIdx
		expWithdrawals.ProcessedPartialWithdrawalsCount = count
	}

	// [New in Gloas:EIP7732] Get builders sweep withdrawals
	if b.Version() >= clparams.GloasVersion {
		buildersSweepWithdrawals, nextIdx, processedBuildersSweepCount := GetBuildersSweepWithdrawals(b, nextWithdrawalIndex, expWithdrawals.Withdrawals)
		expWithdrawals.Withdrawals = append(expWithdrawals.Withdrawals, buildersSweepWithdrawals...)
		nextWithdrawalIndex = nextIdx
		expWithdrawals.ProcessedBuildersSweepCount = processedBuildersSweepCount
	}

	// Sweep for remaining withdrawals
	sweepWithdrawals, nextIdx, processedValidatorsSweepCount := GetValidatorsSweepWithdrawals(b, nextWithdrawalIndex, currentEpoch, expWithdrawals.Withdrawals)
	expWithdrawals.Withdrawals = append(expWithdrawals.Withdrawals, sweepWithdrawals...)
	_ = nextIdx

	expWithdrawals.ProcessedSweepWithdrawalsCount = processedValidatorsSweepCount
	return expWithdrawals
}

// GetValidatorsSweepWithdrawals sweeps through validators starting from next_withdrawal_validator_index,
// collecting full and partial withdrawals.
// Returns the new withdrawals, updated withdrawal index, and the number of processed validators.
func GetValidatorsSweepWithdrawals(b abstract.BeaconState, withdrawalIndex uint64, epoch uint64, priorWithdrawals []*cltypes.Withdrawal) ([]*cltypes.Withdrawal, uint64, uint64) {
	cfg := b.BeaconConfig()
	maxValidators := uint64(b.ValidatorLength())
	validatorsLimit := min(maxValidators, cfg.MaxValidatorsPerWithdrawalsSweep)
	withdrawalsLimit := int(cfg.MaxWithdrawalsPerPayload)

	var processedCount uint64
	var withdrawals []*cltypes.Withdrawal
	validatorIndex := b.NextWithdrawalValidatorIndex()

	for range validatorsLimit {
		if len(priorWithdrawals)+len(withdrawals) >= withdrawalsLimit {
			break
		}

		validator, _ := b.ValidatorForValidatorIndex(int(validatorIndex))
		var balance uint64
		if b.Version() >= clparams.ElectraVersion {
			balance = getBalanceAfterWithdrawals(b, validatorIndex, priorWithdrawals, withdrawals)
		} else {
			balance, _ = b.ValidatorBalance(int(validatorIndex))
		}
		wd := validator.WithdrawalCredentials()

		if isFullyWithdrawableValidator(b, validator, balance, epoch) {
			withdrawals = append(withdrawals, &cltypes.Withdrawal{
				Index:     withdrawalIndex,
				Validator: validatorIndex,
				Address:   common.BytesToAddress(wd[12:]),
				Amount:    balance,
			})
			withdrawalIndex++
		} else if isPartiallyWithdrawableValidator(b, validator, balance) {
			// [Modified in Electra:EIP7251]
			maxEffectiveBalance := GetMaxEffectiveBalanceByVersion(validator, cfg, b.Version())
			withdrawals = append(withdrawals, &cltypes.Withdrawal{
				Index:     withdrawalIndex,
				Validator: validatorIndex,
				Address:   common.BytesToAddress(wd[12:]),
				Amount:    balance - maxEffectiveBalance,
			})
			withdrawalIndex++
		}

		validatorIndex = (validatorIndex + 1) % maxValidators
		processedCount++
	}

	return withdrawals, withdrawalIndex, processedCount
}

// GetBuilderWithdrawals constructs withdrawal entries from builder pending withdrawals,
// respecting the MAX_WITHDRAWALS_PER_PAYLOAD - 1 limit combined with prior withdrawals.
// Returns the new withdrawals, updated withdrawal index, and the number of processed entries.
func GetBuilderWithdrawals(b abstract.BeaconState, withdrawalIndex uint64, priorWithdrawals []*cltypes.Withdrawal) ([]*cltypes.Withdrawal, uint64, uint64) {
	withdrawalsLimit := int(b.BeaconConfig().MaxWithdrawalsPerPayload) - 1
	if len(priorWithdrawals) > withdrawalsLimit {
		log.Warn("GetBuilderWithdrawals: prior withdrawals exceed limit", "prior", len(priorWithdrawals), "limit", withdrawalsLimit)
		return []*cltypes.Withdrawal{}, withdrawalIndex, 0
	}

	var processedCount uint64
	var withdrawals []*cltypes.Withdrawal

	pendingWithdrawals := b.GetBuilderPendingWithdrawals()
	if pendingWithdrawals == nil {
		log.Warn("GetBuilderWithdrawals: builder_pending_withdrawals is nil")
		return withdrawals, withdrawalIndex, processedCount
	}

	pendingWithdrawals.Range(func(_ int, w *cltypes.BuilderPendingWithdrawal, _ int) bool {
		if len(priorWithdrawals)+len(withdrawals) >= withdrawalsLimit {
			return false
		}
		withdrawals = append(withdrawals, &cltypes.Withdrawal{
			Index:     withdrawalIndex,
			Validator: ConvertBuilderIndexToValidatorIndex(w.BuilderIndex),
			Address:   w.FeeRecipient,
			Amount:    w.Amount,
		})
		withdrawalIndex++
		processedCount++
		return true
	})

	return withdrawals, withdrawalIndex, processedCount
}

// GetBuildersSweepWithdrawals sweeps through builders starting from next_withdrawal_builder_index,
// collecting withdrawals for builders whose withdrawable_epoch has passed and have a positive balance.
// Returns the new withdrawals, updated withdrawal index, and the number of processed builders.
func GetBuildersSweepWithdrawals(b abstract.BeaconState, withdrawalIndex uint64, priorWithdrawals []*cltypes.Withdrawal) ([]*cltypes.Withdrawal, uint64, uint64) {
	epoch := Epoch(b)
	cfg := b.BeaconConfig()

	builders := b.GetBuilders()
	if builders == nil {
		log.Warn("GetBuildersSweepWithdrawals: builders is nil")
		return nil, withdrawalIndex, 0
	}

	buildersLen := builders.Len()
	buildersLimit := min(buildersLen, int(cfg.MaxBuildersPerWithdrawalsSweep))
	withdrawalsLimit := int(cfg.MaxWithdrawalsPerPayload) - 1
	if len(priorWithdrawals) > withdrawalsLimit {
		log.Warn("GetBuildersSweepWithdrawals: prior withdrawals exceed limit", "prior", len(priorWithdrawals), "limit", withdrawalsLimit)
		return []*cltypes.Withdrawal{}, withdrawalIndex, 0
	}

	var processedCount uint64
	var withdrawals []*cltypes.Withdrawal
	builderIndex := b.GetNextWithdrawalBuilderIndex()

	for range buildersLimit {
		if len(priorWithdrawals)+len(withdrawals) >= withdrawalsLimit {
			break
		}

		builder := builders.Get(int(builderIndex))
		if builder != nil && builder.WithdrawableEpoch <= epoch && builder.Balance > 0 {
			withdrawals = append(withdrawals, &cltypes.Withdrawal{
				Index:     withdrawalIndex,
				Validator: ConvertBuilderIndexToValidatorIndex(builderIndex),
				Address:   builder.ExecutionAddress,
				Amount:    builder.Balance,
			})
			withdrawalIndex++
		}

		builderIndex = (builderIndex + 1) % uint64(buildersLen)
		processedCount++
	}

	return withdrawals, withdrawalIndex, processedCount
}

// GetNextSyncCommitteeIndices returns the sync committee indices, with possible duplicates,
// for the next sync committee.
// [Modified in Gloas:EIP7732]
func GetNextSyncCommitteeIndices(b *CachingBeaconState) ([]uint64, error) {
	conf := b.BeaconConfig()
	epoch := Epoch(b) + 1

	mixPosition := (epoch + conf.EpochsPerHistoricalVector - conf.MinSeedLookahead - 1) %
		conf.EpochsPerHistoricalVector
	mix := b.GetRandaoMix(int(mixPosition))
	seed := shuffling.GetSeed(conf, mix, epoch, conf.DomainSyncCommittee)

	indices := b.GetActiveValidatorsIndices(epoch)
	return shuffling.ComputeBalanceWeightedSelection(b.BeaconState, indices, seed, conf.SyncCommitteeSize, true)
}
