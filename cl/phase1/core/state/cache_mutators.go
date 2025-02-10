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
	"errors"
	"fmt"

	"github.com/erigontech/erigon-lib/common/math"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes/solid"
)

func (b *CachingBeaconState) getSlashingProposerReward(whistleBlowerReward uint64) uint64 {
	if b.Version() == clparams.Phase0Version {
		return whistleBlowerReward / b.BeaconConfig().ProposerRewardQuotient
	}
	return whistleBlowerReward * b.BeaconConfig().ProposerWeight / b.BeaconConfig().WeightDenominator
}

func (b *CachingBeaconState) SlashValidator(slashedInd uint64, whistleblowerInd *uint64) (uint64, error) {
	epoch := Epoch(b)
	if err := b.InitiateValidatorExit(slashedInd); err != nil {
		return 0, err
	}
	// Record changes in changeset
	slashingsIndex := int(epoch % b.BeaconConfig().EpochsPerSlashingsVector)

	// Change the validator to be slashed
	if err := b.SetValidatorSlashed(int(slashedInd), true); err != nil {
		return 0, err
	}

	currentWithdrawableEpoch, err := b.ValidatorWithdrawableEpoch(int(slashedInd))
	if err != nil {
		return 0, err
	}

	newWithdrawableEpoch := max(currentWithdrawableEpoch, epoch+b.BeaconConfig().EpochsPerSlashingsVector)
	if err := b.SetWithdrawableEpochForValidatorAtIndex(int(slashedInd), newWithdrawableEpoch); err != nil {
		return 0, err
	}

	// Update slashings vector
	currentEffectiveBalance, err := b.ValidatorEffectiveBalance(int(slashedInd))
	if err != nil {
		return 0, err
	}

	b.SetSlashingSegmentAt(slashingsIndex, b.SlashingSegmentAt(slashingsIndex)+currentEffectiveBalance)
	newEffectiveBalance, err := b.ValidatorEffectiveBalance(int(slashedInd))
	if err != nil {
		return 0, err
	}
	if err := DecreaseBalance(b, slashedInd, newEffectiveBalance/b.BeaconConfig().GetMinSlashingPenaltyQuotient(b.Version())); err != nil {
		return 0, err
	}
	proposerInd, err := b.GetBeaconProposerIndex()
	if err != nil {
		return 0, fmt.Errorf("unable to get beacon proposer index: %v", err)
	}
	if whistleblowerInd == nil {
		whistleblowerInd = new(uint64)
		*whistleblowerInd = proposerInd
	}
	whistleBlowerReward := newEffectiveBalance / b.BeaconConfig().GetWhistleBlowerRewardQuotient(b.Version())
	proposerReward := b.getSlashingProposerReward(whistleBlowerReward)
	if err := IncreaseBalance(b, proposerInd, proposerReward); err != nil {
		return 0, err
	}
	return proposerReward, IncreaseBalance(b, *whistleblowerInd, whistleBlowerReward-proposerReward)
}

func (b *CachingBeaconState) InitiateValidatorExit(index uint64) error {
	validatorExitEpoch, err := b.ValidatorExitEpoch(int(index))
	if err != nil {
		return err
	}
	if validatorExitEpoch != b.BeaconConfig().FarFutureEpoch {
		return nil
	}

	var exitQueueEpoch uint64
	switch {
	case b.Version() >= clparams.ElectraVersion:
		// electra and after
		effectiveBalance, err := b.ValidatorEffectiveBalance(int(index))
		if err != nil {
			return err
		}
		exitQueueEpoch = b.ComputeExitEpochAndUpdateChurn(effectiveBalance)
	default:
		currentEpoch := Epoch(b)
		exitQueueEpoch = ComputeActivationExitEpoch(b.BeaconConfig(), currentEpoch)
		b.ForEachValidator(func(v solid.Validator, idx, total int) bool {
			if v.ExitEpoch() != b.BeaconConfig().FarFutureEpoch && v.ExitEpoch() > exitQueueEpoch {
				exitQueueEpoch = v.ExitEpoch()
			}
			return true
		})

		exitQueueChurn := 0
		b.ForEachValidator(func(v solid.Validator, idx, total int) bool {
			if v.ExitEpoch() == exitQueueEpoch {
				exitQueueChurn += 1
			}
			return true
		})
		if exitQueueChurn >= int(b.GetValidatorChurnLimit()) {
			exitQueueEpoch += 1
		}
	}

	var overflow bool
	var newWithdrawableEpoch uint64
	if newWithdrawableEpoch, overflow = math.SafeAdd(exitQueueEpoch, b.BeaconConfig().MinValidatorWithdrawabilityDelay); overflow {
		return errors.New("withdrawable epoch is too big")
	}
	b.SetExitEpochForValidatorAtIndex(int(index), exitQueueEpoch)
	b.SetWithdrawableEpochForValidatorAtIndex(int(index), newWithdrawableEpoch)
	return nil
}

// def compute_exit_epoch_and_update_churn(state: BeaconState, exit_balance: Gwei) -> Epoch
func (b *CachingBeaconState) ComputeExitEpochAndUpdateChurn(exitBalance uint64) uint64 {
	earliestExitEpoch := max(
		b.EarliestExitEpoch(),
		ComputeActivationExitEpoch(b.BeaconConfig(), Epoch(b)),
	)
	perEpochChurn := GetActivationExitChurnLimit(b)

	var exitBalanceToConsume uint64
	if b.EarliestExitEpoch() < earliestExitEpoch {
		exitBalanceToConsume = perEpochChurn
	} else {
		exitBalanceToConsume = b.ExitBalanceToConsume()
	}
	if exitBalance > exitBalanceToConsume {
		// Exit doesn't fit in the current earliest epoch.
		balanceToProcess := exitBalance - exitBalanceToConsume
		addtionalEpochs := (balanceToProcess-1)/perEpochChurn + 1
		earliestExitEpoch += addtionalEpochs
		exitBalanceToConsume += addtionalEpochs * perEpochChurn
	}
	// Consume the balance and update state variables.
	b.SetExitBalanceToConsume(exitBalanceToConsume - exitBalance)
	b.SetEarliestExitEpoch(earliestExitEpoch)
	return earliestExitEpoch
}
