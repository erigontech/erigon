package state

import (
	"fmt"

	"github.com/ledgerwatch/erigon-lib/common/math"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes/solid"
	"github.com/ledgerwatch/erigon/cl/utils"
)

func (b *CachingBeaconState) getSlashingProposerReward(whistleBlowerReward uint64) uint64 {
	if b.Version() == clparams.Phase0Version {
		return whistleBlowerReward / b.BeaconConfig().ProposerRewardQuotient
	}
	return whistleBlowerReward * b.BeaconConfig().ProposerWeight / b.BeaconConfig().WeightDenominator
}

func (b *CachingBeaconState) SlashValidator(slashedInd uint64, whistleblowerInd *uint64) error {
	epoch := Epoch(b)
	if err := b.InitiateValidatorExit(slashedInd); err != nil {
		return err
	}
	// Record changes in changeset
	slashingsIndex := int(epoch % b.BeaconConfig().EpochsPerSlashingsVector)

	// Change the validator to be slashed
	if err := b.SetValidatorSlashed(int(slashedInd), true); err != nil {
		return err
	}

	currentWithdrawableEpoch, err := b.ValidatorWithdrawableEpoch(int(slashedInd))
	if err != nil {
		return err
	}

	newWithdrawableEpoch := utils.Max64(currentWithdrawableEpoch, epoch+b.BeaconConfig().EpochsPerSlashingsVector)
	if err := b.SetWithdrawableEpochForValidatorAtIndex(int(slashedInd), newWithdrawableEpoch); err != nil {
		return err
	}

	// Update slashings vector
	currentEffectiveBalance, err := b.ValidatorEffectiveBalance(int(slashedInd))
	if err != nil {
		return err
	}
	b.IncrementSlashingSegmentAt(slashingsIndex, currentEffectiveBalance)
	newEffectiveBalance, err := b.ValidatorEffectiveBalance(int(slashedInd))
	if err != nil {
		return err
	}
	if err := DecreaseBalance(b, slashedInd, newEffectiveBalance/b.BeaconConfig().GetMinSlashingPenaltyQuotient(b.Version())); err != nil {
		return err
	}
	proposerInd, err := b.GetBeaconProposerIndex()
	if err != nil {
		return fmt.Errorf("unable to get beacon proposer index: %v", err)
	}
	if whistleblowerInd == nil {
		whistleblowerInd = new(uint64)
		*whistleblowerInd = proposerInd
	}
	whistleBlowerReward := newEffectiveBalance / b.BeaconConfig().WhistleBlowerRewardQuotient
	proposerReward := b.getSlashingProposerReward(whistleBlowerReward)
	if err := IncreaseBalance(b, proposerInd, proposerReward); err != nil {
		return err
	}
	return IncreaseBalance(b, *whistleblowerInd, whistleBlowerReward-proposerReward)
}

func (b *CachingBeaconState) InitiateValidatorExit(index uint64) error {
	validatorExitEpoch, err := b.ValidatorExitEpoch(int(index))
	if err != nil {
		return err
	}
	if validatorExitEpoch != b.BeaconConfig().FarFutureEpoch {
		return nil
	}

	currentEpoch := Epoch(b)
	exitQueueEpoch := ComputeActivationExitEpoch(b.BeaconConfig(), currentEpoch)
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

	var overflow bool
	var newWithdrawableEpoch uint64
	if newWithdrawableEpoch, overflow = math.SafeAdd(exitQueueEpoch, b.BeaconConfig().MinValidatorWithdrawabilityDelay); overflow {
		return fmt.Errorf("withdrawable epoch is too big")
	}
	b.SetExitEpochForValidatorAtIndex(int(index), exitQueueEpoch)
	b.SetWithdrawableEpochForValidatorAtIndex(int(index), newWithdrawableEpoch)
	return nil
}
