package state

import (
	"fmt"

	"github.com/ledgerwatch/erigon-lib/common/math"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/utils"
)

func (b *BeaconState) IncreaseBalance(index, delta uint64) error {
	currentBalance, err := b.ValidatorBalance(int(index))
	if err != nil {
		return err
	}
	return b.SetValidatorBalance(int(index), currentBalance+delta)
}

func (b *BeaconState) DecreaseBalance(index, delta uint64) error {
	currentBalance, err := b.ValidatorBalance(int(index))
	if err != nil {
		return err
	}
	var newBalance uint64
	if currentBalance >= delta {
		newBalance = currentBalance - delta
	}
	return b.SetValidatorBalance(int(index), newBalance)
}

func (b *BeaconState) ComputeActivationExitEpoch(epoch uint64) uint64 {
	return epoch + 1 + b.BeaconConfig().MaxSeedLookahead
}

func (b *BeaconState) GetValidatorChurnLimit() uint64 {
	activeIndsCount := uint64(len(b.GetActiveValidatorsIndices(b.Epoch())))
	return utils.Max64(activeIndsCount/b.BeaconConfig().ChurnLimitQuotient, b.BeaconConfig().MinPerEpochChurnLimit)
}

func (b *BeaconState) InitiateValidatorExit(index uint64) error {
	validatorExitEpoch, err := b.ValidatorExitEpoch(int(index))
	if err != nil {
		return err
	}
	if validatorExitEpoch != b.BeaconConfig().FarFutureEpoch {
		return nil
	}

	currentEpoch := b.Epoch()
	exitQueueEpoch := b.ComputeActivationExitEpoch(currentEpoch)
	b.ForEachValidator(func(v *cltypes.Validator, idx, total int) bool {
		if v.ExitEpoch != b.BeaconConfig().FarFutureEpoch && v.ExitEpoch > exitQueueEpoch {
			exitQueueEpoch = v.ExitEpoch
		}
		return true
	})

	exitQueueChurn := 0
	b.ForEachValidator(func(v *cltypes.Validator, idx, total int) bool {
		if v.ExitEpoch == exitQueueEpoch {
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

func (b *BeaconState) getSlashingProposerReward(whistleBlowerReward uint64) uint64 {
	if b.Version() == clparams.Phase0Version {
		return whistleBlowerReward / b.BeaconConfig().ProposerRewardQuotient
	}
	return whistleBlowerReward * b.BeaconConfig().ProposerWeight / b.BeaconConfig().WeightDenominator
}

func (b *BeaconState) SlashValidator(slashedInd uint64, whistleblowerInd *uint64) error {
	epoch := b.Epoch()
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
	if err := b.SetValidatorWithdrawableEpoch(int(slashedInd), newWithdrawableEpoch); err != nil {
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
	if err := b.DecreaseBalance(slashedInd, newEffectiveBalance/b.BeaconConfig().GetMinSlashingPenaltyQuotient(b.Version())); err != nil {
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
	if err := b.IncreaseBalance(proposerInd, proposerReward); err != nil {
		return err
	}
	return b.IncreaseBalance(*whistleblowerInd, whistleBlowerReward-proposerReward)
}
