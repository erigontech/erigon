package state

import (
	"fmt"

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
	return epoch + 1 + b.beaconConfig.MaxSeedLookahead
}

func (b *BeaconState) GetValidatorChurnLimit() uint64 {
	activeIndsCount := uint64(len(b.GetActiveValidatorsIndices(b.Epoch())))
	return utils.Max64(activeIndsCount/b.beaconConfig.ChurnLimitQuotient, b.beaconConfig.MinPerEpochChurnLimit)
}

func (b *BeaconState) InitiateValidatorExit(index uint64) error {
	validator, err := b.ValidatorAt(int(index))
	if err != nil {
		return err
	}
	if validator.ExitEpoch != b.beaconConfig.FarFutureEpoch {
		return nil
	}

	currentEpoch := b.Epoch()
	exitQueueEpoch := b.ComputeActivationExitEpoch(currentEpoch)
	for _, v := range b.validators {
		if v.ExitEpoch != b.beaconConfig.FarFutureEpoch {
			if v.ExitEpoch > exitQueueEpoch {
				exitQueueEpoch = v.ExitEpoch
			}
		}
	}

	exitQueueChurn := 0
	for _, v := range b.validators {
		if v.ExitEpoch == exitQueueEpoch {
			exitQueueChurn += 1
		}
	}
	if exitQueueChurn >= int(b.GetValidatorChurnLimit()) {
		exitQueueEpoch += 1
	}

	validator.ExitEpoch = exitQueueEpoch
	validator.WithdrawableEpoch = exitQueueEpoch + b.beaconConfig.MinValidatorWithdrawabilityDelay
	return b.SetValidatorAt(int(index), &validator)
}

func (b *BeaconState) SlashValidator(slashedInd, whistleblowerInd uint64) error {
	epoch := b.Epoch()
	if err := b.InitiateValidatorExit(slashedInd); err != nil {
		return err
	}
	newValidator := b.validators[slashedInd]
	newValidator.Slashed = true
	withdrawEpoch := epoch + b.beaconConfig.EpochsPerSlashingsVector
	if newValidator.WithdrawableEpoch < withdrawEpoch {
		newValidator.WithdrawableEpoch = withdrawEpoch
	}
	if err := b.SetValidatorAt(int(slashedInd), newValidator); err != nil {
		return err
	}
	segmentIndex := int(epoch % b.beaconConfig.EpochsPerSlashingsVector)
	currentSlashing := b.SlashingSegmentAt(segmentIndex)
	b.SetSlashingSegmentAt(segmentIndex, currentSlashing+newValidator.EffectiveBalance)
	if err := b.DecreaseBalance(slashedInd, newValidator.EffectiveBalance/b.beaconConfig.MinSlashingPenaltyQuotient); err != nil {
		return err
	}

	proposerInd, err := b.GetBeaconProposerIndex()
	if err != nil {
		return fmt.Errorf("unable to get beacon proposer index: %v", err)
	}
	if whistleblowerInd == 0 {
		whistleblowerInd = proposerInd
	}
	whistleBlowerReward := newValidator.EffectiveBalance / b.beaconConfig.WhistleBlowerRewardQuotient
	proposerReward := whistleBlowerReward / b.beaconConfig.ProposerRewardQuotient
	if err := b.IncreaseBalance(proposerInd, proposerReward); err != nil {
		return err
	}
	return b.IncreaseBalance(whistleblowerInd, whistleBlowerReward)
}
