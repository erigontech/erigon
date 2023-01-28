package state

import (
	"fmt"
)

func (b *BeaconState) IncreaseBalance(index int, delta uint64) {
	b.SetValidatorBalance(index, b.balances[index]+delta)
}

func (b *BeaconState) DecreaseBalance(index, delta uint64) {
	curAmount := b.balances[index]
	var newBalance uint64
	if curAmount >= delta {
		newBalance = curAmount - delta
	}
	b.SetValidatorBalance(int(index), newBalance)
}

func (b *BeaconState) ComputeActivationExitEpoch(epoch uint64) uint64 {
	return epoch + 1 + b.beaconConfig.MaxSeedLookahead
}

func (b *BeaconState) GetValidatorChurnLimit() uint64 {
	inds := b.GetActiveValidatorsIndices(b.Epoch())
	churnLimit := uint64(len(inds)) / b.beaconConfig.ChurnLimitQuotient
	if churnLimit > b.beaconConfig.MinPerEpochChurnLimit {
		return churnLimit
	}
	return b.beaconConfig.MinPerEpochChurnLimit
}

func (b *BeaconState) InitiateValidatorExit(index uint64) {
	validator := b.validators[index]
	if validator.ExitEpoch != b.beaconConfig.FarFutureEpoch {
		return
	}

	currentEpoch := b.Epoch()
	exitQueueEpoch := currentEpoch
	activationExitEpoch := b.ComputeActivationExitEpoch(currentEpoch)
	for _, v := range b.validators {
		if v.ExitEpoch != b.beaconConfig.FarFutureEpoch {
			potentialExit := v.ExitEpoch + activationExitEpoch
			if potentialExit > exitQueueEpoch {
				exitQueueEpoch = potentialExit
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
	b.SetValidatorAt(int(index), validator)
}

func (b *BeaconState) SlashValidator(slashedInd, whistleblowerInd uint64) error {
	epoch := b.Epoch()
	b.InitiateValidatorExit(slashedInd)
	newValidator := b.validators[slashedInd]
	newValidator.Slashed = true
	withdrawEpoch := epoch + b.beaconConfig.EpochsPerSlashingsVector
	if newValidator.WithdrawableEpoch < withdrawEpoch {
		newValidator.WithdrawableEpoch = withdrawEpoch
	}
	b.SetValidatorAt(int(slashedInd), newValidator)
	segmentIndex := int(epoch % b.beaconConfig.EpochsPerSlashingsVector)
	currentSlashing := b.SlashingSegmentAt(segmentIndex)
	b.SetSlashingSegmentAt(segmentIndex, currentSlashing+newValidator.EffectiveBalance)
	b.DecreaseBalance(slashedInd, newValidator.EffectiveBalance/b.beaconConfig.MinSlashingPenaltyQuotient)

	proposerInd, err := b.GetBeaconProposerIndex()
	if err != nil {
		return fmt.Errorf("unable to get beacon proposer index: %v", err)
	}
	if whistleblowerInd == 0 {
		whistleblowerInd = proposerInd
	}
	whistleBlowerReward := newValidator.EffectiveBalance / b.beaconConfig.WhistleBlowerRewardQuotient
	proposerReward := whistleBlowerReward / b.beaconConfig.ProposerRewardQuotient
	b.IncreaseBalance(int(proposerInd), proposerReward)
	b.IncreaseBalance(int(whistleblowerInd), whistleBlowerReward)
	return nil
}
