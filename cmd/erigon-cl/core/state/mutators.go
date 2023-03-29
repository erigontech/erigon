package state

import (
	"fmt"

	"github.com/ledgerwatch/erigon-lib/common/math"
	"github.com/ledgerwatch/erigon/cl/clparams"
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
	if b.validators[index].ExitEpoch != b.beaconConfig.FarFutureEpoch {
		return nil
	}

	currentEpoch := b.Epoch()
	exitQueueEpoch := b.ComputeActivationExitEpoch(currentEpoch)
	for _, v := range b.validators {
		if v.ExitEpoch != b.beaconConfig.FarFutureEpoch && v.ExitEpoch > exitQueueEpoch {
			exitQueueEpoch = v.ExitEpoch
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

	var overflow bool
	var newWithdrawableEpoch uint64
	if newWithdrawableEpoch, overflow = math.SafeAdd(exitQueueEpoch, b.beaconConfig.MinValidatorWithdrawabilityDelay); overflow {
		return fmt.Errorf("withdrawable epoch is too big")
	}
	b.SetExitEpochForValidatorAtIndex(int(index), exitQueueEpoch)
	b.SetWithdrawableEpochForValidatorAtIndex(int(index), newWithdrawableEpoch)
	return nil
}

func (b *BeaconState) getSlashingProposerReward(whistleBlowerReward uint64) uint64 {
	if b.version == clparams.Phase0Version {
		return whistleBlowerReward / b.beaconConfig.ProposerRewardQuotient
	}
	return whistleBlowerReward * b.beaconConfig.ProposerWeight / b.beaconConfig.WeightDenominator
}

func (b *BeaconState) SlashValidator(slashedInd uint64, whistleblowerInd *uint64) error {
	epoch := b.Epoch()
	if err := b.InitiateValidatorExit(slashedInd); err != nil {
		return err
	}
	// Record changes in changeset
	slashingsIndex := int(epoch % b.beaconConfig.EpochsPerSlashingsVector)
	if b.reverseChangeset != nil {
		b.reverseChangeset.SlashedChange.AddChange(int(slashedInd), b.validators[slashedInd].Slashed)
		b.reverseChangeset.WithdrawalEpochChange.AddChange(int(slashedInd), b.validators[slashedInd].WithdrawableEpoch)
		b.reverseChangeset.SlashingsChanges.AddChange(slashingsIndex, b.slashings[slashingsIndex])
	}

	// Change the validator to be slashed
	b.validators[slashedInd].Slashed = true
	b.validators[slashedInd].WithdrawableEpoch = utils.Max64(b.validators[slashedInd].WithdrawableEpoch, epoch+b.beaconConfig.EpochsPerSlashingsVector)
	b.touchedLeaves[ValidatorsLeafIndex] = true
	// Update slashings vector
	b.slashings[slashingsIndex] += b.validators[slashedInd].EffectiveBalance
	b.touchedLeaves[SlashingsLeafIndex] = true
	if err := b.DecreaseBalance(slashedInd, b.validators[slashedInd].EffectiveBalance/b.beaconConfig.GetMinSlashingPenaltyQuotient(b.version)); err != nil {
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

	whistleBlowerReward := b.validators[slashedInd].EffectiveBalance / b.beaconConfig.WhistleBlowerRewardQuotient
	proposerReward := b.getSlashingProposerReward(whistleBlowerReward)
	if err := b.IncreaseBalance(proposerInd, proposerReward); err != nil {
		return err
	}
	return b.IncreaseBalance(*whistleblowerInd, whistleBlowerReward-proposerReward)
}
