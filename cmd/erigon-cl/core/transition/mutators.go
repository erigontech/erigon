package transition

import (
	"fmt"

	"github.com/ledgerwatch/erigon/cmd/erigon-cl/core/state"
)

const (
	FAR_FUTURE_EPOCH                    = (1<<64 - 1)
	MAX_SEED_LOOKAHEAD                  = (1 << 2)
	MIN_PER_EPOCH_CHURN_LIMIT           = (1 << 2)
	CHURN_LIMIT_QUOTIENT                = (1 << 16)
	MIN_VALIDATOR_WITHDRAWABILITY_DELAY = (1 << 8)
	EPOCHS_PER_SLASHINGS_VECTOR         = (1 << 13)
	MIN_SLASHING_PENALTY_QUOTIENT       = (1 << 7)
	WHISTLEBLOWER_REWARD_QUOTIENT       = (1 << 9)
	PROPOSER_REWARD_QUOTIENT            = (1 << 3)
)

func IncreaseBalance(state *state.BeaconState, index, delta uint64) {
	state.Balances()[index] += delta
}

func DecreaseBalance(state *state.BeaconState, index, delta uint64) {
	curAmount := state.Balances()[index]
	if curAmount < delta {
		state.Balances()[index] = 0
		return
	}
	state.Balances()[index] -= delta
}

func ComputeActivationExitEpoch(epoch uint64) uint64 {
	return epoch + 1 + MAX_SEED_LOOKAHEAD
}

func GetValidtorChurnLimit(state *state.BeaconState) uint64 {
	inds := GetActiveValidatorIndices(state, GetEpochAtSlot(state.Slot()))
	churnLimit := len(inds) / CHURN_LIMIT_QUOTIENT
	if churnLimit > MIN_PER_EPOCH_CHURN_LIMIT {
		return uint64(churnLimit)
	}
	return MIN_PER_EPOCH_CHURN_LIMIT
}

func InitiateValidatorExit(state *state.BeaconState, index uint64) {
	validator := state.ValidatorAt(int(index))
	if validator.ExitEpoch != FAR_FUTURE_EPOCH {
		return
	}

	currentEpoch := GetEpochAtSlot(state.Slot())
	exitQueueEpoch := currentEpoch
	activationExitEpoch := ComputeActivationExitEpoch(currentEpoch)
	for _, v := range state.Validators() {
		if v.ExitEpoch != FAR_FUTURE_EPOCH {
			potentialExit := v.ExitEpoch + activationExitEpoch
			if potentialExit > exitQueueEpoch {
				exitQueueEpoch = potentialExit
			}
		}
	}

	exitQueueChurn := 0
	for _, v := range state.Validators() {
		if v.ExitEpoch == exitQueueEpoch {
			exitQueueChurn += 1
		}
	}
	if exitQueueChurn >= int(GetValidtorChurnLimit(state)) {
		exitQueueEpoch += 1
	}

	validator.ExitEpoch = exitQueueEpoch
	validator.WithdrawableEpoch = exitQueueEpoch + MIN_VALIDATOR_WITHDRAWABILITY_DELAY
}

func SlashValidator(state *state.BeaconState, slashedInd, whistleblowerInd uint64) error {
	epoch := GetEpochAtSlot(state.Slot())
	InitiateValidatorExit(state, slashedInd)
	newValidator := *state.ValidatorAt(int(slashedInd))
	newValidator.Slashed = true
	withdrawEpoch := epoch + EPOCHS_PER_SLASHINGS_VECTOR
	if newValidator.WithdrawableEpoch < withdrawEpoch {
		newValidator.WithdrawableEpoch = withdrawEpoch
	}
	state.SetValidatorAt(int(slashedInd), &newValidator)
	segmentIndex := int(epoch % EPOCHS_PER_SLASHINGS_VECTOR)
	currentSlashing := state.SlashingSegmentAt(segmentIndex)
	state.SetSlashingSegmentAt(segmentIndex, currentSlashing+newValidator.EffectiveBalance)
	DecreaseBalance(state, slashedInd, newValidator.EffectiveBalance/MIN_SLASHING_PENALTY_QUOTIENT)

	proposerInd, err := GetBeaconProposerIndex(state)
	if err != nil {
		return fmt.Errorf("unable to get beacon proposer index: %v", err)
	}
	if whistleblowerInd == 0 {
		whistleblowerInd = proposerInd
	}
	whistleBlowerReward := newValidator.EffectiveBalance / WHISTLEBLOWER_REWARD_QUOTIENT
	proposerReward := whistleBlowerReward / PROPOSER_REWARD_QUOTIENT
	IncreaseBalance(state, proposerInd, proposerReward)
	IncreaseBalance(state, whistleblowerInd, whistleBlowerReward)
	return nil
}
