package transition

import (
	"github.com/ledgerwatch/erigon/cmd/erigon-cl/core/state"
)

const (
	FAR_FUTURE_EPOCH                    = (1<<64 - 1)
	MAX_SEED_LOOKAHEAD                  = 4
	MIN_PER_EPOCH_CHURN_LIMIT           = 4
	CHURN_LIMIT_QUOTIENT                = (1 << 16)
	MIN_VALIDATOR_WITHDRAWABILITY_DELAY = (1 << 8)
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
