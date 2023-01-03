package transition

import (
	"testing"

	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cmd/erigon-cl/core/state"
)

const (
	testExitEpoch = 53
)

func getTestStateBalances(t *testing.T) *state.BeaconState {
	numVals := uint64(2048)
	balances := make([]uint64, numVals)
	for i := uint64(0); i < numVals; i++ {
		balances[i] = i
	}
	return state.FromBellatrixState(&cltypes.BeaconStateBellatrix{
		Balances: balances,
	})
}

func getTestStateValidators(t *testing.T, numVals int) *state.BeaconState {
	validators := make([]*cltypes.Validator, numVals)
	for i := 0; i < numVals; i++ {
		validators[i] = &cltypes.Validator{
			ActivationEpoch: 0,
			ExitEpoch:       testExitEpoch,
		}
	}
	return state.FromBellatrixState(&cltypes.BeaconStateBellatrix{
		Slot:       testExitEpoch * SLOTS_PER_EPOCH,
		Validators: validators,
	})
}

func TestIncreaseBalance(t *testing.T) {
	state := getTestStateBalances(t)
	testInd := uint64(42)
	amount := uint64(100)
	beforeBalance := state.Balances()[testInd]
	IncreaseBalance(state, testInd, amount)
	afterBalance := state.Balances()[testInd]
	if afterBalance != beforeBalance+amount {
		t.Errorf("unepected after balance: %d, before balance: %d, increase: %d", afterBalance, beforeBalance, amount)
	}
}

func TestDecreaseBalance(t *testing.T) {
	sampleState := getTestStateBalances(t)
	testInd := uint64(42)
	beforeBalance := sampleState.Balances()[testInd]

	testCases := []struct {
		description     string
		delta           uint64
		expectedBalance uint64
	}{
		{
			description:     "zero_remaining",
			delta:           beforeBalance,
			expectedBalance: 0,
		},
		{
			description:     "non_zero_remaining",
			delta:           1,
			expectedBalance: beforeBalance - 1,
		},
		{
			description:     "underflow",
			delta:           beforeBalance + 1,
			expectedBalance: 0,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			state := getTestStateBalances(t)
			DecreaseBalance(state, testInd, tc.delta)
			afterBalance := state.Balances()[testInd]
			if afterBalance != tc.expectedBalance {
				t.Errorf("unexpected resulting balance: got %d, want %d", afterBalance, tc.expectedBalance)
			}
		})
	}
}

func TestInitiatieValidatorExit(t *testing.T) {
	exitDelay := uint64(testExitEpoch + MAX_SEED_LOOKAHEAD + 1)
	testCases := []struct {
		description                string
		numValidators              uint64
		expectedExitEpoch          uint64
		expectedWithdrawlableEpoch uint64
		validator                  *cltypes.Validator
	}{
		{
			description:                "success",
			numValidators:              3,
			expectedExitEpoch:          testExitEpoch + exitDelay,
			expectedWithdrawlableEpoch: testExitEpoch + exitDelay + MIN_VALIDATOR_WITHDRAWABILITY_DELAY,
			validator: &cltypes.Validator{
				ExitEpoch:       FAR_FUTURE_EPOCH,
				ActivationEpoch: 0,
			},
		},
		{
			description:                "exit_epoch_set",
			numValidators:              3,
			expectedExitEpoch:          testExitEpoch,
			expectedWithdrawlableEpoch: testExitEpoch + MIN_VALIDATOR_WITHDRAWABILITY_DELAY,
			validator: &cltypes.Validator{
				ExitEpoch:         testExitEpoch,
				WithdrawableEpoch: testExitEpoch + MIN_VALIDATOR_WITHDRAWABILITY_DELAY,
				ActivationEpoch:   0,
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			state := getTestStateValidators(t, int(tc.numValidators))
			state.SetValidators(append(state.Validators(), tc.validator))
			testInd := uint64(len(state.Validators()) - 1)
			InitiateValidatorExit(state, testInd)
			val := state.ValidatorAt(int(testInd))
			if val.ExitEpoch != tc.expectedExitEpoch {
				t.Errorf("unexpected exit epoch: got %d, want %d", val.ExitEpoch, tc.expectedExitEpoch)
			}
			if val.WithdrawableEpoch != tc.expectedWithdrawlableEpoch {
				t.Errorf("unexpected withdrawable epoch: got %d, want %d", val.WithdrawableEpoch, tc.expectedWithdrawlableEpoch)
			}
		})
	}
}
