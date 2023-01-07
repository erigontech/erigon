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

func TestSlashValidator(t *testing.T) {
	slashedInd := 567
	whistleblowerInd := 678

	successState := getTestState(t)
	successState.SetSlashings([]uint64{0})

	successBalances := []uint64{}
	wantBalances := []uint64{}
	for i := 0; i < len(successState.Validators()); i++ {
		successBalances = append(successBalances, uint64(i+1))
		wantBalances = append(wantBalances, uint64(i+1))
	}
	successState.SetBalances(successBalances)

	// Set up slashed balance.
	preSlashBalance := uint64(1 << 20)
	successState.Balances()[slashedInd] = preSlashBalance
	successState.ValidatorAt(slashedInd).EffectiveBalance = preSlashBalance
	wantBalances[slashedInd] = preSlashBalance - (preSlashBalance / MIN_SLASHING_PENALTY_QUOTIENT)

	// Set up whistleblower & validator balances.
	wbReward := preSlashBalance / WHISTLEBLOWER_REWARD_QUOTIENT
	wantBalances[whistleblowerInd] += wbReward
	valInd, err := GetBeaconProposerIndex(successState)
	if err != nil {
		t.Fatalf("unable to get proposer index for test state: %v", err)
	}
	wantBalances[valInd] += wbReward / PROPOSER_REWARD_QUOTIENT

	failState := getTestState(t)
	failState.SetSlashings([]uint64{0})
	for _, v := range failState.Validators() {
		v.ExitEpoch = 0
	}
	failState.SetBalances(successBalances)

	testCases := []struct {
		description  string
		state        *state.BeaconState
		wantBalances []uint64
		wantErr      bool
	}{
		{
			description:  "success",
			state:        successState,
			wantBalances: wantBalances,
			wantErr:      false,
		},
		{
			description: "fail_no_active_validators",
			state:       failState,
			wantErr:     true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			err := SlashValidator(tc.state, uint64(slashedInd), uint64(whistleblowerInd))
			if tc.wantErr {
				if err == nil {
					t.Errorf("unexpected success, wantErr is true")
				}
				return
			}

			if err != nil {
				t.Errorf("unexpected error, wanted success: %v", err)
			}
			// Check balances.
			for i, bal := range tc.wantBalances {
				if bal != tc.state.Balances()[i] {
					t.Errorf("unexpected balance for index: %d, want %d: got %d", i, bal, tc.state.Balances()[i])
				}
			}
			// Check that the validator is slashed.
			if !tc.state.ValidatorAt(slashedInd).Slashed {
				t.Errorf("slashed index validator not set as slashed")
			}
		})
	}
}
