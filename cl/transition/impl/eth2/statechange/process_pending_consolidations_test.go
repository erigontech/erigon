package statechange_test

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/erigontech/erigon/cl/abstract/mock_services"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/transition/impl/eth2/statechange"
)

const (
	consolidationSourceIndex    = 1
	consolidationTargetIndex    = 2
	consolidationSourceBalance  = 40_000_000_000
	consolidationMovedBalance   = 32_000_000_000
	consolidationDebitedBalance = consolidationSourceBalance - consolidationMovedBalance
)

// errUnknownBalance stands in for the target read that fails, which is what
// drives ProcessPendingConsolidations into its rollback.
var errUnknownBalance = errors.New("validator balance unavailable")

// consolidationState wires up a single pending consolidation whose source is
// eligible. Balances are backed by the map, so the mock answers reads with what
// earlier writes stored and the tests can assert the balance itself. Only the
// source has a balance: the missing target is what makes IncreaseBalance fail.
// setErr, when set, rejects the write that restores the source.
func consolidationState(
	t *testing.T,
	ctrl *gomock.Controller,
	balances map[int]uint64,
	setErr error,
) *mock_services.MockBeaconState {
	t.Helper()

	cfg := &clparams.MainnetBeaconConfig
	source := solid.NewValidator()
	source.SetSlashed(false)
	source.SetWithdrawableEpoch(0)
	source.SetEffectiveBalance(consolidationMovedBalance)

	consolidations := solid.NewPendingConsolidationList(cfg)
	consolidations.Append(&solid.PendingConsolidation{
		SourceIndex: consolidationSourceIndex,
		TargetIndex: consolidationTargetIndex,
	})

	s := mock_services.NewMockBeaconState(ctrl)
	s.EXPECT().BeaconConfig().Return(cfg).AnyTimes()
	s.EXPECT().Slot().Return(uint64(0)).AnyTimes()
	s.EXPECT().GetPendingConsolidations().Return(consolidations).AnyTimes()
	s.EXPECT().ValidatorForValidatorIndex(consolidationSourceIndex).Return(source, nil).AnyTimes()
	s.EXPECT().ValidatorBalance(gomock.Any()).DoAndReturn(func(index int) (uint64, error) {
		balance, ok := balances[index]
		if !ok {
			return 0, errUnknownBalance
		}
		return balance, nil
	}).AnyTimes()
	s.EXPECT().SetValidatorBalance(gomock.Any(), gomock.Any()).DoAndReturn(func(index int, balance uint64) error {
		if setErr != nil && index == consolidationSourceIndex && balance == consolidationSourceBalance {
			return setErr
		}
		balances[index] = balance
		return nil
	}).AnyTimes()
	return s
}

func TestProcessPendingConsolidationsRestoresTheSourceWhenTheTargetIncreaseFails(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	balances := map[int]uint64{consolidationSourceIndex: consolidationSourceBalance}
	s := consolidationState(t, ctrl, balances, nil)

	err := statechange.ProcessPendingConsolidations(s)
	require.ErrorIs(t, err, errUnknownBalance)
	// Without the rollback the source keeps a debit that was never credited.
	require.Equal(t, uint64(consolidationSourceBalance), balances[consolidationSourceIndex])
}

func TestProcessPendingConsolidationsReportsAFailedRollback(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	rollbackErr := errors.New("source restore rejected")
	balances := map[int]uint64{consolidationSourceIndex: consolidationSourceBalance}
	s := consolidationState(t, ctrl, balances, rollbackErr)

	err := statechange.ProcessPendingConsolidations(s)
	require.ErrorIs(t, err, errUnknownBalance)
	require.ErrorIs(t, err, rollbackErr)
	require.Equal(t, uint64(consolidationDebitedBalance), balances[consolidationSourceIndex])
}
