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
	consolidationSourceIndex = 1
	consolidationTargetIndex = 2
	sourceBalance            = 40_000_000_000
	sourceEffectiveBalance   = 32_000_000_000
)

// consolidationState wires up a single pending consolidation whose source is
// eligible, leaving the balance moves to the caller's expectations.
func consolidationState(t *testing.T, ctrl *gomock.Controller) *mock_services.MockBeaconState {
	t.Helper()

	cfg := &clparams.MainnetBeaconConfig
	source := solid.NewValidator()
	source.SetSlashed(false)
	source.SetWithdrawableEpoch(0)
	source.SetEffectiveBalance(sourceEffectiveBalance)

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
	s.EXPECT().ValidatorBalance(consolidationSourceIndex).Return(uint64(sourceBalance), nil).AnyTimes()
	return s
}

func TestProcessPendingConsolidationsRestoresTheSourceWhenTheTargetIncreaseFails(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	increaseErr := errors.New("target balance unavailable")
	s := consolidationState(t, ctrl)

	s.EXPECT().SetValidatorBalance(consolidationSourceIndex, uint64(sourceBalance-sourceEffectiveBalance)).Return(nil)
	s.EXPECT().ValidatorBalance(consolidationTargetIndex).Return(uint64(0), increaseErr)
	// The rollback: without it the source keeps the debit that was never credited.
	s.EXPECT().SetValidatorBalance(consolidationSourceIndex, uint64(sourceBalance)).Return(nil)

	err := statechange.ProcessPendingConsolidations(s)
	require.ErrorIs(t, err, increaseErr)
}

func TestProcessPendingConsolidationsReportsAFailedRollback(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	increaseErr := errors.New("target balance unavailable")
	rollbackErr := errors.New("source restore rejected")
	s := consolidationState(t, ctrl)

	s.EXPECT().SetValidatorBalance(consolidationSourceIndex, uint64(sourceBalance-sourceEffectiveBalance)).Return(nil)
	s.EXPECT().ValidatorBalance(consolidationTargetIndex).Return(uint64(0), increaseErr)
	s.EXPECT().SetValidatorBalance(consolidationSourceIndex, uint64(sourceBalance)).Return(rollbackErr)

	err := statechange.ProcessPendingConsolidations(s)
	require.ErrorIs(t, err, increaseErr)
	require.ErrorIs(t, err, rollbackErr)
}
