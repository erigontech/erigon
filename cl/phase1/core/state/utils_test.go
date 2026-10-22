// Copyright 2024 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package state

import (
	"math"
	"testing"

	"github.com/erigontech/erigon/cl/abstract/mock_services"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/utils"
	"github.com/erigontech/erigon/common"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func TestValidatorSlashing(t *testing.T) {
	state := New(&clparams.MainnetBeaconConfig)
	require.NoError(t, utils.DecodeSSZSnappy(state, stateEncoded, int(clparams.Phase0Version)))
	_, err := state.SlashValidator(1, nil)
	require.NoError(t, err)
	_, err = state.SlashValidator(2, nil)
	require.NoError(t, err)

	exit, err := state.BeaconState.ValidatorExitEpoch(1)
	require.NoError(t, err)
	require.NotEqual(t, exit, uint64(math.MaxUint64))
}

func TestSlashValidatorClearsPendingBuilderPaymentsGloas(t *testing.T) {
	beaconState := New(&clparams.MainnetBeaconConfig)
	require.NoError(t, utils.DecodeSSZSnappy(beaconState, stateEncoded, int(clparams.Phase0Version)))
	beaconState.SetVersion(clparams.GloasVersion)
	pendingPayments := solid.NewVectorSSZ[*cltypes.BuilderPendingPayment](int(2 * beaconState.BeaconConfig().SlotsPerEpoch))
	pendingPayments.Set(1, &cltypes.BuilderPendingPayment{ProposerIndex: 1, Withdrawal: &cltypes.BuilderPendingWithdrawal{Amount: 10}})
	pendingPayments.Set(2, &cltypes.BuilderPendingPayment{ProposerIndex: 2, Withdrawal: &cltypes.BuilderPendingWithdrawal{Amount: 20}})
	pendingPayments.Set(3, &cltypes.BuilderPendingPayment{ProposerIndex: 1, Withdrawal: &cltypes.BuilderPendingWithdrawal{Amount: 30}})
	beaconState.SetBuilderPendingPayments(pendingPayments)

	_, err := beaconState.SlashValidator(1, nil)
	require.NoError(t, err)
	require.Zero(t, pendingPayments.Get(1).Withdrawal.Amount)
	require.Equal(t, uint64(20), pendingPayments.Get(2).Withdrawal.Amount)
	require.Zero(t, pendingPayments.Get(3).Withdrawal.Amount)
}

func TestSlashValidatorPreservesPendingBuilderPaymentsBeforeGloas(t *testing.T) {
	beaconState := New(&clparams.MainnetBeaconConfig)
	require.NoError(t, utils.DecodeSSZSnappy(beaconState, stateEncoded, int(clparams.Phase0Version)))
	pendingPayments := solid.NewVectorSSZ[*cltypes.BuilderPendingPayment](int(2 * beaconState.BeaconConfig().SlotsPerEpoch))
	pendingPayments.Set(1, &cltypes.BuilderPendingPayment{ProposerIndex: 1, Withdrawal: &cltypes.BuilderPendingWithdrawal{Amount: 10}})
	beaconState.SetBuilderPendingPayments(pendingPayments)

	_, err := beaconState.SlashValidator(1, nil)
	require.NoError(t, err)
	require.Equal(t, uint64(10), pendingPayments.Get(1).Withdrawal.Amount)
}

func TestValidatorFromDeposit(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockBeaconState := mock_services.NewMockBeaconState(ctrl)
	mockBeaconState.EXPECT().BeaconConfig().Return(&clparams.MainnetBeaconConfig).AnyTimes()
	mockBeaconState.EXPECT().Version().Return(clparams.DenebVersion).Times(1)

	validator := GetValidatorFromDeposit(mockBeaconState, [48]byte{69}, common.Hash{}, uint64(99999))
	require.Equal(t, [48]byte{69}, validator.PublicKey())
	ctrl.Finish()
}

func TestSyncReward(t *testing.T) {
	state := New(&clparams.MainnetBeaconConfig)
	require.NoError(t, utils.DecodeSSZSnappy(state, stateEncoded, int(clparams.Phase0Version)))

	propReward, partReward, err := state.SyncRewards()
	require.NoError(t, err)

	require.Equal(t, uint64(0x190), partReward)
	require.Equal(t, uint64(0x39), propReward)
}
