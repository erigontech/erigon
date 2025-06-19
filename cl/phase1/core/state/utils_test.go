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

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon/cl/abstract/mock_services"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/utils"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func TestValidatorSlashing(t *testing.T) {
	state := New(&clparams.MainnetBeaconConfig)
	utils.DecodeSSZSnappy(state, stateEncoded, int(clparams.DenebVersion))
	_, err := state.SlashValidator(1, nil)
	require.NoError(t, err)
	_, err = state.SlashValidator(2, nil)
	require.NoError(t, err)

	exit, err := state.BeaconState.ValidatorExitEpoch(1)
	require.NoError(t, err)
	require.NotEqual(t, exit, uint64(math.MaxUint64))
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
	utils.DecodeSSZSnappy(state, stateEncoded, int(clparams.Phase0Version))

	propReward, partReward, err := state.SyncRewards()
	require.NoError(t, err)

	require.Equal(t, uint64(0x190), partReward)
	require.Equal(t, uint64(0x39), propReward)

}
