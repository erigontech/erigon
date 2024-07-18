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

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/utils"
	"github.com/stretchr/testify/require"
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
	validator := ValidatorFromDeposit(&clparams.MainnetBeaconConfig, &cltypes.Deposit{
		Proof: solid.NewHashList(33),
		Data: &cltypes.DepositData{
			PubKey: [48]byte{69},
			Amount: 99999,
		},
	})
	require.Equal(t, validator.PublicKey(), [48]byte{69})
}

func TestSyncReward(t *testing.T) {
	state := New(&clparams.MainnetBeaconConfig)
	utils.DecodeSSZSnappy(state, stateEncoded, int(clparams.Phase0Version))

	propReward, partReward, err := state.SyncRewards()
	require.NoError(t, err)

	require.Equal(t, partReward, uint64(0x190))
	require.Equal(t, propReward, uint64(0x39))

}
