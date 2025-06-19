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

package state_accessors

import (
	"bytes"
	"testing"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/stretchr/testify/require"
)

func TestStaticValidatorTable(t *testing.T) {
	// Make 5 validators.
	vals := []solid.Validator{solid.NewValidator(), solid.NewValidator(), solid.NewValidator(), solid.NewValidator(), solid.NewValidator()}

	table := NewStaticValidatorTable()
	for i := range vals {
		require.NoError(t, table.AddValidator(vals[i], uint64(i), 0))
	}
	// Now let us play with this
	require.NoError(t, table.AddExitEpoch(1, 32, 34))
	require.NoError(t, table.AddExitEpoch(1, 38, 35))
	require.NoError(t, table.AddExitEpoch(1, 2000, 500))

	require.Equal(t, uint64(0), table.ExitEpoch(1, 31))
	require.Equal(t, uint64(34), table.ExitEpoch(1, 32))
	require.Equal(t, uint64(34), table.ExitEpoch(1, 37))
	require.Equal(t, uint64(35), table.ExitEpoch(1, 38))
	require.Equal(t, uint64(35), table.ExitEpoch(1, 450))
	require.Equal(t, uint64(500), table.ExitEpoch(1, 1_000_000))
	// do the same for withdrawable epoch
	require.NoError(t, table.AddWithdrawableEpoch(1, 32, 34))
	require.NoError(t, table.AddWithdrawableEpoch(1, 38, 35))
	require.NoError(t, table.AddWithdrawableEpoch(1, 2000, 500))

	require.Equal(t, uint64(0), table.WithdrawableEpoch(1, 31))
	require.Equal(t, uint64(34), table.WithdrawableEpoch(1, 32))
	require.Equal(t, uint64(34), table.WithdrawableEpoch(1, 37))
	require.Equal(t, uint64(35), table.WithdrawableEpoch(1, 38))
	require.Equal(t, uint64(35), table.WithdrawableEpoch(1, 450))
	require.Equal(t, uint64(500), table.WithdrawableEpoch(1, 1_000_000))
	// now for withdrawal credentials
	require.NoError(t, table.AddWithdrawalCredentials(1, 32, common.HexToHash("0x2")))
	require.NoError(t, table.AddWithdrawalCredentials(1, 38, common.HexToHash("0x3")))
	require.NoError(t, table.AddWithdrawalCredentials(1, 2000, common.HexToHash("0x40")))
	require.Equal(t, common.Hash{}, table.WithdrawalCredentials(1, 31))
	require.Equal(t, table.WithdrawalCredentials(1, 32), common.HexToHash("0x2"))
	require.Equal(t, table.WithdrawalCredentials(1, 37), common.HexToHash("0x2"))
	require.Equal(t, table.WithdrawalCredentials(1, 38), common.HexToHash("0x3"))
	require.Equal(t, table.WithdrawalCredentials(1, 450), common.HexToHash("0x3"))
	require.Equal(t, table.WithdrawalCredentials(1, 1_000_000), common.HexToHash("0x40"))
	// Now lets try to get the validator at a specific epoch
	new := solid.NewValidator()
	table.GetInPlace(1, 38, new)
	require.Equal(t, uint64(35), new.WithdrawableEpoch())
	require.Equal(t, uint64(35), new.ExitEpoch())
	require.Equal(t, new.WithdrawalCredentials(), common.HexToHash("0x3"))
	// Lastly serialize and deserialization
	table.ForEach(func(validatorIndex uint64, validator *StaticValidator) bool {
		var b bytes.Buffer
		require.NoError(t, validator.WriteTo(&b))
		tmp := &StaticValidator{}
		require.NoError(t, tmp.ReadFrom(&b))
		require.Equal(t, validator, tmp)
		return true
	})
}
