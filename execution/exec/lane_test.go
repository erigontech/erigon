// Copyright 2026 The Erigon Authors
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

package exec

import (
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/tracing"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
)

func TestRecipientHasCode(t *testing.T) {
	ibs := state.New(state.NewNoopReader())
	defer ibs.Close()

	absent := common.HexToAddress("0xa1")
	eoa := common.HexToAddress("0xa2")
	contract := common.HexToAddress("0xa3")
	delegated := common.HexToAddress("0xa4")
	require.NoError(t, ibs.AddBalance(accounts.InternAddress(eoa), *uint256.NewInt(1), tracing.BalanceChangeUnspecified))
	require.NoError(t, ibs.SetCode(accounts.InternAddress(contract), []byte{0x60, 0x00}, tracing.CodeChangeUnspecified))
	require.NoError(t, ibs.SetCode(accounts.InternAddress(delegated), types.AddressToDelegation(accounts.InternAddress(contract)), tracing.CodeChangeUnspecified))

	transfer := func(to *common.Address, value uint64) types.Transaction {
		return &types.LegacyTx{CommonTx: types.CommonTx{To: to, Value: *uint256.NewInt(value), GasLimit: 21000}}
	}
	for _, tc := range []struct {
		name string
		tx   types.Transaction
		want bool
	}{
		{"contract creation", transfer(nil, 1), false},
		{"zero value to a contract", transfer(&contract, 0), false},
		{"absent account", transfer(&absent, 1), false},
		{"account without code", transfer(&eoa, 1), false},
		{"contract", transfer(&contract, 1), true},
		{"eip-7702 delegation", transfer(&delegated, 1), true},
	} {
		got, err := recipientHasCode(tc.tx, ibs)
		require.NoError(t, err, tc.name)
		require.Equal(t, tc.want, got, tc.name)
	}
}
