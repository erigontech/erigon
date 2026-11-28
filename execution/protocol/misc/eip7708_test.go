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

package misc

import (
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/tracing"
	"github.com/erigontech/erigon/execution/types/accounts"
)

func TestTransferLogOverwritesAReusedLogEntry(t *testing.T) {
	ibs := state.New(state.NewNoopReader())
	defer ibs.Close()
	from, to := common.HexToAddress("0xaa"), common.HexToAddress("0xbb")
	rules := &chain.Rules{IsAmsterdam: true}
	transfer := func(amount uint256.Int) {
		ibs.Reset()
		ibs.SetTxContext(1, 0)
		require.NoError(t, ibs.AddBalance(accounts.InternAddress(from), amount, tracing.BalanceChangeUnspecified))
		require.NoError(t, Transfer(ibs, accounts.InternAddress(from), accounts.InternAddress(to), amount, false, rules))
	}
	var max uint256.Int
	max.SetAllOne()
	transfer(max)
	transfer(*uint256.NewInt(5))

	logs := ibs.GetRawLogs(0)
	require.Len(t, logs, 1)
	require.Equal(t, params.SystemAddress.Value(), logs[0].Address)
	require.Equal(t, []common.Hash{EthTransferLogEvent, from.Hash(), to.Hash()}, logs[0].Topics)
	five := uint256.NewInt(5).Bytes32()
	require.Equal(t, five[:], []byte(logs[0].Data))
}
