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

package sequencer

import (
	"context"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/eez/protocol"
	"github.com/erigontech/erigon/eez/systemtx"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/txnprovider"
)

type fixed []types.Transaction

func (f fixed) ProvideTxns(context.Context, ...txnprovider.ProvideOption) ([]types.Transaction, error) {
	return append([]types.Transaction(nil), f...), nil
}

func transfer(t *testing.T, nonce uint64, data int, sender common.Address) types.Transaction {
	t.Helper()
	to := common.HexToAddress("0xb0b")
	tx := &types.DynamicFeeTransaction{CommonTx: types.CommonTx{Nonce: nonce, GasLimit: 100_000, To: &to, Data: make([]byte, data)}, ChainID: *uint256.NewInt(6290)}
	tx.SetSender(accounts.InternAddress(sender))
	return tx
}

// TestPoolTxns is T-SEQ-2 for the pool: no blob transaction, no EEZ system
// type or sender, and the block's DA within the budget, a large transaction
// leaving room for smaller ones after it.
func TestPoolTxns(t *testing.T) {
	user := common.HexToAddress("0xa11ce")
	system, err := systemtx.New(protocol.SystemTx{ChainID: 6290, To: protocol.EEZL2Address})
	require.NoError(t, err)
	small, large, last := transfer(t, 0, 10, user), transfer(t, 1, 900, user), transfer(t, 2, 10, user)
	pool := fixed{
		&types.BlobTx{DynamicFeeTransaction: types.DynamicFeeTransaction{ChainID: *uint256.NewInt(6290)}},
		system,
		transfer(t, 0, 0, protocol.SystemAddress),
		small, large, last,
	}
	budget := small.EncodingSize() + last.EncodingSize() + 2*txOverhead
	got, err := newPoolTxns(pool, budget, log.New()).ProvideTxns(t.Context())
	require.NoError(t, err)
	require.Equal(t, []types.Transaction{small, last}, got)

	got, err = newPoolTxns(pool, 1<<20, log.New()).ProvideTxns(t.Context())
	require.NoError(t, err)
	require.Equal(t, []types.Transaction{small, large, last}, got)
}

func TestDABudget(t *testing.T) {
	// A 6-block slot of full blocks fits one batch at the floor price.
	require.Equal(t, 65_738, DABudget(6))
	require.LessOrEqual(t, uint64(6*DABudget(6))*floorGasPerByte+batchReserve, uint64(maxBatchGas))
}
