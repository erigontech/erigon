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

package jsonrpc

import (
	"context"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cmd/rpcdaemon/rpcdaemontest"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/kv/kvcache"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/execution/rlp"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/node/gointerfaces/txpoolproto"
	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/rpc/rpchelper"
)

// A transaction in the pending block is in a block: it is priced at
// min(tip + baseFee, feeCap), not at its fee cap, even though the pending block
// has no hash to report.
func TestGetTransactionByBlockNumberAndIndex_PendingBlockPricesWithBaseFee(t *testing.T) {
	m := execmoduletester.New(t, execmoduletester.WithTxPool())
	stateCache := kvcache.New(kvcache.DefaultCoherentConfig)

	ctx, conn := rpcdaemontest.CreateTestGrpcConn(t, m)
	txPool := txpoolproto.NewTxpoolClient(conn)
	ff := rpchelper.New(ctx, rpchelper.DefaultFiltersConfig, nil, txPool, txpoolproto.NewMiningClient(conn), func() {}, m.Log, nil)

	to := common.HexToAddress("0x1234567890123456789012345678901234567890")
	txn := &types.DynamicFeeTransaction{
		CommonTx: types.CommonTx{
			Nonce:    1,
			GasLimit: 21000,
			To:       &to,
			Value:    *uint256.NewInt(1),
			V:        *uint256.NewInt(1),
			R:        *uint256.NewInt(0x1111),
			S:        *uint256.NewInt(0x2222),
		},
		ChainID: *uint256.NewInt(1),
		TipCap:  *uint256.NewInt(10),
		FeeCap:  *uint256.NewInt(1000),
	}
	header := &types.Header{
		Number:  *uint256.NewInt(1),
		BaseFee: uint256.NewInt(7),
	}
	rlpBlock, err := rlp.EncodeToBytes(types.NewBlock(header, []types.Transaction{txn}, nil, nil, nil, nil))
	require.NoError(t, err)
	ff.HandlePendingBlock(&txpoolproto.OnPendingBlockReply{RplBlock: rlpBlock})

	api := newEthApiForTest(newBaseApiWithFiltersForTest(ff, stateCache, m), m.DB, nil, nil)
	got, err := api.GetTransactionByBlockNumberAndIndex(context.Background(), rpc.PendingBlockNumber, 0)
	require.NoError(t, err)
	require.NotNil(t, got)
	require.Nil(t, got.BlockHash)
	require.Equal(t, uint256.NewInt(17).ToBig(), got.GasPrice.ToInt())
}
