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

package jsonrpc

import (
	"bytes"
	"context"
	"math"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/erigontech/erigon/cmd/rpcdaemon/rpcdaemontest"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/db/kv/kvcache"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/tests/blockgen"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/node/gointerfaces"
	"github.com/erigontech/erigon/node/gointerfaces/txpoolproto"
	"github.com/erigontech/erigon/rpc/rpccfg"
	"github.com/erigontech/erigon/rpc/rpchelper"
)

func TestTxPoolContent(t *testing.T) {
	m := execmoduletester.New(t, execmoduletester.WithTxPool())
	require := require.New(t)
	chain, err := m.GenerateChain(1, func(i int, b *blockgen.BlockGen) {
		b.SetCoinbase(common.Address{1})
	})
	require.NoError(err)
	err = m.InsertChain(chain)
	require.NoError(err)

	ctx, conn := rpcdaemontest.CreateTestGrpcConn(t, m)
	txPool := txpoolproto.NewTxpoolClient(conn)
	ff := rpchelper.New(ctx, rpchelper.DefaultFiltersConfig, nil, txPool, txpoolproto.NewMiningClient(conn), func() {}, m.Log, nil)
	api := NewTxPoolAPI(NewBaseApi(ff, kvcache.New(kvcache.DefaultCoherentConfig), m.BlockReader, m.Engine, &rpccfg.BaseApiConfig{Dirs: m.Dirs}), m.DB, txPool)

	expectValue := uint64(1234)
	txn, err := types.SignTx(types.NewTransaction(0, common.Address{1}, uint256.NewInt(expectValue), params.TxGas, uint256.NewInt(10*common.GWei), nil), *types.LatestSignerForChainID(m.ChainConfig.ChainID), m.Key)
	require.NoError(err)

	buf := bytes.NewBuffer(nil)
	err = txn.MarshalBinary(buf)
	require.NoError(err)

	reply, err := txPool.Add(ctx, &txpoolproto.AddRequest{RlpTxs: [][]byte{buf.Bytes()}})
	require.NoError(err)
	for _, res := range reply.Imported {
		require.Equalf(txpoolproto.ImportResult_SUCCESS, res, "errors: %v", reply.Errors)
	}

	content, err := api.Content(ctx)
	require.NoError(err)

	sender := m.Address.String()
	require.Len(content["pending"][sender], 1)
	require.Equal(expectValue, content["pending"][sender]["0"].Value.ToInt().Uint64())

	status, err := api.Status(ctx)
	require.NoError(err)
	require.Len(status, 2)
	require.Equal(status["pending"], hexutil.Uint(1))
	require.Equal(status["queued"], hexutil.Uint(0))
}

type stubPoolContentClient struct {
	txpoolproto.TxpoolClient
	all    *txpoolproto.AllReply
	status *txpoolproto.StatusReply
}

func (s *stubPoolContentClient) All(context.Context, *txpoolproto.AllRequest, ...grpc.CallOption) (*txpoolproto.AllReply, error) {
	return s.all, nil
}

func (s *stubPoolContentClient) Status(context.Context, *txpoolproto.StatusRequest, ...grpc.CallOption) (*txpoolproto.StatusReply, error) {
	return s.status, nil
}

// Transactions the pool keeps in its base fee sub-pool are nonce-ready and otherwise valid, but pay
// less than the current base fee, which is what Geth reports as pending, so they are pending here too.
func TestTxPoolContentBaseFeeSubPool(t *testing.T) {
	m := execmoduletester.New(t, execmoduletester.WithTxPool())
	require := require.New(t)
	chain, err := m.GenerateChain(1, func(i int, b *blockgen.BlockGen) {
		b.SetCoinbase(common.Address{1})
	})
	require.NoError(err)
	require.NoError(m.InsertChain(chain))

	ctx, conn := rpcdaemontest.CreateTestGrpcConn(t, m)
	ff := rpchelper.New(ctx, rpchelper.DefaultFiltersConfig, nil, txpoolproto.NewTxpoolClient(conn), txpoolproto.NewMiningClient(conn), func() {}, m.Log, nil)

	signer := *types.LatestSignerForChainID(m.ChainConfig.ChainID)
	rlpOf := func(nonce uint64) []byte {
		txn, err := types.SignTx(types.NewTransaction(nonce, common.Address{1}, uint256.NewInt(1234), params.TxGas, uint256.NewInt(10*common.GWei), nil), signer, m.Key)
		require.NoError(err)
		buf := bytes.NewBuffer(nil)
		require.NoError(txn.MarshalBinary(buf))
		return buf.Bytes()
	}

	sender := gointerfaces.ConvertAddressToH160(m.Address)
	pool := &stubPoolContentClient{
		all: &txpoolproto.AllReply{Txs: []*txpoolproto.AllReply_Tx{
			{TxnType: txpoolproto.AllReply_PENDING, Sender: sender, RlpTx: rlpOf(0)},
			{TxnType: txpoolproto.AllReply_PENDING, Sender: sender, RlpTx: rlpOf(1)},
			{TxnType: txpoolproto.AllReply_BASE_FEE, Sender: sender, RlpTx: rlpOf(2)},
			{TxnType: txpoolproto.AllReply_QUEUED, Sender: sender, RlpTx: rlpOf(4)},
		}},
		status: &txpoolproto.StatusReply{PendingCount: 2, BaseFeeCount: 1, QueuedCount: 1},
	}
	api := NewTxPoolAPI(NewBaseApi(ff, kvcache.New(kvcache.DefaultCoherentConfig), m.BlockReader, m.Engine, &rpccfg.BaseApiConfig{Dirs: m.Dirs}), m.DB, pool)

	addr := m.Address.String()

	content, err := api.Content(ctx)
	require.NoError(err)
	require.Len(content, 2)
	require.Len(content["pending"][addr], 3)
	require.Contains(content["pending"][addr], "2")
	require.Len(content["queued"][addr], 1)
	require.Contains(content["queued"][addr], "4")

	contentFrom, err := api.ContentFrom(ctx, m.Address)
	require.NoError(err)
	require.Len(contentFrom, 2)
	require.Len(contentFrom["pending"], 3)
	require.Contains(contentFrom["pending"], "2")
	require.Len(contentFrom["queued"], 1)
	require.Contains(contentFrom["queued"], "4")

	status, err := api.Status(ctx)
	require.NoError(err)
	require.Len(status, 2)
	require.Equal(hexutil.Uint(3), status["pending"])
	require.Equal(hexutil.Uint(1), status["queued"])
}

func TestTxPoolStatusSumsCountsWithoutWrapping(t *testing.T) {
	require := require.New(t)
	pool := &stubPoolContentClient{
		status: &txpoolproto.StatusReply{PendingCount: math.MaxUint32, BaseFeeCount: 1, QueuedCount: 0},
	}
	api := NewTxPoolAPI(nil, nil, pool)

	status, err := api.Status(context.Background())
	require.NoError(err)
	require.Equal(hexutil.Uint(math.MaxUint32)+1, status["pending"])
}
