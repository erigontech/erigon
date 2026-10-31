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
	"bytes"
	"context"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/erigontech/erigon/cmd/rpcdaemon/rpcdaemontest"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/node/gointerfaces/txpoolproto"
)

type onePendingTxPool struct {
	stubTxPoolClient
	rlp []byte
}

func (p onePendingTxPool) Transactions(_ context.Context, in *txpoolproto.TransactionsRequest, _ ...grpc.CallOption) (*txpoolproto.TransactionsReply, error) {
	reply := &txpoolproto.TransactionsReply{RlpTxs: make([][]byte, len(in.Hashes))}
	for i := range in.Hashes {
		reply.RlpTxs[i] = p.rlp
	}
	return reply, nil
}

func TestGetRawTransactionByHashReturnsPoolTransaction(t *testing.T) {
	m, _, _ := rpcdaemontest.CreateTestExecModule(t)
	txn, err := types.SignTx(types.NewTransaction(1000, common.Address{1}, uint256.NewInt(1), params.TxGas, uint256.NewInt(common.GWei), nil), *types.LatestSignerForChainID(m.ChainConfig.ChainID), m.Key)
	require.NoError(t, err)
	var buf bytes.Buffer
	require.NoError(t, txn.MarshalBinary(&buf))

	api := newEthApiForTest(newBaseApiForTest(m), m.DB, onePendingTxPool{rlp: buf.Bytes()}, nil)

	pending, err := api.GetTransactionByHash(context.Background(), txn.Hash())
	require.NoError(t, err)
	require.NotNil(t, pending, "eth_getTransactionByHash sees the pool transaction")

	raw, err := api.GetRawTransactionByHash(context.Background(), txn.Hash())
	require.NoError(t, err)
	require.Equal(t, hexutil.Bytes(buf.Bytes()), raw, "eth_getRawTransactionByHash must return the pool transaction too")
}
