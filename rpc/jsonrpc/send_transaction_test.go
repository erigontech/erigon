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
	"testing"
	"time"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cmd/rpcdaemon/rpcdaemontest"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/tests/blockgen"
	"github.com/erigontech/erigon/execution/tests/mock"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/node/gointerfaces/txpoolproto"
	"github.com/erigontech/erigon/rpc/rpchelper"
	"github.com/erigontech/erigon/txnprovider/txpool/txpoolcfg"
)

// Do 1 step to start txPool
func oneBlockStep(mockSentry *mock.MockSentry, require *require.Assertions) {
	chain, err := blockgen.GenerateChain(mockSentry.ChainConfig, mockSentry.Genesis, mockSentry.Engine, mockSentry.DB, 1 /*number of blocks:*/, func(i int, b *blockgen.BlockGen) {
		b.SetCoinbase(common.Address{1})
	})
	require.NoError(err)
	err = mockSentry.InsertChain(chain)
	require.NoError(err)
}

func TestSendRawTransaction(t *testing.T) {
	if testing.Short() {
		t.Skip("too slow for testing.Short")
	}
	mockSentry := mock.MockWithTxPool(t)
	require := require.New(t)
	oneBlockStep(mockSentry, require)
	expectedValue := uint64(1234)
	txn, err := types.SignTx(types.NewTransaction(0, common.Address{1}, uint256.NewInt(expectedValue), params.TxGas, uint256.NewInt(10*common.GWei), nil), *types.LatestSignerForChainID(mockSentry.ChainConfig.ChainID), mockSentry.Key)
	require.NoError(err)
	ctx, conn := rpcdaemontest.CreateTestGrpcConn(t, mockSentry)
	txPool := txpoolproto.NewTxpoolClient(conn)
	ff := rpchelper.New(ctx, rpchelper.DefaultFiltersConfig, nil, txPool, txpoolproto.NewMiningClient(conn), func() {}, mockSentry.Log)
	api := newEthApiForTest(newBaseApiForTest(mockSentry), mockSentry.DB, txPool, nil)
	buf := bytes.NewBuffer(nil)
	err = txn.MarshalBinary(buf)
	require.NoError(err)
	txsCh, id := ff.SubscribePendingTxs(1)
	defer ff.UnsubscribePendingTxs(id)
	txHash, err := api.SendRawTransaction(ctx, buf.Bytes())
	require.NoError(err)
	select {
	case got := <-txsCh:
		require.Equal(expectedValue, got[0].GetValue().Uint64())
	case <-time.After(20 * time.Second): // Sometimes the channel times out on github actions
		t.Log("Timeout waiting for txn from channel")
		jsonTx, err := api.GetTransactionByHash(ctx, txHash)
		require.NoError(err)
		require.Equal(expectedValue, jsonTx.Value.Uint64())
	}
	//send same txn second time and expect error
	_, err = api.SendRawTransaction(ctx, buf.Bytes())
	require.Error(err)
	expectedErr := txpoolproto.ImportResult_name[int32(txpoolproto.ImportResult_ALREADY_EXISTS)] + ": " + txpoolcfg.AlreadyKnown.String()
	require.Equal(expectedErr, err.Error())
	mockSentry.ReceiveWg.Wait()
	//TODO: make propagation easy to test - now race
	//time.Sleep(time.Second)
	//sent := mockSentry.SentMessage(0)
	//require.Equal(eth.ToProto[mockSentry.MultiClient.Protocol()][eth.NewPooledTransactionHashesMsg], sent.Id)
}

func TestSendRawTransactionUnprotected(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	mockSentry := mock.MockWithTxPool(t)
	require := require.New(t)
	oneBlockStep(mockSentry, require)
	expectedTxValue := uint64(4444)
	// Create a legacy signer pre-155
	unprotectedSigner := types.MakeFrontierSigner()
	txn, err := types.SignTx(types.NewTransaction(0, common.Address{1}, uint256.NewInt(expectedTxValue), params.TxGas, uint256.NewInt(10*common.GWei), nil), *unprotectedSigner, mockSentry.Key)
	require.NoError(err)
	ctx, conn := rpcdaemontest.CreateTestGrpcConn(t, mockSentry)
	txPool := txpoolproto.NewTxpoolClient(conn)
	ff := rpchelper.New(ctx, rpchelper.DefaultFiltersConfig, nil, txPool, txpoolproto.NewMiningClient(conn), func() {}, mockSentry.Log)
	api := newEthApiForTest(newBaseApiForTest(mockSentry), mockSentry.DB, txPool, nil)
	// Enable unprotected txs flag
	api.AllowUnprotectedTxs = true
	buf := bytes.NewBuffer(nil)
	err = txn.MarshalBinary(buf)
	require.NoError(err)
	txsCh, id := ff.SubscribePendingTxs(1)
	defer ff.UnsubscribePendingTxs(id)
	txHash, err := api.SendRawTransaction(ctx, buf.Bytes())
	require.NoError(err)
	select {
	case got := <-txsCh:
		require.Equal(expectedTxValue, got[0].GetValue().Uint64())
	case <-time.After(20 * time.Second): // Sometimes the channel times out on github actions
		t.Log("Timeout waiting for txn from channel")
		jsonTx, err := api.GetTransactionByHash(ctx, txHash)
		require.NoError(err)
		require.Equal(expectedTxValue, jsonTx.Value.Uint64())
	}
}
