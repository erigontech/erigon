// Copyright 2025 The Erigon Authors
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

package engineapi

import (
	"bytes"
	"math/big"
	"testing"
	"time"

	// "github.com/holiman/uint256"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon-lib/common"
	sentry "github.com/erigontech/erigon-lib/gointerfaces/sentryproto"
	txpool "github.com/erigontech/erigon-lib/gointerfaces/txpoolproto"
	txpool_proto "github.com/erigontech/erigon-lib/gointerfaces/txpoolproto"
	"github.com/erigontech/erigon-lib/kv/kvcache"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon-lib/rlp"
	"github.com/erigontech/erigon-lib/wrap"
	"github.com/erigontech/erigon/cmd/rpcdaemon/rpcdaemontest"
	"github.com/erigontech/erigon/core"
	"github.com/erigontech/erigon/core/types"
	"github.com/erigontech/erigon/core/types/typestest"
	"github.com/erigontech/erigon/eth/ethconfig"
	"github.com/erigontech/erigon/eth/protocols/eth"

	// "github.com/erigontech/erigon/params"
	"github.com/erigontech/erigon/rpc/rpccfg"
	"github.com/erigontech/erigon/turbo/jsonrpc"
	"github.com/erigontech/erigon/turbo/rpchelper"
	"github.com/erigontech/erigon/turbo/stages"
	"github.com/erigontech/erigon/turbo/stages/mock"
	"github.com/erigontech/erigon/txnprovider/txpool/txpoolcfg"
)

// Do 1 step to start txPool
func oneBlockStep(mockSentry *mock.MockSentry, require *require.Assertions, t *testing.T) {

	// TODO change the genesis with added account balance
	chain, err := core.GenerateChain(mockSentry.ChainConfig, mockSentry.Genesis, mockSentry.Engine, mockSentry.DB, 1 /*number of blocks:*/, func(i int, b *core.BlockGen) {
		b.SetCoinbase(common.Address{1})
	})
	require.NoError(err)

	// Send NewBlock message
	b, err := rlp.EncodeToBytes(&eth.NewBlockPacket{
		Block: chain.TopBlock,
		TD:    big.NewInt(1), // This is ignored anyway
	})
	require.NoError(err)

	mockSentry.ReceiveWg.Add(1)
	for _, err = range mockSentry.Send(&sentry.InboundMessage{Id: sentry.MessageId_NEW_BLOCK_66, Data: b, PeerId: mockSentry.PeerId}) {
		require.NoError(err)
	}
	// Send all the headers
	b, err = rlp.EncodeToBytes(&eth.BlockHeadersPacket66{
		RequestId:          1,
		BlockHeadersPacket: chain.Headers,
	})
	require.NoError(err)
	mockSentry.ReceiveWg.Add(1)
	for _, err = range mockSentry.Send(&sentry.InboundMessage{Id: sentry.MessageId_BLOCK_HEADERS_66, Data: b, PeerId: mockSentry.PeerId}) {
		require.NoError(err)
	}
	mockSentry.ReceiveWg.Wait() // Wait for all messages to be processed before we proceed

	initialCycle, firstCycle := mock.MockInsertAsInitialCycle, false
	if err := stages.StageLoopIteration(mockSentry.Ctx, mockSentry.DB, wrap.TxContainer{}, mockSentry.Sync, initialCycle, firstCycle, log.New(), mockSentry.BlockReader, nil); err != nil {
		t.Fatal(err)
	}
}

func newBaseApiForTest(m *mock.MockSentry) *jsonrpc.BaseAPI {
	stateCache := kvcache.New(kvcache.DefaultCoherentConfig)
	return jsonrpc.NewBaseApi(nil, stateCache, m.BlockReader, false, rpccfg.DefaultEvmCallTimeout, m.Engine, m.Dirs, nil)
}

func TestGetBlobsV1(t *testing.T) {
	mockSentry, require := mock.MockWithTxPool(t), require.New(t)
	expectedValue := uint64(1234)
	logger := log.New()

	oneBlockStep(mockSentry, require, t)

	// // TODO change it to blob txn
	// txn, err := types.SignTx(types.NewTransaction(0, common.Address{1}, uint256.NewInt(expectedValue), params.TxGas, uint256.NewInt(10*params.GWei), nil), *types.LatestSignerForChainID(mockSentry.ChainConfig.ChainID), mockSentry.Key)

	wrappedTxn := types.BlobTxWrapper{}
	buf := bytes.NewBuffer(nil)
	wrapperRlp, _ := typestest.MakeBlobTxnRlp()
	
	wrappedTxn.DecodeRLP(rlp.NewStream(bytes.NewReader(wrapperRlp[1:]), uint64(len(wrapperRlp) )))
	wrappedTxn.Tx.ChainID = uint256.MustFromBig(mockSentry.ChainConfig.ChainID)
	wrappedTxn.Tx.Gas = 100_000
	txn, err := types.SignTx(&wrappedTxn, *types.LatestSignerForChainID(mockSentry.ChainConfig.ChainID), mockSentry.Key)
	wrappedTxn.Tx.DynamicFeeTransaction = *txn.(*types.DynamicFeeTransaction)

	ctx, conn := rpcdaemontest.CreateTestGrpcConn(t, mockSentry)
	txPool := txpool.NewTxpoolClient(conn)
	ff := rpchelper.New(ctx, rpchelper.DefaultFiltersConfig, nil, txPool, txpool.NewMiningClient(conn), func() {}, mockSentry.Log)
	api := jsonrpc.NewEthAPI(newBaseApiForTest(mockSentry), mockSentry.DB, nil, txPool, nil, 5000000, ethconfig.Defaults.RPCTxFeeCap, 100_000, false, 100_000, 128, logger)

	err = wrappedTxn.MarshalBinary(buf)
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
	require.NotNil(err)
	expectedErr := txpool_proto.ImportResult_name[int32(txpool_proto.ImportResult_ALREADY_EXISTS)] + ": " + txpoolcfg.AlreadyKnown.String()
	require.Equal(expectedErr, err.Error())
	mockSentry.ReceiveWg.Wait()
}