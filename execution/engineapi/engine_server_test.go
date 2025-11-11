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
	"context"
	"math/big"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"

	"github.com/erigontech/erigon/cmd/rpcdaemon/cli/httpcfg"
	"github.com/erigontech/erigon/cmd/rpcdaemon/rpcdaemontest"
	"github.com/erigontech/erigon/cmd/rpcdaemon/rpcservices"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv/kvcache"
	"github.com/erigontech/erigon/execution/rlp"
	"github.com/erigontech/erigon/execution/stagedsync/stageloop"
	"github.com/erigontech/erigon/execution/tests/blockgen"
	"github.com/erigontech/erigon/execution/tests/mock"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/node/direct"
	"github.com/erigontech/erigon/node/ethconfig"
	"github.com/erigontech/erigon/node/gointerfaces/sentryproto"
	"github.com/erigontech/erigon/node/gointerfaces/txpoolproto"
	"github.com/erigontech/erigon/p2p/protocols/eth"
	"github.com/erigontech/erigon/rpc/jsonrpc"
	"github.com/erigontech/erigon/rpc/rpccfg"
	"github.com/erigontech/erigon/rpc/rpchelper"
)

// Do 1 step to start txPool
func oneBlockStep(mockSentry *mock.MockSentry, require *require.Assertions, t *testing.T) {
	chain, err := blockgen.GenerateChain(mockSentry.ChainConfig, mockSentry.Genesis, mockSentry.Engine, mockSentry.DB, 1 /*number of blocks:*/, func(i int, b *blockgen.BlockGen) {
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
	for _, err = range mockSentry.Send(&sentryproto.InboundMessage{Id: sentryproto.MessageId_NEW_BLOCK_66, Data: b, PeerId: mockSentry.PeerId}) {
		require.NoError(err)
	}
	// Send all the headers
	b, err = rlp.EncodeToBytes(&eth.BlockHeadersPacket66{
		RequestId:          1,
		BlockHeadersPacket: chain.Headers,
	})
	require.NoError(err)
	mockSentry.ReceiveWg.Add(1)
	for _, err = range mockSentry.Send(&sentryproto.InboundMessage{Id: sentryproto.MessageId_BLOCK_HEADERS_66, Data: b, PeerId: mockSentry.PeerId}) {
		require.NoError(err)
	}
	mockSentry.ReceiveWg.Wait() // Wait for all messages to be processed before we proceed

	initialCycle, firstCycle := mock.MockInsertAsInitialCycle, false
	if err := stageloop.StageLoopIteration(mockSentry.Ctx, mockSentry.DB, nil, nil, mockSentry.Sync, initialCycle, firstCycle, log.New(), mockSentry.BlockReader, nil); err != nil {
		t.Fatal(err)
	}
}

func newBaseApiForTest(m *mock.MockSentry) *jsonrpc.BaseAPI {
	stateCache := kvcache.New(kvcache.DefaultCoherentConfig)
	return jsonrpc.NewBaseApi(nil, stateCache, m.BlockReader, false, rpccfg.DefaultEvmCallTimeout, m.Engine, m.Dirs, nil)
}

func TestGetBlobsV1(t *testing.T) {
	logger := log.New()
	buf := bytes.NewBuffer(nil)
	mockSentry, require := mock.MockWithTxPoolCancun(t), require.New(t)
	oneBlockStep(mockSentry, require, t)

	wrappedTxn := types.MakeWrappedBlobTxn(uint256.MustFromBig(mockSentry.ChainConfig.ChainID))
	txn, err := types.SignTx(wrappedTxn, *types.LatestSignerForChainID(mockSentry.ChainConfig.ChainID), mockSentry.Key)
	require.NoError(err)
	dt := &wrappedTxn.Tx.DynamicFeeTransaction
	v, r, s := txn.RawSignatureValues()
	dt.V.Set(v)
	dt.R.Set(r)
	dt.S.Set(s)

	ctx, conn := rpcdaemontest.CreateTestGrpcConn(t, mockSentry)
	txPool := direct.NewTxPoolClient(mockSentry.TxPoolGrpcServer)

	ff := rpchelper.New(ctx, rpchelper.DefaultFiltersConfig, nil, txPool, txpoolproto.NewMiningClient(conn), func() {}, mockSentry.Log)
	api := jsonrpc.NewEthAPI(newBaseApiForTest(mockSentry), mockSentry.DB, nil, txPool, nil, 5000000, ethconfig.Defaults.RPCTxFeeCap, 100_000, false, 100_000, 128, logger)

	executionRpc := direct.NewExecutionClientDirect(mockSentry.Eth1ExecutionService)
	eth := rpcservices.NewRemoteBackend(nil, mockSentry.DB, mockSentry.BlockReader)
	engineServer := NewEngineServer(mockSentry.Log, mockSentry.ChainConfig, executionRpc, nil, false, false, true, txPool, DefaultFcuTimeout)
	ctx, cancel := context.WithCancel(ctx)
	var eg errgroup.Group
	t.Cleanup(func() {
		err := eg.Wait() // wait for clean exit
		require.ErrorIs(err, context.Canceled)
	})
	t.Cleanup(cancel)
	eg.Go(func() error {
		return engineServer.Start(ctx, &httpcfg.HttpCfg{}, mockSentry.DB, mockSentry.BlockReader, ff, nil, mockSentry.Engine, eth, nil)
	})

	err = wrappedTxn.MarshalBinaryWrapped(buf)
	require.NoError(err)
	_, err = api.SendRawTransaction(ctx, buf.Bytes())
	require.NoError(err)

	blobHashes := append([]common.Hash{{}}, wrappedTxn.Tx.BlobVersionedHashes...)
	blobsResp, err := engineServer.GetBlobsV1(ctx, blobHashes)
	require.NoError(err)
	require.True(blobsResp[0] == nil)
	require.Equal(blobsResp[1].Blob, hexutil.Bytes(wrappedTxn.Blobs[0][:]))
	require.Equal(blobsResp[2].Blob, hexutil.Bytes(wrappedTxn.Blobs[1][:]))
	require.Equal(blobsResp[1].Proof, hexutil.Bytes(wrappedTxn.Proofs[0][:]))
	require.Equal(blobsResp[2].Proof, hexutil.Bytes(wrappedTxn.Proofs[1][:]))
}

func TestGetBlobsV2(t *testing.T) {
	logger := log.New()
	buf := bytes.NewBuffer(nil)
	mockSentry, require := mock.MockWithTxPoolOsaka(t), require.New(t)
	oneBlockStep(mockSentry, require, t)

	wrappedTxn := types.MakeV1WrappedBlobTxn(uint256.MustFromBig(mockSentry.ChainConfig.ChainID))
	txn, err := types.SignTx(wrappedTxn, *types.LatestSignerForChainID(mockSentry.ChainConfig.ChainID), mockSentry.Key)
	require.NoError(err)
	dt := &wrappedTxn.Tx.DynamicFeeTransaction
	v, r, s := txn.RawSignatureValues()
	dt.V.Set(v)
	dt.R.Set(r)
	dt.S.Set(s)

	ctx, conn := rpcdaemontest.CreateTestGrpcConn(t, mockSentry)
	txPool := direct.NewTxPoolClient(mockSentry.TxPoolGrpcServer)

	ff := rpchelper.New(ctx, rpchelper.DefaultFiltersConfig, nil, txPool, txpoolproto.NewMiningClient(conn), func() {}, mockSentry.Log)
	api := jsonrpc.NewEthAPI(newBaseApiForTest(mockSentry), mockSentry.DB, nil, txPool, nil, 5000000, ethconfig.Defaults.RPCTxFeeCap, 100_000, false, 100_000, 128, logger)

	executionRpc := direct.NewExecutionClientDirect(mockSentry.Eth1ExecutionService)
	eth := rpcservices.NewRemoteBackend(nil, mockSentry.DB, mockSentry.BlockReader)
	engineServer := NewEngineServer(mockSentry.Log, mockSentry.ChainConfig, executionRpc, nil, false, false, true, txPool, DefaultFcuTimeout)
	ctx, cancel := context.WithCancel(ctx)
	var eg errgroup.Group
	t.Cleanup(func() {
		err := eg.Wait() // wait for clean exit
		require.ErrorIs(err, context.Canceled)
	})
	t.Cleanup(cancel)
	eg.Go(func() error {
		return engineServer.Start(ctx, &httpcfg.HttpCfg{}, mockSentry.DB, mockSentry.BlockReader, ff, nil, mockSentry.Engine, eth, nil)
	})

	err = wrappedTxn.MarshalBinaryWrapped(buf)
	require.NoError(err)
	hh, err := api.SendRawTransaction(ctx, buf.Bytes())
	require.NoError(err)
	require.NotEmpty(hh)

	blobHashes := append([]common.Hash{{}}, wrappedTxn.Tx.BlobVersionedHashes...)
	blobsResp, err := engineServer.GetBlobsV2(ctx, blobHashes)
	require.NoError(err)
	require.Nil(blobsResp) // Any one blob not found makes the whole response nil

	blobHashes = blobHashes[1:]
	blobsResp, err = engineServer.GetBlobsV2(ctx, blobHashes)
	require.NoError(err)
	require.Equal(blobsResp[0].Blob, hexutil.Bytes(wrappedTxn.Blobs[0][:]))
	require.Equal(blobsResp[1].Blob, hexutil.Bytes(wrappedTxn.Blobs[1][:]))

	for i := range 128 {
		require.Equal(blobsResp[0].CellProofs[i], hexutil.Bytes(wrappedTxn.Proofs[i][:]))
		require.Equal(blobsResp[1].CellProofs[i], hexutil.Bytes(wrappedTxn.Proofs[i+128][:]))
	}
}
