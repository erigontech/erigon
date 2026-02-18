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
	"testing"
	"time"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"

	"github.com/erigontech/erigon/cmd/rpcdaemon/cli/httpcfg"
	"github.com/erigontech/erigon/cmd/rpcdaemon/rpcdaemontest"
	"github.com/erigontech/erigon/cmd/rpcdaemon/rpcservices"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/kvcache"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/execution/tests/blockgen"
	"github.com/erigontech/erigon/execution/tests/mock"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/node/direct"
	"github.com/erigontech/erigon/node/ethconfig"
	"github.com/erigontech/erigon/node/gointerfaces/txpoolproto"
	"github.com/erigontech/erigon/rpc/jsonrpc"
	"github.com/erigontech/erigon/rpc/rpccfg"
	"github.com/erigontech/erigon/rpc/rpchelper"
)

// Do 1 step to start txPool
func oneBlockSteps(mockSentry *mock.MockSentry, require *require.Assertions, blocks int) {
	chain, err := blockgen.GenerateChain(mockSentry.ChainConfig, mockSentry.Genesis, mockSentry.Engine, mockSentry.DB, blocks, func(i int, b *blockgen.BlockGen) {
		b.SetCoinbase(common.Address{1})
	})
	require.NoError(err)
	err = mockSentry.InsertChain(chain)
	require.NoError(err)
}

// Do 1 step to start txPool
func oneBlockStep(mockSentry *mock.MockSentry, require *require.Assertions) {
	oneBlockSteps(mockSentry, require, 1)
}

func newBaseApiForTest(m *mock.MockSentry) *jsonrpc.BaseAPI {
	stateCache := kvcache.New(kvcache.DefaultCoherentConfig)
	return jsonrpc.NewBaseApi(nil, stateCache, m.BlockReader, false, rpccfg.DefaultEvmCallTimeout, m.Engine, m.Dirs, nil, 0)
}

func newEthApiForTest(base *jsonrpc.BaseAPI, db kv.TemporalRoDB, txPool txpoolproto.TxpoolClient) *jsonrpc.APIImpl {
	cfg := &jsonrpc.EthApiConfig{
		GasCap:                      5000000,
		FeeCap:                      ethconfig.Defaults.RPCTxFeeCap,
		ReturnDataLimit:             100_000,
		AllowUnprotectedTxs:         false,
		MaxGetProofRewindBlockCount: 100_000,
		SubscribeLogsChannelSize:    128,
		RpcTxSyncDefaultTimeout:     20 * time.Second,
		RpcTxSyncMaxTimeout:         1 * time.Minute,
	}
	return jsonrpc.NewEthAPI(base, db, nil, txPool, nil, cfg, log.New())
}

func TestGetBlobsV1(t *testing.T) {
	buf := bytes.NewBuffer(nil)
	mockSentry, require := mock.MockWithTxPoolCancun(t), require.New(t)
	oneBlockStep(mockSentry, require)

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
	api := newEthApiForTest(newBaseApiForTest(mockSentry), mockSentry.DB, txPool)

	executionRpc := direct.NewExecutionClientDirect(mockSentry.Eth1ExecutionService)
	eth := rpcservices.NewRemoteBackend(nil, mockSentry.DB, mockSentry.BlockReader)
	fcuTimeout := ethconfig.Defaults.FcuTimeout
	maxReorgDepth := ethconfig.Defaults.MaxReorgDepth
	engineServer := NewEngineServer(mockSentry.Log, mockSentry.ChainConfig, executionRpc, nil, false, false, true, txPool, fcuTimeout, maxReorgDepth)
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
	buf := bytes.NewBuffer(nil)
	mockSentry, require := mock.MockWithTxPoolOsaka(t), require.New(t)
	oneBlockStep(mockSentry, require)

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
	api := newEthApiForTest(newBaseApiForTest(mockSentry), mockSentry.DB, txPool)

	executionRpc := direct.NewExecutionClientDirect(mockSentry.Eth1ExecutionService)
	eth := rpcservices.NewRemoteBackend(nil, mockSentry.DB, mockSentry.BlockReader)
	fcuTimeout := ethconfig.Defaults.FcuTimeout
	maxReorgDepth := ethconfig.Defaults.MaxReorgDepth
	engineServer := NewEngineServer(mockSentry.Log, mockSentry.ChainConfig, executionRpc, nil, false, false, true, txPool, fcuTimeout, maxReorgDepth)
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
	require.Len(blobsResp, 2)
	require.Equal(blobsResp[0].Blob, hexutil.Bytes(wrappedTxn.Blobs[0][:]))
	require.Equal(blobsResp[1].Blob, hexutil.Bytes(wrappedTxn.Blobs[1][:]))

	for i := range 128 {
		require.Equal(blobsResp[0].CellProofs[i], hexutil.Bytes(wrappedTxn.Proofs[i][:]))
		require.Equal(blobsResp[1].CellProofs[i], hexutil.Bytes(wrappedTxn.Proofs[i+128][:]))
	}
}

func TestGetBlobsV3(t *testing.T) {
	buf := bytes.NewBuffer(nil)
	mockSentry, require := mock.MockWithTxPoolOsaka(t), require.New(t)
	oneBlockStep(mockSentry, require)

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
	api := newEthApiForTest(newBaseApiForTest(mockSentry), mockSentry.DB, txPool)

	executionRpc := direct.NewExecutionClientDirect(mockSentry.Eth1ExecutionService)
	eth := rpcservices.NewRemoteBackend(nil, mockSentry.DB, mockSentry.BlockReader)
	fcuTimeout := ethconfig.Defaults.FcuTimeout
	maxReorgDepth := ethconfig.Defaults.MaxReorgDepth
	engineServer := NewEngineServer(mockSentry.Log, mockSentry.ChainConfig, executionRpc, nil, false, false, true, txPool, fcuTimeout, maxReorgDepth)
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
	blobsResp, err := engineServer.GetBlobsV3(ctx, blobHashes)
	require.NoError(err)
	require.Len(blobsResp, 3) // Unlike GetBlobsV2, only the missing blob should be nil
	require.Nil(blobsResp[0])
	require.Equal(blobsResp[1].Blob, hexutil.Bytes(wrappedTxn.Blobs[0][:]))
	require.Equal(blobsResp[2].Blob, hexutil.Bytes(wrappedTxn.Blobs[1][:]))

	blobHashes = blobHashes[1:]
	blobsResp, err = engineServer.GetBlobsV3(ctx, blobHashes)
	require.NoError(err)
	require.Len(blobsResp, 2)
	require.Equal(blobsResp[0].Blob, hexutil.Bytes(wrappedTxn.Blobs[0][:]))
	require.Equal(blobsResp[1].Blob, hexutil.Bytes(wrappedTxn.Blobs[1][:]))

	for i := range 128 {
		require.Equal(blobsResp[0].CellProofs[i], hexutil.Bytes(wrappedTxn.Proofs[i][:]))
		require.Equal(blobsResp[1].CellProofs[i], hexutil.Bytes(wrappedTxn.Proofs[i+128][:]))
	}
}

func canonicalHashAt(t *testing.T, db kv.TemporalRoDB, blockNum uint64) common.Hash {
	t.Helper()
	var hash common.Hash
	err := db.View(context.Background(), func(tx kv.Tx) error {
		var err error
		hash, err = rawdb.ReadCanonicalHash(tx, blockNum)
		return err
	})
	require.NoError(t, err)
	return hash
}

func writeBlockAccessListBytes(t *testing.T, db kv.TemporalRwDB, blockHash common.Hash, blockNum uint64, balBytes []byte) {
	t.Helper()
	err := db.Update(context.Background(), func(tx kv.RwTx) error {
		return rawdb.WriteBlockAccessListBytes(tx, blockHash, blockNum, balBytes)
	})
	require.NoError(t, err)
}

func TestGetPayloadBodiesByHashV2(t *testing.T) {
	mockSentry, req := mock.MockWithTxPoolOsaka(t), require.New(t)
	oneBlockStep(mockSentry, req)

	executionRpc := direct.NewExecutionClientDirect(mockSentry.Eth1ExecutionService)
	maxReorgDepth := ethconfig.Defaults.MaxReorgDepth
	engineServer := NewEngineServer(mockSentry.Log, mockSentry.ChainConfig, executionRpc, nil, false, false, true, nil, ethconfig.Defaults.FcuTimeout, maxReorgDepth)

	const blockNum = 1
	blockHash := canonicalHashAt(t, mockSentry.DB, blockNum)
	req.NotEqual(common.Hash{}, blockHash)

	ctx := context.Background()

	// BAL should be null when not available
	bodies, err := engineServer.GetPayloadBodiesByHashV2(ctx, []common.Hash{blockHash})
	req.NoError(err)
	req.Len(bodies, 1)
	req.NotNil(bodies[0])
	req.Nil(bodies[0].BlockAccessList)

	balBytes, err := types.EncodeBlockAccessListBytes(nil)
	req.NoError(err)
	writeBlockAccessListBytes(t, mockSentry.DB, blockHash, blockNum, balBytes)

	bodies, err = engineServer.GetPayloadBodiesByHashV2(ctx, []common.Hash{blockHash})
	req.NoError(err)
	req.Len(bodies, 1)
	req.NotNil(bodies[0])
	req.NotNil(bodies[0].BlockAccessList)
	req.Equal(hexutil.Bytes(balBytes), bodies[0].BlockAccessList)
}

func TestGetPayloadBodiesByRangeV2(t *testing.T) {
	mockSentry, req := mock.MockWithTxPoolOsaka(t), require.New(t)
	oneBlockSteps(mockSentry, req, 2)

	executionRpc := direct.NewExecutionClientDirect(mockSentry.Eth1ExecutionService)
	maxReorgDepth := ethconfig.Defaults.MaxReorgDepth
	engineServer := NewEngineServer(mockSentry.Log, mockSentry.ChainConfig, executionRpc, nil, false, false, true, nil, ethconfig.Defaults.FcuTimeout, maxReorgDepth)

	const (
		start = 1
		count = 2
	)
	blockHash1 := canonicalHashAt(t, mockSentry.DB, start)
	blockHash2 := canonicalHashAt(t, mockSentry.DB, start+1)
	req.NotEqual(common.Hash{}, blockHash1)
	req.NotEqual(common.Hash{}, blockHash2)

	ctx := context.Background()

	// BAL should be null when not available
	bodies, err := engineServer.GetPayloadBodiesByRangeV2(ctx, start, count)
	req.NoError(err)
	req.Len(bodies, 2)
	req.NotNil(bodies[0])
	req.NotNil(bodies[1])
	req.Nil(bodies[0].BlockAccessList)
	req.Nil(bodies[1].BlockAccessList)

	balBytes1, err := types.EncodeBlockAccessListBytes(nil)
	req.NoError(err)
	balBytes2 := []byte{0x01, 0x02, 0x03}
	writeBlockAccessListBytes(t, mockSentry.DB, blockHash1, start, balBytes1)
	writeBlockAccessListBytes(t, mockSentry.DB, blockHash2, start+1, balBytes2)

	bodies, err = engineServer.GetPayloadBodiesByRangeV2(ctx, start, count)
	req.NoError(err)
	req.Len(bodies, 2)
	req.NotNil(bodies[0])
	req.NotNil(bodies[1])
	req.NotNil(bodies[0].BlockAccessList)
	req.NotNil(bodies[1].BlockAccessList)
	req.Equal(hexutil.Bytes(balBytes1), bodies[0].BlockAccessList)
	req.Equal(hexutil.Bytes(balBytes2), bodies[1].BlockAccessList)
}
