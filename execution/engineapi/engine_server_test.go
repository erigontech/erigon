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
	"time"

	"github.com/jinzhu/copier"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"

	"github.com/erigontech/erigon/cmd/rpcdaemon/cli/httpcfg"
	"github.com/erigontech/erigon/cmd/rpcdaemon/rpcdaemontest"
	"github.com/erigontech/erigon/cmd/rpcdaemon/rpcservices"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/kvcache"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/execution/tests/blockgen"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/node/direct"
	"github.com/erigontech/erigon/node/ethconfig"
	"github.com/erigontech/erigon/node/gointerfaces/txpoolproto"
	"github.com/erigontech/erigon/rpc/jsonrpc"
	"github.com/erigontech/erigon/rpc/rpccfg"
	"github.com/erigontech/erigon/rpc/rpchelper"
)

// Do 1 step to start txPool
func oneBlockSteps(m *execmoduletester.ExecModuleTester, require *require.Assertions, blocks int) *blockgen.ChainPack {
	chain, err := blockgen.GenerateChain(m.ChainConfig, m.Genesis, m.Engine, m.DB, blocks, func(i int, b *blockgen.BlockGen) {
		b.SetCoinbase(common.Address{1})
	})
	require.NoError(err)
	err = m.InsertChain(chain)
	require.NoError(err)
	return chain
}

// Do 1 step to start txPool
func oneBlockStep(mockSentry *execmoduletester.ExecModuleTester, require *require.Assertions) {
	oneBlockSteps(mockSentry, require, 1)
}

func newBaseApiForTest(m *execmoduletester.ExecModuleTester) *jsonrpc.BaseAPI {
	stateCache := kvcache.New(kvcache.DefaultCoherentConfig)
	return jsonrpc.NewBaseApi(nil, stateCache, m.BlockReader, false, rpccfg.DefaultEvmCallTimeout, m.Engine, m.Dirs, nil, 0, 0, 0)
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
	if testing.Short() {
		t.Skip("slow test")
	}
	buf := bytes.NewBuffer(nil)
	funds := big.NewInt(1 * common.Ether)
	key, _ := crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
	address := crypto.PubkeyToAddress(key.PublicKey)

	var chainConfig chain.Config
	err := copier.CopyWithOption(&chainConfig, chain.AllProtocolChanges, copier.Option{DeepCopy: true})
	require.NoError(t, err)
	chainConfig.PragueTime = nil
	chainConfig.OsakaTime = nil
	chainConfig.AmsterdamTime = nil
	gspec := &types.Genesis{
		Config: &chainConfig,
		Alloc: types.GenesisAlloc{
			address: {Balance: funds},
		},
	}
	mockSentry := execmoduletester.New(
		t,
		execmoduletester.WithGenesisSpec(gspec),
		execmoduletester.WithKey(key),
		execmoduletester.WithTxPool(),
	)
	require := require.New(t)
	oneBlockStep(mockSentry, require)

	wrappedTxn := types.MakeWrappedBlobTxn(mockSentry.ChainConfig.ChainID)
	txn, err := types.SignTx(wrappedTxn, *types.LatestSignerForChainID(mockSentry.ChainConfig.ChainID), mockSentry.Key)
	require.NoError(err)
	dt := &wrappedTxn.Tx.DynamicFeeTransaction
	v, r, s := txn.RawSignatureValues()
	dt.V.Set(v)
	dt.R.Set(r)
	dt.S.Set(s)

	ctx, conn := rpcdaemontest.CreateTestGrpcConn(t, mockSentry)
	txPool := direct.NewTxPoolClient(mockSentry.TxPoolGrpcServer)

	ff := rpchelper.New(ctx, rpchelper.DefaultFiltersConfig, nil, txPool, txpoolproto.NewMiningClient(conn), func() {}, mockSentry.Log, nil)
	api := newEthApiForTest(newBaseApiForTest(mockSentry), mockSentry.DB, txPool)

	executionRpc := mockSentry.ExecModule
	eth := rpcservices.NewRemoteBackend(nil, mockSentry.DB, mockSentry.BlockReader)
	fcuTimeout := ethconfig.Defaults.FcuTimeout
	maxReorgDepth := ethconfig.Defaults.MaxReorgDepth
	engineServer := NewEngineServer(mockSentry.Log, mockSentry.ChainConfig, executionRpc, nil, false, false, false, true, txPool, mockSentry.TxPool, fcuTimeout, maxReorgDepth)
	ctx, cancel := context.WithCancel(ctx)
	var eg errgroup.Group
	t.Cleanup(func() {
		err := eg.Wait() // wait for clean exit
		require.ErrorIs(err, context.Canceled)
	})
	t.Cleanup(cancel)
	eg.Go(func() error {
		return engineServer.Start(ctx, &httpcfg.HttpCfg{}, mockSentry.DB, mockSentry.BlockReader, ff, nil, mockSentry.Engine, eth, nil, nil)
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
	if testing.Short() {
		t.Skip("slow test")
	}
	buf := bytes.NewBuffer(nil)
	mockSentry := execmoduletester.New(t, execmoduletester.WithTxPool(), execmoduletester.WithChainConfig(chain.AllProtocolChanges))
	require := require.New(t)
	oneBlockStep(mockSentry, require)

	wrappedTxn := types.MakeV1WrappedBlobTxn(mockSentry.ChainConfig.ChainID)
	txn, err := types.SignTx(wrappedTxn, *types.LatestSignerForChainID(mockSentry.ChainConfig.ChainID), mockSentry.Key)
	require.NoError(err)
	dt := &wrappedTxn.Tx.DynamicFeeTransaction
	v, r, s := txn.RawSignatureValues()
	dt.V.Set(v)
	dt.R.Set(r)
	dt.S.Set(s)

	ctx, conn := rpcdaemontest.CreateTestGrpcConn(t, mockSentry)
	txPool := direct.NewTxPoolClient(mockSentry.TxPoolGrpcServer)

	ff := rpchelper.New(ctx, rpchelper.DefaultFiltersConfig, nil, txPool, txpoolproto.NewMiningClient(conn), func() {}, mockSentry.Log, nil)
	api := newEthApiForTest(newBaseApiForTest(mockSentry), mockSentry.DB, txPool)

	executionRpc := mockSentry.ExecModule
	eth := rpcservices.NewRemoteBackend(nil, mockSentry.DB, mockSentry.BlockReader)
	fcuTimeout := ethconfig.Defaults.FcuTimeout
	maxReorgDepth := ethconfig.Defaults.MaxReorgDepth
	engineServer := NewEngineServer(mockSentry.Log, mockSentry.ChainConfig, executionRpc, nil, false, false, false, true, txPool, mockSentry.TxPool, fcuTimeout, maxReorgDepth)
	ctx, cancel := context.WithCancel(ctx)
	var eg errgroup.Group
	t.Cleanup(func() {
		err := eg.Wait() // wait for clean exit
		require.ErrorIs(err, context.Canceled)
	})
	t.Cleanup(cancel)
	eg.Go(func() error {
		return engineServer.Start(ctx, &httpcfg.HttpCfg{}, mockSentry.DB, mockSentry.BlockReader, ff, nil, mockSentry.Engine, eth, nil, nil)
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
	if testing.Short() {
		t.Skip("slow test")
	}
	buf := bytes.NewBuffer(nil)
	mockSentry := execmoduletester.New(t, execmoduletester.WithTxPool(), execmoduletester.WithChainConfig(chain.AllProtocolChanges))
	require := require.New(t)
	oneBlockStep(mockSentry, require)

	wrappedTxn := types.MakeV1WrappedBlobTxn(mockSentry.ChainConfig.ChainID)
	txn, err := types.SignTx(wrappedTxn, *types.LatestSignerForChainID(mockSentry.ChainConfig.ChainID), mockSentry.Key)
	require.NoError(err)
	dt := &wrappedTxn.Tx.DynamicFeeTransaction
	v, r, s := txn.RawSignatureValues()
	dt.V.Set(v)
	dt.R.Set(r)
	dt.S.Set(s)

	ctx, conn := rpcdaemontest.CreateTestGrpcConn(t, mockSentry)
	txPool := direct.NewTxPoolClient(mockSentry.TxPoolGrpcServer)

	ff := rpchelper.New(ctx, rpchelper.DefaultFiltersConfig, nil, txPool, txpoolproto.NewMiningClient(conn), func() {}, mockSentry.Log, nil)
	api := newEthApiForTest(newBaseApiForTest(mockSentry), mockSentry.DB, txPool)

	executionRpc := mockSentry.ExecModule
	eth := rpcservices.NewRemoteBackend(nil, mockSentry.DB, mockSentry.BlockReader)
	fcuTimeout := ethconfig.Defaults.FcuTimeout
	maxReorgDepth := ethconfig.Defaults.MaxReorgDepth
	engineServer := NewEngineServer(mockSentry.Log, mockSentry.ChainConfig, executionRpc, nil, false, false, false, true, txPool, mockSentry.TxPool, fcuTimeout, maxReorgDepth)
	ctx, cancel := context.WithCancel(ctx)
	var eg errgroup.Group
	t.Cleanup(func() {
		err := eg.Wait() // wait for clean exit
		require.ErrorIs(err, context.Canceled)
	})
	t.Cleanup(cancel)
	eg.Go(func() error {
		return engineServer.Start(ctx, &httpcfg.HttpCfg{}, mockSentry.DB, mockSentry.BlockReader, ff, nil, mockSentry.Engine, eth, nil, nil)
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

func TestGetPayloadBodiesByHashV2(t *testing.T) {
	mockSentry := execmoduletester.New(t, execmoduletester.WithTxPool(), execmoduletester.WithChainConfig(chain.AllProtocolChanges))
	req := require.New(t)
	// Insert a block carrying its BAL through the unified InsertChain path (BAL
	// stored in the overlay, flushed at commit); serving must reflect that BAL.
	chain := oneBlockSteps(mockSentry, req, 1)

	executionRpc := mockSentry.ExecModule
	maxReorgDepth := ethconfig.Defaults.MaxReorgDepth
	engineServer := NewEngineServer(mockSentry.Log, mockSentry.ChainConfig, executionRpc, nil, false, false, false, true, nil, nil, ethconfig.Defaults.FcuTimeout, maxReorgDepth)

	blockHash := chain.Blocks[0].Hash()
	req.NotEqual(common.Hash{}, blockHash)
	req.NotEmpty(chain.BlockAccessLists[0], "Amsterdam block must carry a BAL from GenerateChain")

	ctx := context.Background()

	bodies, err := engineServer.GetPayloadBodiesByHashV2(ctx, []common.Hash{blockHash})
	req.NoError(err)
	req.Len(bodies, 1)
	req.NotNil(bodies[0])
	req.Equal(hexutil.Bytes(chain.BlockAccessLists[0]), bodies[0].BlockAccessList)
}

func TestGetPayloadBodiesByRangeV2(t *testing.T) {
	mockSentry := execmoduletester.New(t, execmoduletester.WithTxPool(), execmoduletester.WithChainConfig(chain.AllProtocolChanges))
	req := require.New(t)
	chain := oneBlockSteps(mockSentry, req, 2)

	executionRpc := mockSentry.ExecModule
	maxReorgDepth := ethconfig.Defaults.MaxReorgDepth
	engineServer := NewEngineServer(mockSentry.Log, mockSentry.ChainConfig, executionRpc, nil, false, false, false, true, nil, nil, ethconfig.Defaults.FcuTimeout, maxReorgDepth)

	const (
		start = 1
		count = 2
	)
	req.NotEmpty(chain.BlockAccessLists[0], "Amsterdam block must carry a BAL from GenerateChain")
	req.NotEmpty(chain.BlockAccessLists[1], "Amsterdam block must carry a BAL from GenerateChain")

	ctx := context.Background()

	// Serving must reflect the BALs inserted with the blocks (unified path).
	bodies, err := engineServer.GetPayloadBodiesByRangeV2(ctx, start, count)
	req.NoError(err)
	req.Len(bodies, 2)
	req.NotNil(bodies[0])
	req.NotNil(bodies[1])
	req.Equal(hexutil.Bytes(chain.BlockAccessLists[0]), bodies[0].BlockAccessList)
	req.Equal(hexutil.Bytes(chain.BlockAccessLists[1]), bodies[1].BlockAccessList)
}
