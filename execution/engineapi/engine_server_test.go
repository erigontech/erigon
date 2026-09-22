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
	"encoding/json"
	"fmt"
	"math/big"
	"strings"
	"testing"
	"time"

	goethkzg "github.com/crate-crypto/go-eth-kzg"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"

	"github.com/erigontech/erigon/cmd/rpcdaemon/cli/httpcfg"
	"github.com/erigontech/erigon/cmd/rpcdaemon/rpcdaemontest"
	"github.com/erigontech/erigon/cmd/rpcdaemon/rpcservices"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/crypto/kzg"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/kvcache"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/engineapi/engine_types"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/execution/tests/blockgen"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/node/direct"
	"github.com/erigontech/erigon/node/ethconfig"
	"github.com/erigontech/erigon/node/gointerfaces/txpoolproto"
	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/rpc/jsonrpc"
	"github.com/erigontech/erigon/rpc/jsonstream"
	"github.com/erigontech/erigon/rpc/rpccfg"
	"github.com/erigontech/erigon/rpc/rpchelper"
	"github.com/erigontech/erigon/txnprovider/txpool"
)

// Do 1 step to start txPool
func oneBlockSteps(m *execmoduletester.ExecModuleTester, require *require.Assertions, blocks int) {
	chain, err := m.GenerateChain(blocks, func(i int, b *blockgen.BlockGen) {
		b.SetCoinbase(common.Address{1})
	})
	require.NoError(err)
	err = m.InsertChain(chain)
	require.NoError(err)
}

// Do 1 step to start txPool
func oneBlockStep(mockSentry *execmoduletester.ExecModuleTester, require *require.Assertions) {
	oneBlockSteps(mockSentry, require, 1)
}

func newBaseApiForTest(m *execmoduletester.ExecModuleTester) *jsonrpc.BaseAPI {
	stateCache := kvcache.New(kvcache.DefaultCoherentConfig)
	return jsonrpc.NewBaseApi(nil, stateCache, m.BlockReader, m.Engine, &rpccfg.BaseApiConfig{Dirs: m.Dirs})
}

func newEthApiForTest(base *jsonrpc.BaseAPI, db kv.TemporalRoDB, txPool txpoolproto.TxpoolClient) *jsonrpc.APIImpl {
	cfg := &rpccfg.EthApiConfig{
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

	chainConfig := chain.AllProtocolChanges.Copy()
	chainConfig.PragueTime = nil
	chainConfig.OsakaTime = nil
	chainConfig.AmsterdamTime = nil
	gspec := &types.Genesis{
		Config: chainConfig,
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

func newGetBlobsTxPoolFixture(t *testing.T) (context.Context, *EngineServer, *types.BlobTxWrapper) {
	t.Helper()
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

	return ctx, engineServer, wrappedTxn
}

func TestGetBlobsV3(t *testing.T) {
	if testing.Short() {
		t.Skip("slow test")
	}
	ctx, engineServer, wrappedTxn := newGetBlobsTxPoolFixture(t)
	require := require.New(t)

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

func TestGetBlobsV4WithTxPool(t *testing.T) {
	if testing.Short() {
		t.Skip("slow test")
	}
	ctx, engineServer, wrappedTxn := newGetBlobsTxPoolFixture(t)
	require := require.New(t)

	blobHashes := append([]common.Hash{{}}, wrappedTxn.Tx.BlobVersionedHashes...)
	indices := []uint64{0, 64, 127}
	mask := hexutil.MustDecodeHex("0x01000000000000000100000000000080")
	cellBundles, err := engineServer.GetBlobsV4(ctx, blobHashes, mask)
	require.NoError(err)
	require.Len(cellBundles, 3)
	require.Nil(cellBundles[0])
	for i, bundle := range cellBundles[1:] {
		require.NotNil(bundle)
		require.Len(bundle.BlobCells, len(indices))
		require.Len(bundle.Proofs, len(indices))
		cells := make([]*goethkzg.Cell, len(indices))
		proofs := make([]goethkzg.KZGProof, len(indices))
		commitments := make([]goethkzg.KZGCommitment, len(indices))
		for j := range indices {
			require.NotNil(bundle.BlobCells[j])
			require.NotNil(bundle.Proofs[j])
			require.Len(*bundle.BlobCells[j], len(goethkzg.Cell{}))
			require.Len(*bundle.Proofs[j], len(goethkzg.KZGProof{}))
			cells[j] = (*goethkzg.Cell)(*bundle.BlobCells[j])
			proofs[j] = goethkzg.KZGProof(*bundle.Proofs[j])
			commitments[j] = goethkzg.KZGCommitment(wrappedTxn.Commitments[i])
		}
		require.NoError(kzg.Ctx().VerifyCellKZGProofBatch(commitments, indices, cells, proofs))
	}
}

type blobGetterFunc func([]common.Hash) []txpool.PoolBlobBundle

func (f blobGetterFunc) GetBlobs(hashes []common.Hash) []txpool.PoolBlobBundle {
	return f(hashes)
}

type blobGetterMap map[common.Hash]txpool.PoolBlobBundle

func (m blobGetterMap) GetBlobs(hashes []common.Hash) []txpool.PoolBlobBundle {
	bundles := make([]txpool.PoolBlobBundle, len(hashes))
	for i, hash := range hashes {
		bundles[i] = m[hash]
	}
	return bundles
}

func getBlobsV4Fixture(t *testing.T, value byte) (common.Hash, txpool.PoolBlobBundle, []*goethkzg.Cell) {
	t.Helper()
	var blob goethkzg.Blob
	blob[31] = value
	cells, proofs, err := kzg.Ctx().ComputeCellsAndKZGProofs(&blob, 2)
	require.NoError(t, err)
	commitment, err := kzg.Ctx().BlobToKZGCommitment(&blob, 2)
	require.NoError(t, err)
	return common.Hash(kzg.KZGToVersionedHash(commitment)), txpool.PoolBlobBundle{
		Commitment: commitment,
		Blob:       blob[:],
		Proofs:     proofs[:],
	}, cells[:]
}

func newGetBlobsV4Client(t *testing.T, getter txpool.BlobGetter) *rpc.Client {
	t.Helper()
	logger := log.New()
	server := rpc.NewServer(1, false, false, false, logger, 0)
	t.Cleanup(server.Stop)
	require.NoError(t, server.RegisterName("engine", &EngineServer{logger: logger, blobGetter: getter}))
	client := rpc.DialInProc(server, logger)
	t.Cleanup(client.Close)
	return client
}

func TestGetBlobsV4(t *testing.T) {
	hash, bundle, cells := getBlobsV4Fixture(t, 1)
	client := newGetBlobsV4Client(t, blobGetterMap{hash: bundle})
	allIndices := make([]int, goethkzg.CellsPerExtBlob)
	for i := range allIndices {
		allIndices[i] = i
	}
	for _, tc := range []struct {
		name    string
		mask    hexutil.Bytes
		indices []int
	}{
		{"selected_cells", hexutil.MustDecodeHex("0x81010000000000800100000000000080"), []int{0, 7, 8, 63, 64, 127}},
		{"all_cells", bytes.Repeat([]byte{0xff}, 16), allIndices},
		{"no_cells", make(hexutil.Bytes, 16), nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var result []struct {
				BlobCells []hexutil.Bytes `json:"blob_cells"`
				Proofs    []hexutil.Bytes `json:"proofs"`
			}
			require.NoError(t, client.CallContext(t.Context(), &result, "engine_getBlobsV4", []common.Hash{hash}, tc.mask))
			require.Len(t, result, 1)
			require.NotNil(t, result[0].BlobCells)
			require.NotNil(t, result[0].Proofs)
			require.Len(t, result[0].BlobCells, len(tc.indices))
			require.Len(t, result[0].Proofs, len(tc.indices))
			for i, index := range tc.indices {
				require.Equal(t, hexutil.Bytes(cells[index][:]), result[0].BlobCells[i])
				require.Equal(t, hexutil.Bytes(bundle.Proofs[index][:]), result[0].Proofs[i])
			}
		})
	}
}

func TestGetBlobsV4FastJSON(t *testing.T) {
	hash, bundle, _ := getBlobsV4Fixture(t, 1)
	server := &EngineServer{logger: log.New(), blobGetter: blobGetterMap{hash: bundle}}
	result, err := server.GetBlobsV4(t.Context(), []common.Hash{hash, {}}, hexutil.MustDecodeHex("0x01000000000000000100000000000080"))
	require.NoError(t, err)
	marshaler, ok := any(result).(interface {
		MarshalFastJSONTo(*jsonstream.StackStream) error
	})
	require.True(t, ok, "GetBlobsV4 must return a fast JSON result")
	want, err := json.Marshal(result)
	require.NoError(t, err)
	got, err := jsonstream.Marshal(marshaler)
	require.NoError(t, err)
	require.Equal(t, string(want), string(got))
}

func TestGetBlobsV4PartialResponse(t *testing.T) {
	hashA, bundleA, cellsA := getBlobsV4Fixture(t, 1)
	hashB, bundleB, cellsB := getBlobsV4Fixture(t, 2)
	pool := blobGetterMap{hashA: bundleA, hashB: bundleB}
	cells := map[common.Hash][]*goethkzg.Cell{hashA: cellsA, hashB: cellsB}
	client := newGetBlobsV4Client(t, pool)
	hashes := []common.Hash{hashB, {}, hashA, hashB}
	mask := hexutil.Bytes(hexutil.MustDecodeHex("0x80010000000000000000000000000080"))
	var result []*engine_types.BlobCellsAndProofsV1
	require.NoError(t, client.CallContext(t.Context(), &result, "engine_getBlobsV4", hashes, mask))
	require.Len(t, result, len(hashes))
	for i, hash := range hashes {
		bundle, available := pool[hash]
		if !available {
			require.Nil(t, result[i])
			continue
		}
		require.NotNil(t, result[i])
		require.Len(t, result[i].BlobCells, 3)
		require.Len(t, result[i].Proofs, 3)
		for j, index := range []int{7, 8, 127} {
			require.NotNil(t, result[i].BlobCells[j])
			require.NotNil(t, result[i].Proofs[j])
			require.Equal(t, hexutil.Bytes(cells[hash][index][:]), *result[i].BlobCells[j])
			require.Equal(t, hexutil.Bytes(bundle.Proofs[index][:]), *result[i].Proofs[j])
		}
	}
}

func TestGetBlobsV4InvalidMask(t *testing.T) {
	client := newGetBlobsV4Client(t, blobGetterFunc(func(hashes []common.Hash) []txpool.PoolBlobBundle {
		return make([]txpool.PoolBlobBundle, len(hashes))
	}))
	for _, tc := range []struct {
		name string
		mask any
	}{
		{"long", "0x" + strings.Repeat("00", 17)},
		{"short", "0x" + strings.Repeat("00", 15)},
		{"empty", "0x"},
		{"null", nil},
		{"missing_prefix", strings.Repeat("00", 16)},
		{"invalid_hex", "0x" + strings.Repeat("gg", 16)},
		{"wrong_type", 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var result any
			err := client.CallContext(t.Context(), &result, "engine_getBlobsV4", []common.Hash{}, tc.mask)
			var rpcErr rpc.Error
			require.ErrorAs(t, err, &rpcErr)
			require.Equal(t, -32602, rpcErr.ErrorCode())
		})
	}
}

func TestGetBlobsV4RequestLimit(t *testing.T) {
	client := newGetBlobsV4Client(t, blobGetterFunc(func(hashes []common.Hash) []txpool.PoolBlobBundle {
		return make([]txpool.PoolBlobBundle, len(hashes))
	}))
	for _, count := range []int{0, 128, 129} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			var result []*engine_types.BlobCellsAndProofsV1
			err := client.CallContext(t.Context(), &result, "engine_getBlobsV4", make([]common.Hash, count), make(hexutil.Bytes, 16))
			if count > 128 {
				var rpcErr rpc.Error
				require.ErrorAs(t, err, &rpcErr)
				require.Equal(t, -38004, rpcErr.ErrorCode())
				return
			}
			require.NoError(t, err)
			require.NotNil(t, result)
			require.Equal(t, make([]*engine_types.BlobCellsAndProofsV1, count), result)
		})
	}
}

func TestGetBlobsV4PoolDisabled(t *testing.T) {
	client := newGetBlobsV4Client(t, nil)
	var result json.RawMessage
	err := client.CallContext(t.Context(), &result, "engine_getBlobsV4", []common.Hash{{1}}, make(hexutil.Bytes, 16))
	require.EqualError(t, err, txpool.ErrPoolDisabled.Error())
}

func TestGetBlobsV4Unavailable(t *testing.T) {
	for _, tc := range []struct {
		name   string
		getter txpool.BlobGetter
	}{
		{"unavailable", blobGetterFunc(func([]common.Hash) []txpool.PoolBlobBundle { return nil })},
		{"invalid_response_length", blobGetterFunc(func([]common.Hash) []txpool.PoolBlobBundle {
			return make([]txpool.PoolBlobBundle, 2)
		})},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client := newGetBlobsV4Client(t, tc.getter)
			var result json.RawMessage
			err := client.CallContext(t.Context(), &result, "engine_getBlobsV4", []common.Hash{{1}}, make(hexutil.Bytes, 16))
			require.NoError(t, err)
			require.JSONEq(t, "null", string(result))
		})
	}
}

func TestGetBlobsV4JSONRPCClient(t *testing.T) {
	client := &JsonRpcClient{rpcClient: newGetBlobsV4Client(t, blobGetterFunc(func(hashes []common.Hash) []txpool.PoolBlobBundle {
		return make([]txpool.PoolBlobBundle, len(hashes))
	}))}
	result, err := client.GetBlobsV4(t.Context(), []common.Hash{{1}, {2}}, make(hexutil.Bytes, 16))
	require.NoError(t, err)
	require.Equal(t, []*engine_types.BlobCellsAndProofsV1{nil, nil}, result)
}

func TestGetBlobsV4InvalidBlobLength(t *testing.T) {
	for _, size := range []int{1, len(goethkzg.Blob{}) - 1, len(goethkzg.Blob{}) + 1} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			hash := common.Hash{1}
			client := newGetBlobsV4Client(t, blobGetterMap{hash: {
				Blob:   make([]byte, size),
				Proofs: make([]goethkzg.KZGProof, goethkzg.CellsPerExtBlob),
			}})
			var result []*engine_types.BlobCellsAndProofsV1
			err := client.CallContext(t.Context(), &result, "engine_getBlobsV4", []common.Hash{hash}, make(hexutil.Bytes, 16))
			require.NoError(t, err)
			require.Equal(t, []*engine_types.BlobCellsAndProofsV1{nil}, result)
		})
	}
}

func TestGetBlobsV4Canceled(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	server := &EngineServer{logger: log.New(), blobGetter: blobGetterMap{}}
	result, err := server.GetBlobsV4(ctx, []common.Hash{{1}}, make(hexutil.Bytes, 16))
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, result)
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
	mockSentry := execmoduletester.New(t, execmoduletester.WithTxPool(), execmoduletester.WithChainConfig(chain.AllProtocolChanges))
	req := require.New(t)
	oneBlockStep(mockSentry, req)

	executionRpc := mockSentry.ExecModule
	maxReorgDepth := ethconfig.Defaults.MaxReorgDepth
	engineServer := NewEngineServer(mockSentry.Log, mockSentry.ChainConfig, executionRpc, nil, false, false, false, true, nil, nil, ethconfig.Defaults.FcuTimeout, maxReorgDepth)

	const blockNum = 1
	blockHash := canonicalHashAt(t, mockSentry.DB, blockNum)
	req.NotEqual(common.Hash{}, blockHash)

	ctx := context.Background()

	// Amsterdam-enabled chains always have a BAL written by GenerateChain
	bodies, err := engineServer.GetPayloadBodiesByHashV2(ctx, []common.Hash{blockHash})
	req.NoError(err)
	req.Len(bodies, 1)
	req.NotNil(bodies[0])
	req.NotNil(bodies[0].BlockAccessList)
	req.NotEmpty(*bodies[0].BlockAccessList)

	// Overwrite with a non-empty BAL and verify it's returned
	balBytes := []byte{0x01, 0x02, 0x03}
	writeBlockAccessListBytes(t, mockSentry.DB, blockHash, blockNum, balBytes)

	bodies, err = engineServer.GetPayloadBodiesByHashV2(ctx, []common.Hash{blockHash})
	req.NoError(err)
	req.Len(bodies, 1)
	req.NotNil(bodies[0])
	req.NotNil(bodies[0].BlockAccessList)
	req.Equal(hexutil.Bytes(balBytes), *bodies[0].BlockAccessList)
}

func TestGetPayloadBodiesByRangeV2(t *testing.T) {
	mockSentry := execmoduletester.New(t, execmoduletester.WithTxPool(), execmoduletester.WithChainConfig(chain.AllProtocolChanges))
	req := require.New(t)
	oneBlockSteps(mockSentry, req, 2)

	executionRpc := mockSentry.ExecModule
	maxReorgDepth := ethconfig.Defaults.MaxReorgDepth
	engineServer := NewEngineServer(mockSentry.Log, mockSentry.ChainConfig, executionRpc, nil, false, false, false, true, nil, nil, ethconfig.Defaults.FcuTimeout, maxReorgDepth)

	const (
		start = 1
		count = 2
	)
	blockHash1 := canonicalHashAt(t, mockSentry.DB, start)
	blockHash2 := canonicalHashAt(t, mockSentry.DB, start+1)
	req.NotEqual(common.Hash{}, blockHash1)
	req.NotEqual(common.Hash{}, blockHash2)

	ctx := context.Background()

	// Amsterdam-enabled chains always have a BAL written by GenerateChain
	bodies, err := engineServer.GetPayloadBodiesByRangeV2(ctx, start, count)
	req.NoError(err)
	req.Len(bodies, 2)
	req.NotNil(bodies[0])
	req.NotNil(bodies[1])
	req.NotNil(bodies[0].BlockAccessList)
	req.NotNil(bodies[1].BlockAccessList)
	req.NotEmpty(*bodies[0].BlockAccessList)
	req.NotEmpty(*bodies[1].BlockAccessList)

	// Overwrite with non-empty BALs and verify they're returned
	balBytes1 := []byte{0x01, 0x02, 0x03}
	balBytes2 := []byte{0x04, 0x05, 0x06}
	writeBlockAccessListBytes(t, mockSentry.DB, blockHash1, start, balBytes1)
	writeBlockAccessListBytes(t, mockSentry.DB, blockHash2, start+1, balBytes2)

	bodies, err = engineServer.GetPayloadBodiesByRangeV2(ctx, start, count)
	req.NoError(err)
	req.Len(bodies, 2)
	req.NotNil(bodies[0])
	req.NotNil(bodies[1])
	req.NotNil(bodies[0].BlockAccessList)
	req.NotNil(bodies[1].BlockAccessList)
	req.Equal(hexutil.Bytes(balBytes1), *bodies[0].BlockAccessList)
	req.Equal(hexutil.Bytes(balBytes2), *bodies[1].BlockAccessList)
}
