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

	"github.com/erigontech/erigon-lib/direct"
	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon-lib/common"
	sentry "github.com/erigontech/erigon-lib/gointerfaces/sentryproto"
	txpool "github.com/erigontech/erigon-lib/gointerfaces/txpoolproto"

	"github.com/erigontech/erigon-lib/kv/kvcache"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon-lib/rlp"
	"github.com/erigontech/erigon-lib/wrap"
	"github.com/erigontech/erigon/cmd/rpcdaemon/cli/httpcfg"
	"github.com/erigontech/erigon/cmd/rpcdaemon/rpcdaemontest"
	"github.com/erigontech/erigon/cmd/rpcdaemon/rpcservices"
	"github.com/erigontech/erigon/core"
	"github.com/erigontech/erigon/core/types"
	"github.com/erigontech/erigon/eth/ethconfig"
	"github.com/erigontech/erigon/eth/protocols/eth"

	"github.com/erigontech/erigon/rpc/rpccfg"
	"github.com/erigontech/erigon/turbo/jsonrpc"
	"github.com/erigontech/erigon/turbo/rpchelper"
	"github.com/erigontech/erigon/turbo/stages"
	"github.com/erigontech/erigon/turbo/stages/mock"
)

// Do 1 step to start txPool
func oneBlockStep(mockSentry *mock.MockSentry, require *require.Assertions, t *testing.T) {
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
	txPool := txpool.NewTxpoolClient(conn)
	ff := rpchelper.New(ctx, rpchelper.DefaultFiltersConfig, nil, txPool, txpool.NewMiningClient(conn), func() {}, mockSentry.Log)
	api := jsonrpc.NewEthAPI(newBaseApiForTest(mockSentry), mockSentry.DB, nil, txPool, nil, 5000000, ethconfig.Defaults.RPCTxFeeCap, 100_000, false, 100_000, 128, logger)

	executionRpc := direct.NewExecutionClientDirect(mockSentry.Eth1ExecutionService)
	eth := rpcservices.NewRemoteBackend(nil, mockSentry.DB, mockSentry.BlockReader)
	engineServer := NewEngineServer(mockSentry.Log, mockSentry.ChainConfig, executionRpc, mockSentry.HeaderDownload(), nil, false, true, false, true)
	engineServer.Start(ctx, &httpcfg.HttpCfg{}, mockSentry.DB, mockSentry.BlockReader, ff, nil, mockSentry.Engine, eth, txPool, nil)

	err = wrappedTxn.MarshalBinaryWrapped(buf)
	require.NoError(err)
	_, err = api.SendRawTransaction(ctx, buf.Bytes())
	require.NoError(err)

	blobsResp, err := engineServer.GetBlobsV1(ctx, wrappedTxn.Tx.BlobVersionedHashes)
	require.NoError(err)
	require.Equal(blobsResp[0].Blob, wrappedTxn.Blobs[0][:])
	require.Equal(blobsResp[1].Blob, wrappedTxn.Blobs[1][:])
	require.Equal(blobsResp[0].Proof, wrappedTxn.Proofs[0][:])
	require.Equal(blobsResp[1].Proof, wrappedTxn.Proofs[1][:])
}
