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
	"context"
	"encoding/json"
	"math/big"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cmd/rpcdaemon/rpcdaemontest"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/kvcache"
	"github.com/erigontech/erigon/db/kv/prune"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/execution/rlp"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/tests/blockgen"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/node/gointerfaces/txpoolproto"
	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/rpc/ethapi"
	"github.com/erigontech/erigon/rpc/rpccfg"
	"github.com/erigontech/erigon/rpc/rpchelper"
)

type blockAccessListRPCCase struct {
	name       string
	selector   string
	want       json.RawMessage
	errCode    int
	errMessage string
}

func TestGetBlockAccessListRPCSpec(t *testing.T) {
	chainPack, client := newBlockAccessListRPCFixture(t)
	availableJSON := marshalBlockAccessListJSON(t, chainPack.BlockAccessLists[1])
	emptyJSON := marshalBlockAccessListJSON(t, chainPack.BlockAccessLists[2])
	cases := []blockAccessListRPCCase{
		{name: "available by number", selector: "0x2", want: availableJSON},
		{name: "available by tag", selector: "safe", want: availableJSON},
		{name: "available by hash", selector: chainPack.Blocks[1].Hash().Hex(), want: availableJSON},
		{name: "empty by number", selector: "0x3", want: emptyJSON},
		{name: "empty by tag", selector: "latest", want: emptyJSON},
		{name: "empty by hash", selector: chainPack.Blocks[2].Hash().Hex(), want: emptyJSON},
		{name: "unknown number", selector: "0xff", want: json.RawMessage("null")},
		{name: "unknown hash", selector: common.Hash{}.Hex(), want: json.RawMessage("null")},
		{name: "pending", selector: "pending", want: json.RawMessage("null")},
		{name: "pre-Amsterdam by number", selector: "0x1", errCode: -32001, errMessage: "Resource not found"},
		{name: "pre-Amsterdam by tag", selector: "earliest", errCode: -32001, errMessage: "Resource not found"},
		{name: "pre-Amsterdam by hash", selector: chainPack.Blocks[0].Hash().Hex(), errCode: -32001, errMessage: "Resource not found"},
		{name: "pruned by number", selector: "0x4", errCode: 4444, errMessage: "Pruned history unavailable"},
		{name: "pruned by tag", selector: "finalized", errCode: 4444, errMessage: "Pruned history unavailable"},
		{name: "pruned by hash", selector: chainPack.Blocks[3].Hash().Hex(), errCode: 4444, errMessage: "Pruned history unavailable"},
	}
	runBlockAccessListRPCCases(t, client, "eth_getBlockAccessList", cases)
}

func newBlockAccessListRPCFixture(t *testing.T) (*blockgen.ChainPack, *rpc.Client) {
	t.Helper()
	if !dbg.Exec3Parallel {
		t.Skip("requires parallel exec")
	}
	m, chainPack := rpcdaemontest.CreateTestBlockAccessListExecModule(t)
	base := newBaseApiForTest(m)
	ethAPI := newEthApiForTest(base, m.DB, nil, nil)
	debugAPI := NewPrivateDebugAPI(base, m.DB, nil, 0, false)
	server := rpc.NewServer(50, false, false, true, log.New(), 100)
	require.NoError(t, server.RegisterName("eth", EthAPI(ethAPI)))
	require.NoError(t, server.RegisterName("debug", PrivateDebugAPI(debugAPI)))
	client := rpc.DialInProc(server, log.New())
	t.Cleanup(func() {
		client.Close()
		server.Stop()
	})
	return chainPack, client
}

func marshalBlockAccessListJSON(t *testing.T, data []byte) json.RawMessage {
	t.Helper()
	bal, err := types.DecodeBlockAccessListBytes(data)
	require.NoError(t, err)
	encoded, err := json.Marshal(ethapi.MarshalBlockAccessList(bal))
	require.NoError(t, err)
	return encoded
}

func marshalHexBytesJSON(t *testing.T, data []byte) json.RawMessage {
	t.Helper()
	encoded, err := json.Marshal(hexutil.Bytes(data))
	require.NoError(t, err)
	return encoded
}

func runBlockAccessListRPCCases(t *testing.T, client *rpc.Client, method string, cases []blockAccessListRPCCase) {
	t.Helper()
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			var got json.RawMessage
			err := client.CallContext(t.Context(), &got, method, testCase.selector)
			if testCase.errCode != 0 {
				require.Error(t, err)
				var rpcErr rpc.Error
				require.ErrorAs(t, err, &rpcErr)
				require.Equal(t, testCase.errCode, rpcErr.ErrorCode())
				require.Equal(t, testCase.errMessage, err.Error())
				return
			}
			require.NoError(t, err)
			require.JSONEq(t, string(testCase.want), string(got))
		})
	}
}

func TestGetBlockAccessListRegeneratesPrunedBAL(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	privKey, err := crypto.GenerateKey()
	require.NoError(t, err)
	senderAddr := crypto.PubkeyToAddress(privKey.PublicKey)
	genesis := &types.Genesis{Config: chain.AllProtocolChanges, Alloc: types.GenesisAlloc{senderAddr: {Balance: new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil)}}}
	m := execmoduletester.New(t, execmoduletester.WithGenesisSpec(genesis), execmoduletester.WithKey(privKey))
	signer := types.LatestSignerForChainID(m.ChainConfig.ChainID)
	baseFee := uint256.NewInt(m.Genesis.BaseFee().Uint64())
	chainPack, err := blockgen.GenerateChain(m.ChainConfig, m.Genesis, m.Engine, m.DB, 2, func(i int, b *blockgen.BlockGen) {
		txn, err := types.SignTx(types.NewTransaction(uint64(i), common.Address{1}, uint256.NewInt(10_000), 50_000, baseFee, nil), *signer, privKey)
		require.NoError(t, err)
		b.AddTx(txn)
	})
	require.NoError(t, err)
	require.NoError(t, m.InsertChain(chainPack))
	err = m.DB.Update(ctx, func(tx kv.RwTx) error {
		return tx.ForEach(kv.BlockAccessList, nil, func(k, _ []byte) error { return tx.Delete(kv.BlockAccessList, k) })
	})
	require.NoError(t, err)
	err = m.DB.View(ctx, func(tx kv.Tx) error {
		count, err := tx.Count(kv.BlockAccessList)
		require.NoError(t, err)
		require.Zero(t, count, "stored BALs should be fully pruned")
		return nil
	})
	require.NoError(t, err)
	api := NewEthAPI(newBaseApiForTest(m), m.DB, nil, nil, nil, &rpccfg.EthApiConfig{}, log.New())
	for i, block := range chainPack.Blocks {
		blockNum := rpc.BlockNumber(block.NumberU64())
		got, err := api.GetBlockAccessList(ctx, rpc.BlockNumberOrHash{BlockNumber: &blockNum})
		require.NoError(t, err, "block %d", block.NumberU64())
		canonical, err := types.DecodeBlockAccessListBytes(chainPack.BlockAccessLists[i])
		require.NoError(t, err)
		require.Equal(t, ethapi.MarshalBlockAccessList(canonical), got, "block %d", block.NumberU64())
	}
}

// Gets the latest block number with the latest tag
func TestGetBlockByNumberWithLatestTag(t *testing.T) {
	m, _, _ := rpcdaemontest.CreateTestExecModule(t)
	api := newEthApiForTest(newBaseApiForTest(m), m.DB, nil, nil)
	b, err := api.GetBlockByNumber(context.Background(), rpc.LatestBlockNumber, false)
	expected := common.HexToHash("0x9c47d5780744fa24ccdb1543a9b715e53431d5560b9e460b8b7a68f7c58310ae")
	if err != nil {
		t.Errorf("error getting block number with latest tag: %s", err)
	}
	assert.Equal(t, expected, b["hash"])
}

func TestGetBlockByNumberWithLatestTag_WithHeadHashInDb(t *testing.T) {
	m, _, _ := rpcdaemontest.CreateTestExecModule(t)
	ctx := context.Background()
	tx, err := m.DB.BeginRw(ctx)
	require.NoError(t, err)
	defer tx.Rollback()

	latestBlockHash := common.HexToHash("0x6804117de2f3e6ee32953e78ced1db7b20214e0d8c745a03b8fecf7cc8ee76ef")
	latestBlock, err := m.BlockReader.BlockByHash(ctx, tx, latestBlockHash)
	if err != nil {
		tx.Rollback()
		t.Errorf("couldn't retrieve latest block")
	}
	rawdb.WriteHeaderNumber(tx, latestBlockHash, latestBlock.NonceU64())
	rawdb.WriteForkchoiceHead(tx, latestBlockHash)
	if safedHeadBlock := rawdb.ReadForkchoiceHead(tx); safedHeadBlock == (common.Hash{}) {
		tx.Rollback()
		t.Error("didn't find forkchoice head hash")
	}
	tx.Commit()

	api := newEthApiForTest(newBaseApiForTest(m), m.DB, nil, nil)
	block, err := api.GetBlockByNumber(ctx, rpc.LatestBlockNumber, false)
	if err != nil {
		t.Errorf("error retrieving block by number: %s", err)
	}
	expectedHash := common.HexToHash("0x71b89b6ca7b65debfd2fbb01e4f07de7bba343e6617559fa81df19b605f84662")
	assert.Equal(t, expectedHash, block["hash"])
}

func TestGetBlockByNumberWithPendingTag(t *testing.T) {
	m := execmoduletester.New(t, execmoduletester.WithTxPool())
	stateCache := kvcache.New(kvcache.DefaultCoherentConfig)

	ctx, conn := rpcdaemontest.CreateTestGrpcConn(t, m)
	txPool := txpoolproto.NewTxpoolClient(conn)
	ff := rpchelper.New(ctx, rpchelper.DefaultFiltersConfig, nil, txPool, txpoolproto.NewMiningClient(conn), func() {}, m.Log, nil)

	expected := 1
	header := &types.Header{
		Number: *uint256.NewInt(uint64(expected)),
	}

	rlpBlock, err := rlp.EncodeToBytes(types.NewBlockWithHeader(header))
	if err != nil {
		t.Errorf("failed encoding the block: %s", err)
	}
	ff.HandlePendingBlock(&txpoolproto.OnPendingBlockReply{
		RplBlock: rlpBlock,
	})

	api := newEthApiForTest(newBaseApiWithFiltersForTest(ff, stateCache, m), m.DB, nil, nil)
	b, err := api.GetBlockByNumber(context.Background(), rpc.PendingBlockNumber, false)
	if err != nil {
		t.Errorf("error getting block number with pending tag: %s", err)
	}
	expectedNum := (*hexutil.Big)(uint256.NewInt(uint64(expected)).ToBig())
	assert.Equal(t, expectedNum, b["number"])
}

func TestGetBlockByNumber_WithFinalizedTag_NoFinalizedBlockInDb(t *testing.T) {
	m, _, _ := rpcdaemontest.CreateTestExecModule(t)
	ctx := context.Background()
	api := newEthApiForTest(newBaseApiForTest(m), m.DB, nil, nil)
	_, err := api.GetBlockByNumber(ctx, rpc.FinalizedBlockNumber, false)
	if err != nil {
		var customErr *rpc.CustomError
		if assert.ErrorAs(t, err, &customErr) {
			assert.Equal(t, rpchelper.UnknownBlockCode, customErr.Code)
			assert.Contains(t, customErr.Message, "finalized")
		}
	}
}

func TestGetBlockByNumber_WithFinalizedTag_WithFinalizedBlockInDb(t *testing.T) {
	m, _, _ := rpcdaemontest.CreateTestExecModule(t)
	ctx := context.Background()
	tx, err := m.DB.BeginRw(ctx)
	require.NoError(t, err)
	defer tx.Rollback()

	latestBlockHash := common.HexToHash("0x6804117de2f3e6ee32953e78ced1db7b20214e0d8c745a03b8fecf7cc8ee76ef")
	latestBlock, err := m.BlockReader.BlockByHash(ctx, tx, latestBlockHash)
	if err != nil {
		tx.Rollback()
		t.Errorf("couldn't retrieve latest block")
	}
	rawdb.WriteHeaderNumber(tx, latestBlockHash, latestBlock.NonceU64())
	rawdb.WriteForkchoiceFinalized(tx, latestBlockHash)
	if safedFinalizedBlock := rawdb.ReadForkchoiceFinalized(tx); safedFinalizedBlock == (common.Hash{}) {
		tx.Rollback()
		t.Error("didn't find forkchoice finalized hash")
	}
	tx.Commit()

	api := newEthApiForTest(newBaseApiForTest(m), m.DB, nil, nil)
	block, err := api.GetBlockByNumber(ctx, rpc.FinalizedBlockNumber, false)
	if err != nil {
		t.Errorf("error retrieving block by number: %s", err)
	}
	expectedHash := common.HexToHash("0x71b89b6ca7b65debfd2fbb01e4f07de7bba343e6617559fa81df19b605f84662")
	assert.Equal(t, expectedHash, block["hash"])
}

func TestGetBlockByNumber_WithSafeTag_NoSafeBlockInDb(t *testing.T) {
	m, _, _ := rpcdaemontest.CreateTestExecModule(t)
	ctx := context.Background()
	api := newEthApiForTest(newBaseApiForTest(m), m.DB, nil, nil)
	_, err := api.GetBlockByNumber(ctx, rpc.SafeBlockNumber, false)
	if err != nil {
		var customErr *rpc.CustomError
		if assert.ErrorAs(t, err, &customErr) {
			assert.Equal(t, rpchelper.UnknownBlockCode, customErr.Code)
			assert.Contains(t, customErr.Message, "safe")
		}
	}
}

func TestGetBlockByNumber_WithSafeTag_WithSafeBlockInDb(t *testing.T) {
	m, _, _ := rpcdaemontest.CreateTestExecModule(t)
	ctx := context.Background()
	tx, err := m.DB.BeginRw(ctx)
	require.NoError(t, err)
	defer tx.Rollback()

	latestBlockHash := common.HexToHash("0x6804117de2f3e6ee32953e78ced1db7b20214e0d8c745a03b8fecf7cc8ee76ef")
	latestBlock, err := m.BlockReader.BlockByHash(ctx, tx, latestBlockHash)
	if err != nil {
		tx.Rollback()
		t.Errorf("couldn't retrieve latest block")
	}
	rawdb.WriteHeaderNumber(tx, latestBlockHash, latestBlock.NonceU64())
	rawdb.WriteForkchoiceSafe(tx, latestBlockHash)
	if safedSafeBlock := rawdb.ReadForkchoiceSafe(tx); safedSafeBlock == (common.Hash{}) {
		tx.Rollback()
		t.Error("didn't find forkchoice safe block hash")
	}
	tx.Commit()

	api := newEthApiForTest(newBaseApiForTest(m), m.DB, nil, nil)
	block, err := api.GetBlockByNumber(ctx, rpc.SafeBlockNumber, false)
	if err != nil {
		t.Errorf("error retrieving block by number: %s", err)
	}
	expectedHash := common.HexToHash("0x71b89b6ca7b65debfd2fbb01e4f07de7bba343e6617559fa81df19b605f84662")
	assert.Equal(t, expectedHash, block["hash"])
}

func TestGetBlockTransactionCountByHash(t *testing.T) {
	m, _, _ := rpcdaemontest.CreateTestExecModule(t)
	ctx := context.Background()

	api := newEthApiForTest(newBaseApiForTest(m), m.DB, nil, nil)
	blockHash := common.HexToHash("0x6804117de2f3e6ee32953e78ced1db7b20214e0d8c745a03b8fecf7cc8ee76ef")

	tx, err := m.DB.BeginRw(ctx)
	require.NoError(t, err)
	defer tx.Rollback()

	header, err := rawdb.ReadHeaderByHash(tx, blockHash)
	if err != nil {
		tx.Rollback()
		t.Errorf("failed reading block by hash: %s", err)
	}
	bodyWithTx, err := m.BlockReader.BodyWithTransactions(ctx, tx, blockHash, header.Number.Uint64())
	if err != nil {
		tx.Rollback()
		t.Errorf("failed getting body with transactions: %s", err)
	}
	tx.Rollback()

	expectedAmount := hexutil.Uint(len(bodyWithTx.Transactions))

	txCount, err := api.GetBlockTransactionCountByHash(ctx, blockHash)
	if err != nil {
		t.Errorf("failed getting the transaction count, err=%s", err)
	}

	assert.Equal(t, expectedAmount, *txCount)
}

func TestGetBlockTransactionCountByHash_ZeroTx(t *testing.T) {
	m, _, _ := rpcdaemontest.CreateTestExecModule(t)
	ctx := context.Background()
	api := newEthApiForTest(newBaseApiForTest(m), m.DB, nil, nil)
	blockHash := common.HexToHash("0x5883164d4100b95e1d8e931b8b9574586a1dea7507941e6ad3c1e3a2591485fd")

	tx, err := m.DB.BeginRw(ctx)
	require.NoError(t, err)
	defer tx.Rollback()

	header, err := rawdb.ReadHeaderByHash(tx, blockHash)
	if err != nil {
		tx.Rollback()
		t.Errorf("failed reading block by hash: %s", err)
	}
	bodyWithTx, err := m.BlockReader.BodyWithTransactions(ctx, tx, blockHash, header.Number.Uint64())
	if err != nil {
		tx.Rollback()
		t.Errorf("failed getting body with transactions: %s", err)
	}
	tx.Rollback()

	expectedAmount := hexutil.Uint(len(bodyWithTx.Transactions))

	txCount, err := api.GetBlockTransactionCountByHash(ctx, blockHash)
	if err != nil {
		t.Errorf("failed getting the transaction count, err=%s", err)
	}

	assert.Equal(t, expectedAmount, *txCount)
}

func TestGetBlockTransactionCountByNumber(t *testing.T) {
	m, _, _ := rpcdaemontest.CreateTestExecModule(t)
	ctx := context.Background()
	api := newEthApiForTest(newBaseApiForTest(m), m.DB, nil, nil)
	blockHash := common.HexToHash("0x6804117de2f3e6ee32953e78ced1db7b20214e0d8c745a03b8fecf7cc8ee76ef")

	tx, err := m.DB.BeginRw(ctx)
	require.NoError(t, err)
	defer tx.Rollback()

	header, err := rawdb.ReadHeaderByHash(tx, blockHash)
	if err != nil {
		tx.Rollback()
		t.Errorf("failed reading block by hash: %s", err)
	}
	bodyWithTx, err := m.BlockReader.BodyWithTransactions(ctx, tx, blockHash, header.Number.Uint64())
	if err != nil {
		tx.Rollback()
		t.Errorf("failed getting body with transactions: %s", err)
	}
	tx.Rollback()

	expectedAmount := hexutil.Uint(len(bodyWithTx.Transactions))

	txCount, err := api.GetBlockTransactionCountByNumber(ctx, rpc.BlockNumber(header.Number.Uint64()))
	if err != nil {
		t.Errorf("failed getting the transaction count, err=%s", err)
	}

	assert.Equal(t, expectedAmount, *txCount)
}

func TestGetBlockTransactionCountByNumber_ZeroTx(t *testing.T) {
	m, _, _ := rpcdaemontest.CreateTestExecModule(t)
	ctx := context.Background()
	api := newEthApiForTest(newBaseApiForTest(m), m.DB, nil, nil)

	blockHash := common.HexToHash("0x5883164d4100b95e1d8e931b8b9574586a1dea7507941e6ad3c1e3a2591485fd")

	tx, err := m.DB.BeginRw(ctx)
	require.NoError(t, err)
	defer tx.Rollback()

	header, err := rawdb.ReadHeaderByHash(tx, blockHash)
	if err != nil {
		tx.Rollback()
		t.Errorf("failed reading block by hash: %s", err)
	}
	bodyWithTx, err := m.BlockReader.BodyWithTransactions(ctx, tx, blockHash, header.Number.Uint64())
	if err != nil {
		tx.Rollback()
		t.Errorf("failed getting body with transactions: %s", err)
	}
	tx.Rollback()

	expectedAmount := hexutil.Uint(len(bodyWithTx.Transactions))

	txCount, err := api.GetBlockTransactionCountByNumber(ctx, rpc.BlockNumber(header.Number.Uint64()))
	if err != nil {
		t.Errorf("failed getting the transaction count, err=%s", err)
	}

	assert.Equal(t, expectedAmount, *txCount)
}

func TestGetBlockByNumber_BlockPruneGating(t *testing.T) {
	if testing.Short() {
		t.Skip("slow test")
	}
	t.Parallel()

	const chainSize = 20
	const pruneDistance = uint64(10)

	setup := func(t *testing.T, pm prune.Mode) *APIImpl {
		t.Helper()
		m := execmoduletester.New(t, execmoduletester.WithPruneMode(pm))
		c, err := blockgen.GenerateChain(m.ChainConfig, m.Genesis, m.Engine, m.DB, chainSize, func(_ int, _ *blockgen.BlockGen) {})
		require.NoError(t, err)
		require.NoError(t, m.InsertChain(c))

		ctx := t.Context()
		tx, err := m.DB.BeginTemporalRw(ctx)
		require.NoError(t, err)
		defer tx.Rollback()
		_, err = prune.EnsureNotChanged(tx, pm)
		require.NoError(t, err)
		require.NoError(t, tx.Commit())

		return newEthApiForTest(newBaseApiForTest(m), m.DB, nil, nil)
	}

	legacyFull := prune.Mode{
		Initialised: true,
		History:     prune.Distance(pruneDistance),
		Blocks:      prune.KeepPostMergeBlocksPruneMode,
	}
	minimalMode := prune.Mode{
		Initialised: true,
		History:     prune.Distance(pruneDistance),
		Blocks:      prune.Distance(pruneDistance),
	}

	// In full mode, block bodies are in snapshots and KeepPostMergeBlocksPruneMode means no block-body
	// gate — GetBlockByNumber must succeed even for blocks older than the state-history window.
	t.Run("full_mode_old_block_accessible", func(t *testing.T) {
		t.Parallel()
		api := setup(t, legacyFull)
		b, err := api.GetBlockByNumber(t.Context(), rpc.BlockNumber(0), false)
		require.NoError(t, err)
		require.NotNil(t, b)
	})

	// In minimal mode, Blocks=Distance(pruneDistance) gates access: block 0 < head-pruneDistance.
	t.Run("minimal_mode_old_block_pruned", func(t *testing.T) {
		t.Parallel()
		api := setup(t, minimalMode)
		_, err := api.GetBlockByNumber(t.Context(), rpc.BlockNumber(0), false)
		require.ErrorIs(t, err, state.PrunedError)
	})

	// Recent blocks (within the prune window) must always be accessible.
	t.Run("minimal_mode_recent_block_accessible", func(t *testing.T) {
		t.Parallel()
		api := setup(t, minimalMode)
		b, err := api.GetBlockByNumber(t.Context(), rpc.BlockNumber(chainSize), false)
		require.NoError(t, err)
		require.NotNil(t, b)
	})
}
