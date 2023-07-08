package jsonrpc

import (
	"context"
	"math/big"
	"testing"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/txpool"
	"github.com/ledgerwatch/erigon-lib/kv/kvcache"
	"github.com/stretchr/testify/assert"

	"github.com/ledgerwatch/erigon/cmd/rpcdaemon/rpcdaemontest"
	"github.com/ledgerwatch/erigon/common/hexutil"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/rlp"
	"github.com/ledgerwatch/erigon/rpc"
	"github.com/ledgerwatch/erigon/rpc/rpccfg"
	"github.com/ledgerwatch/erigon/turbo/rpchelper"
	"github.com/ledgerwatch/erigon/turbo/stages"
	"github.com/ledgerwatch/log/v3"
)

// Gets the latest block number with the latest tag
func TestGetBlockByNumberWithLatestTag(t *testing.T) {
	m, _, _ := rpcdaemontest.CreateTestSentry(t)
	api := NewEthAPI(newBaseApiForTest(m), m.DB, nil, nil, nil, nil, 5000000, 100_000, log.New())
	b, err := api.GetBlockByNumber(context.Background(), rpc.LatestBlockNumber, false)
	expected := common.HexToHash("0x5883164d4100b95e1d8e931b8b9574586a1dea7507941e6ad3c1e3a2591485fd")
	if err != nil {
		t.Errorf("error getting block number with latest tag: %s", err)
	}
	assert.Equal(t, expected, b["hash"])
}

func TestGetBlockByNumberWithLatestTag_WithHeadHashInDb(t *testing.T) {
	m, _, _ := rpcdaemontest.CreateTestSentry(t)
	ctx := context.Background()
	tx, err := m.DB.BeginRw(ctx)
	if err != nil {
		t.Errorf("could not begin read write transaction: %s", err)
	}
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

	api := NewEthAPI(newBaseApiForTest(m), m.DB, nil, nil, nil, nil, 5000000, 100_000, log.New())
	block, err := api.GetBlockByNumber(ctx, rpc.LatestBlockNumber, false)
	if err != nil {
		t.Errorf("error retrieving block by number: %s", err)
	}
	expectedHash := common.HexToHash("0x71b89b6ca7b65debfd2fbb01e4f07de7bba343e6617559fa81df19b605f84662")
	assert.Equal(t, expectedHash, block["hash"])
}

func TestGetBlockByNumberWithPendingTag(t *testing.T) {
	m := stages.MockWithTxPool(t)
	agg := m.HistoryV3Components()
	stateCache := kvcache.New(kvcache.DefaultCoherentConfig)

	ctx, conn := rpcdaemontest.CreateTestGrpcConn(t, m)
	txPool := txpool.NewTxpoolClient(conn)
	ff := rpchelper.New(ctx, nil, txPool, txpool.NewMiningClient(conn), func() {}, m.Log)

	expected := 1
	header := &types.Header{
		Number: big.NewInt(int64(expected)),
	}

	rlpBlock, err := rlp.EncodeToBytes(types.NewBlockWithHeader(header))
	if err != nil {
		t.Errorf("failed encoding the block: %s", err)
	}
	ff.HandlePendingBlock(&txpool.OnPendingBlockReply{
		RplBlock: rlpBlock,
	})

	api := NewEthAPI(NewBaseApi(ff, stateCache, m.BlockReader, agg, false, rpccfg.DefaultEvmCallTimeout, m.Engine, m.Dirs), m.DB, nil, nil, nil, nil, 5000000, 100_000, log.New())
	b, err := api.GetBlockByNumber(context.Background(), rpc.PendingBlockNumber, false)
	if err != nil {
		t.Errorf("error getting block number with pending tag: %s", err)
	}
	assert.Equal(t, (*hexutil.Big)(big.NewInt(int64(expected))), b["number"])
}

func TestGetBlockByNumber_WithFinalizedTag_NoFinalizedBlockInDb(t *testing.T) {
	m, _, _ := rpcdaemontest.CreateTestSentry(t)
	ctx := context.Background()
	api := NewEthAPI(newBaseApiForTest(m), m.DB, nil, nil, nil, nil, 5000000, 100_000, log.New())
	if _, err := api.GetBlockByNumber(ctx, rpc.FinalizedBlockNumber, false); err != nil {
		assert.ErrorIs(t, rpchelper.UnknownBlockError, err)
	}
}

func TestGetBlockByNumber_WithFinalizedTag_WithFinalizedBlockInDb(t *testing.T) {
	m, _, _ := rpcdaemontest.CreateTestSentry(t)
	ctx := context.Background()
	tx, err := m.DB.BeginRw(ctx)
	if err != nil {
		t.Errorf("could not begin read write transaction: %s", err)
	}
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

	api := NewEthAPI(newBaseApiForTest(m), m.DB, nil, nil, nil, nil, 5000000, 100_000, log.New())
	block, err := api.GetBlockByNumber(ctx, rpc.FinalizedBlockNumber, false)
	if err != nil {
		t.Errorf("error retrieving block by number: %s", err)
	}
	expectedHash := common.HexToHash("0x71b89b6ca7b65debfd2fbb01e4f07de7bba343e6617559fa81df19b605f84662")
	assert.Equal(t, expectedHash, block["hash"])
}

func TestGetBlockByNumber_WithSafeTag_NoSafeBlockInDb(t *testing.T) {
	m, _, _ := rpcdaemontest.CreateTestSentry(t)
	ctx := context.Background()
	api := NewEthAPI(newBaseApiForTest(m), m.DB, nil, nil, nil, nil, 5000000, 100_000, log.New())
	if _, err := api.GetBlockByNumber(ctx, rpc.SafeBlockNumber, false); err != nil {
		assert.ErrorIs(t, rpchelper.UnknownBlockError, err)
	}
}

func TestGetBlockByNumber_WithSafeTag_WithSafeBlockInDb(t *testing.T) {
	m, _, _ := rpcdaemontest.CreateTestSentry(t)
	ctx := context.Background()
	tx, err := m.DB.BeginRw(ctx)
	if err != nil {
		t.Errorf("could not begin read write transaction: %s", err)
	}
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

	api := NewEthAPI(newBaseApiForTest(m), m.DB, nil, nil, nil, nil, 5000000, 100_000, log.New())
	block, err := api.GetBlockByNumber(ctx, rpc.SafeBlockNumber, false)
	if err != nil {
		t.Errorf("error retrieving block by number: %s", err)
	}
	expectedHash := common.HexToHash("0x71b89b6ca7b65debfd2fbb01e4f07de7bba343e6617559fa81df19b605f84662")
	assert.Equal(t, expectedHash, block["hash"])
}

func TestGetBlockTransactionCountByHash(t *testing.T) {
	m, _, _ := rpcdaemontest.CreateTestSentry(t)
	ctx := context.Background()

	api := NewEthAPI(newBaseApiForTest(m), m.DB, nil, nil, nil, nil, 5000000, 100_000, log.New())
	blockHash := common.HexToHash("0x6804117de2f3e6ee32953e78ced1db7b20214e0d8c745a03b8fecf7cc8ee76ef")

	tx, err := m.DB.BeginRw(ctx)
	if err != nil {
		t.Errorf("could not begin read write transaction: %s", err)
	}
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

	txAmount, err := api.GetBlockTransactionCountByHash(ctx, blockHash)
	if err != nil {
		t.Errorf("failed getting the transaction count, err=%s", err)
	}

	assert.Equal(t, expectedAmount, *txAmount)
}

func TestGetBlockTransactionCountByHash_ZeroTx(t *testing.T) {
	m, _, _ := rpcdaemontest.CreateTestSentry(t)
	ctx := context.Background()
	api := NewEthAPI(newBaseApiForTest(m), m.DB, nil, nil, nil, nil, 5000000, 100_000, log.New())
	blockHash := common.HexToHash("0x5883164d4100b95e1d8e931b8b9574586a1dea7507941e6ad3c1e3a2591485fd")

	tx, err := m.DB.BeginRw(ctx)
	if err != nil {
		t.Errorf("could not begin read write transaction: %s", err)
	}
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

	txAmount, err := api.GetBlockTransactionCountByHash(ctx, blockHash)
	if err != nil {
		t.Errorf("failed getting the transaction count, err=%s", err)
	}

	assert.Equal(t, expectedAmount, *txAmount)
}

func TestGetBlockTransactionCountByNumber(t *testing.T) {
	m, _, _ := rpcdaemontest.CreateTestSentry(t)
	ctx := context.Background()
	api := NewEthAPI(newBaseApiForTest(m), m.DB, nil, nil, nil, nil, 5000000, 100_000, log.New())
	blockHash := common.HexToHash("0x6804117de2f3e6ee32953e78ced1db7b20214e0d8c745a03b8fecf7cc8ee76ef")

	tx, err := m.DB.BeginRw(ctx)
	if err != nil {
		t.Errorf("could not begin read write transaction: %s", err)
	}
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

	txAmount, err := api.GetBlockTransactionCountByNumber(ctx, rpc.BlockNumber(header.Number.Uint64()))
	if err != nil {
		t.Errorf("failed getting the transaction count, err=%s", err)
	}

	assert.Equal(t, expectedAmount, *txAmount)
}

func TestGetBlockTransactionCountByNumber_ZeroTx(t *testing.T) {
	m, _, _ := rpcdaemontest.CreateTestSentry(t)
	ctx := context.Background()
	api := NewEthAPI(newBaseApiForTest(m), m.DB, nil, nil, nil, nil, 5000000, 100_000, log.New())

	blockHash := common.HexToHash("0x5883164d4100b95e1d8e931b8b9574586a1dea7507941e6ad3c1e3a2591485fd")

	tx, err := m.DB.BeginRw(ctx)
	if err != nil {
		t.Errorf("could not begin read write transaction: %s", err)
	}
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

	txAmount, err := api.GetBlockTransactionCountByNumber(ctx, rpc.BlockNumber(header.Number.Uint64()))
	if err != nil {
		t.Errorf("failed getting the transaction count, err=%s", err)
	}

	assert.Equal(t, expectedAmount, *txAmount)
}
