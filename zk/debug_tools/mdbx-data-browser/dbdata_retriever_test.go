package main

import (
	"fmt"
	"math/big"
	"testing"

	libcommon "github.com/gateway-fm/cdk-erigon-lib/common"
	"github.com/gateway-fm/cdk-erigon-lib/kv/memdb"
	"github.com/stretchr/testify/require"

	"github.com/ledgerwatch/erigon/common/u256"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/zk/hermez_db"
)

func TestDbDataRetrieverGetBatchByNumber(t *testing.T) {
	const (
		batchNum      = uint64(5)
		blocksInBatch = uint64(6)
	)

	_, dbTx := memdb.NewTestTx(t)
	require.NoError(t, hermez_db.CreateHermezBuckets(dbTx))
	db := hermez_db.NewHermezDb(dbTx)
	expectedBlockHashes := make(map[uint64]libcommon.Hash, blocksInBatch)
	for blockNum := uint64(1); blockNum <= blocksInBatch; blockNum++ {
		require.NoError(t, db.WriteBlockBatch(blockNum, batchNum))
		tx := types.NewTransaction(1, libcommon.HexToAddress(fmt.Sprintf("0x100%d", blockNum)), u256.Num1, 1, u256.Num1, nil)
		block := createBlock(t, blockNum, types.Transactions{tx})
		require.NoError(t, rawdb.WriteCanonicalHash(dbTx, block.Hash(), block.NumberU64()))
		require.NoError(t, rawdb.WriteBlock(dbTx, block))
		expectedBlockHashes[blockNum] = block.Hash()
	}

	dbReader := NewDbDataRetriever(dbTx)
	batch, err := dbReader.GetBatchByNumber(batchNum, true)
	require.NoError(t, err)

	require.Equal(t, batchNum, uint64(batch.Number))
	require.Len(t, expectedBlockHashes, int(blocksInBatch))
	for _, blockGeneric := range batch.Blocks {
		block, ok := blockGeneric.(*types.Block)
		require.True(t, ok)
		expectedHash, exists := expectedBlockHashes[block.NumberU64()]
		require.True(t, exists)
		require.Equal(t, expectedHash, block.Hash())
	}
}

func TestDbDataRetrieverGetBlockByNumber(t *testing.T) {
	t.Run("querying an existing block", func(t *testing.T) {
		// arrange
		_, dbTx := memdb.NewTestTx(t)
		tx1 := types.NewTransaction(1, libcommon.HexToAddress("0x1050"), u256.Num1, 1, u256.Num1, nil)
		tx2 := types.NewTransaction(2, libcommon.HexToAddress("0x100"), u256.Num27, 2, u256.Num2, nil)

		block := createBlock(t, 5, types.Transactions{tx1, tx2})

		require.NoError(t, rawdb.WriteCanonicalHash(dbTx, block.Hash(), block.NumberU64()))
		require.NoError(t, rawdb.WriteBlock(dbTx, block))

		// act and assert
		dbReader := NewDbDataRetriever(dbTx)
		result, err := dbReader.GetBlockByNumber(block.NumberU64(), true, true)
		require.NoError(t, err)
		require.Equal(t, block.Hash(), result.Hash)
		require.Equal(t, block.Number().Uint64(), uint64(result.Number))
	})

	t.Run("querying non-existent block", func(t *testing.T) {
		blockNum := uint64(10)

		_, tx := memdb.NewTestTx(t)
		dbReader := NewDbDataRetriever(tx)
		result, err := dbReader.GetBlockByNumber(blockNum, true, true)
		require.ErrorContains(t, err, fmt.Sprintf("block %d not found", blockNum))
		require.Nil(t, result)
	})
}

// createBlock is a helper function, that allows creating block
func createBlock(t *testing.T, number uint64, txs types.Transactions) *types.Block {
	t.Helper()

	block := types.NewBlockWithHeader(
		&types.Header{
			Number:      new(big.Int).SetUint64(number),
			Extra:       []byte("some random data"),
			UncleHash:   types.EmptyUncleHash,
			TxHash:      types.EmptyRootHash,
			ReceiptHash: types.EmptyRootHash,
		})

	if txs.Len() > 0 {
		block = block.WithBody(txs, nil)
	}

	return block
}
