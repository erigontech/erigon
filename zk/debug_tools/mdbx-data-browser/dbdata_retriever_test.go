package main

import (
	"fmt"
	"math/big"
	"testing"
	"time"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv/memdb"
	"github.com/stretchr/testify/require"

	"github.com/ledgerwatch/erigon/common/u256"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/erigon/zk/hermez_db"
	rpcTypes "github.com/ledgerwatch/erigon/zk/rpcdaemon"
)

func TestDbDataRetrieverGetBatchByNumber(t *testing.T) {
	const (
		batchNum      = uint64(5)
		blocksInBatch = uint64(6)
	)

	_, dbTx := memdb.NewTestTx(t)
	require.NoError(t, hermez_db.CreateHermezBuckets(dbTx))
	db := hermez_db.NewHermezDb(dbTx)

	block := createBlock(t, 0, nil)
	require.NoError(t, rawdb.WriteCanonicalHash(dbTx, block.Hash(), block.NumberU64()))
	require.NoError(t, rawdb.WriteBlock(dbTx, block))

	expectedBlockHashes := make(map[uint64]libcommon.Hash, blocksInBatch)
	for blockNum := uint64(1); blockNum <= blocksInBatch; blockNum++ {
		require.NoError(t, db.WriteBlockBatch(blockNum, batchNum))
		tx := types.NewTransaction(1, libcommon.HexToAddress(fmt.Sprintf("0x100%d", blockNum)), u256.Num1, 1, u256.Num1, nil)
		block := createBlock(t, blockNum, types.Transactions{tx})
		require.NoError(t, rawdb.WriteCanonicalHash(dbTx, block.Hash(), block.NumberU64()))
		require.NoError(t, rawdb.WriteBlock(dbTx, block))
		expectedBlockHashes[blockNum] = block.Hash()
	}

	err := stages.SaveStageProgress(dbTx, stages.Execution, blocksInBatch)
	require.NoError(t, err)

	dbReader := NewDbDataRetriever(dbTx)
	batch, err := dbReader.GetBatchByNumber(batchNum, true)
	require.NoError(t, err)

	require.Equal(t, batchNum, uint64(batch.Number))
	require.Len(t, expectedBlockHashes, int(blocksInBatch))
	for _, blockGeneric := range batch.Blocks {
		block, ok := blockGeneric.(*rpcTypes.BlockWithInfoRootAndGer)
		require.True(t, ok)
		expectedHash, exists := expectedBlockHashes[uint64(block.Number)]
		require.True(t, exists)
		require.Equal(t, expectedHash, block.Hash)
	}
}

func TestDbDataRetrieverGetBlockByNumber(t *testing.T) {
	t.Run("querying an existing block", func(t *testing.T) {
		// arrange
		_, tx := memdb.NewTestTx(t)
		tx1 := types.NewTransaction(1, libcommon.HexToAddress("0x1050"), u256.Num1, 1, u256.Num1, nil)
		tx2 := types.NewTransaction(2, libcommon.HexToAddress("0x100"), u256.Num27, 2, u256.Num2, nil)

		block := createBlock(t, 5, types.Transactions{tx1, tx2})

		require.NoError(t, rawdb.WriteCanonicalHash(tx, block.Hash(), block.NumberU64()))
		require.NoError(t, rawdb.WriteBlock(tx, block))

		// act and assert
		dbReader := NewDbDataRetriever(tx)
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

func TestDbDataRetrieverGetBatchAffiliation(t *testing.T) {
	testCases := []struct {
		name           string
		blocksInBatch  int
		batchesCount   int
		blockNums      []uint64
		expectedErrMsg string
		expectedResult []*BatchAffiliationInfo
	}{
		{
			name:          "Basic case with three blocks and two requested",
			blocksInBatch: 3,
			batchesCount:  2,
			blockNums:     []uint64{1, 3},
			expectedResult: []*BatchAffiliationInfo{
				{Number: 1, Blocks: []uint64{1, 3}},
			},
		},
		{
			name:          "All blocks in batch requested",
			blocksInBatch: 3,
			batchesCount:  2,
			blockNums:     []uint64{4, 5, 6},
			expectedResult: []*BatchAffiliationInfo{
				{Number: 2, Blocks: []uint64{4, 5, 6}},
			},
		},
		{
			name:          "Request multiple batches",
			blocksInBatch: 2,
			batchesCount:  3,
			blockNums:     []uint64{1, 2, 6},
			expectedResult: []*BatchAffiliationInfo{
				{Number: 1, Blocks: []uint64{1, 2}},
				{Number: 3, Blocks: []uint64{6}},
			},
		},
		{
			name:           "Request non-existent block",
			blocksInBatch:  2,
			batchesCount:   2,
			blockNums:      []uint64{5},
			expectedErrMsg: "batch is not found for block num 5",
			expectedResult: nil,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			_, dbTx := memdb.NewTestTx(t)
			require.NoError(t, hermez_db.CreateHermezBuckets(dbTx))
			db := hermez_db.NewHermezDb(dbTx)

			// Write the blocks according to the test case
			blockNum := uint64(1)
			for batchNum := uint64(1); batchNum <= uint64(tc.batchesCount); batchNum++ {
				for i := 0; i < tc.blocksInBatch; i++ {
					require.NoError(t, db.WriteBlockBatch(blockNum, batchNum))
					blockNum++
				}
			}

			dbReader := NewDbDataRetriever(dbTx)
			batchAffiliation, err := dbReader.GetBatchAffiliation(tc.blockNums)

			// Check if an error was expected
			if tc.expectedErrMsg != "" {
				require.ErrorContains(t, err, tc.expectedErrMsg)
			} else {
				require.NoError(t, err)
			}

			require.Equal(t, tc.expectedResult, batchAffiliation)
		})
	}
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
			Time:        uint64(time.Now().Unix()),
		})

	if txs.Len() > 0 {
		block = block.WithBody(txs, nil)
	}

	return block
}
