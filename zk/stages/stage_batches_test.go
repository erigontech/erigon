package stages

import (
	"context"
	"encoding/hex"
	"strings"
	"testing"

	"github.com/ledgerwatch/erigon-lib/chain"
	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/memdb"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/eth/stagedsync"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/erigon/smt/pkg/db"
	"github.com/ledgerwatch/erigon/zk/datastream/types"
	"github.com/ledgerwatch/erigon/zk/erigon_db"
	"github.com/ledgerwatch/erigon/zk/hermez_db"

	"github.com/ledgerwatch/erigon/eth/ethconfig"
	"github.com/stretchr/testify/require"
)

func TestUnwindBatches(t *testing.T) {
	currentBlockNumber := 10
	fullL2Blocks := createTestL2Blocks(t, currentBlockNumber)

	gerUpdates := []types.GerUpdate{}
	for i := currentBlockNumber + 1; i <= currentBlockNumber+5; i++ {
		gerUpdates = append(gerUpdates, types.GerUpdate{
			BatchNumber:    1 + uint64(i/2),
			Timestamp:      uint64(i) * 10000,
			GlobalExitRoot: common.Hash{byte(i)},
			Coinbase:       common.Address{byte(i)},
			ForkId:         1 + uint16(i)/3,
			ChainId:        uint32(1),
			StateRoot:      common.Hash{byte(i)},
		})
	}

	ctx, db1 := context.Background(), memdb.NewTestDB(t)
	tx := memdb.BeginRw(t, db1)
	err := hermez_db.CreateHermezBuckets(tx)
	require.NoError(t, err)

	err = db.CreateEriDbBuckets(tx)
	require.NoError(t, err)

	dsClient := NewTestDatastreamClient(fullL2Blocks, gerUpdates)

	tmpDSClientCreator := func(_ context.Context, _ *ethconfig.Zk, _ uint64) (DatastreamClient, error) {
		return NewTestDatastreamClient(fullL2Blocks, gerUpdates), nil
	}
	cfg := StageBatchesCfg(db1, dsClient, &ethconfig.Zk{}, &chain.Config{}, nil, WithDSClientCreator(tmpDSClientCreator))

	s := &stagedsync.StageState{ID: stages.Batches, BlockNumber: 0}
	u := &stagedsync.Sync{}
	us := &stagedsync.UnwindState{ID: stages.Batches, UnwindPoint: 0, CurrentBlockNumber: uint64(currentBlockNumber)}
	hDB := hermez_db.NewHermezDb(tx)
	err = hDB.WriteBlockBatch(0, 0)
	require.NoError(t, err)
	err = stages.SaveStageProgress(tx, stages.L1VerificationsBatchNo, 20)
	require.NoError(t, err)

	// get bucket sizes pre inserts
	bucketSized := make(map[string]uint64)
	buckets, err := tx.ListBuckets()
	require.NoError(t, err)
	for _, bucket := range buckets {
		size, err := tx.BucketSize(bucket)
		require.NoError(t, err)
		bucketSized[bucket] = size
	}

	/////////
	// ACT //
	/////////
	err = SpawnStageBatches(s, u, ctx, tx, cfg)
	require.NoError(t, err)
	tx.Commit()

	tx2 := memdb.BeginRw(t, db1)

	// unwind to zero and check if there is any data in the tables
	err = UnwindBatchesStage(us, tx2, cfg, ctx)
	require.NoError(t, err)
	tx2.Commit()

	////////////////
	// ASSERTIONS //
	////////////////
	// check if there is any data in the tables
	tx3 := memdb.BeginRw(t, db1)
	buckets, err = tx3.ListBuckets()
	require.NoError(t, err)
	for _, bucket := range buckets {
		//currently not decrementing sequence
		// Unwinded headers will be added to BadHeaderNumber bucket
		// Allow store non-canonical blocks/senders: https://github.com/ledgerwatch/erigon/pull/7648
		if bucket == kv.Sequence || bucket == kv.BadHeaderNumber || bucket == kv.Headers {
			continue
		}
		// this table is deleted in execution stage
		if bucket == kv.TX_PRICE_PERCENTAGE {
			continue
		}
		// header tables (number, canonical, headers)
		if bucket == kv.HeaderNumber || bucket == kv.HeaderCanonical || bucket == kv.Headers {
			continue
		}
		size, err := tx3.BucketSize(bucket)
		require.NoError(t, err)
		require.Equal(t, bucketSized[bucket], size, "bucket %s is not empty", bucket)
	}
}

func TestFindCommonAncestor(t *testing.T) {
	blocksCount := 40
	l2Blocks := createTestL2Blocks(t, blocksCount)

	testCases := []struct {
		name                  string
		dbBlocksCount         int
		dsBlocksCount         int
		latestBlockNum        uint64
		divergentBlockHistory bool
		expectedBlockNum      uint64
		expectedHash          common.Hash
		expectedError         error
	}{
		{
			name:             "Successful search (db lagging behind the data stream)",
			dbBlocksCount:    5,
			dsBlocksCount:    10,
			latestBlockNum:   5,
			expectedBlockNum: 5,
			expectedHash:     common.Hash{byte(5)},
			expectedError:    nil,
		},
		{
			name:             "Successful search (db leading the data stream)",
			dbBlocksCount:    20,
			dsBlocksCount:    10,
			latestBlockNum:   10,
			expectedBlockNum: 10,
			expectedHash:     common.Hash{byte(10)},
			expectedError:    nil,
		},
		{
			name:           "Failed to find common ancestor block (latest block number is 0)",
			dbBlocksCount:  10,
			dsBlocksCount:  10,
			latestBlockNum: 0,
			expectedError:  ErrFailedToFindCommonAncestor,
		},
		{
			name:                  "Failed to find common ancestor block (different blocks in the data stream and db)",
			dbBlocksCount:         10,
			dsBlocksCount:         10,
			divergentBlockHistory: true,
			latestBlockNum:        20,
			expectedError:         ErrFailedToFindCommonAncestor,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// ARRANGE
			testDb, tx := memdb.NewTestTx(t)
			defer testDb.Close()
			defer tx.Rollback()
			err := hermez_db.CreateHermezBuckets(tx)
			require.NoError(t, err)

			err = db.CreateEriDbBuckets(tx)
			require.NoError(t, err)

			hermezDb := hermez_db.NewHermezDb(tx)
			erigonDb := erigon_db.NewErigonDb(tx)

			dsBlocks := l2Blocks[:tc.dsBlocksCount]
			dbBlocks := l2Blocks[:tc.dbBlocksCount]
			if tc.divergentBlockHistory {
				dbBlocks = l2Blocks[tc.dsBlocksCount : tc.dbBlocksCount+tc.dsBlocksCount]
			}

			dsClient := NewTestDatastreamClient(dsBlocks, nil)
			for _, l2Block := range dbBlocks {
				require.NoError(t, hermezDb.WriteBlockBatch(l2Block.L2BlockNumber, l2Block.BatchNumber))
				require.NoError(t, rawdb.WriteCanonicalHash(tx, l2Block.L2Blockhash, l2Block.L2BlockNumber))
			}

			// ACT
			ancestorNum, ancestorHash, err := findCommonAncestor(erigonDb, hermezDb, dsClient, tc.latestBlockNum)

			// ASSERT
			if tc.expectedError != nil {
				require.Error(t, err)
				require.Equal(t, tc.expectedError.Error(), err.Error())
				require.Equal(t, uint64(0), ancestorNum)
				require.Equal(t, emptyHash, ancestorHash)
			} else {
				require.NoError(t, err)
				require.Equal(t, tc.expectedBlockNum, ancestorNum)
				require.Equal(t, tc.expectedHash, ancestorHash)
			}
		})
	}
}

func createTestL2Blocks(t *testing.T, blocksCount int) []types.FullL2Block {
	post155 := "0xf86780843b9aca00826163941275fbb540c8efc58b812ba83b0d0b8b9917ae98808464fbb77c1ba0b7d2a666860f3c6b8f5ef96f86c7ec5562e97fd04c2e10f3755ff3a0456f9feba0246df95217bf9082f84f9e40adb0049c6664a5bb4c9cbe34ab1a73e77bab26ed"
	post155Bytes, err := hex.DecodeString(strings.TrimPrefix(post155, "0x"))
	require.NoError(t, err)

	l2Blocks := make([]types.FullL2Block, 0, blocksCount)
	for i := 1; i <= blocksCount; i++ {
		l2Blocks = append(l2Blocks, types.FullL2Block{
			BatchNumber:     1 + uint64(i/2),
			L2BlockNumber:   uint64(i),
			Timestamp:       int64(i) * 10000,
			DeltaTimestamp:  uint32(i) * 10,
			L1InfoTreeIndex: uint32(i) + 20,
			GlobalExitRoot:  common.Hash{byte(i)},
			Coinbase:        common.Address{byte(i)},
			ForkId:          1 + uint64(i)/3,
			L1BlockHash:     common.Hash{byte(i)},
			L2Blockhash:     common.Hash{byte(i)},
			StateRoot:       common.Hash{byte(i)},
			L2Txs: []types.L2TransactionProto{
				{
					EffectiveGasPricePercentage: 255,
					IsValid:                     true,
					IntermediateStateRoot:       common.Hash{byte(i + 1)},
					Encoded:                     post155Bytes,
				},
			},
			ParentHash: common.Hash{byte(i - 1)},
		})
	}

	return l2Blocks
}
