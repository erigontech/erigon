package stages

import (
	"context"
	"math/big"
	"testing"
	"time"

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon-lib/chain"
	"github.com/ledgerwatch/erigon-lib/common"
	cMocks "github.com/ledgerwatch/erigon-lib/kv/kvcache/mocks"
	"github.com/ledgerwatch/erigon-lib/kv/memdb"
	"github.com/ledgerwatch/erigon-lib/txpool/txpoolcfg"
	"github.com/ledgerwatch/erigon/consensus"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/eth/ethconfig"
	"github.com/ledgerwatch/erigon/eth/stagedsync"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/erigon/ethdb/prune"
	"github.com/ledgerwatch/erigon/smt/pkg/db"
	dsMocks "github.com/ledgerwatch/erigon/zk/datastream/mocks"
	"github.com/ledgerwatch/erigon/zk/hermez_db"
	"github.com/ledgerwatch/erigon/zk/l1infotree"
	verifier "github.com/ledgerwatch/erigon/zk/legacy_executor_verifier"
	"github.com/ledgerwatch/erigon/zk/syncer"
	"github.com/ledgerwatch/erigon/zk/syncer/mocks"
	"github.com/ledgerwatch/erigon/zk/txpool"
	zkTypes "github.com/ledgerwatch/erigon/zk/types"
	"github.com/ledgerwatch/log/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func TestSpawnSequencingStage(t *testing.T) {
	// Arrange
	ctx, db1, txPoolDb := context.Background(), memdb.NewTestDB(t), memdb.NewTestDB(t)
	tx := memdb.BeginRw(t, db1)
	err := hermez_db.CreateHermezBuckets(tx)
	require.NoError(t, err)
	err = db.CreateEriDbBuckets(tx)
	require.NoError(t, err)

	chainID := *uint256.NewInt(1)
	forkID := uint64(11)
	latestBatchNumber := uint64(20)
	latestL1BlockNumber := big.NewInt(100)
	latestL2BlockNumber := big.NewInt(100)
	l1ContractAddresses := []common.Address{
		common.HexToAddress("0x1"),
		common.HexToAddress("0x2"),
		common.HexToAddress("0x3"),
	}
	l1ContractTopics := [][]common.Hash{
		[]common.Hash{common.HexToHash("0x1")},
		[]common.Hash{common.HexToHash("0x2")},
		[]common.Hash{common.HexToHash("0x3")},
	}

	hDB := hermez_db.NewHermezDb(tx)

	err = hDB.WriteForkId(latestBatchNumber, forkID)
	require.NoError(t, err)

	err = hDB.WriteNewForkHistory(forkID, latestBatchNumber)
	require.NoError(t, err)

	err = stages.SaveStageProgress(tx, stages.HighestSeenBatchNumber, latestBatchNumber)
	require.NoError(t, err)

	err = stages.SaveStageProgress(tx, stages.Execution, latestL1BlockNumber.Uint64())
	require.NoError(t, err)

	hDB.WriteL1InfoTreeUpdate(&zkTypes.L1InfoTreeUpdate{
		Index:           1,
		GER:             common.HexToHash("0x1"),
		MainnetExitRoot: common.HexToHash("0x2"),
		RollupExitRoot:  common.HexToHash("0x3"),
		ParentHash:      common.HexToHash("0x4"),
		Timestamp:       100,
		BlockNumber:     latestL2BlockNumber.Uint64(),
	})

	latestL2BlockParentHash := common.HexToHash("0x123456789")
	latestL2BlockTime := uint64(time.Now().Unix())
	latestL2BlockHeader := &types.Header{ParentHash: latestL2BlockParentHash, Number: latestL2BlockNumber, Time: latestL2BlockTime}
	latestL2Block := types.NewBlockWithHeader(latestL2BlockHeader)

	err = rawdb.WriteBlock(tx, latestL2Block)
	require.NoError(t, err)
	err = rawdb.WriteCanonicalHash(tx, latestL2Block.Hash(), latestL2Block.NumberU64())
	require.NoError(t, err)

	err = tx.Commit()
	require.NoError(t, err)

	s := &stagedsync.StageState{ID: stages.HighestSeenBatchNumber, BlockNumber: latestBatchNumber}
	u := &stagedsync.Sync{}

	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()
	dataStreamServerMock := dsMocks.NewMockDataStreamServer(mockCtrl)
	ethermanMock := mocks.NewMockIEtherman(mockCtrl)
	engineMock := consensus.NewMockEngine(mockCtrl)

	dataStreamServerMock.EXPECT().GetHighestBatchNumber().Return(latestBatchNumber, nil).AnyTimes()
	dataStreamServerMock.EXPECT().GetHighestClosedBatch().Return(latestBatchNumber, nil).AnyTimes()
	dataStreamServerMock.EXPECT().GetHighestBlockNumber().Return(latestL2BlockNumber.Uint64(), nil).AnyTimes()
	dataStreamServerMock.EXPECT().
		WriteBlockWithBatchStartToStream(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil).
		AnyTimes()

	latestL1BlockParentHash := common.HexToHash("0x123456789")
	latestL1BlockTime := uint64(time.Now().Unix())
	latestL1BlockHeader := &types.Header{ParentHash: latestL1BlockParentHash, Number: latestL1BlockNumber, Time: latestL1BlockTime}
	latestL1Block := types.NewBlockWithHeader(latestL1BlockHeader)

	ethermanMock.EXPECT().BlockByNumber(gomock.Any(), nil).Return(latestL1Block, nil).AnyTimes()

	l1Syncer := syncer.NewL1Syncer(ctx, []syncer.IEtherman{ethermanMock}, l1ContractAddresses, l1ContractTopics, 10, 0, "latest")
	updater := l1infotree.NewUpdater(&ethconfig.Zk{}, l1Syncer)

	cacheMock := cMocks.NewMockCache(mockCtrl)
	cacheMock.EXPECT().View(gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()

	txPool, err := txpool.New(nil, txPoolDb, txpoolcfg.Config{}, &ethconfig.Config{}, cacheMock, chainID, nil, nil, nil)
	require.NoError(t, err)

	engineMock.EXPECT().
		Type().
		Return(chain.CliqueConsensus).
		AnyTimes()
	engineMock.EXPECT().
		FinalizeAndAssemble(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(config *chain.Config, header *types.Header, state *state.IntraBlockState, txs types.Transactions, uncles []*types.Header, receipts types.Receipts, withdrawals []*types.Withdrawal, chain consensus.ChainReader, syscall consensus.SystemCall, call consensus.Call, logger log.Logger) (*types.Block, types.Transactions, types.Receipts, error) {
			finalBlock := types.NewBlockWithHeader(header)
			return finalBlock, txs, receipts, nil
		}).
		AnyTimes()

	zkCfg := &ethconfig.Zk{
		SequencerResequence: false,
		// lower batch close time ensures only 1 block will be created on 1 turn, as the test expects
		SequencerBatchSealTime:      4 * time.Second,
		SequencerBlockSealTime:      5 * time.Second,
		SequencerEmptyBlockSealTime: 5 * time.Second,
		InfoTreeUpdateInterval:      5 * time.Second,
	}

	legacyVerifier := verifier.NewLegacyExecutorVerifier(*zkCfg, nil, db1, nil, nil)

	cfg := SequenceBlockCfg{
		dataStreamServer: dataStreamServerMock,
		db:               db1,
		zk:               zkCfg,
		infoTreeUpdater:  updater,
		txPool:           txPool,
		chainConfig:      &chain.Config{ChainID: chainID.ToBig()},
		txPoolDb:         txPoolDb,
		engine:           engineMock,
		legacyVerifier:   legacyVerifier,
	}
	historyCfg := stagedsync.StageHistoryCfg(db1, prune.DefaultMode, "")
	quiet := true

	// Act
	err = SpawnSequencingStage(s, u, ctx, cfg, historyCfg, quiet)
	require.NoError(t, err)

	// Assert
	tx = memdb.BeginRw(t, db1)
	hDB = hermez_db.NewHermezDb(tx)

	// WriteBlockL1InfoTreeIndex
	l1InfoTreeIndex, err := hDB.GetBlockL1InfoTreeIndex(101)
	require.NoError(t, err)
	assert.Equal(t, uint64(1), l1InfoTreeIndex)

	// WriteBlockL1InfoTreeIndexProgress
	blockNumber, l1InfoTreeIndex, err := hDB.GetLatestBlockL1InfoTreeIndexProgress()
	require.NoError(t, err)
	assert.Equal(t, uint64(101), blockNumber)
	assert.Equal(t, uint64(1), l1InfoTreeIndex)

	// WriteBlockInfoRoot
	root, err := hDB.GetBlockInfoRoot(101)
	require.NoError(t, err)
	assert.Equal(t, uint64(101), blockNumber)
	assert.NotEmpty(t, root.String())

	// IncrementStateVersionByBlockNumberIfNeeded
	blockNumber, stateVersion, err := rawdb.GetLatestStateVersion(tx)
	require.NoError(t, err)
	assert.Equal(t, uint64(101), blockNumber)
	assert.Equal(t, uint64(1), stateVersion)
	tx.Rollback()
}
