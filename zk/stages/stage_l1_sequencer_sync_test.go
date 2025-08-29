package stages

import (
	"context"
	"math/big"
	"testing"
	"time"

	ethereum "github.com/erigontech/erigon"
	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/kv/memdb"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/core/types"
	"github.com/erigontech/erigon/eth/ethconfig"
	"github.com/erigontech/erigon/eth/stagedsync"
	"github.com/erigontech/erigon/eth/stagedsync/stages"
	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/smt/pkg/db"
	"github.com/erigontech/erigon/zk/contracts"
	"github.com/erigontech/erigon/zk/hermez_db"
	"github.com/erigontech/erigon/zk/syncer"
	"github.com/erigontech/erigon/zk/syncer/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func TestSpawnL1SequencerSyncStage(t *testing.T) {
	// arrange
	ctx, db1 := context.Background(), memdb.NewTestDB(t)
	tx := memdb.BeginRw(t, db1)
	err := hermez_db.CreateHermezBuckets(tx)
	require.NoError(t, err)
	err = db.CreateEriDbBuckets(tx)
	require.NoError(t, err)

	hDB := hermez_db.NewHermezDb(tx)
	err = hDB.WriteBlockBatch(0, 0)
	require.NoError(t, err)
	err = stages.SaveStageProgress(tx, stages.L1SequencerSync, 0)
	require.NoError(t, err)

	s := &stagedsync.StageState{ID: stages.L1SequencerSync, BlockNumber: 0}
	u := &stagedsync.Sync{}

	// mocks
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()
	EthermanMock := mocks.NewMockIEtherman(mockCtrl)

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

	l1FirstBlock := big.NewInt(20)

	finalizedBlockParentHash := common.HexToHash("0x123456789")
	finalizedBlockTime := uint64(time.Now().Unix())
	finalizedBlockNumber := big.NewInt(21)
	finalizedBlockHeader := &types.Header{ParentHash: finalizedBlockParentHash, Number: finalizedBlockNumber, Time: finalizedBlockTime}
	finalizedBlock := types.NewBlockWithHeader(finalizedBlockHeader)

	latestBlockParentHash := finalizedBlock.Hash()
	latestBlockTime := uint64(time.Now().Unix())
	latestBlockNumber := big.NewInt(22)
	latestBlockHeader := &types.Header{ParentHash: latestBlockParentHash, Number: latestBlockNumber, Time: latestBlockTime}
	latestBlock := types.NewBlockWithHeader(latestBlockHeader)

	EthermanMock.EXPECT().HeaderByNumber(gomock.Any(), finalizedBlockNumber).Return(finalizedBlockHeader, nil).AnyTimes()
	EthermanMock.EXPECT().BlockByNumber(gomock.Any(), big.NewInt(rpc.FinalizedBlockNumber.Int64())).Return(finalizedBlock, nil).AnyTimes()
	EthermanMock.EXPECT().HeaderByNumber(gomock.Any(), latestBlockNumber).Return(latestBlockHeader, nil).AnyTimes()
	EthermanMock.EXPECT().BlockByNumber(gomock.Any(), nil).Return(latestBlock, nil).AnyTimes()

	filterQuery := ethereum.FilterQuery{
		FromBlock: l1FirstBlock,
		ToBlock:   latestBlockNumber,
		Addresses: l1ContractAddresses,
		Topics:    l1ContractTopics,
	}

	type testCase struct {
		name   string
		getLog func(hDB *hermez_db.HermezDb) (types.Log, error)
		assert func(t *testing.T, hDB *hermez_db.HermezDb)
	}

	const (
		forkIdBytesStartPosition = 64
		forkIdBytesEndPosition   = 96
		rollupDataSize           = 128

		injectedBatchLogTransactionStartByte = 128
		injectedBatchLastGerStartByte        = 32
		injectedBatchLastGerEndByte          = 64
		injectedBatchSequencerStartByte      = 76
		injectedBatchSequencerEndByte        = 96
	)

	testCases := []testCase{
		{
			name: "InitialSequenceBatchesTopic",
			getLog: func(hDB *hermez_db.HermezDb) (types.Log, error) {
				ger := common.HexToHash("0x111111111")
				sequencer := common.HexToAddress("0x222222222")
				batchL2Data := common.HexToHash("0x333333333")

				initialSequenceBatchesData := make([]byte, 200)
				copy(initialSequenceBatchesData[injectedBatchLastGerStartByte:injectedBatchLastGerEndByte], ger.Bytes())
				copy(initialSequenceBatchesData[injectedBatchSequencerStartByte:injectedBatchSequencerEndByte], sequencer.Bytes())
				copy(initialSequenceBatchesData[injectedBatchLogTransactionStartByte:], batchL2Data.Bytes())
				return types.Log{
					BlockNumber: latestBlockNumber.Uint64(),
					Address:     l1ContractAddresses[0],
					Topics:      []common.Hash{contracts.InitialSequenceBatchesTopic},
					Data:        initialSequenceBatchesData,
				}, nil
			},
			assert: func(t *testing.T, hDB *hermez_db.HermezDb) {
				ger := common.HexToHash("0x111111111")
				sequencer := common.HexToAddress("0x222222222")
				batchL2Data := common.HexToHash("0x333333333")

				l1InjectedBatch, err := hDB.GetL1InjectedBatch(0)
				require.NoError(t, err)

				assert.Equal(t, l1InjectedBatch.L1BlockNumber, latestBlock.NumberU64())
				assert.Equal(t, l1InjectedBatch.Timestamp, latestBlock.Time())
				assert.Equal(t, l1InjectedBatch.L1BlockHash, latestBlock.Hash())
				assert.Equal(t, l1InjectedBatch.L1ParentHash, latestBlock.ParentHash())
				assert.Equal(t, l1InjectedBatch.LastGlobalExitRoot.String(), ger.String())
				assert.Equal(t, l1InjectedBatch.Sequencer.String(), sequencer.String())
				assert.ElementsMatch(t, l1InjectedBatch.Transaction, batchL2Data.Bytes())
			},
		},
		{
			name: "AddNewRollupType",
			getLog: func(hDB *hermez_db.HermezDb) (types.Log, error) {
				rollupType := uint64(1)
				rollupTypeHash := common.BytesToHash(big.NewInt(0).SetUint64(rollupType).Bytes())
				rollupData := make([]byte, rollupDataSize)
				rollupForkId := uint64(111)
				rollupForkIdHash := common.BytesToHash(big.NewInt(0).SetUint64(rollupForkId).Bytes())
				copy(rollupData[forkIdBytesStartPosition:forkIdBytesEndPosition], rollupForkIdHash.Bytes())
				return types.Log{
					BlockNumber: latestBlockNumber.Uint64(),
					Address:     l1ContractAddresses[0],
					Topics:      []common.Hash{contracts.AddNewRollupTypeTopic, rollupTypeHash},
					Data:        rollupData,
				}, nil
			},
			assert: func(t *testing.T, hDB *hermez_db.HermezDb) {
				forkID, err := hDB.GetForkFromRollupType(uint64(1))
				require.NoError(t, err)

				assert.Equal(t, forkID, uint64(111))
			},
		},
		{
			name: "AddNewRollupTypeTopicBanana",
			getLog: func(hDB *hermez_db.HermezDb) (types.Log, error) {
				rollupType := uint64(2)
				rollupTypeHash := common.BytesToHash(big.NewInt(0).SetUint64(rollupType).Bytes())
				rollupData := make([]byte, rollupDataSize)
				rollupForkId := uint64(222)
				rollupForkIdHash := common.BytesToHash(big.NewInt(0).SetUint64(rollupForkId).Bytes())
				copy(rollupData[forkIdBytesStartPosition:forkIdBytesEndPosition], rollupForkIdHash.Bytes())
				return types.Log{
					BlockNumber: latestBlockNumber.Uint64(),
					Address:     l1ContractAddresses[0],
					Topics:      []common.Hash{contracts.AddNewRollupTypeTopicBanana, rollupTypeHash},
					Data:        rollupData,
				}, nil
			},
			assert: func(t *testing.T, hDB *hermez_db.HermezDb) {
				forkID, err := hDB.GetForkFromRollupType(uint64(2))
				require.NoError(t, err)

				assert.Equal(t, forkID, uint64(222))
			},
		},
		{
			name: "CreateNewRollupTopic",
			getLog: func(hDB *hermez_db.HermezDb) (types.Log, error) {
				rollupID := uint64(99999)
				rollupIDHash := common.BytesToHash(big.NewInt(0).SetUint64(rollupID).Bytes())
				rollupType := uint64(33)
				rollupForkID := uint64(333)
				if funcErr := hDB.WriteRollupType(rollupType, rollupForkID); funcErr != nil {
					return types.Log{}, funcErr
				}
				newRollupDataCreation := common.BytesToHash(big.NewInt(0).SetUint64(rollupType).Bytes()).Bytes()

				return types.Log{
					BlockNumber: latestBlockNumber.Uint64(),
					Address:     l1ContractAddresses[0],
					Topics:      []common.Hash{contracts.CreateNewRollupTopic, rollupIDHash},
					Data:        newRollupDataCreation,
				}, nil
			},
			assert: func(t *testing.T, hDB *hermez_db.HermezDb) {
				forks, batches, err := hDB.GetAllForkHistory()
				for i := 0; i < len(forks); i++ {
					if forks[i] == uint64(333) {
						assert.Equal(t, batches[i], uint64(0))
						break
					}
				}
				require.NoError(t, err)
			},
		},
		{
			name: "UpdateRollupTopic",
			getLog: func(hDB *hermez_db.HermezDb) (types.Log, error) {
				rollupID := uint64(99999)
				rollupIDHash := common.BytesToHash(big.NewInt(0).SetUint64(rollupID).Bytes())
				rollupType := uint64(44)
				rollupTypeHash := common.BytesToHash(big.NewInt(0).SetUint64(rollupType).Bytes())
				rollupForkID := uint64(444)
				if funcErr := hDB.WriteRollupType(rollupType, rollupForkID); funcErr != nil {
					return types.Log{}, funcErr
				}
				latestVerified := uint64(4444)
				latestVerifiedHash := common.BytesToHash(big.NewInt(0).SetUint64(latestVerified).Bytes())
				updateRollupData := rollupTypeHash.Bytes()
				updateRollupData = append(updateRollupData, latestVerifiedHash.Bytes()...)

				return types.Log{
					BlockNumber: latestBlockNumber.Uint64(),
					Address:     l1ContractAddresses[0],
					Topics:      []common.Hash{contracts.UpdateRollupTopic, rollupIDHash},
					Data:        updateRollupData,
				}, nil
			},
			assert: func(t *testing.T, hDB *hermez_db.HermezDb) {
				forks, batches, err := hDB.GetAllForkHistory()
				for i := 0; i < len(forks); i++ {
					if forks[i] == uint64(444) {
						assert.Equal(t, batches[i], uint64(4444))
						break
					}
				}
				require.NoError(t, err)
			},
		},
	}

	filteredLogs := []types.Log{}
	for _, tc := range testCases {
		ll, err := tc.getLog(hDB)
		require.NoError(t, err)
		filteredLogs = append(filteredLogs, ll)
	}

	EthermanMock.EXPECT().FilterLogs(gomock.Any(), filterQuery).Return(filteredLogs, nil).AnyTimes()

	l1Syncer := syncer.NewL1Syncer(ctx, []syncer.IEtherman{EthermanMock}, l1ContractAddresses, l1ContractTopics, 10, 0, "latest", 0)
	zkCfg := &ethconfig.Zk{
		L1RollupId:                  uint64(99999),
		L1FirstBlock:                l1FirstBlock.Uint64(),
		L1FinalizedBlockRequirement: uint64(21),
	}
	cfg := StageL1SequencerSyncCfg(db1, zkCfg, l1Syncer)

	// act
	err = SpawnL1SequencerSyncStage(s, u, tx, cfg, ctx, log.New())
	require.NoError(t, err)

	// assert
	for _, tc := range testCases {
		tc.assert(t, hDB)
	}
}

func TestUnwindL1SequencerSyncStage(t *testing.T) {
	err := UnwindL1SequencerSyncStage(nil, nil, L1SequencerSyncCfg{}, context.Background())
	assert.Nil(t, err)
}

func TestPruneL1SequencerSyncStage(t *testing.T) {
	err := PruneL1SequencerSyncStage(nil, nil, L1SequencerSyncCfg{}, context.Background())
	assert.Nil(t, err)
}
