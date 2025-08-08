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
	"github.com/erigontech/erigon/smt/pkg/db"
	"github.com/erigontech/erigon/zk/contracts"
	"github.com/erigontech/erigon/zk/hermez_db"
	"github.com/erigontech/erigon/zk/l1infotree"
	"github.com/erigontech/erigon/zk/syncer"
	"github.com/erigontech/erigon/zk/syncer/mocks"
	"github.com/iden3/go-iden3-crypto/keccak256"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func TestSpawnL1InfoTreeStage(t *testing.T) {
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
	err = stages.SaveStageProgress(tx, stages.L1InfoTree, 20)
	require.NoError(t, err)

	s := &stagedsync.StageState{ID: stages.L1InfoTree, BlockNumber: 0}
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

	latestBlockParentHash := common.HexToHash("0x123456789")
	latestBlockTime := uint64(time.Now().Unix())
	latestBlockNumber := big.NewInt(21)
	latestBlockHeader := &types.Header{ParentHash: latestBlockParentHash, Number: latestBlockNumber, Time: latestBlockTime}
	latestBlock := types.NewBlockWithHeader(latestBlockHeader)

	EthermanMock.EXPECT().HeaderByNumber(gomock.Any(), latestBlockNumber).Return(latestBlockHeader, nil).AnyTimes()
	EthermanMock.EXPECT().BlockByNumber(gomock.Any(), nil).Return(latestBlock, nil).AnyTimes()
	filterQuery := ethereum.FilterQuery{
		FromBlock: latestBlockNumber,
		ToBlock:   latestBlockNumber,
		Addresses: l1ContractAddresses,
		Topics:    l1ContractTopics,
	}
	mainnetExitRoot := common.HexToHash("0x111")
	rollupExitRoot := common.HexToHash("0x222")

	l1InfoTreeLog := types.Log{
		BlockNumber: latestBlockNumber.Uint64(),
		Address:     l1ContractAddresses[0],
		Topics:      []common.Hash{contracts.UpdateL1InfoTreeTopic, mainnetExitRoot, rollupExitRoot},
	}
	filteredLogs := []types.Log{l1InfoTreeLog}
	EthermanMock.EXPECT().FilterLogs(gomock.Any(), filterQuery).Return(filteredLogs, nil).AnyTimes()

	l1Syncer := syncer.NewL1Syncer(ctx, []syncer.IEtherman{EthermanMock}, l1ContractAddresses, l1ContractTopics, 10, 0, "latest", 0)
	updater := l1infotree.NewUpdater(&ethconfig.Zk{}, l1Syncer, l1infotree.NewInfoTreeL2RpcSyncer(ctx, &ethconfig.Zk{
		L2RpcUrl: "http://127.0.0.1:8545",
	}))
	cfg := StageL1InfoTreeCfg(db1, &ethconfig.Zk{}, updater)

	// act
	err = SpawnL1InfoTreeStage(s, u, tx, cfg, ctx, log.New())
	require.NoError(t, err)

	// assert
	// check tree
	tree, err := l1infotree.InitialiseL1InfoTree(hDB)
	require.NoError(t, err)

	combined := append(mainnetExitRoot.Bytes(), rollupExitRoot.Bytes()...)
	gerBytes := keccak256.Hash(combined)
	ger := common.BytesToHash(gerBytes)
	leafBytes := l1infotree.HashLeafData(ger, latestBlockParentHash, latestBlockTime)

	assert.True(t, tree.LeafExists(leafBytes))

	// check WriteL1InfoTreeLeaf
	leaves, err := hDB.GetAllL1InfoTreeLeaves()
	require.NoError(t, err)

	leafHash := common.BytesToHash(leafBytes[:])
	assert.Len(t, leaves, 1)
	assert.Equal(t, leafHash.String(), leaves[0].String())

	// check WriteL1InfoTreeUpdate
	l1InfoTreeUpdate, err := hDB.GetL1InfoTreeUpdate(0)
	require.NoError(t, err)

	assert.Equal(t, uint64(0), l1InfoTreeUpdate.Index)
	assert.Equal(t, ger, l1InfoTreeUpdate.GER)
	assert.Equal(t, mainnetExitRoot, l1InfoTreeUpdate.MainnetExitRoot)
	assert.Equal(t, rollupExitRoot, l1InfoTreeUpdate.RollupExitRoot)
	assert.Equal(t, latestBlockNumber.Uint64(), l1InfoTreeUpdate.BlockNumber)
	assert.Equal(t, latestBlockTime, l1InfoTreeUpdate.Timestamp)
	assert.Equal(t, latestBlockParentHash, l1InfoTreeUpdate.ParentHash)

	//check  WriteL1InfoTreeUpdateToGer
	l1InfoTreeUpdateToGer, err := hDB.GetL1InfoTreeUpdateByGer(ger)
	require.NoError(t, err)

	assert.Equal(t, uint64(0), l1InfoTreeUpdateToGer.Index)
	assert.Equal(t, ger, l1InfoTreeUpdateToGer.GER)
	assert.Equal(t, mainnetExitRoot, l1InfoTreeUpdateToGer.MainnetExitRoot)
	assert.Equal(t, rollupExitRoot, l1InfoTreeUpdateToGer.RollupExitRoot)
	assert.Equal(t, latestBlockNumber.Uint64(), l1InfoTreeUpdateToGer.BlockNumber)
	assert.Equal(t, latestBlockTime, l1InfoTreeUpdateToGer.Timestamp)
	assert.Equal(t, latestBlockParentHash, l1InfoTreeUpdateToGer.ParentHash)

	// check WriteL1InfoTreeRoot
	root, _, _ := tree.GetCurrentRootCountAndSiblings()
	index, found, err := hDB.GetL1InfoTreeIndexByRoot(root)
	assert.NoError(t, err)
	assert.Equal(t, uint64(0), index)
	assert.True(t, found)

	// check SaveStageProgress
	progress, err := stages.GetStageProgress(tx, stages.L1InfoTree)
	require.NoError(t, err)
	assert.Equal(t, latestBlockNumber.Uint64(), progress)
}
