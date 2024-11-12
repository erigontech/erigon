package stages

import (
	"context"
	"math/big"
	"os"
	"testing"

	"github.com/0xPolygonHermez/zkevm-data-streamer/datastreamer"
	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv/memdb"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/eth/stagedsync"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/erigon/smt/pkg/db"
	mocks "github.com/ledgerwatch/erigon/zk/datastream/mock_services"
	"github.com/ledgerwatch/erigon/zk/hermez_db"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func TestSpawnStageDataStreamCatchup(t *testing.T) {
	// Arrange
	os.Setenv("CDK_ERIGON_SEQUENCER", "1")

	ctx, db1 := context.Background(), memdb.NewTestDB(t)
	tx1 := memdb.BeginRw(t, db1)
	err := hermez_db.CreateHermezBuckets(tx1)
	require.NoError(t, err)
	err = db.CreateEriDbBuckets(tx1)
	require.NoError(t, err)

	s := &stagedsync.StageState{ID: stages.DataStream, BlockNumber: 0}

	hDB := hermez_db.NewHermezDb(tx1)

	err = hDB.WriteBlockBatch(0, 0)
	require.NoError(t, err)

	genesisHeader := &types.Header{
		Number:      big.NewInt(0),
		Time:        0,
		Difficulty:  big.NewInt(1),
		GasLimit:    8000000,
		GasUsed:     0,
		ParentHash:  common.HexToHash("0x1"),
		TxHash:      common.HexToHash("0x2"),
		ReceiptHash: common.HexToHash("0x3"),
	}

	txs := []types.Transaction{}
	uncles := []*types.Header{}
	receipts := []*types.Receipt{}
	withdrawals := []*types.Withdrawal{}

	genesisBlock := types.NewBlock(genesisHeader, txs, uncles, receipts, withdrawals)

	err = rawdb.WriteBlock(tx1, genesisBlock)
	require.NoError(t, err)
	err = rawdb.WriteCanonicalHash(tx1, genesisBlock.Hash(), genesisBlock.NumberU64())
	require.NoError(t, err)

	err = stages.SaveStageProgress(tx1, stages.DataStream, 0)
	require.NoError(t, err)
	err = stages.SaveStageProgress(tx1, stages.Execution, 20)
	require.NoError(t, err)

	chainID := uint64(1)
	streamVersion := 1

	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()
	streamServerMock := mocks.NewMockStreamServer(mockCtrl)
	dataStreamServerMock := mocks.NewMockDataStreamServer(mockCtrl)

	streamServerHeader := datastreamer.HeaderEntry{TotalEntries: 0}
	streamServerMock.EXPECT().GetHeader().Return(streamServerHeader)

	dataStreamServerMock.EXPECT().GetHighestBlockNumber().Return(uint64(0), nil)
	dataStreamServerMock.EXPECT().GetStreamServer().Return(streamServerMock)

	hDBReaderMatcher := gomock.AssignableToTypeOf(&hermez_db.HermezDbReader{})

	dataStreamServerMock.EXPECT().WriteGenesisToStream(gomock.Cond(func(x any) bool {
		return x.(*types.Block).Hash() == genesisBlock.Hash()
	}), hDBReaderMatcher, tx1).Return(nil)

	dataStreamServerMock.EXPECT().WriteBlocksToStreamConsecutively(ctx, s.LogPrefix(), tx1, hDBReaderMatcher, uint64(1), uint64(20)).Return(nil)

	cfg := StageDataStreamCatchupCfg(dataStreamServerMock, db1, chainID, streamVersion, true)

	// Act
	err = SpawnStageDataStreamCatchup(s, ctx, tx1, cfg)
	require.NoError(t, err)

	// Assert

}
