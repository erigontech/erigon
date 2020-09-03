package stagedsync

import (
	"context"
	"runtime"
	"time"

	"github.com/ledgerwatch/turbo-geth/consensus"
	"github.com/ledgerwatch/turbo-geth/core"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/eth/stagedsync/stages"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/params"
)

func InsertBlocksInStages(db ethdb.Database, config *params.ChainConfig, engine consensus.Engine, blocks []*types.Block, bc *core.BlockChain) (int, error) {
	for i, block := range blocks {
		if err := InsertBlockInStages(db, config, engine, block, bc); err != nil {
			return i, err
		}
	}
	return len(blocks), nil
}

func InsertBlockInStages(db ethdb.Database, config *params.ChainConfig, engine consensus.Engine, block *types.Block, bc *core.BlockChain) error {
	num := block.Number().Uint64()
	// Stage 1
	if _, _, err := InsertHeaderChain(db, []*types.Header{block.Header()}, config, engine, 1); err != nil {
		return err
	}
	if err := stages.SaveStageProgress(db, stages.Headers, num, nil); err != nil {
		return err
	}
	// Stage 2
	if err := SpawnBlockHashStage(&StageState{
		BlockNumber: num - 1,
	}, db, "", nil); err != nil {
		return err
	}

	if err := stages.SaveStageProgress(db, stages.BlockHashes, num, nil); err != nil {
		return err
	}

	// Stage 3
	if _, err := bc.InsertBodyChain(context.TODO(), []*types.Block{block}); err != nil {
		return err
	}

	if err := stages.SaveStageProgress(db, stages.Bodies, num, nil); err != nil {
		return err
	}
	// Stage 4
	const batchSize = 10000
	const blockSize = 4096
	n := runtime.NumCPU()

	cfg := Stage3Config{
		BatchSize:       batchSize,
		BlockSize:       blockSize,
		BufferSize:      (blockSize * 10 / 20) * 10000, // 20*4096
		StartTrace:      false,
		Prof:            false,
		NumOfGoroutines: n,
		ReadChLen:       4,
		Now:             time.Now(),
	}
	if err := SpawnRecoverSendersStage(cfg, &StageState{
		BlockNumber: num - 1,
	}, db, config, 0, "", nil); err != nil {
		return err
	}

	if err := stages.SaveStageProgress(db, stages.Senders, num, nil); err != nil {
		return err
	}
	// Stage 5
	if err := SpawnExecuteBlocksStage(&StageState{
		BlockNumber: num - 1,
	}, db, config, bc, bc.GetVMConfig(), 0, nil, true, false, nil); err != nil {
		return err
	}

	if err := stages.SaveStageProgress(db, stages.Execution, num, nil); err != nil {
		return err
	}

	// Stage 6
	if err := SpawnHashStateStage(&StageState{
		BlockNumber: num - 1,
	}, db, "", nil); err != nil {
		return err
	}

	if err := stages.SaveStageProgress(db, stages.HashState, num, nil); err != nil {
		return err
	}

	// Stage 7
	if err := SpawnIntermediateHashesStage(&StageState{
		BlockNumber: num - 1,
	}, db, "", nil); err != nil {
		return err
	}

	if err := stages.SaveStageProgress(db, stages.IntermediateHashes, num, nil); err != nil {
		return err
	}

	// Stage 8
	if err := SpawnAccountHistoryIndex(&StageState{
		BlockNumber: num - 1,
	}, db, "", nil); err != nil {
		return err
	}

	if err := stages.SaveStageProgress(db, stages.AccountHistoryIndex, num, nil); err != nil {
		return err
	}

	// Stage 9
	if err := SpawnStorageHistoryIndex(&StageState{
		BlockNumber: num - 1,
	}, db, "", nil); err != nil {
		return err
	}

	if err := stages.SaveStageProgress(db, stages.StorageHistoryIndex, num, nil); err != nil {
		return err
	}

	// Stage 10
	if err := SpawnTxLookup(&StageState{}, db, "", nil); err != nil {
		return err
	}

	if err := stages.SaveStageProgress(db, stages.TxLookup, num, nil); err != nil {
		return err
	}

	return nil
}
