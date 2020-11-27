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
	if _, _, err := InsertHeaderChain("logPrefix", db, []*types.Header{block.Header()}, config, engine, 1); err != nil {
		return err
	}
	if err := stages.SaveStageProgress(db, stages.Headers, num, nil); err != nil {
		return err
	}
	// Stage 2
	if err := SpawnBlockHashStage(&StageState{
		Stage:       stages.BlockHashes,
		BlockNumber: num - 1,
	}, db, "", nil); err != nil {
		return err
	}

	// Stage 3
	if _, err := bc.InsertBodyChain("logPrefix", context.TODO(), []*types.Block{block}); err != nil {
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
		NumOfGoroutines: n,
		ReadChLen:       4,
		Now:             time.Now(),
	}
	if err := SpawnRecoverSendersStage(cfg, &StageState{
		Stage:       stages.Senders,
		BlockNumber: num - 1,
	}, db, config, 0, "", nil); err != nil {
		return err
	}

	cc := &core.TinyChainContext{}
	cc.SetDB(db)
	cc.SetEngine(bc.Engine())

	// Stage 5
	if err := SpawnExecuteBlocksStage(&StageState{
		Stage:       stages.Execution,
		BlockNumber: num - 1,
	}, db, config, cc, bc.GetVMConfig(), nil, ExecuteBlockStageParams{
		WriteReceipts: true,
		CacheSize:     16 * 1024,
		BatchSize:     8 * 1024,
	}); err != nil {
		return err
	}

	// Stage 6
	if err := SpawnHashStateStage(&StageState{
		Stage:       stages.HashState,
		BlockNumber: num - 1,
	}, db, "", nil); err != nil {
		return err
	}

	// Stage 7
	if err := SpawnIntermediateHashesStage(&StageState{
		Stage:       stages.IntermediateHashes,
		BlockNumber: num - 1,
	}, db, "", nil); err != nil {
		return err
	}

	// Stage 8
	if err := SpawnAccountHistoryIndex(&StageState{
		Stage:       stages.AccountHistoryIndex,
		BlockNumber: num - 1,
	}, db, "", nil); err != nil {
		return err
	}

	// Stage 9
	if err := SpawnStorageHistoryIndex(&StageState{
		Stage:       stages.StorageHistoryIndex,
		BlockNumber: num - 1,
	}, db, "", nil); err != nil {
		return err
	}

	// Stage 10
	if err := SpawnTxLookup(&StageState{
		Stage: stages.TxLookup,
	}, db, "", nil); err != nil {
		return err
	}

	return nil
}
