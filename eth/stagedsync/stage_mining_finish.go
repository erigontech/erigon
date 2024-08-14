package stagedsync

import (
	"fmt"

	"github.com/ledgerwatch/log/v3"

	"github.com/ledgerwatch/erigon-lib/chain"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/consensus"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/turbo/builder"
	"github.com/ledgerwatch/erigon/turbo/services"
)

type MiningFinishCfg struct {
	db                    kv.RwDB
	chainConfig           chain.Config
	engine                consensus.Engine
	sealCancel            chan struct{}
	miningState           MiningState
	blockReader           services.FullBlockReader
	latestBlockBuiltStore *builder.LatestBlockBuiltStore
}

func StageMiningFinishCfg(
	db kv.RwDB,
	chainConfig chain.Config,
	engine consensus.Engine,
	miningState MiningState,
	sealCancel chan struct{},
	blockReader services.FullBlockReader,
	latestBlockBuiltStore *builder.LatestBlockBuiltStore,
) MiningFinishCfg {
	return MiningFinishCfg{
		db:                    db,
		chainConfig:           chainConfig,
		engine:                engine,
		miningState:           miningState,
		sealCancel:            sealCancel,
		blockReader:           blockReader,
		latestBlockBuiltStore: latestBlockBuiltStore,
	}
}

func SpawnMiningFinishStage(s *StageState, tx kv.RwTx, cfg MiningFinishCfg, quit <-chan struct{}, logger log.Logger) error {
	logPrefix := s.LogPrefix()
	current := cfg.miningState.MiningBlock

	// Short circuit when receiving duplicate result caused by resubmitting.
	//if w.chain.HasBlock(block.Hash(), block.NumberU64()) {
	//	continue
	//}

	block := types.NewBlockForAsembling(current.Header, current.Txs, current.Uncles, current.Receipts, current.Withdrawals, current.Requests)
	blockWithReceipts := &types.BlockWithReceipts{Block: block, Receipts: current.Receipts}
	*current = MiningBlock{} // hack to clean global data

	//sealHash := engine.SealHash(block.Header())
	// Reject duplicate sealing work due to resubmitting.
	//if sealHash == prev {
	//	s.Done()
	//	return nil
	//}
	//prev = sealHash
	cfg.latestBlockBuiltStore.AddBlockBuilt(block)

	// Tests may set pre-calculated nonce
	if block.NonceU64() != 0 {
		// Note: To propose a new signer for Clique consensus, the block nonce should be set to 0xFFFFFFFFFFFFFFFF.
		if cfg.engine.Type() != chain.CliqueConsensus {
			cfg.miningState.MiningResultCh <- blockWithReceipts
			return nil
		}
	}

	cfg.miningState.PendingResultCh <- block

	if block.Transactions().Len() > 0 {
		logger.Info(fmt.Sprintf("[%s] block ready for seal", logPrefix),
			"block", block.NumberU64(),
			"transactions", block.Transactions().Len(),
			"gasUsed", block.GasUsed(),
			"gasLimit", block.GasLimit(),
			"difficulty", block.Difficulty(),
		)
	}
	// interrupt aborts the in-flight sealing task.
	select {
	case cfg.sealCancel <- struct{}{}:
	default:
		logger.Trace("No in-flight sealing task.")
	}
	chain := ChainReader{Cfg: cfg.chainConfig, Db: tx, BlockReader: cfg.blockReader, Logger: logger}
	if err := cfg.engine.Seal(chain, blockWithReceipts, cfg.miningState.MiningResultCh, cfg.sealCancel); err != nil {
		logger.Warn("Block sealing failed", "err", err)
	}

	return nil
}
