package stagedsync

import (
	"fmt"

	"github.com/ledgerwatch/erigon-lib/chain"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/log/v3"

	"github.com/ledgerwatch/erigon/consensus"
	"github.com/ledgerwatch/erigon/core/types"
)

type MiningFinishCfg struct {
	db          kv.RwDB
	chainConfig chain.Config
	engine      consensus.Engine
	sealCancel  chan struct{}
	miningState MiningState
}

func StageMiningFinishCfg(
	db kv.RwDB,
	chainConfig chain.Config,
	engine consensus.Engine,
	miningState MiningState,
	sealCancel chan struct{},
) MiningFinishCfg {
	return MiningFinishCfg{
		db:          db,
		chainConfig: chainConfig,
		engine:      engine,
		miningState: miningState,
		sealCancel:  sealCancel,
	}
}

func SpawnMiningFinishStage(s *StageState, tx kv.RwTx, cfg MiningFinishCfg, quit <-chan struct{}, logger log.Logger) error {
	logPrefix := s.LogPrefix()
	current := cfg.miningState.MiningBlock

	// Short circuit when receiving duplicate result caused by resubmitting.
	//if w.chain.HasBlock(block.Hash(), block.NumberU64()) {
	//	continue
	//}

	block := types.NewBlock(current.Header, current.Txs, current.Uncles, current.Receipts, current.Withdrawals)
	blockWithReceipts := &types.BlockWithReceipts{Block: block, Receipts: current.Receipts}
	*current = MiningBlock{} // hack to clean global data

	//sealHash := engine.SealHash(block.Header())
	// Reject duplicate sealing work due to resubmitting.
	//if sealHash == prev {
	//	s.Done()
	//	return nil
	//}
	//prev = sealHash

	if cfg.miningState.MiningResultPOSCh != nil {
		cfg.miningState.MiningResultPOSCh <- blockWithReceipts
		return nil
	}

	// Tests may set pre-calculated nonce
	if block.NonceU64() != 0 {
		// Note: To propose a new signer for Clique consensus, the block nonce should be set to 0xFFFFFFFFFFFFFFFF.
		if cfg.engine.Type() != chain.CliqueConsensus {
			cfg.miningState.MiningResultCh <- block
			return nil
		}
	}

	cfg.miningState.PendingResultCh <- block

	if block.Transactions().Len() > 0 {
		logger.Info(fmt.Sprintf("[%s] block ready for seal", logPrefix),
			"block_num", block.NumberU64(),
			"transactions", block.Transactions().Len(),
			"gas_used", block.GasUsed(),
			"gas_limit", block.GasLimit(),
			"difficulty", block.Difficulty(),
		)
	}
	// interrupt aborts the in-flight sealing task.
	select {
	case cfg.sealCancel <- struct{}{}:
	default:
		logger.Trace("None in-flight sealing task.")
	}
	chain := ChainReader{Cfg: cfg.chainConfig, Db: tx}
	if err := cfg.engine.Seal(chain, block, cfg.miningState.MiningResultCh, cfg.sealCancel); err != nil {
		logger.Warn("Block sealing failed", "err", err)
	}

	return nil
}
