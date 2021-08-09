package stagedsync

import (
	"fmt"

	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/consensus"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/params"
	"github.com/ledgerwatch/log/v3"
)

type MiningFinishCfg struct {
	db          kv.RwDB
	chainConfig params.ChainConfig
	engine      consensus.Engine
	sealCancel  <-chan struct{}
	miningState MiningState
}

func StageMiningFinishCfg(
	db kv.RwDB,
	chainConfig params.ChainConfig,
	engine consensus.Engine,
	miningState MiningState,
	sealCancel <-chan struct{},
) MiningFinishCfg {
	return MiningFinishCfg{
		db:          db,
		chainConfig: chainConfig,
		engine:      engine,
		miningState: miningState,
		sealCancel:  sealCancel,
	}
}

func SpawnMiningFinishStage(s *StageState, tx kv.RwTx, cfg MiningFinishCfg, quit <-chan struct{}) error {
	logPrefix := s.LogPrefix()
	current := cfg.miningState.MiningBlock

	// Short circuit when receiving duplicate result caused by resubmitting.
	//if w.chain.HasBlock(block.Hash(), block.NumberU64()) {
	//	continue
	//}

	block := types.NewBlock(current.Header, current.Txs, current.Uncles, current.Receipts)
	*current = MiningBlock{} // hack to clean global data

	//sealHash := engine.SealHash(block.Header())
	// Reject duplicate sealing work due to resubmitting.
	//if sealHash == prev {
	//	s.Done()
	//	return nil
	//}
	//prev = sealHash

	// Tests may set pre-calculated nonce
	if block.Header().Nonce.Uint64() != 0 {
		cfg.miningState.MiningResultCh <- block
		return nil
	}

	cfg.miningState.PendingResultCh <- block

	log.Info(fmt.Sprintf("[%s] block ready for seal", logPrefix),
		"number", block.NumberU64(),
		"transactions", block.Transactions().Len(),
		"gas_used", block.GasUsed(),
		"gas_limit", block.GasLimit(),
		"difficulty", block.Difficulty(),
	)

	chain := ChainReader{Cfg: cfg.chainConfig, Db: tx}
	if err := cfg.engine.Seal(chain, block, cfg.miningState.MiningResultCh, cfg.sealCancel); err != nil {
		log.Warn("Block sealing failed", "err", err)
	}

	return nil
}
