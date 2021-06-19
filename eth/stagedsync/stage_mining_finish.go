package stagedsync

import (
	"fmt"

	"github.com/ledgerwatch/erigon/consensus"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/ethdb"
	"github.com/ledgerwatch/erigon/ethdb/kv"
	"github.com/ledgerwatch/erigon/log"
	"github.com/ledgerwatch/erigon/params"
)

type MiningFinishCfg struct {
	db              ethdb.RwKV
	chainConfig     params.ChainConfig
	engine          consensus.Engine
	pendingBlocksCh chan<- *types.Block
	minedBlocksCh   chan<- *types.Block
	sealCancel      <-chan struct{}
}

func StageMiningFinishCfg(
	db ethdb.RwKV,
	chainConfig params.ChainConfig,
	engine consensus.Engine,
	pendingBlocksCh chan<- *types.Block,
	minedBlocksCh chan<- *types.Block,
	sealCancel <-chan struct{},
) MiningFinishCfg {
	return MiningFinishCfg{
		db:              db,
		chainConfig:     chainConfig,
		engine:          engine,
		pendingBlocksCh: pendingBlocksCh,
		minedBlocksCh:   minedBlocksCh,
		sealCancel:      sealCancel,
	}
}

func SpawnMiningFinishStage(s *StageState, tx ethdb.RwTx, current *miningBlock, cfg MiningFinishCfg, quit <-chan struct{}) error {
	logPrefix := s.state.LogPrefix()

	// Short circuit when receiving duplicate result caused by resubmitting.
	//if w.chain.HasBlock(block.Hash(), block.NumberU64()) {
	//	continue
	//}

	block := types.NewBlock(current.Header, current.Txs, current.Uncles, current.Receipts)
	*current = miningBlock{} // hack to clean global data

	//sealHash := engine.SealHash(block.Header())
	// Reject duplicate sealing work due to resubmitting.
	//if sealHash == prev {
	//	s.Done()
	//	return nil
	//}
	//prev = sealHash

	// Tests may set pre-calculated nonce
	if block.Header().Nonce.Uint64() != 0 {
		cfg.minedBlocksCh <- block
		s.Done()
		return nil
	}

	cfg.pendingBlocksCh <- block

	log.Info(fmt.Sprintf("[%s] block ready for seal", logPrefix),
		"number", block.NumberU64(),
		"transactions", block.Transactions().Len(),
		"gas_used", block.GasUsed(),
		"gas_limit", block.GasLimit(),
		"difficulty", block.Difficulty(),
	)

	chain := ChainReader{Cfg: cfg.chainConfig, Db: kv.WrapIntoTxDB(tx)}
	if err := cfg.engine.Seal(chain, block, cfg.minedBlocksCh, cfg.sealCancel); err != nil {
		log.Warn("Block sealing failed", "err", err)
	}

	s.Done()
	return nil
}
