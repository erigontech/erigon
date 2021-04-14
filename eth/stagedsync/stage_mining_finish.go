package stagedsync

import (
	"fmt"

	"github.com/ledgerwatch/turbo-geth/consensus"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/ledgerwatch/turbo-geth/params"
)

//var prev common.Hash

func SpawnMiningFinishStage(s *StageState, tx ethdb.Database, current *miningBlock, engine consensus.Engine, chainConfig *params.ChainConfig, results chan<- *types.Block, sealCancel <-chan struct{}, quit <-chan struct{}) error {
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
		results <- block
		s.Done()
		return nil
	}

	log.Info(fmt.Sprintf("[%s] block ready for seal", logPrefix),
		"number", block.NumberU64(),
		"transactions", block.Transactions().Len(),
		"gas_used", block.GasUsed(),
		"gas_limit", block.GasLimit(),
		"difficulty", block.Difficulty(),
	)

	chain := ChainReader{Cfg: chainConfig, Db: tx}
	if err := engine.Seal(chain, block, results, sealCancel); err != nil {
		log.Warn("Block sealing failed", "err", err)
	}

	s.Done()
	return nil
}
