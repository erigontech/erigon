package stagedsync

import (
	"github.com/ledgerwatch/turbo-geth/consensus"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/ledgerwatch/turbo-geth/params"
)

//var prev common.Hash

func SpawnMiningFinishStage(s *StageState, tx ethdb.Database, current *miningBlock, engine consensus.Engine, chainConfig *params.ChainConfig, resultCh chan consensus.ResultWithContext, consensusCtx consensus.Cancel, quit <-chan struct{}) error {
	//logPrefix := s.state.LogPrefix()

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
		resultCh <- consensus.ResultWithContext{Cancel: consensusCtx, Block: block}
		s.Done()
		return nil
	}

	chain := ChainReader{chainConfig, tx}
	if err := engine.Seal(consensusCtx, chain, block, resultCh, consensusCtx.Done()); err != nil {
		log.Warn("Block sealing failed", "err", err)
	}

	//select {
	//case <-ctx.Done():
	//	return ctx.Err()
	//case <-quit:
	//	ctx.CancelFunc()
	//	return common.ErrStopped
	//case result := <-resultCh:
	//	block = result.Block
	//}
	//
	//log.Info(fmt.Sprintf("[%s] mined block", logPrefix), "txs", block.Transactions().Len())
	//result <- *block

	s.Done()
	return nil
}
