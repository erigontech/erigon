package stagedsync

import (
	"fmt"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/consensus"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/ledgerwatch/turbo-geth/params"
)

//var prev common.Hash

func SpawnMiningFinishStage(s *StageState, tx ethdb.Database, current *miningBlock, engine consensus.Engine, chainConfig *params.ChainConfig, quit <-chan struct{}) (*types.Block, error) {
	logPrefix := s.state.LogPrefix()

	// Short circuit when receiving duplicate result caused by resubmitting.
	//if w.chain.HasBlock(block.Hash(), block.NumberU64()) {
	//	continue
	//}

	block := types.NewBlock(current.Header, current.Txs, current.Uncles, current.Receipts)

	//sealHash := engine.SealHash(block.Header())
	// Reject duplicate sealing work due to resubmitting.
	//if sealHash == prev {
	//	s.Done()
	//	return nil
	//}
	//prev = sealHash

	// Tests may set pre-calculated nonce
	if block.Header().Nonce.Uint64() != 0 {
		s.Done()
		*current = miningBlock{} // hack to clean global data
		return block, nil
	}

	chain := ChainReader{chainConfig, tx}
	ctx := consensus.NewCancel()
	resultCh := make(chan consensus.ResultWithContext, 1)
	if err := engine.Seal(ctx, chain, block, resultCh, ctx.Done()); err != nil {
		log.Warn("Block sealing failed", "err", err)
	}

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-quit:
		ctx.CancelFunc()
		return nil, common.ErrStopped
	case result := <-resultCh:
		block = result.Block
	}

	log.Info(fmt.Sprintf("[%s] mined block", logPrefix), "txs", block.Transactions().Len())
	// Broadcast the block and announce chain insertion event
	//if err := mux.Post(core.NewMinedBlockEvent{Block: block}); err != nil {
	//	return err
	//}

	s.Done()
	*current = miningBlock{} // hack to clean global data
	return block, nil
}
