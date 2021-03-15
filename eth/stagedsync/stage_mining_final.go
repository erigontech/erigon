package stagedsync

import (
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/consensus"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/ledgerwatch/turbo-geth/params"
)

var prev common.Hash

func SpawnMiningFinishStage(s *StageState, tx ethdb.Database, current *miningBlock, engine consensus.Engine, chainConfig *params.ChainConfig, quit <-chan struct{}) (*types.Block, error) {
	// Short circuit when receiving duplicate result caused by resubmitting.
	//if w.chain.HasBlock(block.Hash(), block.NumberU64()) {
	//	continue
	//}

	block := types.NewBlock(current.header, current.txs, current.uncles, current.receipts)

	sealHash := engine.SealHash(block.Header())
	// Reject duplicate sealing work due to resubmitting.
	if sealHash == prev {
		return nil, nil
	}
	prev = sealHash

	chain := ChainReader{chainConfig, tx}
	ctx := consensus.NewCancel()
	resultCh := make(chan consensus.ResultWithContext, 1)
	//TODO: how abut quit channel?
	if err := engine.Seal(ctx, chain, block, resultCh, ctx.Done()); err != nil {
		log.Warn("Block sealing failed", "err", err)
	}

	// Broadcast the block and announce chain insertion event
	//_ = mux.Post(core.NewMinedBlockEvent{Block: block})

	s.Done()
	return block, nil
}
