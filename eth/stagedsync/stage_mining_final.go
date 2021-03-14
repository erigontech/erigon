package stagedsync

import (
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/ethdb"
)

func SpawnMiningFinalStage(s *StageState, tx ethdb.Database, current *miningBlock, quit <-chan struct{}) (*types.Block, error) {
	block := types.NewBlock(current.header, current.txs, current.uncles, current.receipts)
	s.Done()
	return block, nil
}
