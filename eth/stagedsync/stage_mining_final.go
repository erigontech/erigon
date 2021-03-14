package stagedsync

import (
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/ethdb"
)

func SpawnMiningFinalStage(s *StageState, tx ethdb.Database, block *types.Block, receipts types.Receipts, stateRoot common.Hash, quit <-chan struct{}) (*types.Block, error) {
	header := block.Header()
	header.Root = stateRoot
	s.Done()
	return types.NewBlock(header, block.Transactions(), block.Uncles(), receipts), nil
}

// copyReceipts makes a deep copy of the given receipts.
func copyReceipts(receipts []*types.Receipt) []*types.Receipt {
	result := make([]*types.Receipt, len(receipts))
	for i, l := range receipts {
		cpy := *l
		result[i] = &cpy
	}
	return result
}
