package core

func calculateNumOfPrunedBlocks(currentBlock, lastPrunedBlock uint64, blocksBeforePruning uint64, blocksBatch uint64) (uint64, uint64, bool) {
	//underflow see https://github.com/ledgerwatch/turbo-geth/issues/115
	if currentBlock <= lastPrunedBlock {
		return lastPrunedBlock, lastPrunedBlock, false
	}

	diff := currentBlock - lastPrunedBlock
	if diff <= blocksBeforePruning {
		return lastPrunedBlock, lastPrunedBlock, false
	}
	diff = diff - blocksBeforePruning
	switch {
	case diff >= blocksBatch:
		return lastPrunedBlock, lastPrunedBlock + blocksBatch, true
	case diff < blocksBatch:
		return lastPrunedBlock, lastPrunedBlock + diff, true
	default:
		return lastPrunedBlock, lastPrunedBlock, false
	}
}

type Keys [][]byte
type Batch struct {
	bucket string
	keys   Keys
}
