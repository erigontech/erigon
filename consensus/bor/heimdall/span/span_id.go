package span

import "github.com/ledgerwatch/erigon-lib/chain"

const (
	spanLength    = 6400 // Number of blocks in a span
	zerothSpanEnd = 255  // End block of 0th span
)

// IDAt returns the corresponding span id for the given block number.
func IDAt(blockNum uint64) uint64 {
	if blockNum > zerothSpanEnd {
		return 1 + (blockNum-zerothSpanEnd-1)/spanLength
	}
	return 0
}

// EndBlockNum returns the number of the last block in the given span.
func EndBlockNum(spanID uint64) uint64 {
	if spanID > 0 {
		return spanID*spanLength + zerothSpanEnd
	}
	return zerothSpanEnd
}

// BlockInLastSprintOfSpan returns true if a block num is within the last sprint of a span and false otherwise.
func BlockInLastSprintOfSpan(blockNum uint64, config *chain.BorConfig) bool {
	spanNum := IDAt(blockNum)
	endBlockNum := EndBlockNum(spanNum)
	sprintLen := config.CalculateSprint(blockNum)
	startBlockNum := endBlockNum - sprintLen + 1
	return startBlockNum <= blockNum && blockNum <= endBlockNum
}
