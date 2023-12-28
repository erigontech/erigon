package span

import "github.com/ledgerwatch/erigon-lib/chain"

const (
	spanLength    = 6400 // Number of blocks in a span
	zerothSpanEnd = 255  // End block of 0th span
)

// NumAt returns the corresponding span number for the given block number.
func NumAt(blockNum uint64) uint64 {
	if blockNum > zerothSpanEnd {
		return 1 + (blockNum-zerothSpanEnd-1)/spanLength
	}
	return 0
}

// EndBlockNum returns the number of the last block in the given span number.
func EndBlockNum(spanNum uint64) uint64 {
	if spanNum > 0 {
		return spanNum*spanLength + zerothSpanEnd
	}
	return zerothSpanEnd
}

func BlockInLastSprintOfSpan(blockNum uint64, config *chain.BorConfig) bool {
	spanNum := NumAt(blockNum)
	endBlockNum := EndBlockNum(spanNum)
	sprintLen := config.CalculateSprint(blockNum)
	startBlockNum := endBlockNum - sprintLen + 1
	return startBlockNum <= blockNum && blockNum <= endBlockNum
}
