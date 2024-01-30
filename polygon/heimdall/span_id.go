package heimdall

import (
	"github.com/ledgerwatch/erigon/polygon/bor/borcfg"
)

type SpanId uint64

const (
	spanLength    = 6400 // Number of blocks in a span
	zerothSpanEnd = 255  // End block of 0th span
)

// SpanIdAt returns the corresponding span id for the given block number.
func SpanIdAt(blockNum uint64) SpanId {
	if blockNum > zerothSpanEnd {
		return SpanId(1 + (blockNum-zerothSpanEnd-1)/spanLength)
	}
	return 0
}

// SpanEndBlockNum returns the number of the last block in the given span.
func SpanEndBlockNum(spanId SpanId) uint64 {
	if spanId > 0 {
		return uint64(spanId)*spanLength + zerothSpanEnd
	}
	return zerothSpanEnd
}

// IsBlockInLastSprintOfSpan returns true if a block num is within the last sprint of a span and false otherwise.
func IsBlockInLastSprintOfSpan(blockNum uint64, config *borcfg.BorConfig) bool {
	spanNum := SpanIdAt(blockNum)
	endBlockNum := SpanEndBlockNum(spanNum)
	sprintLen := config.CalculateSprintLength(blockNum)
	startBlockNum := endBlockNum - sprintLen + 1
	return startBlockNum <= blockNum && blockNum <= endBlockNum
}

// IsSecondSprintStart returns true if the block num is first block of second sprint
// E.g. for sprint length 16, it will return true only for block 16. This is
// to handle a special case for fetching 1st span.
func IsSecondSprintStart(blockNum uint64, config *borcfg.BorConfig) bool {
	return blockNum == config.CalculateSprintLength(blockNum)
}
