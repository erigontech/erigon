package bor

import (
	"github.com/ledgerwatch/erigon/polygon/bor/borcfg"
)

const (
	spanLength    = 6400 // Number of blocks in a span
	zerothSpanEnd = 255  // End block of 0th span
)

// SpanIDAt returns the corresponding span id for the given block number.
func SpanIDAt(blockNum uint64) uint64 {
	if blockNum > zerothSpanEnd {
		return 1 + (blockNum-zerothSpanEnd-1)/spanLength
	}
	return 0
}

// SpanEndBlockNum returns the number of the last block in the given span.
func SpanEndBlockNum(spanID uint64) uint64 {
	if spanID > 0 {
		return spanID*spanLength + zerothSpanEnd
	}
	return zerothSpanEnd
}

// IsBlockInLastSprintOfSpan returns true if a block num is within the last sprint of a span and false otherwise.
func IsBlockInLastSprintOfSpan(blockNum uint64, config *borcfg.BorConfig) bool {
	spanNum := SpanIDAt(blockNum)
	endBlockNum := SpanEndBlockNum(spanNum)
	sprintLen := config.CalculateSprintLength(blockNum)
	startBlockNum := endBlockNum - sprintLen + 1
	return startBlockNum <= blockNum && blockNum <= endBlockNum
}
