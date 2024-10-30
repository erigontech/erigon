// Copyright 2024 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package heimdall

import (
	"github.com/erigontech/erigon/v3/polygon/bor/borcfg"
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
