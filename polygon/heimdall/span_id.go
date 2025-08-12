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
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon/polygon/bor/borcfg"
)

type SpanId uint64

const (
	spanLength    = 6400 // Number of blocks in a span
	zerothSpanEnd = 255  // End block of 0th span
)

var (
	ErrSpanNotFound = errors.New("span not found")
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

// Update span index table: span.StartBlock=> span.Id
// This is needed for SprintIdAt(blockNum) lookups
func UpdateSpansIndex(tx kv.RwTx, span Span) error {
	kByte := make([]byte, 8)
	vByte := make([]byte, 8)
	binary.BigEndian.PutUint64(kByte, span.StartBlock)
	binary.BigEndian.PutUint64(vByte, (uint64)(span.Id))
	return tx.Put(kv.BorSpansIndex, kByte, vByte)
}

// Read Span Id of the span where block given by blockNum belongs to
func SpanIdAtNew(tx kv.Tx, blockNum uint64) (SpanId, error) {
	cursor, err := tx.Cursor(kv.BorSpansIndex)
	if err != nil {
		return 0, err
	}

	key := rangeIndexKey(blockNum)
	_, value, err := cursor.Seek(key[:])
	if err != nil {
		return 0, err
	}
	// not found
	if value == nil {
		return 0, fmt.Errorf("SpanIdAt(%d) error %w", blockNum, ErrSpanNotFound)
	}

	spanId := uint64(binary.BigEndian.Uint64(value))

	return SpanId(spanId), nil

}
