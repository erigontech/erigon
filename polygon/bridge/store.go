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

package bridge

import (
	"context"
	"time"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon/execution/rlp"
)

type Store interface {
	Prepare(ctx context.Context) error
	Close()

	LastEventId(ctx context.Context) (uint64, error)
	LastEventIdWithinWindow(ctx context.Context, fromID uint64, toTime time.Time) (uint64, error)
	LastProcessedEventId(ctx context.Context) (uint64, error)
	LastProcessedBlockInfo(ctx context.Context) (ProcessedBlockInfo, bool, error)
	LastFrozenEventId() uint64
	LastFrozenEventBlockNum() uint64

	EventTxnToBlockNum(ctx context.Context, borTxHash common.Hash) (uint64, bool, error)
	BlockEventIdsRange(ctx context.Context, blockHash common.Hash, blockNum uint64) (start uint64, end uint64, ok bool, err error) // [start,end)
	EventsByTimeframe(ctx context.Context, timeFrom, timeTo uint64) ([][]byte, []uint64, error)                                    // [timeFrom, timeTo)

	PutEventTxnToBlockNum(ctx context.Context, eventTxnToBlockNum map[common.Hash]uint64) error
	PutEvents(ctx context.Context, events []*EventRecordWithTime) error
	PutBlockNumToEventId(ctx context.Context, blockNumToEventId map[uint64]uint64) error
	PutProcessedBlockInfo(ctx context.Context, info []ProcessedBlockInfo) error

	Unwind(ctx context.Context, blockNum uint64) error

	// block reader compatibility
	BorStartEventId(ctx context.Context, hash common.Hash, blockHeight uint64) (uint64, error)
	EventsByBlock(ctx context.Context, hash common.Hash, blockNum uint64) ([]rlp.RawValue, error)
	EventsByIdFromSnapshot(from uint64, to time.Time, limit int) ([]*EventRecordWithTime, bool, error)
	PruneEvents(ctx context.Context, blocksTo uint64, blocksDeleteLimit int) (deleted int, err error)
}
