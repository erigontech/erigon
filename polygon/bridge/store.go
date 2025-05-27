package bridge

import (
	"context"
	"time"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/rlp"
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
	Events(ctx context.Context, start, end uint64) ([][]byte, error)
	BlockEventIdsRange(ctx context.Context, blockNum uint64) (start uint64, end uint64, ok bool, err error)         // [start,end]
	EventsByTimeframe(ctx context.Context, timeFrom, timeTo uint64) (events [][]byte, eventIds []uint64, err error) // [timeFrom, timeTo)

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
