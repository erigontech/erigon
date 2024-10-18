package bridge

import (
	"context"
	"time"

	libcommon "github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon/polygon/heimdall"
	"github.com/erigontech/erigon/rlp"
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

	EventLookup(ctx context.Context, borTxHash libcommon.Hash) (uint64, bool, error)
	Events(ctx context.Context, start, end uint64) ([][]byte, error)
	BlockEventIdsRange(ctx context.Context, blockNum uint64) (start uint64, end uint64, err error) // [start,end)

	PutEventTxnToBlockNum(ctx context.Context, eventTxnToBlockNum map[libcommon.Hash]uint64) error
	PutEvents(ctx context.Context, events []*heimdall.EventRecordWithTime) error
	PutBlockNumToEventId(ctx context.Context, blockNumToEventId map[uint64]uint64) error
	PutProcessedBlockInfo(ctx context.Context, info ProcessedBlockInfo) error

	Unwind(ctx context.Context, blockNum uint64) error

	// block reader compatibility
	BorStartEventId(ctx context.Context, hash libcommon.Hash, blockHeight uint64) (uint64, error)
	EventsByBlock(ctx context.Context, hash libcommon.Hash, blockNum uint64) ([]rlp.RawValue, error)
	EventsByIdFromSnapshot(from uint64, to time.Time, limit int) ([]*heimdall.EventRecordWithTime, bool, error)
}
