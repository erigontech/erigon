package heimdall

import (
	"context"

	libcommon "github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/polygon/bor/valset"
)

type Reader struct {
	logger                    log.Logger
	store                     Store
	spanBlockProducersTracker *spanBlockProducersTracker
}

// AssembleReader creates and opens the MDBX store. For use cases where the store is only being read from. Must call Close.
func AssembleReader(ctx context.Context, calculateSprintNumber CalculateSprintNumberFunc, dataDir string, tmpDir string, logger log.Logger) (*Reader, error) {
	store := NewMdbxStore(logger, dataDir)

	err := store.Prepare(ctx)
	if err != nil {
		return nil, err
	}

	return NewReader(calculateSprintNumber, store, logger), nil
}

func NewReader(calculateSprintNumber CalculateSprintNumberFunc, store Store, logger log.Logger) *Reader {
	return &Reader{
		logger:                    logger,
		store:                     store,
		spanBlockProducersTracker: newSpanBlockProducersTracker(logger, calculateSprintNumber, store.SpanBlockProducerSelections()),
	}
}

func (r *Reader) Span(ctx context.Context, id uint64) (*Span, bool, error) {
	return r.store.Spans().Entity(ctx, id)
}

func (r *Reader) CheckpointsFromBlock(ctx context.Context, startBlock uint64) (Waypoints, error) {
	entities, err := r.store.Checkpoints().RangeFromBlockNum(ctx, startBlock)
	return libcommon.SliceMap(entities, castEntityToWaypoint[*Checkpoint]), err
}

func (r *Reader) MilestonesFromBlock(ctx context.Context, startBlock uint64) (Waypoints, error) {
	entities, err := r.store.Milestones().RangeFromBlockNum(ctx, startBlock)
	return libcommon.SliceMap(entities, castEntityToWaypoint[*Milestone]), err
}

func (r *Reader) Producers(ctx context.Context, blockNum uint64) (*valset.ValidatorSet, error) {
	return r.spanBlockProducersTracker.Producers(ctx, blockNum)
}

func (r *Reader) Close() {
	r.store.Close()
}
