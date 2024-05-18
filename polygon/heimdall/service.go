package heimdall

import (
	"context"
	"time"

	"github.com/ledgerwatch/log/v3"
	"golang.org/x/sync/errgroup"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/polygon/polygoncommon"
	"github.com/ledgerwatch/erigon/turbo/services"
)

type Service interface {
	Heimdall
	Run(ctx context.Context) error
}

type service struct {
	scraper *Scraper

	checkpointStore entityStore[*Checkpoint]
	milestoneStore  entityStore[*Milestone]
	spanStore       entityStore[*Span]
}

func newCheckpointStore(tx kv.RwTx, reader services.BorCheckpointReader, blockNumToIdIndexFactory func(context.Context) (*RangeIndex, error)) entityStore[*Checkpoint] {
	makeEntity := func() *Checkpoint { return new(Checkpoint) }
	return newEntityStore(tx, kv.BorCheckpoints, makeEntity, reader.LastCheckpointId, reader.Checkpoint, blockNumToIdIndexFactory)
}

func newMilestoneStore(tx kv.RwTx, reader services.BorMilestoneReader, blockNumToIdIndexFactory func(context.Context) (*RangeIndex, error)) entityStore[*Milestone] {
	makeEntity := func() *Milestone { return new(Milestone) }
	return newEntityStore(tx, kv.BorMilestones, makeEntity, reader.LastMilestoneId, reader.Milestone, blockNumToIdIndexFactory)
}

func newSpanStore(tx kv.RwTx, reader services.BorSpanReader, blockNumToIdIndexFactory func(context.Context) (*RangeIndex, error)) entityStore[*Span] {
	makeEntity := func() *Span { return new(Span) }
	return newEntityStore(tx, kv.BorSpans, makeEntity, reader.LastSpanId, reader.Span, blockNumToIdIndexFactory)
}

func NewService(
	heimdallUrl string,
	tmpDir string,
	logger log.Logger,
) Service {
	// TODO: implementing these is an upcoming task
	txProvider := func() kv.RwTx { /* TODO */ return nil }
	readerProvider := func() reader { /* TODO */ return nil }

	tx := txProvider()
	if tx == nil {
		// TODO: implement and remove
		logger.Warn("heimdall.Service txProvider is not implemented yet")
		return nil
	}
	reader := readerProvider()
	if reader == nil {
		// TODO: implement and remove
		logger.Warn("heimdall.Service readerProvider is not implemented yet")
		return nil
	}

	blockNumToIdIndexFactory := func(ctx context.Context) (*RangeIndex, error) {
		return NewRangeIndex(ctx, tmpDir, logger)
	}

	checkpointStore := newCheckpointStore(tx, reader, blockNumToIdIndexFactory)
	milestoneStore := newMilestoneStore(tx, reader, blockNumToIdIndexFactory)
	spanStore := newSpanStore(tx, reader, blockNumToIdIndexFactory)

	client := NewHeimdallClient(heimdallUrl, logger)
	scraper := NewScraper(
		checkpointStore,
		milestoneStore,
		spanStore,
		client,
		1*time.Second,
		logger,
	)

	return &service{
		scraper: scraper,

		checkpointStore: checkpointStore,
		milestoneStore:  milestoneStore,
		spanStore:       spanStore,
	}
}

func (s *service) FetchLatestSpan(ctx context.Context) (*Span, error) {
	s.scraper.Synchronize(ctx)
	return s.spanStore.GetLastEntity(ctx)
}

func castEntityToWaypoint[TEntity Waypoint](entity TEntity) Waypoint {
	return entity
}

func (s *service) FetchCheckpointsFromBlock(ctx context.Context, startBlock uint64) (Waypoints, error) {
	s.scraper.Synchronize(ctx)
	entities, err := s.checkpointStore.RangeFromBlockNum(ctx, startBlock)
	return libcommon.SliceMap(entities, castEntityToWaypoint[*Checkpoint]), err
}

func (s *service) FetchMilestonesFromBlock(ctx context.Context, startBlock uint64) (Waypoints, error) {
	s.scraper.Synchronize(ctx)
	entities, err := s.milestoneStore.RangeFromBlockNum(ctx, startBlock)
	return libcommon.SliceMap(entities, castEntityToWaypoint[*Milestone]), err
}

// TODO: this limit is a temporary solution to avoid piping thousands of events
// during the first sync. Let's discuss alternatives. Hopefully we can remove this limit.
const maxEntityEvents = 5

func (s *service) RegisterMilestoneObserver(callback func(*Milestone)) polygoncommon.UnregisterFunc {
	return s.scraper.RegisterMilestoneObserver(func(entities []*Milestone) {
		for _, entity := range libcommon.SliceTakeLast(entities, maxEntityEvents) {
			callback(entity)
		}
	})
}

func (s *service) RegisterSpanObserver(callback func(*Span)) polygoncommon.UnregisterFunc {
	return s.scraper.RegisterSpanObserver(func(entities []*Span) {
		for _, entity := range libcommon.SliceTakeLast(entities, maxEntityEvents) {
			callback(entity)
		}
	})
}

func (s *service) Run(ctx context.Context) error {
	defer s.checkpointStore.Close()
	defer s.milestoneStore.Close()
	defer s.spanStore.Close()

	prepareStoresGroup, prepareStoresGroupCtx := errgroup.WithContext(ctx)
	prepareStoresGroup.Go(func() error { return s.checkpointStore.Prepare(prepareStoresGroupCtx) })
	prepareStoresGroup.Go(func() error { return s.milestoneStore.Prepare(prepareStoresGroupCtx) })
	prepareStoresGroup.Go(func() error { return s.spanStore.Prepare(prepareStoresGroupCtx) })
	err := prepareStoresGroup.Wait()
	if err != nil {
		return err
	}

	return s.scraper.Run(ctx)
}
