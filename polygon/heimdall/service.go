package heimdall

import (
	"context"
	"time"

	"github.com/ledgerwatch/log/v3"
	"golang.org/x/sync/errgroup"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/polygon/polygoncommon"
)

type Service interface {
	Heimdall
	Run(ctx context.Context) error
}

type service struct {
	scraper *Scraper

	db              *Database
	checkpointStore entityStore[*Checkpoint]
	milestoneStore  entityStore[*Milestone]
	spanStore       entityStore[*Span]
}

func makeType[T any]() *T {
	return new(T)
}

func NewService(
	heimdallUrl string,
	openDatabase OpenDatabaseFunc,
	tmpDir string,
	logger log.Logger,
) Service {
	db := NewDatabase(openDatabase, logger)

	blockNumToIdIndexFactory := func(ctx context.Context) (*RangeIndex, error) {
		return NewRangeIndex(ctx, tmpDir, logger)
	}

	checkpointStore := newEntityStore(db, kv.BorCheckpoints, makeType[Checkpoint], blockNumToIdIndexFactory)
	milestoneStore := newEntityStore(db, kv.BorMilestones, makeType[Milestone], blockNumToIdIndexFactory)
	spanStore := newEntityStore(db, kv.BorSpans, makeType[Span], blockNumToIdIndexFactory)

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

		db:              db,
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
	defer s.db.Close()
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
