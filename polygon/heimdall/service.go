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
	checkpointScraper *Scraper[*Checkpoint]
	milestoneScraper  *Scraper[*Milestone]
	spanScraper       *Scraper[*Span]

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
	dataDir string,
	tmpDir string,
	logger log.Logger,
) Service {
	db := NewDatabase(dataDir, logger)

	blockNumToIdIndexFactory := func(ctx context.Context) (*RangeIndex, error) {
		return NewRangeIndex(ctx, tmpDir, logger)
	}

	checkpointStore := newEntityStore(db, kv.BorCheckpoints, makeType[Checkpoint], blockNumToIdIndexFactory)
	milestoneStore := newEntityStore(db, kv.BorMilestones, makeType[Milestone], blockNumToIdIndexFactory)
	spanStore := newEntityStore(db, kv.BorSpans, makeType[Span], blockNumToIdIndexFactory)

	client := NewHeimdallClient(heimdallUrl, logger)
	checkpointFetcher := newCheckpointFetcher(client, logger)
	milestoneFetcher := newMilestoneFetcher(client, logger)
	spanFetcher := newSpanFetcher(client, logger)

	checkpointScraper := NewScraper(
		checkpointStore,
		checkpointFetcher,
		1*time.Second,
		logger,
	)

	milestoneScraper := NewScraper(
		milestoneStore,
		milestoneFetcher,
		1*time.Second,
		logger,
	)

	spanScraper := NewScraper(
		spanStore,
		spanFetcher,
		1*time.Second,
		logger,
	)

	return &service{
		checkpointScraper: checkpointScraper,
		milestoneScraper:  milestoneScraper,
		spanScraper:       spanScraper,

		db:              db,
		checkpointStore: checkpointStore,
		milestoneStore:  milestoneStore,
		spanStore:       spanStore,
	}
}

func newCheckpointFetcher(client HeimdallClient, logger log.Logger) entityFetcher[*Checkpoint] {
	return newEntityFetcher(
		"CheckpointFetcher",
		nil,
		client.FetchCheckpointCount,
		client.FetchCheckpoint,
		client.FetchCheckpoints,
		10_000, // fetchEntitiesPageLimit
		logger,
	)
}

func newMilestoneFetcher(client HeimdallClient, logger log.Logger) entityFetcher[*Milestone] {
	return newEntityFetcher(
		"MilestoneFetcher",
		client.FetchFirstMilestoneNum,
		client.FetchMilestoneCount,
		client.FetchMilestone,
		nil,
		0,
		logger,
	)
}

func newSpanFetcher(client HeimdallClient, logger log.Logger) entityFetcher[*Span] {
	fetchLastEntityId := func(ctx context.Context) (int64, error) {
		span, err := client.FetchLatestSpan(ctx)
		if err != nil {
			return 0, err
		}
		return int64(span.Id), nil
	}

	fetchEntity := func(ctx context.Context, id int64) (*Span, error) {
		return client.FetchSpan(ctx, uint64(id))
	}

	return newEntityFetcher(
		"SpanFetcher",
		nil,
		fetchLastEntityId,
		fetchEntity,
		nil,
		0,
		logger,
	)
}

func (s *service) FetchLatestSpan(ctx context.Context) (*Span, error) {
	s.checkpointScraper.Synchronize(ctx)
	return s.spanStore.GetLastEntity(ctx)
}

func castEntityToWaypoint[TEntity Waypoint](entity TEntity) Waypoint {
	return entity
}

func (s *service) synchronizeScrapers(ctx context.Context) {
	s.checkpointScraper.Synchronize(ctx)
	s.milestoneScraper.Synchronize(ctx)
	s.spanScraper.Synchronize(ctx)
}

func (s *service) FetchCheckpointsFromBlock(ctx context.Context, startBlock uint64) (Waypoints, error) {
	s.synchronizeScrapers(ctx)
	entities, err := s.checkpointStore.RangeFromBlockNum(ctx, startBlock)
	return libcommon.SliceMap(entities, castEntityToWaypoint[*Checkpoint]), err
}

func (s *service) FetchMilestonesFromBlock(ctx context.Context, startBlock uint64) (Waypoints, error) {
	s.synchronizeScrapers(ctx)
	entities, err := s.milestoneStore.RangeFromBlockNum(ctx, startBlock)
	return libcommon.SliceMap(entities, castEntityToWaypoint[*Milestone]), err
}

// TODO: this limit is a temporary solution to avoid piping thousands of events
// during the first sync. Let's discuss alternatives. Hopefully we can remove this limit.
const maxEntityEvents = 5

func (s *service) RegisterMilestoneObserver(callback func(*Milestone)) polygoncommon.UnregisterFunc {
	return s.milestoneScraper.RegisterObserver(func(entities []*Milestone) {
		for _, entity := range libcommon.SliceTakeLast(entities, maxEntityEvents) {
			callback(entity)
		}
	})
}

func (s *service) RegisterSpanObserver(callback func(*Span)) polygoncommon.UnregisterFunc {
	return s.spanScraper.RegisterObserver(func(entities []*Span) {
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

	scrapersGroup, scrapersGroupCtx := errgroup.WithContext(ctx)
	scrapersGroup.Go(func() error { return s.checkpointScraper.Run(scrapersGroupCtx) })
	scrapersGroup.Go(func() error { return s.milestoneScraper.Run(scrapersGroupCtx) })
	scrapersGroup.Go(func() error { return s.spanScraper.Run(scrapersGroupCtx) })
	return scrapersGroup.Wait()
}
