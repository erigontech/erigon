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

type Scraper struct {
	txProvider     func() kv.RwTx
	readerProvider func() reader

	client    HeimdallClient
	pollDelay time.Duration

	checkpointObservers *polygoncommon.Observers[[]*Checkpoint]
	milestoneObservers  *polygoncommon.Observers[[]*Milestone]
	spanObservers       *polygoncommon.Observers[[]*Span]

	checkpointSyncEvent *polygoncommon.EventNotifier
	milestoneSyncEvent  *polygoncommon.EventNotifier
	spanSyncEvent       *polygoncommon.EventNotifier

	tmpDir string
	logger log.Logger
}

func NewScraperTODO(
	client HeimdallClient,
	pollDelay time.Duration,
	tmpDir string,
	logger log.Logger,
) *Scraper {
	return NewScraper(
		func() kv.RwTx { /* TODO */ return nil },
		func() reader { /* TODO */ return nil },
		client,
		pollDelay,
		tmpDir,
		logger,
	)
}

func NewScraper(
	txProvider func() kv.RwTx,
	readerProvider func() reader,

	client HeimdallClient,
	pollDelay time.Duration,
	tmpDir string,
	logger log.Logger,
) *Scraper {
	return &Scraper{
		txProvider:     txProvider,
		readerProvider: readerProvider,

		client:    client,
		pollDelay: pollDelay,

		checkpointObservers: polygoncommon.NewObservers[[]*Checkpoint](),
		milestoneObservers:  polygoncommon.NewObservers[[]*Milestone](),
		spanObservers:       polygoncommon.NewObservers[[]*Span](),

		checkpointSyncEvent: polygoncommon.NewEventNotifier(),
		milestoneSyncEvent:  polygoncommon.NewEventNotifier(),
		spanSyncEvent:       polygoncommon.NewEventNotifier(),

		tmpDir: tmpDir,
		logger: logger,
	}
}

func (s *Scraper) syncEntity(
	ctx context.Context,
	store entityStore,
	fetcher entityFetcher,
	callback func([]Entity),
	syncEvent *polygoncommon.EventNotifier,
) error {
	defer store.Close()
	if err := store.Prepare(ctx); err != nil {
		return err
	}

	for ctx.Err() == nil {
		lastKnownId, hasLastKnownId, err := store.GetLastEntityId(ctx)
		if err != nil {
			return err
		}

		var idRange ClosedRange
		if hasLastKnownId {
			idRange.Start = lastKnownId + 1
		} else {
			idRange.Start = 1
		}

		idRange.End, err = fetcher.FetchLastEntityId(ctx)
		if err != nil {
			return err
		}

		if idRange.Start > idRange.End {
			syncEvent.SetAndBroadcast()
			libcommon.Sleep(ctx, s.pollDelay)
			if ctx.Err() != nil {
				syncEvent.Reset()
			}
		} else {
			entities, err := fetcher.FetchEntitiesRange(ctx, idRange)
			if err != nil {
				return err
			}

			for i, entity := range entities {
				if err = store.PutEntity(ctx, idRange.Start+uint64(i), entity); err != nil {
					return err
				}
			}

			if callback != nil {
				go callback(entities)
			}
		}
	}
	return ctx.Err()
}

func newCheckpointStore(tx kv.RwTx, reader services.BorCheckpointReader, blockNumToIdIndexFactory func() *RangeIndex) entityStore {
	makeEntity := func() Entity { return new(Checkpoint) }
	return newEntityStore(tx, kv.BorCheckpoints, makeEntity, reader.LastCheckpointId, reader.Checkpoint, blockNumToIdIndexFactory())
}

func newMilestoneStore(tx kv.RwTx, reader services.BorMilestoneReader, blockNumToIdIndexFactory func() *RangeIndex) entityStore {
	makeEntity := func() Entity { return new(Milestone) }
	return newEntityStore(tx, kv.BorMilestones, makeEntity, reader.LastMilestoneId, reader.Milestone, blockNumToIdIndexFactory())
}

func newSpanStore(tx kv.RwTx, reader services.BorSpanReader, blockNumToIdIndexFactory func() *RangeIndex) entityStore {
	makeEntity := func() Entity { return new(Span) }
	return newEntityStore(tx, kv.BorSpans, makeEntity, reader.LastSpanId, reader.Span, blockNumToIdIndexFactory())
}

func newCheckpointFetcher(client HeimdallClient, logger log.Logger) entityFetcher {
	fetchEntity := func(ctx context.Context, id int64) (Entity, error) { return client.FetchCheckpoint(ctx, id) }

	fetchEntitiesPage := func(ctx context.Context, page uint64, limit uint64) ([]Entity, error) {
		entities, err := client.FetchCheckpoints(ctx, page, limit)
		return libcommon.SliceMap(entities, func(c *Checkpoint) Entity { return c }), err
	}

	return newEntityFetcher(
		"CheckpointFetcher",
		client.FetchCheckpointCount,
		fetchEntity,
		fetchEntitiesPage,
		logger,
	)
}

func newMilestoneFetcher(client HeimdallClient, logger log.Logger) entityFetcher {
	fetchEntity := func(ctx context.Context, id int64) (Entity, error) { return client.FetchMilestone(ctx, id) }

	return newEntityFetcher(
		"MilestoneFetcher",
		client.FetchMilestoneCount,
		fetchEntity,
		nil,
		logger,
	)
}

func newSpanFetcher(client HeimdallClient, logger log.Logger) entityFetcher {
	fetchLastEntityId := func(ctx context.Context) (int64, error) {
		span, err := client.FetchLatestSpan(ctx)
		if err != nil {
			return 0, err
		}
		return int64(span.Id), nil
	}

	fetchEntity := func(ctx context.Context, id int64) (Entity, error) {
		return client.FetchSpan(ctx, uint64(id))
	}

	return newEntityFetcher(
		"SpanFetcher",
		fetchLastEntityId,
		fetchEntity,
		nil,
		logger,
	)
}

func downcastCheckpointEntity(e Entity) *Checkpoint {
	return e.(*Checkpoint)
}

func downcastMilestoneEntity(e Entity) *Milestone {
	return e.(*Milestone)
}

func downcastSpanEntity(e Entity) *Span {
	return e.(*Span)
}

func (s *Scraper) RegisterCheckpointObserver(observer func([]*Checkpoint)) polygoncommon.UnregisterFunc {
	return s.checkpointObservers.Register(observer)
}

func (s *Scraper) RegisterMilestoneObserver(observer func([]*Milestone)) polygoncommon.UnregisterFunc {
	return s.milestoneObservers.Register(observer)
}

func (s *Scraper) RegisterSpanObserver(observer func([]*Span)) polygoncommon.UnregisterFunc {
	return s.spanObservers.Register(observer)
}

func (s *Scraper) Synchronize(ctx context.Context) {
	s.checkpointSyncEvent.Wait(ctx)
	s.milestoneSyncEvent.Wait(ctx)
	s.spanSyncEvent.Wait(ctx)
}

func (s *Scraper) Run(parentCtx context.Context) error {
	tx := s.txProvider()
	if tx == nil {
		// TODO: implement and remove
		s.logger.Warn("heimdall.Scraper txProvider is not implemented yet")
		return nil
	}
	reader := s.readerProvider()
	if reader == nil {
		// TODO: implement and remove
		s.logger.Warn("heimdall.Scraper readerProvider is not implemented yet")
		return nil
	}

	blockNumToIdIndexFactory := func() *RangeIndex {
		index, err := NewRangeIndex(parentCtx, s.tmpDir, s.logger)
		if err != nil {
			panic(err)
		}
		return index
	}

	group, ctx := errgroup.WithContext(parentCtx)

	// sync checkpoints
	group.Go(func() error {
		return s.syncEntity(
			ctx,
			newCheckpointStore(tx, reader, blockNumToIdIndexFactory),
			newCheckpointFetcher(s.client, s.logger),
			func(entities []Entity) {
				s.checkpointObservers.Notify(libcommon.SliceMap(entities, downcastCheckpointEntity))
			},
			s.checkpointSyncEvent,
		)
	})

	// sync milestones
	group.Go(func() error {
		return s.syncEntity(
			ctx,
			newMilestoneStore(tx, reader, blockNumToIdIndexFactory),
			newMilestoneFetcher(s.client, s.logger),
			func(entities []Entity) {
				s.milestoneObservers.Notify(libcommon.SliceMap(entities, downcastMilestoneEntity))
			},
			s.milestoneSyncEvent,
		)
	})

	// sync spans
	group.Go(func() error {
		return s.syncEntity(
			ctx,
			newSpanStore(tx, reader, blockNumToIdIndexFactory),
			newSpanFetcher(s.client, s.logger),
			func(entities []Entity) {
				s.spanObservers.Notify(libcommon.SliceMap(entities, downcastSpanEntity))
			},
			s.spanSyncEvent,
		)
	})

	defer func() {
		s.checkpointObservers.Close()
		s.milestoneObservers.Close()
		s.spanObservers.Close()
	}()

	return group.Wait()
}
