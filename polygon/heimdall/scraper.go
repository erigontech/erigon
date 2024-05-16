package heimdall

import (
	"context"
	"time"

	"github.com/ledgerwatch/log/v3"
	"golang.org/x/sync/errgroup"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/polygon/polygoncommon"
)

type Scraper struct {
	checkpointStore entityStore
	milestoneStore  entityStore
	spanStore       entityStore

	client    HeimdallClient
	pollDelay time.Duration

	checkpointObservers *polygoncommon.Observers[[]*Checkpoint]
	milestoneObservers  *polygoncommon.Observers[[]*Milestone]
	spanObservers       *polygoncommon.Observers[[]*Span]

	checkpointSyncEvent *polygoncommon.EventNotifier
	milestoneSyncEvent  *polygoncommon.EventNotifier
	spanSyncEvent       *polygoncommon.EventNotifier

	logger log.Logger
}

func NewScraper(
	checkpointStore entityStore,
	milestoneStore entityStore,
	spanStore entityStore,
	client HeimdallClient,
	pollDelay time.Duration,
	logger log.Logger,
) *Scraper {
	return &Scraper{
		checkpointStore: checkpointStore,
		milestoneStore:  milestoneStore,
		spanStore:       spanStore,

		client:    client,
		pollDelay: pollDelay,

		checkpointObservers: polygoncommon.NewObservers[[]*Checkpoint](),
		milestoneObservers:  polygoncommon.NewObservers[[]*Milestone](),
		spanObservers:       polygoncommon.NewObservers[[]*Span](),

		checkpointSyncEvent: polygoncommon.NewEventNotifier(),
		milestoneSyncEvent:  polygoncommon.NewEventNotifier(),
		spanSyncEvent:       polygoncommon.NewEventNotifier(),

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
	if e == nil {
		return nil
	}
	return e.(*Checkpoint)
}

func downcastMilestoneEntity(e Entity) *Milestone {
	if e == nil {
		return nil
	}
	return e.(*Milestone)
}

func downcastSpanEntity(e Entity) *Span {
	if e == nil {
		return nil
	}
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
	group, ctx := errgroup.WithContext(parentCtx)

	// sync checkpoints
	group.Go(func() error {
		return s.syncEntity(
			ctx,
			s.checkpointStore,
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
			s.milestoneStore,
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
			s.spanStore,
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
