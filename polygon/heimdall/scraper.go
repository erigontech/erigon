package heimdall

import (
	"context"
	"time"

	"github.com/ledgerwatch/log/v3"
	"golang.org/x/sync/errgroup"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/turbo/services"
)

type Scraper struct {
	txProvider     func() kv.RwTx
	readerProvider func() reader

	client    HeimdallClient
	pollDelay time.Duration
	logger    log.Logger
}

func NewScraperTODO(
	client HeimdallClient,
	pollDelay time.Duration,
	logger log.Logger,
) *Scraper {
	return NewScraper(
		func() kv.RwTx { /* TODO */ return nil },
		func() reader { /* TODO */ return nil },
		client,
		pollDelay,
		logger,
	)
}

func NewScraper(
	txProvider func() kv.RwTx,
	readerProvider func() reader,

	client HeimdallClient,
	pollDelay time.Duration,
	logger log.Logger,
) *Scraper {
	return &Scraper{
		txProvider:     txProvider,
		readerProvider: readerProvider,

		client:    client,
		pollDelay: pollDelay,
		logger:    logger,
	}
}

func (s *Scraper) syncEntity(
	ctx context.Context,
	store entityStore,
	fetcher entityFetcher,
	callback func(ClosedRange),
) error {
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
			libcommon.Sleep(ctx, s.pollDelay)
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
				go callback(idRange)
			}
		}
	}
	return ctx.Err()
}

func newCheckpointStore(tx kv.RwTx, reader services.BorCheckpointReader) entityStore {
	return newGenericEntityStore[Checkpoint](tx, kv.BorCheckpoints, reader.LastCheckpointId, reader.Checkpoint)
}

func newMilestoneStore(tx kv.RwTx, reader services.BorMilestoneReader) entityStore {
	return newGenericEntityStore[Milestone](tx, kv.BorMilestones, reader.LastMilestoneId, reader.Milestone)
}

func newSpanStore(tx kv.RwTx, reader services.BorSpanReader) entityStore {
	return newGenericEntityStore[Span](tx, kv.BorSpans, reader.LastSpanId, reader.Span)
}

func newCheckpointFetcher(client HeimdallClient, logger log.Logger) entityFetcher {
	return newGenericEntityFetcher[Checkpoint](
		"CheckpointFetcher",
		client.FetchCheckpointCount,
		client.FetchCheckpoint,
		client.FetchCheckpoints,
		func(entity *Checkpoint) uint64 { return entity.StartBlock().Uint64() },
		logger,
	)
}

func newMilestoneFetcher(client HeimdallClient, logger log.Logger) entityFetcher {
	return newGenericEntityFetcher[Milestone](
		"MilestoneFetcher",
		client.FetchMilestoneCount,
		client.FetchMilestone,
		nil,
		func(entity *Milestone) uint64 { return entity.StartBlock().Uint64() },
		logger,
	)
}

func newSpanFetcher(client HeimdallClient, logger log.Logger) entityFetcher {
	fetchLastSpanId := func(ctx context.Context) (int64, error) {
		span, err := client.FetchLatestSpan(ctx)
		if err != nil {
			return 0, err
		}
		return int64(span.Id), nil
	}

	fetchSpan := func(ctx context.Context, id int64) (*Span, error) {
		return client.FetchSpan(ctx, uint64(id))
	}

	return newGenericEntityFetcher[Span](
		"SpanFetcher",
		fetchLastSpanId,
		fetchSpan,
		nil,
		func(entity *Span) uint64 { return entity.StartBlock },
		logger,
	)
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

	group, ctx := errgroup.WithContext(parentCtx)

	// sync checkpoints
	group.Go(func() error {
		return s.syncEntity(ctx, newCheckpointStore(tx, reader), newCheckpointFetcher(s.client, s.logger), nil /* TODO */)
	})
	// sync milestones
	group.Go(func() error {
		return s.syncEntity(ctx, newMilestoneStore(tx, reader), newMilestoneFetcher(s.client, s.logger), nil /* TODO */)
	})
	// sync spans
	group.Go(func() error {
		return s.syncEntity(ctx, newSpanStore(tx, reader), newSpanFetcher(s.client, s.logger), nil /* TODO */)
	})

	return group.Wait()
}
