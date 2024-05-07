package heimdall

import (
	"context"
	"time"

	"github.com/ledgerwatch/log/v3"
	"golang.org/x/sync/errgroup"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv"
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

type entityStore interface {
	GetLastEntityId(ctx context.Context) (uint64, bool, error)
	PutEntity(ctx context.Context, id uint64, entity any) error
}

type EntityIdRange struct {
	Start uint64
	End   uint64
}

func (r EntityIdRange) Len() uint64 {
	return r.End + 1 - r.Start
}

type entityFetcher interface {
	FetchLastEntityId(ctx context.Context) (uint64, error)
	FetchEntitiesRange(ctx context.Context, idRange EntityIdRange) ([]any, error)
}

func (s *Scraper) syncEntity(
	ctx context.Context,
	store entityStore,
	fetcher entityFetcher,
	callback func(EntityIdRange),
) error {
	for ctx.Err() == nil {
		lastKnownId, hasLastKnownId, err := store.GetLastEntityId(ctx)
		if err != nil {
			return err
		}

		var idRange EntityIdRange
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
