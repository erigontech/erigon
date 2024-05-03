package heimdall

import (
	"context"
	"time"

	"github.com/ledgerwatch/log/v3"
	"golang.org/x/sync/errgroup"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
)

type Scraper struct {
	store     Store
	client    HeimdallClient
	pollDelay time.Duration
	logger    log.Logger
}

func NewScraper(
	store Store,
	client HeimdallClient,
	pollDelay time.Duration,
	logger log.Logger,
) *Scraper {
	return &Scraper{
		store:     store,
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
	// TODO: checkpoint store implementing entityStore using s.store
	// TODO: milestone store implementing entityStore using s.store
	// TODO: span store implementing entityStore using s.store

	// TODO: checkpoint fetcher implementing entityFetcher using s.client
	// TODO: milestone fetcher implementing entityFetcher using s.client
	// TODO: span fetcher implementing entityFetcher using s.client

	group, ctx := errgroup.WithContext(parentCtx)

	// sync checkpoints
	group.Go(func() error {
		return s.syncEntity(ctx, nil /* TODO */, nil /* TODO */, nil /* TODO */)
	})
	// sync milestones
	group.Go(func() error {
		return s.syncEntity(ctx, nil /* TODO */, nil /* TODO */, nil /* TODO */)
	})
	// sync spans
	group.Go(func() error {
		return s.syncEntity(ctx, nil /* TODO */, nil /* TODO */, nil /* TODO */)
	})

	return group.Wait()
}
