package heimdall

import (
	"context"
	"time"

	"github.com/ledgerwatch/log/v3"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/polygon/polygoncommon"
)

type Scraper[TEntity Entity] struct {
	store entityStore[TEntity]

	fetcher   entityFetcher[TEntity]
	pollDelay time.Duration

	observers *polygoncommon.Observers[[]TEntity]
	syncEvent *polygoncommon.EventNotifier

	logger log.Logger
}

func NewScraper[TEntity Entity](
	store entityStore[TEntity],
	fetcher entityFetcher[TEntity],
	pollDelay time.Duration,
	logger log.Logger,
) *Scraper[TEntity] {
	return &Scraper[TEntity]{
		store: store,

		fetcher:   fetcher,
		pollDelay: pollDelay,

		observers: polygoncommon.NewObservers[[]TEntity](),
		syncEvent: polygoncommon.NewEventNotifier(),

		logger: logger,
	}
}

func (s *Scraper[TEntity]) Run(ctx context.Context) error {
	defer s.store.Close()
	if err := s.store.Prepare(ctx); err != nil {
		return err
	}

	for ctx.Err() == nil {
		lastKnownId, hasLastKnownId, err := s.store.GetLastEntityId(ctx)
		if err != nil {
			return err
		}

		idRange, err := s.fetcher.FetchEntityIdRange(ctx)
		if err != nil {
			return err
		}

		if hasLastKnownId {
			idRange.Start = max(idRange.Start, lastKnownId+1)
		}

		if idRange.Start > idRange.End {
			s.syncEvent.SetAndBroadcast()
			if err := libcommon.Sleep(ctx, s.pollDelay); err != nil {
				s.syncEvent.Reset()
				return err
			}
		} else {
			entities, err := s.fetcher.FetchEntitiesRange(ctx, idRange)
			if err != nil {
				return err
			}

			for i, entity := range entities {
				if err = s.store.PutEntity(ctx, idRange.Start+uint64(i), entity); err != nil {
					return err
				}
			}

			go s.observers.Notify(entities)
		}
	}
	return ctx.Err()
}

func (s *Scraper[TEntity]) RegisterObserver(observer func([]TEntity)) polygoncommon.UnregisterFunc {
	return s.observers.Register(observer)
}

func (s *Scraper[TEntity]) Synchronize(ctx context.Context) {
	s.syncEvent.Wait(ctx)
}
