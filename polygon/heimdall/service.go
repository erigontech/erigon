// Copyright 2024 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package heimdall

import (
	"context"
	"errors"
	"fmt"
	"time"

	"golang.org/x/sync/errgroup"

	libcommon "github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/polygon/bor/borcfg"
	"github.com/erigontech/erigon/polygon/bor/valset"
	"github.com/erigontech/erigon/polygon/polygoncommon"
)

type Service interface {
	Span(ctx context.Context, id uint64) (*Span, bool, error)
	CheckpointsFromBlock(ctx context.Context, startBlock uint64) (Waypoints, error)
	MilestonesFromBlock(ctx context.Context, startBlock uint64) (Waypoints, error)
	Producers(ctx context.Context, blockNum uint64) (*valset.ValidatorSet, error)
	RegisterMilestoneObserver(callback func(*Milestone), opts ...ObserverOption) polygoncommon.UnregisterFunc
	Run(ctx context.Context) error
	Synchronize(ctx context.Context)
}

type service struct {
	store                     ServiceStore
	checkpointScraper         *scraper[*Checkpoint]
	milestoneScraper          *scraper[*Milestone]
	spanScraper               *scraper[*Span]
	spanBlockProducersTracker *spanBlockProducersTracker
}

func AssembleService(borConfig *borcfg.BorConfig, heimdallUrl string, dataDir string, tmpDir string, logger log.Logger) Service {
	store := NewMdbxServiceStore(logger, dataDir, tmpDir)
	client := NewHeimdallClient(heimdallUrl, logger)
	return NewService(borConfig, client, store, logger)
}

func NewService(borConfig *borcfg.BorConfig, client HeimdallClient, store ServiceStore, logger log.Logger) Service {
	checkpointFetcher := newCheckpointFetcher(client, logger)
	milestoneFetcher := newMilestoneFetcher(client, logger)
	spanFetcher := newSpanFetcher(client, logger)

	checkpointScraper := newScrapper(
		store.Checkpoints(),
		checkpointFetcher,
		1*time.Second,
		logger,
	)

	milestoneScraper := newScrapper(
		store.Milestones(),
		milestoneFetcher,
		1*time.Second,
		logger,
	)

	spanScraper := newScrapper(
		store.Spans(),
		spanFetcher,
		1*time.Second,
		logger,
	)

	return &service{
		store:                     store,
		checkpointScraper:         checkpointScraper,
		milestoneScraper:          milestoneScraper,
		spanScraper:               spanScraper,
		spanBlockProducersTracker: newSpanBlockProducersTracker(logger, borConfig, store.SpanBlockProducerSelections()),
	}
}

func newCheckpointFetcher(client HeimdallClient, logger log.Logger) entityFetcher[*Checkpoint] {
	return newEntityFetcher(
		"CheckpointFetcher",
		func(ctx context.Context) (int64, error) {
			return 1, nil
		},
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
		func(ctx context.Context) (int64, error) {
			return 0, nil
		},
		fetchLastEntityId,
		fetchEntity,
		nil,
		0,
		logger,
	)
}

func (s *service) Span(ctx context.Context, id uint64) (*Span, bool, error) {
	span, ok, err := s.store.Spans().Entity(ctx, id)
	if err != nil || !ok {
		return nil, ok, err
	}

	return span, ok, nil
}

func castEntityToWaypoint[TEntity Waypoint](entity TEntity) Waypoint {
	return entity
}

func (s *service) Synchronize(ctx context.Context) {
	s.checkpointScraper.Synchronize(ctx)
	s.milestoneScraper.Synchronize(ctx)
	s.spanScraper.Synchronize(ctx)
	s.spanBlockProducersTracker.Synchronize(ctx)
}

func (s *service) CheckpointsFromBlock(ctx context.Context, startBlock uint64) (Waypoints, error) {
	s.Synchronize(ctx)
	entities, err := s.store.Checkpoints().RangeFromBlockNum(ctx, startBlock)
	return libcommon.SliceMap(entities, castEntityToWaypoint[*Checkpoint]), err
}

func (s *service) MilestonesFromBlock(ctx context.Context, startBlock uint64) (Waypoints, error) {
	s.Synchronize(ctx)
	entities, err := s.store.Milestones().RangeFromBlockNum(ctx, startBlock)
	return libcommon.SliceMap(entities, castEntityToWaypoint[*Milestone]), err
}

func (s *service) Producers(ctx context.Context, blockNum uint64) (*valset.ValidatorSet, error) {
	s.Synchronize(ctx)
	return s.spanBlockProducersTracker.Producers(ctx, blockNum)
}

func (s *service) RegisterMilestoneObserver(callback func(*Milestone), opts ...ObserverOption) polygoncommon.UnregisterFunc {
	options := NewObserverOptions(opts...)
	return s.milestoneScraper.RegisterObserver(func(entities []*Milestone) {
		for _, entity := range libcommon.SliceTakeLast(entities, options.eventsLimit) {
			callback(entity)
		}
	})
}

func (s *service) RegisterSpanObserver(callback func(*Span), opts ...ObserverOption) polygoncommon.UnregisterFunc {
	options := NewObserverOptions(opts...)
	return s.spanScraper.RegisterObserver(func(entities []*Span) {
		for _, entity := range libcommon.SliceTakeLast(entities, options.eventsLimit) {
			callback(entity)
		}
	})
}

func (s *service) Run(ctx context.Context) error {
	defer s.store.Close()

	if err := s.store.Prepare(ctx); err != nil {
		return nil
	}

	if err := s.replayUntrackedSpans(ctx); err != nil {
		return err
	}

	s.RegisterSpanObserver(func(span *Span) {
		s.spanBlockProducersTracker.ObserveSpanAsync(span)
	})

	eg, ctx := errgroup.WithContext(ctx)
	eg.Go(func() error { return s.checkpointScraper.Run(ctx) })
	eg.Go(func() error { return s.milestoneScraper.Run(ctx) })
	eg.Go(func() error { return s.spanScraper.Run(ctx) })
	eg.Go(func() error { return s.spanBlockProducersTracker.Run(ctx) })
	return eg.Wait()
}

func (s *service) replayUntrackedSpans(ctx context.Context) error {
	lastSpanId, ok, err := s.store.Spans().LastEntityId(ctx)
	if err != nil {
		return err
	}
	if !ok {
		return nil
	}

	lastProducerSelectionId, ok, err := s.store.SpanBlockProducerSelections().LastEntityId(ctx)
	if err != nil {
		return err
	}

	var start uint64
	if ok {
		start = lastProducerSelectionId + 1
	} else {
		start = lastProducerSelectionId
	}

	for id := start; id <= lastSpanId; id++ {
		span, ok, err := s.store.Spans().Entity(ctx, id)
		if err != nil {
			return err
		}
		if !ok {
			return fmt.Errorf("%w: %d", errors.New("can't replay missing span"), id)
		}

		err = s.spanBlockProducersTracker.ObserveSpan(ctx, span)
		if err != nil {
			return err
		}
	}

	return nil
}
