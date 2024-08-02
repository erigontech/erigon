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
	"slices"
	"time"

	"golang.org/x/sync/errgroup"

	libcommon "github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/polygon/polygoncommon"
)

type Service interface {
	Span(ctx context.Context, id uint64) (*Span, bool, error)
	LatestSpans(ctx context.Context, count uint) ([]*Span, error)
	CheckpointsFromBlock(ctx context.Context, startBlock uint64) (Waypoints, error)
	MilestonesFromBlock(ctx context.Context, startBlock uint64) (Waypoints, error)

	RegisterMilestoneObserver(callback func(*Milestone), opts ...ObserverOption) polygoncommon.UnregisterFunc
	RegisterSpanObserver(callback func(*Span), opts ...ObserverOption) polygoncommon.UnregisterFunc
	Run(ctx context.Context) error
}

type service struct {
	store             ServiceStore
	checkpointScraper *scraper[*Checkpoint]
	milestoneScraper  *scraper[*Milestone]
	spanScraper       *scraper[*Span]
}

func AssembleService(heimdallUrl string, dataDir string, tmpDir string, logger log.Logger) Service {
	store := NewMdbxServiceStore(logger, dataDir, tmpDir)
	client := NewHeimdallClient(heimdallUrl, logger)
	return NewService(client, store, logger)
}

func NewService(client HeimdallClient, store ServiceStore, logger log.Logger) Service {
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
		store:             store,
		checkpointScraper: checkpointScraper,
		milestoneScraper:  milestoneScraper,
		spanScraper:       spanScraper,
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

func (s *service) FetchLatestSpan(ctx context.Context) (*Span, bool, error) {
	s.checkpointScraper.Synchronize(ctx)
	return s.store.Spans().LastEntity(ctx)
}

func (s *service) LatestSpans(ctx context.Context, count uint) ([]*Span, error) {
	if count == 0 {
		return nil, errors.New("can't fetch 0 latest spans")
	}

	span, ok, err := s.FetchLatestSpan(ctx)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, errors.New("can't fetch latest span")
	}

	latestSpans := make([]*Span, 0, count)
	latestSpans = append(latestSpans, span)
	count--

	for count > 0 {
		prevSpanRawId := span.RawId()
		if prevSpanRawId == 0 {
			break
		}

		span, ok, err = s.store.Spans().Entity(ctx, prevSpanRawId-1)
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, errors.New(fmt.Sprintf("can't fetch span %v", prevSpanRawId-1))
		}

		latestSpans = append(latestSpans, span)
		count--
	}

	slices.Reverse(latestSpans)
	return latestSpans, nil
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

func (s *service) synchronizeScrapers(ctx context.Context) {
	s.checkpointScraper.Synchronize(ctx)
	s.milestoneScraper.Synchronize(ctx)
	s.spanScraper.Synchronize(ctx)
}

func (s *service) CheckpointsFromBlock(ctx context.Context, startBlock uint64) (Waypoints, error) {
	s.synchronizeScrapers(ctx)
	entities, err := s.store.Checkpoints().RangeFromBlockNum(ctx, startBlock)
	return libcommon.SliceMap(entities, castEntityToWaypoint[*Checkpoint]), err
}

func (s *service) MilestonesFromBlock(ctx context.Context, startBlock uint64) (Waypoints, error) {
	s.synchronizeScrapers(ctx)
	entities, err := s.store.Milestones().RangeFromBlockNum(ctx, startBlock)
	return libcommon.SliceMap(entities, castEntityToWaypoint[*Milestone]), err
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

	scrapersGroup, scrapersGroupCtx := errgroup.WithContext(ctx)
	scrapersGroup.Go(func() error { return s.checkpointScraper.Run(scrapersGroupCtx) })
	scrapersGroup.Go(func() error { return s.milestoneScraper.Run(scrapersGroupCtx) })
	scrapersGroup.Go(func() error { return s.spanScraper.Run(scrapersGroupCtx) })
	return scrapersGroup.Wait()
}
