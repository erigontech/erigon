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
	"sync"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/event"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/polygon/bor/borcfg"
	"github.com/erigontech/erigon/polygon/heimdall/poshttp"
)

const (
	isCatchingDelaySec = 600
)

type ServiceConfig struct {
	Store     Store
	BorConfig *borcfg.BorConfig
	Client    Client
	Logger    log.Logger
}

type Service struct {
	logger                    log.Logger
	store                     Store
	reader                    *Reader
	checkpointScraper         *Scraper[*Checkpoint]
	milestoneScraper          *Scraper[*Milestone]
	spanScraper               *Scraper[*Span]
	spanBlockProducersTracker *spanBlockProducersTracker
	client                    Client
	ready                     ready
}

func NewService(config ServiceConfig) *Service {
	logger := config.Logger
	borConfig := config.BorConfig
	store := config.Store
	client := config.Client
	checkpointFetcher := NewCheckpointFetcher(client, logger)
	milestoneFetcher := NewMilestoneFetcher(client, logger)
	spanFetcher := NewSpanFetcher(client, logger)

	checkpointScraper := NewScraper(
		"checkpoints",
		store.Checkpoints(),
		checkpointFetcher,
		1*time.Second,
		poshttp.TransientErrors,
		logger,
	)

	// ErrNotInMilestoneList transient error configuration is needed because there may be an unfortunate edge
	// case where FetchFirstMilestoneNum returned 10 but by the time our request reaches heimdall milestone=10
	// has been already pruned. Additionally, we've been observing this error happening sporadically for the
	// latest milestone.
	milestoneScraperTransientErrors := []error{ErrNotInMilestoneList}
	milestoneScraperTransientErrors = append(milestoneScraperTransientErrors, poshttp.TransientErrors...)
	milestoneScraper := NewScraper(
		"milestones",
		store.Milestones(),
		milestoneFetcher,
		1*time.Second,
		milestoneScraperTransientErrors,
		logger,
	)

	spanScraper := NewScraper(
		"spans",
		store.Spans(),
		spanFetcher,
		200*time.Millisecond,
		poshttp.TransientErrors,
		logger,
	)

	return &Service{
		logger:                    logger,
		store:                     store,
		reader:                    NewReader(borConfig, store, logger),
		checkpointScraper:         checkpointScraper,
		milestoneScraper:          milestoneScraper,
		spanScraper:               spanScraper,
		spanBlockProducersTracker: newSpanBlockProducersTracker(logger, borConfig, store.SpanBlockProducerSelections()),
		client:                    client,
	}
}

func NewCheckpointFetcher(client Client, logger log.Logger) *EntityFetcher[*Checkpoint] {
	return NewEntityFetcher(
		"CheckpointFetcher",
		func(ctx context.Context) (int64, error) {
			return 1, nil
		},
		client.FetchCheckpointCount,
		client.FetchCheckpoint,
		client.FetchCheckpoints,
		CheckpointsFetchLimit,
		1,
		logger,
	)
}

func NewMilestoneFetcher(client Client, logger log.Logger) *EntityFetcher[*Milestone] {
	return NewEntityFetcher(
		"MilestoneFetcher",
		client.FetchFirstMilestoneNum,
		client.FetchMilestoneCount,
		client.FetchMilestone,
		nil,
		0,
		1,
		logger,
	)
}

func NewSpanFetcher(client Client, logger log.Logger) *EntityFetcher[*Span] {
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

	return NewEntityFetcher(
		"SpanFetcher",
		func(ctx context.Context) (int64, error) {
			return 0, nil
		},
		fetchLastEntityId,
		fetchEntity,
		client.FetchSpans,
		SpansFetchLimit,
		0,
		logger,
	)
}

func (s *Service) Span(ctx context.Context, id uint64) (*Span, bool, error) {
	return s.reader.Span(ctx, id)
}

func (s *Service) SynchronizeCheckpoints(ctx context.Context) (*Checkpoint, bool, error) {
	s.logger.Info(heimdallLogPrefix("synchronizing checkpoints..."))
	return s.checkpointScraper.Synchronize(ctx)
}

func (s *Service) SynchronizeMilestones(ctx context.Context) (*Milestone, bool, error) {
	s.logger.Info(heimdallLogPrefix("synchronizing milestones..."))
	return s.milestoneScraper.Synchronize(ctx)
}

func (s *Service) AnticipateNewSpanWithTimeout(ctx context.Context, timeout time.Duration) (bool, error) {
	s.logger.Info(heimdallLogPrefix(fmt.Sprintf("anticipating new span update within %.0f seconds", timeout.Seconds())))
	return s.spanBlockProducersTracker.AnticipateNewSpanWithTimeout(ctx, timeout)
}

func (s *Service) SynchronizeSpans(ctx context.Context, blockNum uint64) error {
	s.logger.Debug(heimdallLogPrefix("synchronizing spans..."), "blockNum", blockNum)

	lastSpan, ok, err := s.store.Spans().LastEntity(ctx)
	if err != nil {
		return err
	}
	if !ok {
		return s.synchronizeSpans(ctx)
	}

	lastProducerSelection, ok, err := s.store.SpanBlockProducerSelections().LastEntity(ctx)
	if err != nil {
		return err
	}
	if !ok {
		return s.synchronizeSpans(ctx)
	}

	if lastSpan.EndBlock < blockNum || lastProducerSelection.EndBlock < blockNum {
		return s.synchronizeSpans(ctx)
	}

	return nil
}

func (s *Service) synchronizeSpans(ctx context.Context) error {
	_, ok, err := s.spanScraper.Synchronize(ctx)
	if err != nil {
		return err
	}
	if !ok {
		return errors.New("unexpected last entity not available")
	}

	if err := s.spanBlockProducersTracker.Synchronize(ctx); err != nil {
		return err
	}

	return nil
}

func (s *Service) CheckpointsFromBlock(ctx context.Context, startBlock uint64) ([]*Checkpoint, error) {
	return s.reader.CheckpointsFromBlock(ctx, startBlock)
}

func (s *Service) MilestonesFromBlock(ctx context.Context, startBlock uint64) ([]*Milestone, error) {
	return s.reader.MilestonesFromBlock(ctx, startBlock)
}

func (s *Service) Producers(ctx context.Context, blockNum uint64) (*ValidatorSet, error) {
	return s.reader.Producers(ctx, blockNum)
}

func (s *Service) RegisterMilestoneObserver(callback func(*Milestone), opts ...ObserverOption) event.UnregisterFunc {
	options := NewObserverOptions(opts...)
	return s.milestoneScraper.RegisterObserver(func(entities []*Milestone) {
		for _, entity := range common.SliceTakeLast(entities, options.eventsLimit) {
			callback(entity)
		}
	})
}

func (s *Service) RegisterCheckpointObserver(callback func(*Checkpoint), opts ...ObserverOption) event.UnregisterFunc {
	options := NewObserverOptions(opts...)
	return s.checkpointScraper.RegisterObserver(func(entities []*Checkpoint) {
		for _, entity := range common.SliceTakeLast(entities, options.eventsLimit) {
			callback(entity)
		}
	})
}

func (s *Service) RegisterSpanObserver(callback func(*Span), opts ...ObserverOption) event.UnregisterFunc {
	options := NewObserverOptions(opts...)
	return s.spanScraper.RegisterObserver(func(entities []*Span) {
		for _, entity := range common.SliceTakeLast(entities, options.eventsLimit) {
			callback(entity)
		}
	})
}

type ready struct {
	mu     sync.Mutex
	on     chan struct{}
	state  bool
	inited bool
}

func (r *ready) On() <-chan struct{} {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.init()
	return r.on
}

func (r *ready) init() {
	if r.inited {
		return
	}
	r.on = make(chan struct{})
	r.inited = true
}

func (r *ready) set() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.init()
	if r.state {
		return
	}
	r.state = true
	close(r.on)
}

func (s *Service) Ready(ctx context.Context) <-chan error {
	errc := make(chan error)

	go func() {
		select {
		case <-ctx.Done():
			errc <- ctx.Err()
		case <-s.ready.On():
			errc <- nil
		}

		close(errc)
	}()

	return errc
}

func (s *Service) Run(ctx context.Context) error {
	s.logger.Info(heimdallLogPrefix("running heimdall service component"))

	defer s.store.Close()
	if err := s.store.Prepare(ctx); err != nil {
		return nil
	}

	s.ready.set()

	if err := s.replayUntrackedSpans(ctx); err != nil {
		return err
	}

	s.RegisterSpanObserver(func(span *Span) {
		s.spanBlockProducersTracker.ObserveSpanAsync(ctx, span)
	})

	milestoneObserver := s.RegisterMilestoneObserver(func(milestone *Milestone) {
		poshttp.UpdateObservedWaypointMilestoneLength(milestone.Length())
	})
	defer milestoneObserver()

	checkpointObserver := s.RegisterCheckpointObserver(func(checkpoint *Checkpoint) {
		poshttp.UpdateObservedWaypointCheckpointLength(checkpoint.Length())
	}, WithEventsLimit(5))
	defer checkpointObserver()

	eg, ctx := errgroup.WithContext(ctx)
	eg.Go(func() error {
		if err := s.checkpointScraper.Run(ctx); err != nil {
			return fmt.Errorf("checkpoint scraper failed: %w", err)
		}

		return nil
	})
	eg.Go(func() error {
		if err := s.milestoneScraper.Run(ctx); err != nil {
			return fmt.Errorf("milestone scraper failed: %w", err)
		}

		return nil
	})
	eg.Go(func() error {
		if err := s.spanScraper.Run(ctx); err != nil {
			return fmt.Errorf("span scraper failed: %w", err)
		}

		return nil
	})
	eg.Go(func() error {
		if err := s.spanBlockProducersTracker.Run(ctx); err != nil {
			return fmt.Errorf("span producer tracker failed: %w", err)
		}

		return nil
	})
	return eg.Wait()
}

func (s *Service) replayUntrackedSpans(ctx context.Context) error {
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

func (s *Service) IsCatchingUp(ctx context.Context) (bool, error) {
	status, err := s.client.FetchStatus(ctx)
	if err != nil {
		return false, err
	}

	if status.CatchingUp {
		return true, nil
	}

	parsed, err := time.Parse(time.RFC3339, status.LatestBlockTime)
	if err != nil {
		return false, err
	}

	if parsed.Unix() < time.Now().Unix()-isCatchingDelaySec {
		return true, nil
	}

	return false, nil
}
