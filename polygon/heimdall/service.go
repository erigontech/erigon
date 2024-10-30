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

	libcommon "github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/v3/polygon/bor/borcfg"
	"github.com/erigontech/erigon/v3/polygon/bor/valset"
	"github.com/erigontech/erigon/v3/polygon/polygoncommon"
)

type ServiceConfig struct {
	Store       Store
	BorConfig   *borcfg.BorConfig
	HeimdallURL string
	Logger      log.Logger
}

type Service interface {
	Span(ctx context.Context, id uint64) (*Span, bool, error)
	CheckpointsFromBlock(ctx context.Context, startBlock uint64) ([]*Checkpoint, error)
	MilestonesFromBlock(ctx context.Context, startBlock uint64) ([]*Milestone, error)
	Producers(ctx context.Context, blockNum uint64) (*valset.ValidatorSet, error)
	RegisterMilestoneObserver(callback func(*Milestone), opts ...ObserverOption) polygoncommon.UnregisterFunc
	Run(ctx context.Context) error
	SynchronizeCheckpoints(ctx context.Context) (latest *Checkpoint, err error)
	SynchronizeMilestones(ctx context.Context) (latest *Milestone, err error)
	SynchronizeSpans(ctx context.Context, blockNum uint64) error
	Ready(ctx context.Context) <-chan error
}

type service struct {
	logger                    log.Logger
	store                     Store
	reader                    *Reader
	checkpointScraper         *scraper[*Checkpoint]
	milestoneScraper          *scraper[*Milestone]
	spanScraper               *scraper[*Span]
	spanBlockProducersTracker *spanBlockProducersTracker
	ready                     ready
}

func AssembleService(config ServiceConfig) Service {
	client := NewHeimdallClient(config.HeimdallURL, config.Logger)
	return NewService(config.BorConfig, client, config.Store, config.Logger)
}

func NewService(borConfig *borcfg.BorConfig, client HeimdallClient, store Store, logger log.Logger) Service {
	return newService(borConfig, client, store, logger)
}

var TransientErrors = []error{
	ErrBadGateway,
	ErrServiceUnavailable,
	context.DeadlineExceeded,
}

func newService(borConfig *borcfg.BorConfig, client HeimdallClient, store Store, logger log.Logger) *service {
	checkpointFetcher := newCheckpointFetcher(client, logger)
	milestoneFetcher := newMilestoneFetcher(client, logger)
	spanFetcher := newSpanFetcher(client, logger)

	checkpointScraper := newScraper(
		store.Checkpoints(),
		checkpointFetcher,
		1*time.Second,
		TransientErrors,
		logger,
	)

	// ErrNotInMilestoneList transient error configuration is needed because there may be an unfortunate edge
	// case where FetchFirstMilestoneNum returned 10 but by the time our request reaches heimdall milestone=10
	// has been already pruned. Additionally, we've been observing this error happening sporadically for the
	// latest milestone.
	milestoneScraperTransientErrors := []error{ErrNotInMilestoneList}
	milestoneScraperTransientErrors = append(milestoneScraperTransientErrors, TransientErrors...)
	milestoneScraper := newScraper(
		store.Milestones(),
		milestoneFetcher,
		1*time.Second,
		milestoneScraperTransientErrors,
		logger,
	)

	spanScraper := newScraper(
		store.Spans(),
		spanFetcher,
		1*time.Second,
		TransientErrors,
		logger,
	)

	return &service{
		logger:                    logger,
		store:                     store,
		reader:                    NewReader(borConfig, store, logger),
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
		CheckpointsFetchLimit,
		1,
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
		1,
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
		client.FetchSpans,
		SpansFetchLimit,
		0,
		logger,
	)
}

func (s *service) Span(ctx context.Context, id uint64) (*Span, bool, error) {
	return s.reader.Span(ctx, id)
}

func (s *service) SynchronizeCheckpoints(ctx context.Context) (*Checkpoint, error) {
	s.logger.Info(heimdallLogPrefix("synchronizing checkpoints..."))
	return s.checkpointScraper.Synchronize(ctx)
}

func (s *service) SynchronizeMilestones(ctx context.Context) (*Milestone, error) {
	s.logger.Info(heimdallLogPrefix("synchronizing milestones..."))
	return s.milestoneScraper.Synchronize(ctx)
}

func (s *service) SynchronizeSpans(ctx context.Context, blockNum uint64) error {
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

func (s *service) synchronizeSpans(ctx context.Context) error {
	if _, err := s.spanScraper.Synchronize(ctx); err != nil {
		return err
	}

	if err := s.spanBlockProducersTracker.Synchronize(ctx); err != nil {
		return err
	}

	return nil
}

func (s *service) CheckpointsFromBlock(ctx context.Context, startBlock uint64) ([]*Checkpoint, error) {
	return s.reader.CheckpointsFromBlock(ctx, startBlock)
}

func (s *service) MilestonesFromBlock(ctx context.Context, startBlock uint64) ([]*Milestone, error) {
	return s.reader.MilestonesFromBlock(ctx, startBlock)
}

func (s *service) Producers(ctx context.Context, blockNum uint64) (*valset.ValidatorSet, error) {
	return s.reader.Producers(ctx, blockNum)
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

func (s *service) Ready(ctx context.Context) <-chan error {
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

func (s *service) Run(ctx context.Context) error {
	defer s.store.Close()

	if err := s.store.Prepare(ctx); err != nil {
		return nil
	}

	s.ready.set()

	if err := s.replayUntrackedSpans(ctx); err != nil {
		return err
	}

	s.RegisterSpanObserver(func(span *Span) {
		s.spanBlockProducersTracker.ObserveSpanAsync(span)
	})

	eg, ctx := errgroup.WithContext(ctx)
	eg.Go(func() error {
		err := s.checkpointScraper.Run(ctx)

		if err != nil {
			err = fmt.Errorf("checkpoint scraper failed: %w", err)
		}

		return err
	})
	eg.Go(func() error {
		err := s.milestoneScraper.Run(ctx)

		if err != nil {
			err = fmt.Errorf("milestone scraper failed: %w", err)
		}

		return err

	})
	eg.Go(func() error {
		err := s.spanScraper.Run(ctx)

		if err != nil {
			err = fmt.Errorf("span scraper failed: %w", err)
		}

		return err

	})
	eg.Go(func() error {
		err := s.spanBlockProducersTracker.Run(ctx)

		if err != nil {
			err = fmt.Errorf("span producer tracker failed: %w", err)
		}

		return err

	})
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
