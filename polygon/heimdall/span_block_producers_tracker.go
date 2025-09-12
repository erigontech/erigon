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
	"sync/atomic"
	"time"

	lru "github.com/hashicorp/golang-lru/v2"

	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/polygon/bor/borcfg"
)

func newSpanBlockProducersTracker(
	logger log.Logger,
	borConfig *borcfg.BorConfig,
	store EntityStore[*SpanBlockProducerSelection],
) *spanBlockProducersTracker {
	recentSelectionsLru, err := lru.New[uint64, SpanBlockProducerSelection](1024)
	if err != nil {
		panic(err)
	}

	return &spanBlockProducersTracker{
		logger:              logger,
		borConfig:           borConfig,
		store:               store,
		recentSelections:    recentSelectionsLru,
		newSpans:            make(chan *Span),
		idleSignal:          make(chan struct{}),
		spanProcessedSignal: make(chan struct{}),
	}
}

type spanBlockProducersTracker struct {
	logger              log.Logger
	borConfig           *borcfg.BorConfig
	store               EntityStore[*SpanBlockProducerSelection]
	recentSelections    *lru.Cache[uint64, SpanBlockProducerSelection] // sprint number -> SpanBlockProducerSelection
	newSpans            chan *Span
	queued              atomic.Int32
	idleSignal          chan struct{}
	spanProcessedSignal chan struct{} // signal that a new span was fully processed
}

func (t *spanBlockProducersTracker) Run(ctx context.Context) error {
	t.logger.Info(heimdallLogPrefix("running span block producers tracker component"))

	defer close(t.idleSignal)
	defer close(t.spanProcessedSignal)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case newSpan := <-t.newSpans:
			err := t.ObserveSpan(ctx, newSpan)
			if err != nil {
				return err
			}

			// signal that the span was observed (non-blocking)
			select {
			case t.spanProcessedSignal <- struct{}{}:
			default:
			}

			t.queued.Add(-1)
			if t.queued.Load() == 0 {
				select {
				case t.idleSignal <- struct{}{}:
				default: // continue if a signal is already queued
				}
			}
		}
	}
}

// Anticipates a new span to be observe and fully processed withing the given timeout period.
// Returns true if a new span was processed, false if no new span was processed
func (t *spanBlockProducersTracker) AnticipateNewSpanWithTimeout(ctx context.Context, timeout time.Duration) (bool, error) {
	select {
	case <-ctx.Done():
		return false, ctx.Err()
	case _, ok := <-t.spanProcessedSignal:
		if !ok {
			return false, errors.New("spanProcessed channel was closed")
		}
		return true, nil

	case <-time.After(timeout): // timeout
	}
	return false, nil
}

func (t *spanBlockProducersTracker) Synchronize(ctx context.Context) error {
	if t.queued.Load() == 0 {
		return nil
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case _, ok := <-t.idleSignal:
		if !ok {
			return errors.New("idleSignal channel closed")
		}
		return nil
	}
}

func (t *spanBlockProducersTracker) ObserveSpanAsync(ctx context.Context, span *Span) {
	select {
	case <-ctx.Done():
		return
	case t.newSpans <- span:
		t.queued.Add(1)
		return
	}
}

func (t *spanBlockProducersTracker) ObserveSpan(ctx context.Context, newSpan *Span) error {
	t.logger.Debug(heimdallLogPrefix("block producers tracker observing span"), "newSpan", newSpan)

	lastProducerSelection, ok, err := t.store.LastEntity(ctx)
	if err != nil {
		return err
	}
	if !ok {
		if newSpan.Id != 0 {
			return errors.New("expected first new span to be span 0")
		}

		newProducerSelection := &SpanBlockProducerSelection{
			SpanId:     newSpan.Id,
			StartBlock: newSpan.StartBlock,
			EndBlock:   newSpan.EndBlock,
			// https://github.com/maticnetwork/genesis-contracts/blob/master/contracts/BorValidatorSet.template#L82-L89
			// initial producers == initial validators
			Producers: NewValidatorSet(newSpan.ValidatorSet.Validators),
		}
		err = t.store.PutEntity(ctx, uint64(newProducerSelection.SpanId), newProducerSelection)
		if err != nil {
			return err
		}

		return nil
	}

	if newSpan.Id > lastProducerSelection.SpanId+1 {
		return fmt.Errorf(
			"%w: last=%d,new=%d",
			errors.New("unexpected span gap"),
			lastProducerSelection.SpanId,
			newSpan.Id,
		)
	}

	if newSpan.Id <= lastProducerSelection.SpanId {
		return nil
	}

	producers := lastProducerSelection.Producers
	producers.UpdateValidatorMap()
	err = producers.UpdateTotalVotingPower()
	if err != nil {
		return err
	}

	spanStartSprintNum := t.borConfig.CalculateSprintNumber(lastProducerSelection.StartBlock)
	spanEndSprintNum := t.borConfig.CalculateSprintNumber(lastProducerSelection.EndBlock)
	increments := int(spanEndSprintNum - spanStartSprintNum)
	for i := 0; i < increments; i++ {
		producers = GetUpdatedValidatorSet(producers, producers.Validators, t.logger)
		producers.IncrementProposerPriority(1)
	}

	newProducers := GetUpdatedValidatorSet(producers, newSpan.Producers(), t.logger)
	newProducers.IncrementProposerPriority(1)
	newProducerSelection := &SpanBlockProducerSelection{
		SpanId:     newSpan.Id,
		StartBlock: newSpan.StartBlock,
		EndBlock:   newSpan.EndBlock,
		Producers:  newProducers,
	}

	err = t.store.PutEntity(ctx, uint64(newProducerSelection.SpanId), newProducerSelection)
	if err != nil {
		return err
	}

	return nil
}

func (t *spanBlockProducersTracker) Producers(ctx context.Context, blockNum uint64) (*ValidatorSet, error) {
	startTime := time.Now()

	producers, increments, err := t.producers(ctx, blockNum)
	if err != nil {
		return nil, err
	}

	t.logger.Debug(
		heimdallLogPrefix("producers api timing"),
		"blockNum", blockNum,
		"time", time.Since(startTime),
		"increments", increments,
	)

	return producers, nil
}

func (t *spanBlockProducersTracker) producers(ctx context.Context, blockNum uint64) (*ValidatorSet, int, error) {
	currentSprintNum := t.borConfig.CalculateSprintNumber(blockNum)

	// have we previously calculated the producers for the previous sprint num of the same span (chain tip optimisation)
	spanId, ok, err := t.store.EntityIdFromBlockNum(ctx, blockNum)
	if err != nil {
		return nil, 0, err
	}
	if !ok {
		return nil, 0, fmt.Errorf("could not get spanId from blockNum=%d", blockNum)
	}

	producerSelection, ok, err := t.store.Entity(ctx, spanId)
	if err != nil {
		return nil, 0, err
	}
	if !ok {
		return nil, 0, errors.New("no producers found for block num")
	}

	producers := producerSelection.Producers
	producers.UpdateValidatorMap()
	err = producers.UpdateTotalVotingPower()
	if err != nil {
		return nil, 0, err
	}

	spanStartSprintNum := t.borConfig.CalculateSprintNumber(producerSelection.StartBlock)
	increments := int(currentSprintNum - spanStartSprintNum)
	for i := 0; i < increments; i++ {
		producers = GetUpdatedValidatorSet(producers, producers.Validators, t.logger)
		producers.IncrementProposerPriority(1)
	}
	return producers, increments, nil
}
