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

	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/polygon/bor/valset"
)

type CalculateSprintNumberFunc func(uint64) uint64

func newSpanBlockProducersTracker(
	logger log.Logger,
	calculateSprintNumber CalculateSprintNumberFunc,
	store EntityStore[*SpanBlockProducerSelection],
) *spanBlockProducersTracker {
	return &spanBlockProducersTracker{
		logger:                logger,
		calculateSprintNumber: calculateSprintNumber,
		store:                 store,
		newSpans:              make(chan *Span),
		idleSignal:            make(chan struct{}),
	}
}

type spanBlockProducersTracker struct {
	logger                log.Logger
	calculateSprintNumber CalculateSprintNumberFunc
	store                 EntityStore[*SpanBlockProducerSelection]
	newSpans              chan *Span
	queued                atomic.Int32
	idleSignal            chan struct{}
}

func (t *spanBlockProducersTracker) Run(ctx context.Context) error {
	defer close(t.idleSignal)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case newSpan := <-t.newSpans:
			err := t.ObserveSpan(ctx, newSpan)
			if err != nil {
				return err
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

func (t *spanBlockProducersTracker) ObserveSpanAsync(span *Span) {
	t.queued.Add(1)
	t.newSpans <- span
}

func (t *spanBlockProducersTracker) ObserveSpan(ctx context.Context, newSpan *Span) error {
	t.logger.Debug(heimdallLogPrefix("block producers tracker observing span"), "id", newSpan.Id)

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
			Producers: valset.NewValidatorSet(newSpan.ValidatorSet.Validators),
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

	spanStartSprintNum := t.calculateSprintNumber(lastProducerSelection.StartBlock)
	spanEndSprintNum := t.calculateSprintNumber(lastProducerSelection.EndBlock)
	increments := int(spanEndSprintNum - spanStartSprintNum)
	for i := 0; i < increments; i++ {
		producers = valset.GetUpdatedValidatorSet(producers, producers.Validators, t.logger)
		producers.IncrementProposerPriority(1)
	}

	newProducers := valset.GetUpdatedValidatorSet(producers, newSpan.Producers(), t.logger)
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

func (t *spanBlockProducersTracker) Producers(ctx context.Context, blockNum uint64) (*valset.ValidatorSet, error) {
	spanId := SpanIdAt(blockNum)
	producerSelection, ok, err := t.store.Entity(ctx, uint64(spanId))
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, errors.New("no producers found for block num")
	}

	producers := producerSelection.Producers
	producers.UpdateValidatorMap()
	err = producers.UpdateTotalVotingPower()
	if err != nil {
		return nil, err
	}

	spanStartSprintNum := t.calculateSprintNumber(producerSelection.StartBlock)
	currentSprintNum := t.calculateSprintNumber(blockNum)
	increments := int(currentSprintNum - spanStartSprintNum)
	for i := 0; i < increments; i++ {
		producers = valset.GetUpdatedValidatorSet(producers, producers.Validators, t.logger)
		producers.IncrementProposerPriority(1)
	}

	return producers, nil
}
