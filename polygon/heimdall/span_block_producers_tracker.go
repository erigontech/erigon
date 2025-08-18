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
		logger:           logger,
		borConfig:        borConfig,
		store:            store,
		recentSelections: recentSelectionsLru,
		newSpans:         make(chan *Span),
		idleSignal:       make(chan struct{}),
	}
}

type spanBlockProducersTracker struct {
	logger           log.Logger
	borConfig        *borcfg.BorConfig
	store            EntityStore[*SpanBlockProducerSelection]
	recentSelections *lru.Cache[uint64, SpanBlockProducerSelection] // sprint number -> SpanBlockProducerSelection
	newSpans         chan *Span
	queued           atomic.Int32
	idleSignal       chan struct{}
}

func (t *spanBlockProducersTracker) Run(ctx context.Context) error {
	t.logger.Info(heimdallLogPrefix("running span block producers tracker component"))

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
	t.logger.Info(heimdallLogPrefix("block producers tracker observing span"), "id", newSpan.Id)

	fmt.Printf("OBSERVE_SPAN: %+v\n", newSpan)

	lastProducerSelection, ok, err := t.store.LastEntity(ctx)
	if err != nil {
		return err
	}
	if !ok {
		fmt.Printf("NOT OKKKKKKKKKKKKKKKK, INITIAL_PRODUCERS == INITIAL_VALIDATORS")
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
	fmt.Printf("UPDATE_PRIORITY(observe_span(producers)):  validatorset=%+v\n", producers)

	fmt.Printf(">>>>>>>>>>>>>>> newSpan.Id=%d newsSpan.Producers()=%+v\n", newSpan.Id, newSpan.Producers())

	newProducers := GetUpdatedValidatorSet(producers, newSpan.Producers(), t.logger)
	newProducers.IncrementProposerPriority(1)
	newProducerSelection := &SpanBlockProducerSelection{
		SpanId:     newSpan.Id,
		StartBlock: newSpan.StartBlock,
		EndBlock:   newSpan.EndBlock,
		Producers:  newProducers,
	}

	fmt.Printf("UPDATE_PRIORITY(observe_span(newProducers)):  validatorset=%+v\n", newProducers)

	sprintNum := t.borConfig.CalculateSprintNumber(newSpan.StartBlock)
	oldValset, cacheHit := t.recentSelections.Get(sprintNum)

	// there is already a selection for this sprint number, but we observed a new span
	// for the same sprint number. This is due to a span rotation in the same sprint.
	// Hence, we need to invalidate the cache, so that we don't see stale producer set.
	if cacheHit {
		t.logger.Debug("CACHE HIT: invalidating cache because of new validator set due to span rotation", "oldValset", oldValset, "newValset", newSpan)
		t.recentSelections.Remove(sprintNum)
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

	// have we previously calculated the producers for the same sprint num (chain tip optimisation)
	if selection, ok := t.recentSelections.Get(currentSprintNum); ok {
		fmt.Printf("PRODUCERS_1: blockNum=%d , spanId=%d, producers=%+v\n", blockNum, selection.SpanId, selection.Producers.Copy())
		return selection.Producers.Copy(), 0, nil
	}

	// have we previously calculated the producers for the previous sprint num of the same span (chain tip optimisation)
	spanId, ok, err := t.store.EntityIdFromBlockNum(ctx, blockNum)
	if err != nil {
		return nil, 0, err
	}
	if !ok {
		return nil, 0, fmt.Errorf("could not get spanId from blockNum=%d", blockNum)
	}
	var prevSprintNum uint64
	if currentSprintNum > 0 {
		prevSprintNum = currentSprintNum - 1
	}
	if selection, ok := t.recentSelections.Get(prevSprintNum); ok && SpanId(spanId) == selection.SpanId {
		producersCopy := selection.Producers.Copy()
		producersCopy.IncrementProposerPriority(1)
		selectionCopy := selection
		selectionCopy.Producers = producersCopy
		t.recentSelections.Add(currentSprintNum, selectionCopy)
		fmt.Printf("INCREASE_PRIORITY(1), PRODUCERS_2: blockNum=%d, spanId=%d, prevSprintNum=%d,  currentSprintNum=-%d, validatorset=%+v\n", blockNum, spanId, prevSprintNum, currentSprintNum, producersCopy)
		return producersCopy, 1, nil
	}

	// no recent selection that we can easily use, re-calculate from DB
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
	fmt.Printf("INCREASE_PRIORITY(2), PRODUCERS_3: blockNum=%d, spanId=%d,  currentSprintNum=%d, validatorset=%+v\n", blockNum, spanId, currentSprintNum, producers)

	t.recentSelections.Add(currentSprintNum, *producerSelection)
	return producers, increments, nil
}
