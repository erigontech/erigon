package heimdall

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"

	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/polygon/bor/borcfg"
	"github.com/erigontech/erigon/polygon/bor/valset"
)

func newSpanBlockProducersTracker(
	logger log.Logger,
	borConfig *borcfg.BorConfig,
	store EntityStore[*SpanBlockProducerSelection],
) *spanBlockProducersTracker {
	return &spanBlockProducersTracker{
		logger:     logger,
		borConfig:  borConfig,
		store:      store,
		newSpans:   make(chan *Span),
		idleSignal: make(chan struct{}),
	}
}

type spanBlockProducersTracker struct {
	logger     log.Logger
	borConfig  *borcfg.BorConfig
	store      EntityStore[*SpanBlockProducerSelection]
	newSpans   chan *Span
	queued     atomic.Int32
	idleSignal chan struct{}
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

func (t *spanBlockProducersTracker) Synchronize(ctx context.Context) {
	if t.queued.Load() == 0 {
		return
	}

	select {
	case <-ctx.Done():
		return
	case <-t.idleSignal:
		return
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
			Producers:  valset.NewValidatorSet(newSpan.Producers()),
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

	startSprintNumInSpan := t.borConfig.CalculateSprintNumber(lastProducerSelection.StartBlock)
	endSprintNumInSpan := t.borConfig.CalculateSprintNumber(lastProducerSelection.EndBlock)
	numSprintsInSpan := int(endSprintNumInSpan-startSprintNumInSpan) + 1
	producers.IncrementProposerPriority(numSprintsInSpan)
	newProducers := valset.GetUpdatedValidatorSet(producers, newSpan.Producers(), t.logger)
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

	currentSprintNumber := t.borConfig.CalculateSprintNumber(blockNum)
	startSprintNumInSpan := t.borConfig.CalculateSprintNumber(producerSelection.StartBlock)
	incrementsNeeded := int(currentSprintNumber-startSprintNumInSpan) + 1
	producers.IncrementProposerPriority(incrementsNeeded)
	return producers, nil
}
