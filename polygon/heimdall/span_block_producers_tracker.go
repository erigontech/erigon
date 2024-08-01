package heimdall

import (
	"context"
	"errors"
	"fmt"

	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/polygon/bor/borcfg"
	"github.com/erigontech/erigon/polygon/bor/valset"
	"github.com/erigontech/erigon/polygon/polygoncommon"
)

type spanBlockProducersTracker struct {
	logger    log.Logger
	borConfig *borcfg.BorConfig
	store     EntityStore[*SpanBlockProducerSelection]
	newSpans  chan *Span
	idle      *polygoncommon.EventNotifier
	wake      *polygoncommon.EventNotifier
}

func (t spanBlockProducersTracker) Run(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case newSpan := <-t.newSpans:
			t.idle.Reset()
			t.wake.Reset()
			err := t.ObserveSpan(ctx, newSpan)
			if err != nil {
				return err
			}
		default:
			t.idle.SetAndBroadcast()
			t.wake.Wait(ctx)
		}
	}
}

func (t spanBlockProducersTracker) Synchronize(ctx context.Context) {
	t.idle.Wait(ctx)
}

func (t spanBlockProducersTracker) ObserveSpanAsync(span *Span) {
	t.wake.SetAndBroadcast()
	t.newSpans <- span
}

func (t spanBlockProducersTracker) ObserveSpan(ctx context.Context, newSpan *Span) error {
	t.logger.Debug(heimdallLogPrefix("block producers tracker observing span"), "id", newSpan.Id)

	lastProducerSelection, ok, err := t.store.GetLastEntity(ctx)
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
			Producers:  newSpan.Producers(),
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

	startSprintNumInSpan := t.borConfig.CalculateSprintNumber(lastProducerSelection.StartBlock)
	endSprintNumInSpan := t.borConfig.CalculateSprintNumber(lastProducerSelection.EndBlock)
	numSprintsInSpan := int(endSprintNumInSpan - startSprintNumInSpan)
	validatorSet := valset.NewValidatorSet(lastProducerSelection.Producers)
	validatorSet.IncrementProposerPriority(numSprintsInSpan)
	newValidatorSet := valset.GetUpdatedValidatorSet(validatorSet, newSpan.Producers(), t.logger)
	newProducerSelection := &SpanBlockProducerSelection{
		SpanId:     newSpan.Id,
		StartBlock: newSpan.StartBlock,
		EndBlock:   newSpan.EndBlock,
		Producers:  newValidatorSet.Validators,
	}

	err = t.store.PutEntity(ctx, uint64(newProducerSelection.SpanId), newProducerSelection)
	if err != nil {
		return err
	}

	return nil
}

func (t spanBlockProducersTracker) Producers(ctx context.Context, blockNum uint64) (*valset.ValidatorSet, error) {
	spanId := SpanIdAt(blockNum)
	producerSelection, ok, err := t.store.GetEntity(ctx, uint64(spanId))
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, errors.New("no producers found for block num")
	}

	startSprintNumInSpan := t.borConfig.CalculateSprintNumber(uint64(producerSelection.SpanId))
	blockSprintNum := t.borConfig.CalculateSprintNumber(blockNum)
	numSprints := int(blockSprintNum - startSprintNumInSpan)
	validatorSet := valset.NewValidatorSet(producerSelection.Producers)
	validatorSet.IncrementProposerPriority(numSprints)
	return validatorSet, nil
}
