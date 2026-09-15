// Copyright 2026 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

package epbs

import (
	"bytes"
	"context"
	"errors"
	"math/big"
	"time"

	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/common/log/v3"
)

type ShadowPayloadMeasurer interface {
	MeasureValidatedPreferences(context.Context, *cltypes.SignedProposerPreferences, PayloadParentIdentity) (PayloadMeasurement, error)
}

type ShadowValueCurveSample struct {
	Parent      PayloadParentIdentity
	Delay       time.Duration
	LeadTime    time.Duration
	StartedAt   time.Time
	Elapsed     time.Duration
	Measurement PayloadMeasurement
	Err         error
}

type shadowValueCurveRequest struct {
	preferences *cltypes.SignedProposerPreferences
	parent      PayloadParentIdentity
	baseline    PayloadMeasurement
	nextDelay   int
}

type shadowValueCurveRunner struct {
	measurer      ShadowPayloadMeasurer
	clock         SlotClock
	maxPending    int
	retryInterval time.Duration
	baselineDelay time.Duration
	delays        []time.Duration
	newTicker     func(time.Duration) runnerTicker
	now           func() time.Time
	slotTime      func(uint64) time.Time
	submissions   chan shadowValueCurveRequest
	observe       func(ShadowValueCurveSample)
}

func newShadowValueCurveRunner(
	measurer ShadowPayloadMeasurer,
	clock SlotClock,
	maxPending int,
	retryInterval time.Duration,
	baselineDelay time.Duration,
	delays []time.Duration,
	newTicker func(time.Duration) runnerTicker,
	now func() time.Time,
	slotTime func(uint64) time.Time,
) (*shadowValueCurveRunner, error) {
	if isNilDependency(measurer) || isNilDependency(clock) || newTicker == nil || now == nil || slotTime == nil {
		return nil, errors.New("epbs/shadow value curve: missing dependency")
	}
	if maxPending <= 0 {
		return nil, errors.New("epbs/shadow value curve: max pending must be positive")
	}
	if retryInterval < minValidatedPreferencesRetryInterval {
		return nil, errors.New("epbs/shadow value curve: retry interval is too short")
	}
	if len(delays) == 0 {
		return nil, errors.New("epbs/shadow value curve: missing delays")
	}
	ownedDelays := append([]time.Duration(nil), delays...)
	for i, delay := range ownedDelays {
		if delay <= 0 || i > 0 && delay <= ownedDelays[i-1] {
			return nil, errors.New("epbs/shadow value curve: delays must be positive and increasing")
		}
	}
	if baselineDelay <= 0 || baselineDelay >= ownedDelays[0] {
		return nil, errors.New("epbs/shadow value curve: baseline delay must precede shadow delays")
	}
	return &shadowValueCurveRunner{
		measurer: measurer, clock: clock, maxPending: maxPending, retryInterval: retryInterval,
		baselineDelay: baselineDelay, delays: ownedDelays, newTicker: newTicker, now: now, slotTime: slotTime,
		submissions: make(chan shadowValueCurveRequest, maxPending), observe: logShadowValueCurveSample,
	}, nil
}

func (r *shadowValueCurveRunner) Submit(
	preferences *cltypes.SignedProposerPreferences,
	parent PayloadParentIdentity,
	baseline PayloadMeasurement,
) bool {
	if r == nil || preferences == nil || preferences.Message == nil || preferences.Message.ProposalSlot != parent.Slot ||
		baseline.Slot != parent.Slot || baseline.ParentBlockRoot != parent.ParentBlockRoot ||
		baseline.ParentBlockHash != parent.ParentBlockHash || baseline.BlockValueWei == nil ||
		baseline.AssemblyStartedAt.IsZero() || baseline.AssemblyElapsed < 0 {
		return false
	}
	owned, ok := preferences.Clone().(*cltypes.SignedProposerPreferences)
	if !ok || owned == nil || owned.Message == nil {
		return false
	}
	baseline.BlockValueWei = new(big.Int).Set(baseline.BlockValueWei)
	request := shadowValueCurveRequest{
		preferences: owned, parent: parent, baseline: baseline,
	}
	select {
	case r.submissions <- request:
		return true
	default:
		return false
	}
}

func (r *shadowValueCurveRunner) Run(ctx context.Context) error {
	if ctx == nil {
		return errors.New("epbs/shadow value curve: nil context")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	ticker := r.newTicker(r.retryInterval)
	if ticker == nil {
		return errors.New("epbs/shadow value curve: ticker is nil")
	}
	defer ticker.Stop()
	pending := make(map[PayloadParentIdentity]*shadowValueCurveRequest, r.maxPending)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case request := <-r.submissions:
			added := false
			_, exists := pending[request.parent]
			if !exists && len(pending) < r.maxPending {
				owned := request
				pending[request.parent] = &owned
				added = true
			}
			if r.observe != nil {
				sample := ShadowValueCurveSample{
					Parent: request.parent, Delay: r.baselineDelay,
					LeadTime:  r.slotTime(request.parent.Slot).Sub(request.baseline.AssemblyStartedAt),
					StartedAt: request.baseline.AssemblyStartedAt,
					Elapsed:   request.baseline.AssemblyElapsed, Measurement: request.baseline,
				}
				if !added {
					if exists {
						sample.Err = errors.New("epbs/shadow value curve: parent is already pending")
					} else {
						sample.Err = errors.New("epbs/shadow value curve: pending capacity reached")
					}
				}
				r.observe(sample)
			}
			r.runOneDue(ctx, pending)
		case <-ticker.Chan():
			r.runOneDue(ctx, pending)
		}
	}
}

func (r *shadowValueCurveRunner) runOneDue(
	ctx context.Context,
	pending map[PayloadParentIdentity]*shadowValueCurveRequest,
) {
	now := r.now()
	currentSlot := r.clock.GetCurrentSlot()
	var selected *shadowValueCurveRequest
	var scheduled time.Time
	for parent, request := range pending {
		if parent.Slot <= currentSlot || request.nextDelay >= len(r.delays) {
			delete(pending, parent)
			continue
		}
		precedingSlot := parent.Slot - 1
		due := r.slotTime(precedingSlot).Add(r.delays[request.nextDelay])
		if now.Before(due) || selected != nil && !shadowRequestLess(request, due, selected, scheduled) {
			continue
		}
		selected = request
		scheduled = due
	}
	if selected == nil {
		return
	}
	delay := r.delays[selected.nextDelay]
	startedAt := r.now()
	remaining := r.slotTime(selected.parent.Slot).Sub(startedAt)
	if remaining <= 0 {
		delete(pending, selected.parent)
		return
	}
	measureCtx, cancel := context.WithTimeout(ctx, remaining)
	measurement, err := r.measurer.MeasureValidatedPreferences(measureCtx, selected.preferences, selected.parent)
	cancel()
	if err == nil && measurement.BlockValueWei == nil {
		err = errors.New("epbs/shadow value curve: measurement has no block value")
	}
	finishedAt := r.now()
	elapsed := max(finishedAt.Sub(startedAt), 0)
	if err == nil && measurement.AssemblyStartedAt.IsZero() {
		err = errors.New("epbs/shadow value curve: measurement has no assembly start")
	}
	if err == nil {
		startedAt = measurement.AssemblyStartedAt
		elapsed = measurement.AssemblyElapsed
		remaining = r.slotTime(selected.parent.Slot).Sub(startedAt)
	}
	sample := ShadowValueCurveSample{
		Parent: selected.parent, Delay: delay, LeadTime: remaining, StartedAt: startedAt, Elapsed: elapsed,
		Measurement: measurement, Err: err,
	}
	selected.nextDelay++
	if errors.Is(err, ErrPayloadParentChanged) || selected.nextDelay >= len(r.delays) {
		delete(pending, selected.parent)
	}
	if r.observe != nil {
		r.observe(sample)
	}
}

func shadowRequestLess(
	left *shadowValueCurveRequest,
	leftDue time.Time,
	right *shadowValueCurveRequest,
	rightDue time.Time,
) bool {
	if !leftDue.Equal(rightDue) {
		return leftDue.Before(rightDue)
	}
	if left.parent.Slot != right.parent.Slot {
		return left.parent.Slot < right.parent.Slot
	}
	if cmp := bytes.Compare(left.parent.ParentBlockRoot[:], right.parent.ParentBlockRoot[:]); cmp != 0 {
		return cmp < 0
	}
	return bytes.Compare(left.parent.ParentBlockHash[:], right.parent.ParentBlockHash[:]) < 0
}

func logShadowValueCurveSample(sample ShadowValueCurveSample) {
	fields := []any{
		"slot", sample.Parent.Slot,
		"shadowDelay", sample.Delay,
		"leadTime", sample.LeadTime,
		"parentBlockRoot", sample.Parent.ParentBlockRoot,
		"parentBlockHash", sample.Parent.ParentBlockHash,
		"elapsed", sample.Elapsed,
	}
	if sample.Err != nil {
		log.Info("Embedded builder shadow payload failed", append(fields, "err", sample.Err)...)
		return
	}
	valueGwei := new(big.Int).Quo(new(big.Int).Set(sample.Measurement.BlockValueWei), big.NewInt(weiPerGwei))
	log.Info("Embedded builder shadow payload", append(fields,
		"blockHash", sample.Measurement.BlockHash,
		"blockValueWei", sample.Measurement.BlockValueWei,
		"blockValueGwei", valueGwei,
		"transactions", sample.Measurement.TransactionCount,
		"gasUsed", sample.Measurement.GasUsed,
		"blobs", sample.Measurement.BlobCount,
	)...)
}
