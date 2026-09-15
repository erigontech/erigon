// Copyright 2026 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

package epbs

import (
	"context"
	"math/big"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/common"
)

type recordingShadowMeasurer struct {
	mu      sync.Mutex
	calls   []PayloadParentIdentity
	results []PayloadMeasurement
	errors  []error
	called  chan PayloadParentIdentity
}

func (m *recordingShadowMeasurer) MeasureValidatedPreferences(
	_ context.Context,
	_ *cltypes.SignedProposerPreferences,
	parent PayloadParentIdentity,
) (PayloadMeasurement, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls = append(m.calls, parent)
	if m.called != nil {
		m.called <- parent
	}
	var result PayloadMeasurement
	if len(m.results) > 0 {
		result = m.results[0]
		m.results = m.results[1:]
	}
	var err error
	if len(m.errors) > 0 {
		err = m.errors[0]
		m.errors = m.errors[1:]
	}
	return result, err
}

func (m *recordingShadowMeasurer) snapshot() []PayloadParentIdentity {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]PayloadParentIdentity(nil), m.calls...)
}

func TestShadowValueCurveRunnerMeasuresPublishedParentAtConfiguredDelays(t *testing.T) {
	clock := &manualRunnerClock{slot: 10}
	ticker := &manualRunnerTicker{ticks: make(chan time.Time, 4)}
	measurer := &recordingShadowMeasurer{called: make(chan PayloadParentIdentity, 2), results: []PayloadMeasurement{
		{Slot: 11, BlockValueWei: big.NewInt(1), AssemblyStartedAt: clock.GetSlotTime(10).Add(7 * time.Second), AssemblyElapsed: time.Millisecond},
		{Slot: 11, BlockValueWei: big.NewInt(2), AssemblyStartedAt: clock.GetSlotTime(10).Add(9 * time.Second), AssemblyElapsed: time.Millisecond},
	}}
	var nowMu sync.Mutex
	now := clock.GetSlotTime(10).Add(6*time.Second + 999*time.Millisecond)
	baselineStartedAt := clock.GetSlotTime(10).Add(5 * time.Second)
	nowFn := func() time.Time {
		nowMu.Lock()
		defer nowMu.Unlock()
		return now
	}
	samples := make(chan ShadowValueCurveSample, 4)
	runner, err := newShadowValueCurveRunner(
		measurer, clock, 4, time.Second, 5*time.Second, []time.Duration{7 * time.Second, 9 * time.Second},
		func(time.Duration) runnerTicker { return ticker }, nowFn, clock.GetSlotTime,
	)
	require.NoError(t, err)
	runner.observe = func(sample ShadowValueCurveSample) { samples <- sample }

	preferences := runnerPreferences(11, common.HexToHash("0x11"))
	parent := PayloadParentIdentity{
		Slot: 11, ParentBlockRoot: common.HexToHash("0x22"), ParentBlockHash: common.HexToHash("0x33"),
	}
	require.True(t, runner.Submit(preferences, parent, PayloadMeasurement{
		Slot: 11, ParentBlockRoot: parent.ParentBlockRoot, ParentBlockHash: parent.ParentBlockHash,
		BlockValueWei: big.NewInt(3), AssemblyStartedAt: baselineStartedAt, AssemblyElapsed: time.Millisecond,
	}))
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- runner.Run(ctx) }()
	baseline := <-samples
	require.Equal(t, 5*time.Second, baseline.Delay)
	require.Equal(t, big.NewInt(3), baseline.Measurement.BlockValueWei)
	require.Equal(t, baselineStartedAt, baseline.StartedAt)

	ticker.tick()
	require.Empty(t, measurer.snapshot())
	nowMu.Lock()
	now = clock.GetSlotTime(10).Add(7 * time.Second)
	nowMu.Unlock()
	ticker.tick()
	require.Equal(t, parent, <-measurer.called)
	first := <-samples
	require.Equal(t, 7*time.Second, first.Delay)
	require.NoError(t, first.Err)

	nowMu.Lock()
	now = clock.GetSlotTime(10).Add(9 * time.Second)
	nowMu.Unlock()
	ticker.tick()
	require.Equal(t, parent, <-measurer.called)
	second := <-samples
	require.Equal(t, 9*time.Second, second.Delay)
	require.NoError(t, second.Err)
	require.Equal(t, []PayloadParentIdentity{parent, parent}, measurer.snapshot())
	require.True(t, runner.Submit(preferences, parent, PayloadMeasurement{
		Slot: 11, ParentBlockRoot: parent.ParentBlockRoot, ParentBlockHash: parent.ParentBlockHash,
		BlockValueWei: big.NewInt(4), AssemblyStartedAt: baselineStartedAt, AssemblyElapsed: time.Millisecond,
	}))
	select {
	case repeated := <-samples:
		t.Fatalf("duplicate baseline was observed: %+v", repeated)
	case <-time.After(20 * time.Millisecond):
	}
	require.Equal(t, []PayloadParentIdentity{parent, parent}, measurer.snapshot())

	cancel()
	require.ErrorIs(t, <-done, context.Canceled)
}

func TestShadowValueCurveRunnerDropsRemainingSamplesWhenParentChanges(t *testing.T) {
	clock := &manualRunnerClock{slot: 10}
	measurer := &recordingShadowMeasurer{errors: []error{ErrPayloadParentChanged}}
	now := clock.GetSlotTime(10).Add(9 * time.Second)
	runner, err := newShadowValueCurveRunner(
		measurer, clock, 2, time.Second, 5*time.Second, []time.Duration{7 * time.Second, 9 * time.Second},
		func(time.Duration) runnerTicker { return &manualRunnerTicker{} }, func() time.Time { return now }, clock.GetSlotTime,
	)
	require.NoError(t, err)
	request := &shadowValueCurveRequest{parent: PayloadParentIdentity{
		Slot: 11, ParentBlockRoot: common.HexToHash("0x22"), ParentBlockHash: common.HexToHash("0x33"),
	}, preferences: runnerPreferences(11, common.HexToHash("0x11"))}
	pending := map[PayloadParentIdentity]*shadowValueCurveRequest{request.parent: request}

	runner.runOneDue(t.Context(), pending)
	require.Len(t, pending, 1)
	runner.runOneDue(t.Context(), pending)
	require.Len(t, measurer.snapshot(), 1)
	clock.slot = 11
	runner.runOneDue(t.Context(), pending)
	require.Empty(t, pending)

}

func TestShadowValueCurveRunnerReleasesAdmissionWhenTargetTimeArrivesBeforeClockAdvances(t *testing.T) {
	clock := &manualRunnerClock{slot: 10}
	now := clock.GetSlotTime(11)
	runner, err := newShadowValueCurveRunner(
		new(recordingShadowMeasurer), clock, 1, time.Second, 5*time.Second, []time.Duration{7 * time.Second},
		func(time.Duration) runnerTicker { return &manualRunnerTicker{} }, func() time.Time { return now }, clock.GetSlotTime,
	)
	require.NoError(t, err)
	first := PayloadParentIdentity{Slot: 11, ParentBlockRoot: common.Hash{0x01}, ParentBlockHash: common.Hash{0x02}}
	runner.admitted[first] = struct{}{}
	pending := map[PayloadParentIdentity]*shadowValueCurveRequest{
		first: {parent: first, preferences: runnerPreferences(11, common.Hash{0x11})},
	}

	runner.runOneDue(t.Context(), pending)
	require.Empty(t, pending)
	second := PayloadParentIdentity{Slot: 12, ParentBlockRoot: common.Hash{0x03}, ParentBlockHash: common.Hash{0x04}}
	require.True(t, runner.Submit(runnerPreferences(12, common.Hash{0x12}), second, PayloadMeasurement{
		Slot: 12, ParentBlockRoot: second.ParentBlockRoot, ParentBlockHash: second.ParentBlockHash,
		BlockValueWei: big.NewInt(1), AssemblyStartedAt: now,
	}))
}

type blockingShadowMeasurer struct {
	started chan struct{}
}

func (m *blockingShadowMeasurer) MeasureValidatedPreferences(
	ctx context.Context,
	_ *cltypes.SignedProposerPreferences,
	_ PayloadParentIdentity,
) (PayloadMeasurement, error) {
	close(m.started)
	<-ctx.Done()
	return PayloadMeasurement{}, ctx.Err()
}

func TestShadowValueCurveRunnerCancellationStopsActiveMeasurement(t *testing.T) {
	clock := &manualRunnerClock{slot: 10}
	ticker := &manualRunnerTicker{ticks: make(chan time.Time, 1)}
	measurer := &blockingShadowMeasurer{started: make(chan struct{})}
	now := clock.GetSlotTime(10).Add(7 * time.Second)
	runner, err := newShadowValueCurveRunner(
		measurer, clock, 1, time.Second, 5*time.Second, []time.Duration{7 * time.Second},
		func(time.Duration) runnerTicker { return ticker }, func() time.Time { return now }, clock.GetSlotTime,
	)
	require.NoError(t, err)
	parent := PayloadParentIdentity{
		Slot: 11, ParentBlockRoot: common.HexToHash("0x22"), ParentBlockHash: common.HexToHash("0x33"),
	}
	require.True(t, runner.Submit(
		runnerPreferences(11, common.HexToHash("0x11")), parent,
		PayloadMeasurement{
			Slot: 11, ParentBlockRoot: parent.ParentBlockRoot, ParentBlockHash: parent.ParentBlockHash,
			BlockValueWei: big.NewInt(1), AssemblyStartedAt: now,
		},
	))
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- runner.Run(ctx) }()
	<-measurer.started

	cancel()
	require.ErrorIs(t, <-done, context.Canceled)
}

func TestShadowValueCurveRunnerSubmissionCapacityIsBounded(t *testing.T) {
	clock := &manualRunnerClock{slot: 10}
	now := clock.GetSlotTime(10).Add(6 * time.Second)
	runner, err := newShadowValueCurveRunner(
		new(recordingShadowMeasurer), clock, 1, time.Second, 5*time.Second, []time.Duration{7 * time.Second},
		func(time.Duration) runnerTicker { return &manualRunnerTicker{} }, func() time.Time { return now }, clock.GetSlotTime,
	)
	require.NoError(t, err)
	measurement := PayloadMeasurement{
		Slot: 11, ParentBlockRoot: common.HexToHash("0x22"), ParentBlockHash: common.HexToHash("0x33"),
		BlockValueWei: big.NewInt(1), AssemblyStartedAt: now,
	}
	require.True(t, runner.Submit(runnerPreferences(11, common.HexToHash("0x11")), PayloadParentIdentity{
		Slot: 11, ParentBlockRoot: measurement.ParentBlockRoot, ParentBlockHash: measurement.ParentBlockHash,
	}, measurement))
	measurement.ParentBlockRoot = common.HexToHash("0x44")
	measurement.ParentBlockHash = common.HexToHash("0x55")
	require.False(t, runner.Submit(runnerPreferences(11, common.HexToHash("0x12")), PayloadParentIdentity{
		Slot: 11, ParentBlockRoot: measurement.ParentBlockRoot, ParentBlockHash: measurement.ParentBlockHash,
	}, measurement))
}

func TestShadowValueCurveRunnerDuplicateSubmissionDoesNotConsumeCapacity(t *testing.T) {
	clock := &manualRunnerClock{slot: 10}
	now := clock.GetSlotTime(10).Add(6 * time.Second)
	runner, err := newShadowValueCurveRunner(
		new(recordingShadowMeasurer), clock, 2, time.Second, 5*time.Second, []time.Duration{7 * time.Second},
		func(time.Duration) runnerTicker { return &manualRunnerTicker{} }, func() time.Time { return now }, clock.GetSlotTime,
	)
	require.NoError(t, err)
	measurement := func(parent PayloadParentIdentity) PayloadMeasurement {
		return PayloadMeasurement{
			Slot: parent.Slot, ParentBlockRoot: parent.ParentBlockRoot, ParentBlockHash: parent.ParentBlockHash,
			BlockValueWei: big.NewInt(1), AssemblyStartedAt: now,
		}
	}
	first := PayloadParentIdentity{Slot: 11, ParentBlockRoot: common.Hash{0x01}, ParentBlockHash: common.Hash{0x02}}
	second := PayloadParentIdentity{Slot: 11, ParentBlockRoot: common.Hash{0x03}, ParentBlockHash: common.Hash{0x04}}

	require.True(t, runner.Submit(runnerPreferences(11, common.Hash{0x11}), first, measurement(first)))
	for range 10 {
		require.True(t, runner.Submit(runnerPreferences(11, common.Hash{0x11}), first, measurement(first)))
	}
	require.True(t, runner.Submit(runnerPreferences(11, common.Hash{0x12}), second, measurement(second)))
}

func TestShadowValueCurveRunnerIgnoresExpiredSubmission(t *testing.T) {
	clock := &manualRunnerClock{slot: 10}
	now := clock.GetSlotTime(11)
	runner, err := newShadowValueCurveRunner(
		new(recordingShadowMeasurer), clock, 1, time.Second, 5*time.Second, []time.Duration{7 * time.Second},
		func(time.Duration) runnerTicker { return &manualRunnerTicker{} }, func() time.Time { return now }, clock.GetSlotTime,
	)
	require.NoError(t, err)
	expired := PayloadParentIdentity{Slot: 11, ParentBlockRoot: common.Hash{0x01}, ParentBlockHash: common.Hash{0x02}}
	require.True(t, runner.Submit(runnerPreferences(11, common.Hash{0x11}), expired, PayloadMeasurement{
		Slot: 11, ParentBlockRoot: expired.ParentBlockRoot, ParentBlockHash: expired.ParentBlockHash,
		BlockValueWei: big.NewInt(1), AssemblyStartedAt: now.Add(-time.Second),
	}))
	require.Empty(t, runner.submissions)

	future := PayloadParentIdentity{Slot: 12, ParentBlockRoot: common.Hash{0x03}, ParentBlockHash: common.Hash{0x04}}
	require.True(t, runner.Submit(runnerPreferences(12, common.Hash{0x12}), future, PayloadMeasurement{
		Slot: 12, ParentBlockRoot: future.ParentBlockRoot, ParentBlockHash: future.ParentBlockHash,
		BlockValueWei: big.NewInt(1), AssemblyStartedAt: now,
	}))
}

func TestShadowValueCurveRunnerDropsSubmissionThatExpiresBeforeConsumption(t *testing.T) {
	clock := &manualRunnerClock{slot: 10}
	now := clock.GetSlotTime(10).Add(6 * time.Second)
	runner, err := newShadowValueCurveRunner(
		new(recordingShadowMeasurer), clock, 1, time.Second, 5*time.Second, []time.Duration{7 * time.Second},
		func(time.Duration) runnerTicker { return &manualRunnerTicker{} }, func() time.Time { return now }, clock.GetSlotTime,
	)
	require.NoError(t, err)
	samples := make(chan ShadowValueCurveSample, 1)
	runner.observe = func(sample ShadowValueCurveSample) { samples <- sample }
	parent := PayloadParentIdentity{Slot: 11, ParentBlockRoot: common.Hash{0x01}, ParentBlockHash: common.Hash{0x02}}
	require.True(t, runner.Submit(runnerPreferences(11, common.Hash{0x11}), parent, PayloadMeasurement{
		Slot: 11, ParentBlockRoot: parent.ParentBlockRoot, ParentBlockHash: parent.ParentBlockHash,
		BlockValueWei: big.NewInt(1), AssemblyStartedAt: now,
	}))
	now = clock.GetSlotTime(11)
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- runner.Run(ctx) }()

	select {
	case sample := <-samples:
		t.Fatalf("expired baseline was observed: %+v", sample)
	case <-time.After(20 * time.Millisecond):
	}
	runner.admissionMu.Lock()
	require.Empty(t, runner.admitted)
	runner.admissionMu.Unlock()
	cancel()
	require.ErrorIs(t, <-done, context.Canceled)
}

func TestShadowValueCurveRunnerObservesRejectedSubmission(t *testing.T) {
	clock := &manualRunnerClock{slot: 10}
	now := clock.GetSlotTime(10).Add(6 * time.Second)
	runner, err := newShadowValueCurveRunner(
		new(recordingShadowMeasurer), clock, 1, time.Second, 5*time.Second, []time.Duration{7 * time.Second},
		func(time.Duration) runnerTicker { return &manualRunnerTicker{} }, func() time.Time { return now }, clock.GetSlotTime,
	)
	require.NoError(t, err)
	samples := make(chan ShadowValueCurveSample, 3)
	runner.observe = func(sample ShadowValueCurveSample) { samples <- sample }
	measurement := func(root, hash common.Hash) PayloadMeasurement {
		return PayloadMeasurement{
			Slot: 11, ParentBlockRoot: root, ParentBlockHash: hash,
			BlockValueWei: big.NewInt(1), AssemblyStartedAt: now.Add(-time.Second),
		}
	}
	first := PayloadParentIdentity{Slot: 11, ParentBlockRoot: common.Hash{0x01}, ParentBlockHash: common.Hash{0x02}}
	second := PayloadParentIdentity{Slot: 11, ParentBlockRoot: common.Hash{0x03}, ParentBlockHash: common.Hash{0x04}}

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- runner.Run(ctx) }()
	require.True(t, runner.Submit(runnerPreferences(11, common.Hash{0x11}), first, measurement(first.ParentBlockRoot, first.ParentBlockHash)))
	require.NoError(t, (<-samples).Err)
	require.False(t, runner.Submit(runnerPreferences(11, common.Hash{0x12}), second, measurement(second.ParentBlockRoot, second.ParentBlockHash)))
	select {
	case sample := <-samples:
		require.Equal(t, second, sample.Parent)
		require.ErrorContains(t, sample.Err, "capacity")
	case <-time.After(time.Second):
		t.Fatal("rejected submission was not observed")
	}
	cancel()
	require.ErrorIs(t, <-done, context.Canceled)
}
