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
	"errors"
	"math/big"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/builder/epbs/eladapter"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/execution/builder"
)

type staticSlotInputResolver struct {
	input SlotInput
	err   error
	calls int
}

func (r *staticSlotInputResolver) Resolve(context.Context, *cltypes.SignedProposerPreferences) (SlotInput, error) {
	r.calls++
	return r.input, r.err
}

type countingSlotInputFreshness struct {
	calls  int
	failAt int
	err    error
	stale  bool
	onCall func(int)
}

func (f *countingSlotInputFreshness) ValidateCurrent(context.Context, SlotInput) error {
	f.calls++
	if f.onCall != nil {
		f.onCall(f.calls)
	}
	if f.stale || f.calls == f.failAt {
		return f.err
	}
	return nil
}

type stagedLiveAssembler struct {
	delegate      *coordinatorAssembler
	afterAssemble func()
	afterGet      func()
	getCalls      int
}

func (a *stagedLiveAssembler) AssemblePayload(ctx context.Context, parameters *builder.Parameters) (uint64, error) {
	payloadID, err := a.delegate.AssemblePayload(ctx, parameters)
	if err == nil && a.afterAssemble != nil {
		a.afterAssemble()
	}
	return payloadID, err
}

func (a *stagedLiveAssembler) GetPayload(ctx context.Context, payloadID uint64) (*eladapter.AssembledPayload, error) {
	payload, err := a.delegate.GetPayload(ctx, payloadID)
	a.getCalls++
	if err == nil && a.afterGet != nil {
		a.afterGet()
	}
	return payload, err
}

func TestLiveCoordinatorHandlesValidatedPreferences(t *testing.T) {
	config := gloasCoordinatorConfig()
	input := validCoordinatorSlotInput(config)
	assembler := &coordinatorAssembler{
		payloadID: 7,
		payload:   validCoordinatorPayload(&config, input, big.NewInt(1_000_000_000)),
	}
	publisher := new(coordinatorPublisher)
	coordinator := NewCoordinator(&config, new(coordinatorSigner), FixedMarginStrategy{Margin: 1}, assembler, publisher, 1)
	resolver := &staticSlotInputResolver{input: input}
	freshness := new(countingSlotInputFreshness)
	live := NewLiveCoordinator(coordinator, resolver, freshness)

	bid, err := live.HandleValidatedPreferences(t.Context(), input.ValidatedPreferences)

	require.NoError(t, err)
	require.NotNil(t, bid)
	require.Equal(t, 1, publisher.calls)
	require.Equal(t, 4, freshness.calls)
}

func TestLiveCoordinatorRejectsMalformedPreferencesBeforeResolve(t *testing.T) {
	for _, test := range []struct {
		name        string
		preferences *cltypes.SignedProposerPreferences
	}{
		{name: "nil preferences"},
		{name: "nil message", preferences: new(cltypes.SignedProposerPreferences)},
	} {
		t.Run(test.name, func(t *testing.T) {
			resolver := new(staticSlotInputResolver)
			live := NewLiveCoordinator(new(Coordinator), resolver, new(countingSlotInputFreshness))

			_, err := live.HandleValidatedPreferences(t.Context(), test.preferences)

			require.Error(t, err)
			require.Zero(t, resolver.calls)
		})
	}
}

func TestLiveCoordinatorReleasesAuctionWhenInputStalesDuringAssembly(t *testing.T) {
	config := gloasCoordinatorConfig()
	input := validCoordinatorSlotInput(config)
	delegate := &coordinatorAssembler{
		payloadID: 7,
		payload:   validCoordinatorPayload(&config, input, big.NewInt(1_000_000_000)),
	}
	assembler := &stagedLiveAssembler{delegate: delegate}
	publisher := new(coordinatorPublisher)
	coordinator := NewCoordinator(&config, new(coordinatorSigner), FixedMarginStrategy{Margin: 1}, assembler, publisher, 1)
	stale := errors.New("head changed")
	freshness := &countingSlotInputFreshness{err: stale}
	assembler.afterAssemble = func() { freshness.stale = true }
	live := NewLiveCoordinator(coordinator, &staticSlotInputResolver{input: input}, freshness)

	_, err := live.HandleValidatedPreferences(t.Context(), input.ValidatedPreferences)
	require.ErrorIs(t, err, stale)
	require.Zero(t, publisher.calls)
	require.Equal(t, 1, assembler.getCalls)

	freshness.stale = false
	assembler.afterAssemble = nil
	_, err = live.HandleValidatedPreferences(t.Context(), input.ValidatedPreferences)
	require.NoError(t, err)
	require.Equal(t, 1, publisher.calls)
}

func TestLiveCoordinatorReleasesAuctionWhenInputStalesDuringGetPayload(t *testing.T) {
	config := gloasCoordinatorConfig()
	input := validCoordinatorSlotInput(config)
	delegate := &coordinatorAssembler{
		payloadID: 7,
		payload:   validCoordinatorPayload(&config, input, big.NewInt(1_000_000_000)),
	}
	assembler := &stagedLiveAssembler{delegate: delegate}
	publisher := new(coordinatorPublisher)
	coordinator := NewCoordinator(&config, new(coordinatorSigner), FixedMarginStrategy{Margin: 1}, assembler, publisher, 1)
	stale := errors.New("head changed")
	freshness := &countingSlotInputFreshness{err: stale}
	assembler.afterGet = func() { freshness.stale = true }
	live := NewLiveCoordinator(coordinator, &staticSlotInputResolver{input: input}, freshness)

	_, err := live.HandleValidatedPreferences(t.Context(), input.ValidatedPreferences)
	require.ErrorIs(t, err, stale)
	require.Zero(t, publisher.calls)

	freshness.stale = false
	assembler.afterGet = nil
	_, err = live.HandleValidatedPreferences(t.Context(), input.ValidatedPreferences)
	require.NoError(t, err)
	require.Equal(t, 1, publisher.calls)
}

func TestLiveCoordinatorPreservesFreshnessErrorBeforePublish(t *testing.T) {
	config := gloasCoordinatorConfig()
	input := validCoordinatorSlotInput(config)
	assembled := validCoordinatorPayload(&config, input, big.NewInt(1_000_000_000))
	assembler := &coordinatorAssembler{payloadID: 7, payload: assembled}
	publisher := new(coordinatorPublisher)
	coordinator := NewCoordinator(&config, new(coordinatorSigner), FixedMarginStrategy{Margin: 1}, assembler, publisher, 1)
	stale := errors.New("head changed")
	freshness := &countingSlotInputFreshness{failAt: 4, err: stale}
	live := NewLiveCoordinator(coordinator, &staticSlotInputResolver{input: input}, freshness)

	_, err := live.HandleValidatedPreferences(t.Context(), input.ValidatedPreferences)
	require.ErrorIs(t, err, stale)
	require.Zero(t, publisher.calls)
	_, retained, payloadErr := coordinator.Payload(payloadIdentity(input, assembled))
	require.NoError(t, payloadErr)
	require.False(t, retained)

	freshness.failAt = 0
	_, err = live.HandleValidatedPreferences(t.Context(), input.ValidatedPreferences)
	require.NoError(t, err)
	require.Equal(t, 1, publisher.calls)
}

func TestLiveCoordinatorDoesNotPublishWhenContextExpiresAtFinalCheckpoint(t *testing.T) {
	config := gloasCoordinatorConfig()
	input := validCoordinatorSlotInput(config)
	assembler := &coordinatorAssembler{
		payloadID: 7,
		payload:   validCoordinatorPayload(&config, input, big.NewInt(1_000_000_000)),
	}
	publisher := new(coordinatorPublisher)
	coordinator := NewCoordinator(&config, new(coordinatorSigner), FixedMarginStrategy{Margin: 1}, assembler, publisher, 1)
	ctx, cancel := context.WithCancel(t.Context())
	freshness := &countingSlotInputFreshness{
		onCall: func(call int) {
			if call == 4 {
				cancel()
			}
		},
	}
	live := NewLiveCoordinator(coordinator, &staticSlotInputResolver{input: input}, freshness)

	_, err := live.HandleValidatedPreferences(ctx, input.ValidatedPreferences)
	require.ErrorIs(t, err, context.Canceled)
	require.Zero(t, publisher.calls)

	freshness.onCall = nil
	_, err = live.HandleValidatedPreferences(t.Context(), input.ValidatedPreferences)
	require.NoError(t, err)
	require.Equal(t, 1, publisher.calls)
}
