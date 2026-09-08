// Copyright 2026 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

package epbs

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/phase1/execution_client"
	"github.com/erigontech/erigon/cl/phase1/forkchoice"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
	"github.com/erigontech/erigon/common"
)

func TestLiveSlotInputResolverRejectsUnavailableFullParentEnvelope(t *testing.T) {
	tests := []struct {
		name      string
		configure func(*cltypes.SignedExecutionPayloadEnvelope, *resolverForkchoice)
	}{
		{
			name: "missing envelope",
			configure: func(_ *cltypes.SignedExecutionPayloadEnvelope, fc *resolverForkchoice) {
				fc.envelope = nil
			},
		},
		{
			name: "read failure",
			configure: func(_ *cltypes.SignedExecutionPayloadEnvelope, fc *resolverForkchoice) {
				fc.envelopeErr = errors.New("read failed")
			},
		},
		{
			name: "missing execution requests",
			configure: func(envelope *cltypes.SignedExecutionPayloadEnvelope, _ *resolverForkchoice) {
				envelope.Message.ExecutionRequests = nil
			},
		},
		{
			name: "incomplete execution requests",
			configure: func(envelope *cltypes.SignedExecutionPayloadEnvelope, _ *resolverForkchoice) {
				envelope.Message.ExecutionRequests.BuilderExits = nil
			},
		},
		{
			name: "identity mismatch",
			configure: func(envelope *cltypes.SignedExecutionPayloadEnvelope, _ *resolverForkchoice) {
				envelope.Message.BeaconBlockRoot = common.HexToHash("0xdead")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg, headState, preferences, headRoot, _, _ := liveResolverFixture(t)
			parentBid := headState.GetLatestExecutionPayloadBid()
			envelope := liveResolverEnvelope(t, &cfg, headState, headRoot)
			fc := &resolverForkchoice{
				headNode:    forkchoice.ForkChoiceNode{Root: headRoot, PayloadStatus: cltypes.PayloadStatusFull},
				envelope:    envelope,
				hasEnvelope: true,
				buildOnFull: true,
				gasLimits:   map[common.Hash]uint64{parentBid.BlockHash: 30_000_000},
				recentStatuses: map[common.Hash]execution_client.PayloadStatus{
					parentBid.BlockHash: execution_client.PayloadStatusValidated,
				},
			}
			tt.configure(envelope, fc)
			clock := eth_clock.NewMockEthereumClock(gomock.NewController(t))
			clock.EXPECT().GetCurrentSlot().Return(preferences.Message.ProposalSlot).AnyTimes()
			clock.EXPECT().GenesisValidatorsRoot().Return(headState.GenesisValidatorsRoot()).AnyTimes()
			resolver := NewLiveSlotInputResolver(
				&cfg,
				new(coordinatorSigner),
				clock,
				&resolverHeadSource{state: headState, root: headRoot, identitySlot: headState.Slot()},
				fc,
			)

			_, err := resolver.Resolve(t.Context(), preferences)
			require.Error(t, err)
		})
	}
}

func TestLiveSlotInputResolverRejectsSameRootFreshnessChanges(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*resolverForkchoice, common.Hash)
	}{
		{
			name: "payload status",
			mutate: func(fc *resolverForkchoice, _ common.Hash) {
				fc.headNode.PayloadStatus = cltypes.PayloadStatusEmpty
			},
		},
		{
			name: "envelope availability",
			mutate: func(fc *resolverForkchoice, _ common.Hash) {
				fc.hasEnvelope = false
			},
		},
		{
			name: "build on full decision",
			mutate: func(fc *resolverForkchoice, _ common.Hash) {
				fc.buildOnFull = false
			},
		},
		{
			name: "gas limit",
			mutate: func(fc *resolverForkchoice, parentHash common.Hash) {
				fc.gasLimits[parentHash]++
			},
		},
		{
			name: "recent status",
			mutate: func(fc *resolverForkchoice, parentHash common.Hash) {
				fc.recentStatuses[parentHash] = execution_client.PayloadStatusInvalidated
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg, headState, preferences, headRoot, _, _ := liveResolverFixture(t)
			parentBid := headState.GetLatestExecutionPayloadBid()
			envelope := liveResolverEnvelope(t, &cfg, headState, headRoot)
			fc := &resolverForkchoice{
				headNode:    forkchoice.ForkChoiceNode{Root: headRoot, PayloadStatus: cltypes.PayloadStatusFull},
				envelope:    envelope,
				hasEnvelope: true,
				buildOnFull: true,
				gasLimits:   map[common.Hash]uint64{parentBid.BlockHash: 30_000_000},
				recentStatuses: map[common.Hash]execution_client.PayloadStatus{
					parentBid.BlockHash: execution_client.PayloadStatusValidated,
				},
			}
			clock := eth_clock.NewMockEthereumClock(gomock.NewController(t))
			clock.EXPECT().GetCurrentSlot().Return(preferences.Message.ProposalSlot).AnyTimes()
			clock.EXPECT().GenesisValidatorsRoot().Return(headState.GenesisValidatorsRoot()).AnyTimes()
			resolver := NewLiveSlotInputResolver(
				&cfg,
				new(coordinatorSigner),
				clock,
				&resolverHeadSource{state: headState, root: headRoot, identitySlot: headState.Slot()},
				fc,
			)

			input, err := resolver.Resolve(t.Context(), preferences)
			require.NoError(t, err)
			tt.mutate(fc, parentBid.BlockHash)

			err = resolver.ValidateCurrent(t.Context(), input)
			require.ErrorIs(t, err, ErrSlotInputStale)
		})
	}
}

func TestLiveSlotInputResolverKeepsFreshParentAcrossEquivalentPayloadStatusTransition(t *testing.T) {
	cfg, headState, preferences, headRoot, parentHash, _ := liveResolverFixture(t)
	fc := &resolverForkchoice{
		headNode:  forkchoice.ForkChoiceNode{Root: headRoot, PayloadStatus: cltypes.PayloadStatusPending},
		gasLimits: map[common.Hash]uint64{parentHash: 30_000_000},
		recentStatuses: map[common.Hash]execution_client.PayloadStatus{
			parentHash: execution_client.PayloadStatusValidated,
		},
	}
	clock := eth_clock.NewMockEthereumClock(gomock.NewController(t))
	clock.EXPECT().GetCurrentSlot().Return(preferences.Message.ProposalSlot).AnyTimes()
	clock.EXPECT().GenesisValidatorsRoot().Return(headState.GenesisValidatorsRoot()).AnyTimes()
	resolver := NewLiveSlotInputResolver(
		&cfg,
		new(coordinatorSigner),
		clock,
		&resolverHeadSource{state: headState, root: headRoot, identitySlot: headState.Slot()},
		fc,
	)
	input, err := resolver.Resolve(t.Context(), preferences)
	require.NoError(t, err)

	fc.headNode.PayloadStatus = cltypes.PayloadStatusFull
	fc.hasEnvelope = true
	fc.buildOnFull = false

	require.NoError(t, resolver.ValidateCurrent(t.Context(), input))
}

func TestLiveSlotInputResolverRejectsPayloadStatusTransitionChangingParent(t *testing.T) {
	cfg, headState, preferences, headRoot, parentHash, _ := liveResolverFixture(t)
	fc := &resolverForkchoice{
		headNode:  forkchoice.ForkChoiceNode{Root: headRoot, PayloadStatus: cltypes.PayloadStatusPending},
		gasLimits: map[common.Hash]uint64{parentHash: 30_000_000},
		recentStatuses: map[common.Hash]execution_client.PayloadStatus{
			parentHash: execution_client.PayloadStatusValidated,
		},
	}
	clock := eth_clock.NewMockEthereumClock(gomock.NewController(t))
	clock.EXPECT().GetCurrentSlot().Return(preferences.Message.ProposalSlot).AnyTimes()
	clock.EXPECT().GenesisValidatorsRoot().Return(headState.GenesisValidatorsRoot()).AnyTimes()
	resolver := NewLiveSlotInputResolver(
		&cfg,
		new(coordinatorSigner),
		clock,
		&resolverHeadSource{state: headState, root: headRoot, identitySlot: headState.Slot()},
		fc,
	)
	input, err := resolver.Resolve(t.Context(), preferences)
	require.NoError(t, err)

	fc.headNode.PayloadStatus = cltypes.PayloadStatusFull
	fc.hasEnvelope = true
	fc.buildOnFull = true

	require.ErrorIs(t, resolver.ValidateCurrent(t.Context(), input), ErrSlotInputStale)
}
