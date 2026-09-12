// Copyright 2026 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

package epbs

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/phase1/execution_client"
	"github.com/erigontech/erigon/cl/phase1/forkchoice"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
	"github.com/erigontech/erigon/common"
)

func TestLiveSlotInputResolverNonBuildingHeadsUseCachedParent(t *testing.T) {
	tests := []struct {
		name            string
		payloadStatus   cltypes.PayloadStatus
		hasEnvelope     bool
		shouldBuildFull bool
	}{
		{name: "pending", payloadStatus: cltypes.PayloadStatusPending},
		{name: "full without build-on-full", payloadStatus: cltypes.PayloadStatusFull, hasEnvelope: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg, headState, preferences, headRoot, parentHash, _ := liveResolverFixture(t)
			resolver := matrixResolver(t, &cfg, headState, headRoot, preferences.Message.ProposalSlot, &resolverForkchoice{
				headNode:    forkchoice.ForkChoiceNode{Root: headRoot, PayloadStatus: tt.payloadStatus},
				hasEnvelope: tt.hasEnvelope,
				buildOnFull: tt.shouldBuildFull,
				gasLimits:   map[common.Hash]uint64{parentHash: 30_000_000},
				recentStatuses: map[common.Hash]execution_client.PayloadStatus{
					parentHash: execution_client.PayloadStatusValidated,
				},
			})

			input, err := resolver.Resolve(t.Context(), preferences)
			require.NoError(t, err)
			require.Equal(t, parentHash, input.ParentBlockHash)
			require.Len(t, input.Withdrawals, 1)
			require.Equal(t, uint64(11), uint64(input.Withdrawals[0].Amount))

			headState.GetPayloadExpectedWithdrawals().Get(0).Amount++
			require.Equal(t, uint64(11), uint64(input.Withdrawals[0].Amount))
		})
	}
}

func TestLiveSlotInputResolverRejectsExecutionRequestsRootMismatch(t *testing.T) {
	cfg, headState, preferences, headRoot, _, _ := liveResolverFixture(t)
	envelope := liveResolverEnvelope(t, &cfg, headState, headRoot)
	parentBid := headState.GetLatestExecutionPayloadBid()
	parentBid.ExecutionRequestsRoot[0] ^= 1
	resolver := matrixResolver(t, &cfg, headState, headRoot, preferences.Message.ProposalSlot, &resolverForkchoice{
		headNode:    forkchoice.ForkChoiceNode{Root: headRoot, PayloadStatus: cltypes.PayloadStatusFull},
		envelope:    envelope,
		hasEnvelope: true,
		buildOnFull: true,
		gasLimits:   map[common.Hash]uint64{parentBid.BlockHash: 30_000_000},
		recentStatuses: map[common.Hash]execution_client.PayloadStatus{
			parentBid.BlockHash: execution_client.PayloadStatusValidated,
		},
	})

	_, err := resolver.Resolve(t.Context(), preferences)
	require.ErrorContains(t, err, "execution requests root mismatch")
}

func TestLiveSlotInputResolverRejectsExhaustedOrOverflowingCollateral(t *testing.T) {
	tests := []struct {
		name        string
		amounts     []uint64
		balance     func(clparams.BeaconChainConfig) uint64
		errorSubstr string
	}{
		{
			name:        "exact reserved collateral",
			amounts:     []uint64{25},
			balance:     func(cfg clparams.BeaconChainConfig) uint64 { return cfg.MinDepositAmount + 25 },
			errorSubstr: "no available collateral",
		},
		{
			name:        "pending liability overflow",
			amounts:     []uint64{math.MaxUint64, 1},
			balance:     func(clparams.BeaconChainConfig) uint64 { return math.MaxUint64 },
			errorSubstr: "pending builder collateral overflow",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg, headState, preferences, headRoot, parentHash, _ := liveResolverFixture(t)
			pending := solid.NewStaticListSSZ[*cltypes.BuilderPendingWithdrawal](
				int(cfg.BuilderPendingWithdrawalsLimit), new(cltypes.BuilderPendingWithdrawal).EncodingSizeSSZ(),
			)
			for _, amount := range tt.amounts {
				pending.Append(&cltypes.BuilderPendingWithdrawal{BuilderIndex: 0, Amount: amount})
			}
			headState.SetBuilderPendingWithdrawals(pending)
			headState.GetBuilders().Get(0).Balance = tt.balance(cfg)
			resolver := matrixResolver(t, &cfg, headState, headRoot, preferences.Message.ProposalSlot, &resolverForkchoice{
				headNode:  forkchoice.ForkChoiceNode{Root: headRoot, PayloadStatus: cltypes.PayloadStatusEmpty},
				gasLimits: map[common.Hash]uint64{parentHash: 30_000_000},
				recentStatuses: map[common.Hash]execution_client.PayloadStatus{
					parentHash: execution_client.PayloadStatusValidated,
				},
			})

			_, err := resolver.Resolve(t.Context(), preferences)
			require.ErrorContains(t, err, tt.errorSubstr)
		})
	}
}

func matrixResolver(
	t *testing.T,
	cfg *clparams.BeaconChainConfig,
	headState *state.CachingBeaconState,
	headRoot common.Hash,
	targetSlot uint64,
	fc *resolverForkchoice,
) *LiveSlotInputResolver {
	t.Helper()
	clock := eth_clock.NewMockEthereumClock(gomock.NewController(t))
	clock.EXPECT().GetCurrentSlot().Return(targetSlot).AnyTimes()
	clock.EXPECT().GenesisValidatorsRoot().Return(headState.GenesisValidatorsRoot()).AnyTimes()
	return NewLiveSlotInputResolver(
		cfg,
		new(coordinatorSigner),
		clock,
		&resolverHeadSource{state: headState, root: headRoot, identitySlot: headState.Slot()},
		fc,
	)
}
