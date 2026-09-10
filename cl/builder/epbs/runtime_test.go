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
	"math"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/erigontech/erigon/cl/builder/epbs/epbscfg"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/gossip"
	"github.com/erigontech/erigon/cl/phase1/execution_client"
	"github.com/erigontech/erigon/cl/phase1/forkchoice"
	"github.com/erigontech/erigon/cl/utils/bls"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
	"github.com/erigontech/erigon/common"
)

type runtimePublisher struct {
	published chan string
}

func (p *runtimePublisher) Publish(_ context.Context, topic string, _ []byte) error {
	p.published <- topic
	return nil
}

func TestRuntimePublishesBidForValidatedPreferences(t *testing.T) {
	cfg, headState, preferences, headRoot, parentHash, _ := liveResolverFixture(t)
	privateKey, err := bls.GenerateKey()
	require.NoError(t, err)
	keyPath := filepath.Join(t.TempDir(), "builder.key")
	require.NoError(t, os.WriteFile(keyPath, privateKey.Bytes(), 0o600))
	signer, err := NewLocalSignerFromBytes(privateKey.Bytes())
	require.NoError(t, err)
	headState.GetBuilders().Get(0).Pubkey = signer.Pubkey()

	clock := eth_clock.NewMockEthereumClock(gomock.NewController(t))
	clock.EXPECT().GetCurrentSlot().Return(preferences.Message.ProposalSlot).AnyTimes()
	clock.EXPECT().GenesisValidatorsRoot().Return(headState.GenesisValidatorsRoot()).AnyTimes()
	head := &resolverHeadSource{state: headState, root: headRoot, identitySlot: headState.Slot()}
	fc := &resolverForkchoice{
		headNode:  forkchoice.ForkChoiceNode{Root: headRoot, PayloadStatus: 0},
		gasLimits: map[common.Hash]uint64{parentHash: 30_000_000},
		recentStatuses: map[common.Hash]execution_client.PayloadStatus{
			parentHash: execution_client.PayloadStatusValidated,
		},
	}
	resolver := NewLiveSlotInputResolver(&cfg, signer, clock, head, fc)
	input, err := resolver.Resolve(t.Context(), preferences)
	require.NoError(t, err)
	payload := validCoordinatorPayload(&cfg, input, big.NewInt(10_000_000_000))
	for _, withdrawal := range input.Withdrawals {
		payload.Eth1Block.Withdrawals.Append(&cltypes.Withdrawal{
			Index: withdrawal.Index, Validator: withdrawal.Validator,
			Address: withdrawal.Address, Amount: withdrawal.Amount,
		})
	}
	assembler := &coordinatorAssembler{
		payloadID: 7,
		payload:   payload,
	}
	publisher := &runtimePublisher{published: make(chan string, 1)}
	runtimeCfg := epbscfg.DefaultConfig()
	runtimeCfg.Enabled = true
	runtimeCfg.KeyPath = keyPath
	runtime, err := NewRuntime(runtimeCfg, RuntimeDependencies{
		BeaconConfig: &cfg,
		Clock:        clock,
		Head:         head,
		Forkchoice:   fc,
		Assembler:    assembler,
		Publisher:    publisher,
	})
	require.NoError(t, err)

	runCtx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- runtime.Run(runCtx) }()
	runtime.SubmitValidatedPreferences(preferences)
	select {
	case topic := <-publisher.published:
		require.Equal(t, gossip.TopicNameExecutionPayloadBid, topic)
	case <-time.After(time.Second):
		t.Fatal("validated preferences did not publish a bid")
	}
	cancel()
	require.ErrorIs(t, <-done, context.Canceled)
}

func TestRuntimeDisabledNeedsNoDependencies(t *testing.T) {
	runtime, err := NewRuntime(epbscfg.Config{}, RuntimeDependencies{})
	require.NoError(t, err)
	require.Nil(t, runtime)
}

func TestRuntimeRejectsInvalidStartupConfiguration(t *testing.T) {
	cfg := gloasCoordinatorConfig()
	keyPath := filepath.Join(t.TempDir(), "builder.key")
	privateKey, err := bls.GenerateKey()
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(keyPath, privateKey.Bytes(), 0o600))
	valid := epbscfg.DefaultConfig()
	valid.Enabled = true
	valid.KeyPath = keyPath
	deps := RuntimeDependencies{
		BeaconConfig: &cfg,
		Clock:        eth_clock.NewMockEthereumClock(gomock.NewController(t)),
		Head:         new(resolverHeadSource),
		Forkchoice:   new(resolverForkchoice),
		Assembler:    new(coordinatorAssembler),
		Publisher:    &runtimePublisher{published: make(chan string, 1)},
	}

	for _, test := range []struct {
		name   string
		mutate func(*epbscfg.Config, *RuntimeDependencies)
	}{
		{name: "missing key", mutate: func(cfg *epbscfg.Config, _ *RuntimeDependencies) { cfg.KeyPath = "" }},
		{name: "invalid margin", mutate: func(cfg *epbscfg.Config, _ *RuntimeDependencies) { cfg.BidMargin = math.NaN() }},
		{name: "missing dependency", mutate: func(_ *epbscfg.Config, deps *RuntimeDependencies) { deps.Publisher = nil }},
		{name: "gloas unavailable", mutate: func(_ *epbscfg.Config, deps *RuntimeDependencies) {
			copy := *deps.BeaconConfig
			copy.GloasForkEpoch = copy.FarFutureEpoch
			deps.BeaconConfig = &copy
		}},
		{name: "zero slots per epoch", mutate: func(_ *epbscfg.Config, deps *RuntimeDependencies) {
			copy := *deps.BeaconConfig
			copy.SlotsPerEpoch = 0
			deps.BeaconConfig = &copy
		}},
		{name: "negative pending capacity", mutate: func(cfg *epbscfg.Config, _ *RuntimeDependencies) { cfg.MaxPending = -1 }},
		{name: "pending capacity shorter than one epoch", mutate: func(cfg *epbscfg.Config, deps *RuntimeDependencies) {
			cfg.MaxPending = int(deps.BeaconConfig.SlotsPerEpoch) - 1
		}},
		{name: "zero retained capacity", mutate: func(cfg *epbscfg.Config, _ *RuntimeDependencies) { cfg.MaxRetained = 0 }},
		{name: "retry cadence too short", mutate: func(cfg *epbscfg.Config, _ *RuntimeDependencies) {
			cfg.RetryInterval = minValidatedPreferencesRetryInterval - time.Nanosecond
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			testCfg := valid
			testDeps := deps
			test.mutate(&testCfg, &testDeps)
			runtime, err := NewRuntime(testCfg, testDeps)
			require.Error(t, err)
			require.Nil(t, runtime)
		})
	}
}

func TestRuntimeDefaultPendingCapacityTracksChain(t *testing.T) {
	cfg := gloasCoordinatorConfig()
	cfg.SlotsPerEpoch = 64
	keyPath := filepath.Join(t.TempDir(), "builder.key")
	privateKey, err := bls.GenerateKey()
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(keyPath, privateKey.Bytes(), 0o600))
	runtimeCfg := epbscfg.DefaultConfig()
	runtimeCfg.Enabled = true
	runtimeCfg.KeyPath = keyPath
	runtime, err := NewRuntime(runtimeCfg, RuntimeDependencies{
		BeaconConfig: &cfg,
		Clock:        eth_clock.NewMockEthereumClock(gomock.NewController(t)),
		Head:         new(resolverHeadSource),
		Forkchoice:   new(resolverForkchoice),
		Assembler:    new(coordinatorAssembler),
		Publisher:    &runtimePublisher{published: make(chan string, 1)},
	})
	require.NoError(t, err)
	require.Equal(t, int(cfg.SlotsPerEpoch), runtime.runner.maxPending)
}
