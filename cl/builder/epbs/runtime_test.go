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
	"math"
	"math/big"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/erigontech/erigon/cl/beacon/beaconevents"
	"github.com/erigontech/erigon/cl/builder/epbs/epbscfg"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	peerdasutils "github.com/erigontech/erigon/cl/das/utils"
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

type runtimeBidProcessor struct {
	processed chan []byte
}

func (p *runtimeBidProcessor) ProcessMessage(_ context.Context, _ *uint64, bid *cltypes.SignedExecutionPayloadBid) error {
	encoded, err := bid.EncodeSSZ(nil)
	if err != nil {
		return err
	}
	if p.processed != nil {
		p.processed <- encoded
	}
	return nil
}

type retryingRuntimePublisher struct {
	processed <-chan []byte
	published chan []byte
	local     []byte
	attempts  int
}

type runtimePublication struct {
	topic string
	data  []byte
}

type runtimePayloadProcessor struct {
	processed chan *cltypes.SignedExecutionPayloadEnvelope
}

func (p *runtimePayloadProcessor) ProcessMessage(_ context.Context, _ *uint64, envelope *cltypes.SignedExecutionPayloadEnvelope) error {
	owned, ok := envelope.Clone().(*cltypes.SignedExecutionPayloadEnvelope)
	if !ok {
		return errors.New("unexpected envelope clone")
	}
	if p.processed != nil {
		p.processed <- owned
	}
	return nil
}

type runtimeLifecyclePublisher struct {
	payloadProcessed <-chan *cltypes.SignedExecutionPayloadEnvelope
	publications     chan runtimePublication
	localPayload     []byte
	payloadAttempts  int
	payloadFailures  int
}

func (p *runtimeLifecyclePublisher) Publish(_ context.Context, topic string, data []byte) error {
	if topic == gossip.TopicNameExecutionPayload {
		if p.localPayload == nil {
			select {
			case processed := <-p.payloadProcessed:
				encoded, err := processed.EncodeSSZ(nil)
				if err != nil {
					return err
				}
				p.localPayload = encoded
			default:
				return errors.New("envelope was gossiped before local processing")
			}
		}
		if !bytes.Equal(p.localPayload, data) {
			return errors.New("local processor and gossip received different envelope bytes")
		}
		p.payloadAttempts++
	}
	p.publications <- runtimePublication{topic: topic, data: bytes.Clone(data)}
	if topic == gossip.TopicNameExecutionPayload && p.payloadAttempts <= p.payloadFailures {
		return errors.New("payload publication outcome unknown")
	}
	return nil
}

type runtimeAcceptedBlockReader struct {
	mu     sync.RWMutex
	blocks map[common.Hash]*cltypes.SignedBeaconBlock
}

func (r *runtimeAcceptedBlockReader) GetBlock(root common.Hash) (*cltypes.SignedBeaconBlock, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	block, ok := r.blocks[root]
	return block, ok
}

func (r *runtimeAcceptedBlockReader) setBlock(root common.Hash, block *cltypes.SignedBeaconBlock) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.blocks[root] = block
}

func (p *retryingRuntimePublisher) Publish(_ context.Context, topic string, data []byte) error {
	if topic != gossip.TopicNameExecutionPayloadBid {
		return errors.New("unexpected topic")
	}
	if p.local == nil {
		select {
		case p.local = <-p.processed:
		default:
			return errors.New("bid was gossiped before local processing")
		}
	}
	if !bytes.Equal(p.local, data) {
		return errors.New("local processor and gossip received different bid bytes")
	}
	p.attempts++
	p.published <- bytes.Clone(data)
	if p.attempts == 1 {
		return errors.New("publication outcome unknown")
	}
	return nil
}

func TestRuntimePublishesBidForValidatedPreferences(t *testing.T) {
	cfg, headState, preferences, headRoot, parentHash, _ := liveResolverFixture(t)
	cfg.NumberOfColumns = peerdasutils.CELLS_PER_EXT_BLOB
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
	payload.BlobsBundle = validBlobDataBundle(t)
	for _, withdrawal := range input.Withdrawals {
		payload.Eth1Block.Withdrawals.Append(&cltypes.Withdrawal{
			Index: uint64(withdrawal.Index), Validator: uint64(withdrawal.Validator),
			Address: withdrawal.Address, Amount: uint64(withdrawal.Amount),
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
		PendingDirectory: filepath.Join(t.TempDir(), "pending"),
		BeaconConfig:     &cfg,
		Clock:            clock,
		Head:             head,
		Forkchoice:       fc,
		Assembler:        assembler,
		Publisher:        publisher,
		ColumnStorage:    new(recordingColumnWriter),
		BidProcessor:     &runtimeBidProcessor{},
		PayloadProcessor: &runtimePayloadProcessor{},
		AcceptedBlocks:   &runtimeAcceptedBlockReader{blocks: make(map[common.Hash]*cltypes.SignedBeaconBlock)},
		Events:           beaconevents.NewEventEmitter(),
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

func TestRuntimeProcessesBidLocallyBeforeRetryingIdenticalPublication(t *testing.T) {
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
			Index: uint64(withdrawal.Index), Validator: uint64(withdrawal.Validator),
			Address: withdrawal.Address, Amount: uint64(withdrawal.Amount),
		})
	}
	processor := &runtimeBidProcessor{processed: make(chan []byte, 1)}
	publisher := &retryingRuntimePublisher{processed: processor.processed, published: make(chan []byte, 2)}
	runtimeCfg := epbscfg.DefaultConfig()
	runtimeCfg.Enabled = true
	runtimeCfg.KeyPath = keyPath
	runtimeCfg.RetryInterval = minValidatedPreferencesRetryInterval
	runtime, err := NewRuntime(runtimeCfg, RuntimeDependencies{
		PendingDirectory: filepath.Join(t.TempDir(), "pending"),
		BeaconConfig:     &cfg,
		Clock:            clock,
		Head:             head,
		Forkchoice:       fc,
		Assembler:        &coordinatorAssembler{payloadID: 7, payload: payload},
		Publisher:        publisher,
		ColumnStorage:    new(recordingColumnWriter),
		BidProcessor:     processor,
		PayloadProcessor: &runtimePayloadProcessor{},
		AcceptedBlocks:   &runtimeAcceptedBlockReader{blocks: make(map[common.Hash]*cltypes.SignedBeaconBlock)},
		Events:           beaconevents.NewEventEmitter(),
	})
	require.NoError(t, err)

	runCtx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- runtime.Run(runCtx) }()
	runtime.SubmitValidatedPreferences(preferences)
	first := <-publisher.published
	second := <-publisher.published
	require.Equal(t, first, second)
	cancel()
	require.ErrorIs(t, <-done, context.Canceled)
}

func TestRuntimeRevealsRetainedPayloadSelectedByAcceptedBlock(t *testing.T) {
	testRuntimeRevealsRetainedPayloadSelectedByBlockEvent(t, false, 0)
}

func TestRuntimeRevealsRetainedPayloadSelectedByGossipValidatedBlock(t *testing.T) {
	testRuntimeRevealsRetainedPayloadSelectedByBlockEvent(t, true, 0)
}

func TestRuntimeRevealsPendingPayloadAfterRestartBeforeSelection(t *testing.T) {
	testRuntimeRevealsRetainedPayloadSelectedByBlockEvent(t, false, 1)
}

func TestRuntimeReconcilesSelectedPayloadAfterRestart(t *testing.T) {
	testRuntimeRevealsRetainedPayloadSelectedByBlockEvent(t, false, 2)
}

func testRuntimeRevealsRetainedPayloadSelectedByBlockEvent(t *testing.T, gossipValidated bool, restartPhase int) {
	cfg, headState, preferences, headRoot, parentHash, _ := liveResolverFixture(t)
	cfg.NumberOfColumns = peerdasutils.CELLS_PER_EXT_BLOB
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
	clock.EXPECT().GetSlotTime(preferences.Message.ProposalSlot).Return(time.Now().Add(time.Minute)).AnyTimes()
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
	payload.BlobsBundle = validBlobDataBundle(t)
	for _, withdrawal := range input.Withdrawals {
		payload.Eth1Block.Withdrawals.Append(&cltypes.Withdrawal{
			Index: uint64(withdrawal.Index), Validator: uint64(withdrawal.Validator),
			Address: withdrawal.Address, Amount: uint64(withdrawal.Amount),
		})
	}
	payloadProcessor := &runtimePayloadProcessor{processed: make(chan *cltypes.SignedExecutionPayloadEnvelope, 1)}
	publisher := &runtimeLifecyclePublisher{
		payloadProcessed: payloadProcessor.processed,
		publications:     make(chan runtimePublication, int(cfg.NumberOfColumns)+3),
		payloadFailures:  1,
	}
	columnWriter := new(recordingColumnWriter)
	acceptedBlocks := &runtimeAcceptedBlockReader{blocks: make(map[common.Hash]*cltypes.SignedBeaconBlock)}
	emitters := beaconevents.NewEventEmitter()
	runtimeCfg := epbscfg.DefaultConfig()
	runtimeCfg.Enabled = true
	runtimeCfg.KeyPath = keyPath
	runtimeCfg.RetryInterval = minValidatedPreferencesRetryInterval
	deps := RuntimeDependencies{
		PendingDirectory: filepath.Join(t.TempDir(), "pending"),
		BeaconConfig:     &cfg,
		Clock:            clock,
		Head:             head,
		Forkchoice:       fc,
		Assembler:        &coordinatorAssembler{payloadID: 7, payload: payload},
		Publisher:        publisher,
		ColumnStorage:    columnWriter,
		BidProcessor:     &runtimeBidProcessor{},
		PayloadProcessor: payloadProcessor,
		AcceptedBlocks:   acceptedBlocks,
		Events:           emitters,
	}
	runtime, err := NewRuntime(runtimeCfg, deps)
	require.NoError(t, err)

	runCtx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- runtime.Run(runCtx) }()
	runtime.SubmitValidatedPreferences(preferences)
	bidPublication := <-publisher.publications
	require.Equal(t, gossip.TopicNameExecutionPayloadBid, bidPublication.topic)
	selectedBid := &cltypes.SignedExecutionPayloadBid{}
	require.NoError(t, selectedBid.DecodeSSZStrict(bidPublication.data, int(clparams.GloasVersion)))
	block := cltypes.NewSignedBeaconBlock(&cfg, clparams.GloasVersion)
	block.Block.Slot = selectedBid.Message.Slot
	block.Block.ParentRoot = selectedBid.Message.ParentBlockRoot
	block.Block.Body.SignedExecutionPayloadBid = selectedBid
	blockRoot, err := block.Block.HashSSZ()
	require.NoError(t, err)
	if restartPhase != 0 {
		if restartPhase == 2 {
			acceptedBlocks.setBlock(common.Hash(blockRoot), block)
			fc.headNode.Root = common.Hash(blockRoot)
		}
		cancel()
		require.ErrorIs(t, <-done, context.Canceled)
		runtime, err = NewRuntime(runtimeCfg, deps)
		require.NoError(t, err)
		runCtx, cancel = context.WithCancel(t.Context())
		done = make(chan error, 1)
		go func() { done <- runtime.Run(runCtx) }()
	}
	switch {
	case restartPhase != 2 && gossipValidated:
		emitters.State().SendBlockGossip(&beaconevents.BlockGossipData{
			Slot: block.Block.Slot, Block: common.Hash(blockRoot),
		})
		emitters.State().SendBlockGossip(&beaconevents.BlockGossipData{
			Slot: block.Block.Slot, Block: common.Hash{1}, SignedBlock: block,
		})
		select {
		case publication := <-publisher.publications:
			t.Fatalf("invalid gossip block triggered publication on %s", publication.topic)
		case <-time.After(20 * time.Millisecond):
		}
		emitters.State().SendBlockGossip(&beaconevents.BlockGossipData{
			Slot: block.Block.Slot, Block: common.Hash(blockRoot), SignedBlock: block,
		})
	case restartPhase == 1:
		acceptedBlocks.setBlock(common.Hash(blockRoot), block)
		require.True(t, runtime.reveals.SubmitAcceptedBlock(common.Hash(blockRoot)))
	case restartPhase != 2:
		acceptedBlocks.setBlock(common.Hash(blockRoot), block)
		emitters.State().SendBlock(&beaconevents.BlockData{Slot: block.Block.Slot, Block: common.Hash(blockRoot)})
	}

	var firstEnvelopePublication runtimePublication
	select {
	case firstEnvelopePublication = <-publisher.publications:
	case <-time.After(time.Second):
		t.Fatal("gossip-validated selected block did not trigger payload reveal")
	}
	require.Equal(t, gossip.TopicNameExecutionPayload, firstEnvelopePublication.topic)
	for range cfg.NumberOfColumns {
		publication := <-publisher.publications
		require.True(t, gossip.IsTopicDataColumnSidecar(publication.topic))
	}
	require.Eventually(t, func() bool {
		return len(columnWriter.snapshot()) == int(cfg.NumberOfColumns)
	}, time.Second, time.Millisecond)
	envelopePublication := <-publisher.publications
	require.Equal(t, firstEnvelopePublication, envelopePublication)
	require.Equal(t, gossip.TopicNameExecutionPayload, envelopePublication.topic)
	envelope := &cltypes.SignedExecutionPayloadEnvelope{Message: cltypes.NewExecutionPayloadEnvelope(&cfg)}
	require.NoError(t, envelope.DecodeSSZStrict(envelopePublication.data, int(clparams.GloasVersion)))
	require.Equal(t, selectedBid.Message.BlockHash, envelope.Message.Payload.BlockHash)
	require.Equal(t, selectedBid.Message.BuilderIndex, envelope.Message.BuilderIndex)
	require.Equal(t, common.Hash(blockRoot), envelope.Message.BeaconBlockRoot)
	require.Equal(t, selectedBid.Message.ParentBlockRoot, envelope.Message.ParentBeaconBlockRoot)
	require.NotEqual(t, common.Bytes96{}, envelope.Signature)
	if gossipValidated {
		acceptedBlocks.setBlock(common.Hash(blockRoot), block)
		emitters.State().SendBlock(&beaconevents.BlockData{Slot: block.Block.Slot, Block: common.Hash(blockRoot)})
	} else {
		emitters.State().SendBlockGossip(&beaconevents.BlockGossipData{
			Slot: block.Block.Slot, Block: common.Hash(blockRoot), SignedBlock: block,
		})
	}
	select {
	case publication := <-publisher.publications:
		t.Fatalf("accepted block retriggered publication on %s", publication.topic)
	case <-time.After(20 * time.Millisecond):
	}

	cancel()
	require.ErrorIs(t, <-done, context.Canceled)
}

func TestRuntimeDisabledNeedsNoDependencies(t *testing.T) {
	runtime, err := NewRuntime(epbscfg.Config{}, RuntimeDependencies{})
	require.NoError(t, err)
	require.Nil(t, runtime)
}

func TestPayloadRevealDeadlineHandlesLargestSupportedSlotDuration(t *testing.T) {
	cfg := gloasCoordinatorConfig()
	cfg.SecondsPerSlot = uint64(math.MaxInt64 / int64(time.Second))
	cfg.PayloadDueBps = clparams.BpsFactor
	clock := eth_clock.NewMockEthereumClock(gomock.NewController(t))
	start := time.Unix(1, 0)
	clock.EXPECT().GetSlotTime(uint64(1)).Return(start)

	deadline, ok := payloadRevealDeadline(clock, &cfg, 1)

	require.True(t, ok)
	require.Equal(t, time.Duration(cfg.SecondsPerSlot)*time.Second, deadline.Sub(start))
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
		PendingDirectory: filepath.Join(t.TempDir(), "pending"),
		BeaconConfig:     &cfg,
		Clock:            eth_clock.NewMockEthereumClock(gomock.NewController(t)),
		Head:             new(resolverHeadSource),
		Forkchoice:       new(resolverForkchoice),
		Assembler:        new(coordinatorAssembler),
		Publisher:        &runtimePublisher{published: make(chan string, 1)},
		ColumnStorage:    new(recordingColumnWriter),
		BidProcessor:     &runtimeBidProcessor{},
		PayloadProcessor: &runtimePayloadProcessor{},
		AcceptedBlocks:   &runtimeAcceptedBlockReader{blocks: make(map[common.Hash]*cltypes.SignedBeaconBlock)},
		Events:           beaconevents.NewEventEmitter(),
	}

	for _, test := range []struct {
		name   string
		mutate func(*epbscfg.Config, *RuntimeDependencies)
	}{
		{name: "missing key", mutate: func(cfg *epbscfg.Config, _ *RuntimeDependencies) { cfg.KeyPath = "" }},
		{name: "invalid margin", mutate: func(cfg *epbscfg.Config, _ *RuntimeDependencies) { cfg.BidMargin = math.NaN() }},
		{name: "missing dependency", mutate: func(_ *epbscfg.Config, deps *RuntimeDependencies) { deps.Publisher = nil }},
		{name: "missing column storage", mutate: func(_ *epbscfg.Config, deps *RuntimeDependencies) { deps.ColumnStorage = nil }},
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
		{name: "payload deadline outside slot", mutate: func(_ *epbscfg.Config, deps *RuntimeDependencies) {
			copy := *deps.BeaconConfig
			copy.PayloadDueBps = clparams.BpsFactor + 1
			deps.BeaconConfig = &copy
		}},
		{name: "slot duration overflows time", mutate: func(_ *epbscfg.Config, deps *RuntimeDependencies) {
			copy := *deps.BeaconConfig
			copy.SecondsPerSlot = uint64(math.MaxInt64/int64(time.Second)) + 1
			deps.BeaconConfig = &copy
		}},
		{name: "zero slot duration", mutate: func(_ *epbscfg.Config, deps *RuntimeDependencies) {
			copy := *deps.BeaconConfig
			copy.SecondsPerSlot = 0
			deps.BeaconConfig = &copy
		}},
		{name: "zero data columns", mutate: func(_ *epbscfg.Config, deps *RuntimeDependencies) {
			copy := *deps.BeaconConfig
			copy.NumberOfColumns = 0
			deps.BeaconConfig = &copy
		}},
		{name: "zero data column subnets", mutate: func(_ *epbscfg.Config, deps *RuntimeDependencies) {
			copy := *deps.BeaconConfig
			copy.DataColumnSidecarSubnetCount = 0
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
		{name: "negative bid delay", mutate: func(cfg *epbscfg.Config, _ *RuntimeDependencies) {
			cfg.BidDelay = -time.Nanosecond
		}},
		{name: "negative private orderflow window", mutate: func(cfg *epbscfg.Config, _ *RuntimeDependencies) {
			cfg.PrivateOrderflowWindow = -time.Nanosecond
		}},
		{name: "bid delay reaches target slot", mutate: func(cfg *epbscfg.Config, deps *RuntimeDependencies) {
			cfg.BidDelay = time.Duration(deps.BeaconConfig.SecondsPerSlot) * time.Second
		}},
		{name: "bid delay leaves no retry cadence", mutate: func(cfg *epbscfg.Config, deps *RuntimeDependencies) {
			cfg.BidDelay = time.Duration(deps.BeaconConfig.SecondsPerSlot)*time.Second - cfg.RetryInterval
		}},
		{name: "bid delay exceeds retry cadence boundary", mutate: func(cfg *epbscfg.Config, deps *RuntimeDependencies) {
			cfg.BidDelay = time.Duration(deps.BeaconConfig.SecondsPerSlot)*time.Second - cfg.RetryInterval + time.Nanosecond
		}},
		{name: "private orderflow window exceeds bid budget", mutate: func(cfg *epbscfg.Config, deps *RuntimeDependencies) {
			cfg.PrivateOrderflowWindow = time.Duration(deps.BeaconConfig.SecondsPerSlot)*time.Second - cfg.BidDelay - cfg.RetryInterval + time.Nanosecond
		}},
		{name: "bid timing sum overflows", mutate: func(cfg *epbscfg.Config, deps *RuntimeDependencies) {
			copy := *deps.BeaconConfig
			copy.SecondsPerSlot = uint64(math.MaxInt64 / int64(time.Second))
			deps.BeaconConfig = &copy
			cfg.BidDelay = 6_000_000_000_000_000_000
			cfg.PrivateOrderflowWindow = 6_000_000_000_000_000_000
		}},
		{name: "shadow curve without bid delay", mutate: func(cfg *epbscfg.Config, _ *RuntimeDependencies) {
			cfg.ShadowValueCurve = true
			cfg.BidDelay = 0
		}},
		{name: "shadow curve reaches target slot", mutate: func(cfg *epbscfg.Config, deps *RuntimeDependencies) {
			cfg.ShadowValueCurve = true
			cfg.BidDelay = time.Duration(deps.BeaconConfig.SecondsPerSlot)*time.Second - 4*time.Second - cfg.RetryInterval + time.Nanosecond
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

func TestRuntimeAcceptsBidDelayBeforeRetryCadenceBoundary(t *testing.T) {
	beaconCfg := gloasCoordinatorConfig()
	keyPath := filepath.Join(t.TempDir(), "builder.key")
	privateKey, err := bls.GenerateKey()
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(keyPath, privateKey.Bytes(), 0o600))
	cfg := epbscfg.DefaultConfig()
	cfg.Enabled = true
	cfg.KeyPath = keyPath
	cfg.BidDelay = time.Duration(beaconCfg.SecondsPerSlot)*time.Second - cfg.RetryInterval - time.Nanosecond

	_, _, err = prepareRuntimeConfig(cfg, &beaconCfg)
	require.NoError(t, err)
}

func TestRuntimeAcceptsLongRetryWithoutTimedOffsets(t *testing.T) {
	beaconCfg := gloasCoordinatorConfig()
	keyPath := filepath.Join(t.TempDir(), "builder.key")
	privateKey, err := bls.GenerateKey()
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(keyPath, privateKey.Bytes(), 0o600))
	cfg := epbscfg.DefaultConfig()
	cfg.Enabled = true
	cfg.KeyPath = keyPath
	cfg.RetryInterval = time.Duration(beaconCfg.SecondsPerSlot) * time.Second

	_, _, err = prepareRuntimeConfig(cfg, &beaconCfg)
	require.NoError(t, err)
}

func TestRuntimeAppliesRunnerConfiguration(t *testing.T) {
	cfg := gloasCoordinatorConfig()
	cfg.SlotsPerEpoch = 64
	keyPath := filepath.Join(t.TempDir(), "builder.key")
	privateKey, err := bls.GenerateKey()
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(keyPath, privateKey.Bytes(), 0o600))
	runtimeCfg := epbscfg.DefaultConfig()
	runtimeCfg.Enabled = true
	runtimeCfg.KeyPath = keyPath
	runtimeCfg.BidDelay = 1200 * time.Millisecond
	runtimeCfg.PrivateOrderflowWindow = 350 * time.Millisecond
	runtimeCfg.ShadowValueCurve = true
	clock := eth_clock.NewMockEthereumClock(gomock.NewController(t))
	clock.EXPECT().GetCurrentSlot().Return(uint64(0)).AnyTimes()
	deps := RuntimeDependencies{
		PendingDirectory: filepath.Join(t.TempDir(), "pending"),
		BeaconConfig:     &cfg,
		Clock:            clock,
		Head:             new(resolverHeadSource),
		Forkchoice:       new(resolverForkchoice),
		Assembler:        new(coordinatorAssembler),
		Publisher:        &runtimePublisher{published: make(chan string, 1)},
		ColumnStorage:    new(recordingColumnWriter),
		BidProcessor:     &runtimeBidProcessor{},
		PayloadProcessor: &runtimePayloadProcessor{},
		AcceptedBlocks:   &runtimeAcceptedBlockReader{blocks: make(map[common.Hash]*cltypes.SignedBeaconBlock)},
		Events:           beaconevents.NewEventEmitter(),
	}
	runtime, err := NewRuntime(runtimeCfg, deps)
	require.NoError(t, err)
	require.Equal(t, int(cfg.SlotsPerEpoch), runtime.runner.maxPending)
	require.Equal(t, runtimeCfg.BidDelay, runtime.runner.bidDelay)
	require.Equal(t, runtimeCfg.PrivateOrderflowWindow, runtime.coordinator.privateOrderflowWindow)
	require.NotNil(t, runtime.shadow)
	require.Equal(t, []time.Duration{3200 * time.Millisecond, 5200 * time.Millisecond}, runtime.shadow.delays)

	blockedPath := filepath.Join(t.TempDir(), "not-a-directory")
	require.NoError(t, os.WriteFile(blockedPath, nil, 0o600))
	deps.PendingDirectory = blockedPath
	_, err = NewRuntime(runtimeCfg, deps)
	require.ErrorIs(t, err, ErrPendingPayloadStore)
}
