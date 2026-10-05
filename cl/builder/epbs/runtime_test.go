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
	"github.com/erigontech/erigon/cl/builder/epbs/eladapter"
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
	executionbuilder "github.com/erigontech/erigon/execution/builder"
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
	payloadGate      <-chan struct{}
	localPayload     []byte
	payloadAttempts  int
	payloadFailures  int
}

func (p *runtimeLifecyclePublisher) Publish(ctx context.Context, topic string, data []byte) error {
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
	if topic == gossip.TopicNameExecutionPayload && p.payloadGate != nil {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-p.payloadGate:
		}
	}
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
	clock.EXPECT().GetSlotTime(preferences.Message.ProposalSlot - 1).Return(time.Now().Add(-10 * time.Second)).AnyTimes()
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
	runtimeCfg.BidPublishLead = 0
	runtimeCfg.Enabled = true
	runtimeCfg.KeyPath = keyPath
	status := executionbuilder.NewEmbeddedBuilderStatus(true)
	deps := RuntimeDependencies{
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
		HighestBids:      new(coordinatorHighestBidReader),
		Events:           beaconevents.NewEventEmitter(),
		Status:           status,
	}
	runtime, err := NewRuntime(runtimeCfg, deps)
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
	require.Equal(t, executionbuilder.BuilderPhaseRunning, status.Snapshot().Phase)
	cancel()
	require.ErrorIs(t, <-done, context.Canceled)
	require.Equal(t, executionbuilder.BuilderPhaseStopped, status.Snapshot().Phase)
	require.Equal(t, executionbuilder.BuilderStoppedNode, status.Snapshot().Reason)

	deadlineStatus := executionbuilder.NewEmbeddedBuilderStatus(true)
	deps.PendingDirectory = filepath.Join(t.TempDir(), "deadline-pending")
	deps.Status = deadlineStatus
	deadlineRuntime, err := NewRuntime(runtimeCfg, deps)
	require.NoError(t, err)
	deadlineCtx, deadlineCancel := context.WithDeadline(t.Context(), time.Now().Add(-time.Second))
	defer deadlineCancel()
	require.ErrorIs(t, deadlineRuntime.Run(deadlineCtx), context.DeadlineExceeded)
	require.Equal(t, executionbuilder.BuilderPhaseStopped, deadlineStatus.Snapshot().Phase)
	require.Equal(t, executionbuilder.BuilderStoppedNode, deadlineStatus.Snapshot().Reason)
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
	clock.EXPECT().GetSlotTime(preferences.Message.ProposalSlot - 1).Return(time.Now().Add(-10 * time.Second)).AnyTimes()
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
	runtimeCfg.BidPublishLead = 0
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
		HighestBids:      new(coordinatorHighestBidReader),
		Events:           beaconevents.NewEventEmitter(),
	})
	require.NoError(t, err)

	runCtx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- runtime.Run(runCtx) }()
	runtime.SubmitValidatedPreferences(preferences)
	first := receiveRevealTestValue(t, publisher.published, "first bid publication did not arrive")
	second := receiveRevealTestValue(t, publisher.published, "retried bid publication did not arrive")
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

func TestRuntimeResumesPayloadRevealAfterRestart(t *testing.T) {
	testRuntimeRevealsRetainedPayloadSelectedByBlockEvent(t, false, 3)
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
	clock.EXPECT().GetSlotTime(preferences.Message.ProposalSlot - 1).Return(time.Now().Add(-10 * time.Second)).AnyTimes()
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
	if restartPhase == 3 {
		publisher.payloadGate = make(chan struct{})
	}
	columnWriter := new(recordingColumnWriter)
	acceptedBlocks := &runtimeAcceptedBlockReader{blocks: make(map[common.Hash]*cltypes.SignedBeaconBlock)}
	emitters := beaconevents.NewEventEmitter()
	runtimeCfg := epbscfg.DefaultConfig()
	runtimeCfg.BidPublishLead = 0
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
		HighestBids:      new(coordinatorHighestBidReader),
		Events:           emitters,
	}
	runtime, err := NewRuntime(runtimeCfg, deps)
	require.NoError(t, err)

	runCtx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- runtime.Run(runCtx) }()
	runtime.SubmitValidatedPreferences(preferences)
	bidPublication := receiveRevealTestValue(t, publisher.publications, "bid was not published")
	require.Equal(t, gossip.TopicNameExecutionPayloadBid, bidPublication.topic)
	selectedBid := &cltypes.SignedExecutionPayloadBid{}
	require.NoError(t, selectedBid.DecodeSSZStrict(bidPublication.data, int(clparams.GloasVersion)))
	block := cltypes.NewSignedBeaconBlock(&cfg, clparams.GloasVersion)
	block.Block.Slot = selectedBid.Message.Slot
	block.Block.ParentRoot = selectedBid.Message.ParentBlockRoot
	block.Block.Body.SignedExecutionPayloadBid = selectedBid
	blockRoot, err := block.Block.HashSSZ()
	require.NoError(t, err)
	if restartPhase == 1 || restartPhase == 2 {
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
	if restartPhase == 3 {
		cancel()
		require.ErrorIs(t, <-done, context.Canceled)
		fc.headNode.Root = common.Hash(blockRoot)
		payloadProcessor = &runtimePayloadProcessor{processed: make(chan *cltypes.SignedExecutionPayloadEnvelope, 1)}
		publisher = &runtimeLifecyclePublisher{
			payloadProcessed: payloadProcessor.processed,
			publications:     make(chan runtimePublication, int(cfg.NumberOfColumns)+3),
			payloadFailures:  1,
		}
		columnWriter = new(recordingColumnWriter)
		deps.PayloadProcessor = payloadProcessor
		deps.Publisher = publisher
		deps.ColumnStorage = columnWriter
		runtime, err = NewRuntime(runtimeCfg, deps)
		require.NoError(t, err)
		runCtx, cancel = context.WithCancel(t.Context())
		done = make(chan error, 1)
		go func() { done <- runtime.Run(runCtx) }()
		select {
		case firstEnvelopePublication = <-publisher.publications:
		case <-time.After(time.Second):
			t.Fatal("restarted runtime did not resume payload reveal")
		}
		require.Equal(t, gossip.TopicNameExecutionPayload, firstEnvelopePublication.topic)
	}
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
		HighestBids:      new(coordinatorHighestBidReader),
		Events:           beaconevents.NewEventEmitter(),
	}
	validDeps := deps
	validDeps.PendingDirectory = filepath.Join(t.TempDir(), "valid-pending")
	runtime, err := NewRuntime(valid, validDeps)
	require.NoError(t, err)
	require.NotNil(t, runtime)

	for _, test := range []struct {
		name      string
		mutate    func(*epbscfg.Config, *RuntimeDependencies)
		wantError string
	}{
		{name: "missing key", mutate: func(cfg *epbscfg.Config, _ *RuntimeDependencies) { cfg.KeyPath = "" }, wantError: "builder key path is required"},
		{name: "invalid margin", mutate: func(cfg *epbscfg.Config, _ *RuntimeDependencies) { cfg.BidMargin = math.NaN() }, wantError: "bid margin must be between zero and one"},
		{name: "maximum below bid margin", mutate: func(cfg *epbscfg.Config, _ *RuntimeDependencies) { cfg.MaxBidMargin = cfg.BidMargin - 0.01 }, wantError: "maximum bid margin must be between the bid margin and one"},
		{name: "maximum above one", mutate: func(cfg *epbscfg.Config, _ *RuntimeDependencies) { cfg.MaxBidMargin = 1.01 }, wantError: "maximum bid margin must be between the bid margin and one"},
		{name: "maximum is NaN", mutate: func(cfg *epbscfg.Config, _ *RuntimeDependencies) { cfg.MaxBidMargin = math.NaN() }, wantError: "maximum bid margin must be between the bid margin and one"},
		{name: "maximum is positive infinity", mutate: func(cfg *epbscfg.Config, _ *RuntimeDependencies) { cfg.MaxBidMargin = math.Inf(1) }, wantError: "maximum bid margin must be between the bid margin and one"},
		{name: "maximum is negative infinity", mutate: func(cfg *epbscfg.Config, _ *RuntimeDependencies) { cfg.MaxBidMargin = math.Inf(-1) }, wantError: "maximum bid margin must be between the bid margin and one"},
		{name: "missing dependency", mutate: func(_ *epbscfg.Config, deps *RuntimeDependencies) { deps.Publisher = nil }, wantError: "missing dependency"},
		{name: "missing column storage", mutate: func(_ *epbscfg.Config, deps *RuntimeDependencies) { deps.ColumnStorage = nil }, wantError: "missing dependency"},
		{name: "missing highest bids", mutate: func(_ *epbscfg.Config, deps *RuntimeDependencies) { deps.HighestBids = nil }, wantError: "missing dependency"},
		{name: "gloas unavailable", mutate: func(_ *epbscfg.Config, deps *RuntimeDependencies) {
			copy := *deps.BeaconConfig
			copy.GloasForkEpoch = copy.FarFutureEpoch
			deps.BeaconConfig = &copy
		}, wantError: "Gloas is not configured"},
		{name: "zero slots per epoch", mutate: func(_ *epbscfg.Config, deps *RuntimeDependencies) {
			copy := *deps.BeaconConfig
			copy.SlotsPerEpoch = 0
			deps.BeaconConfig = &copy
		}, wantError: "slots per epoch must be positive"},
		{name: "payload deadline outside slot", mutate: func(_ *epbscfg.Config, deps *RuntimeDependencies) {
			copy := *deps.BeaconConfig
			copy.PayloadDueBps = clparams.BpsFactor + 1
			deps.BeaconConfig = &copy
		}, wantError: "payload deadline must be within the slot"},
		{name: "slot duration overflows time", mutate: func(_ *epbscfg.Config, deps *RuntimeDependencies) {
			copy := *deps.BeaconConfig
			copy.SecondsPerSlot = uint64(math.MaxInt64/int64(time.Second)) + 1
			deps.BeaconConfig = &copy
		}, wantError: "slot duration is outside the supported range"},
		{name: "zero slot duration", mutate: func(_ *epbscfg.Config, deps *RuntimeDependencies) {
			copy := *deps.BeaconConfig
			copy.SecondsPerSlot = 0
			deps.BeaconConfig = &copy
		}, wantError: "slot duration is outside the supported range"},
		{name: "zero data columns", mutate: func(_ *epbscfg.Config, deps *RuntimeDependencies) {
			copy := *deps.BeaconConfig
			copy.NumberOfColumns = 0
			deps.BeaconConfig = &copy
		}, wantError: "data column and subnet counts must be positive"},
		{name: "zero data column subnets", mutate: func(_ *epbscfg.Config, deps *RuntimeDependencies) {
			copy := *deps.BeaconConfig
			copy.DataColumnSidecarSubnetCount = 0
			deps.BeaconConfig = &copy
		}, wantError: "data column and subnet counts must be positive"},
		{name: "negative pending capacity", mutate: func(cfg *epbscfg.Config, _ *RuntimeDependencies) { cfg.MaxPending = -1 }, wantError: "capacities must not be negative and retained capacity must be positive"},
		{name: "pending capacity shorter than one epoch", mutate: func(cfg *epbscfg.Config, deps *RuntimeDependencies) {
			cfg.MaxPending = int(deps.BeaconConfig.SlotsPerEpoch) - 1
		}, wantError: "pending capacity must cover one epoch"},
		{name: "zero retained capacity", mutate: func(cfg *epbscfg.Config, _ *RuntimeDependencies) { cfg.MaxRetained = 0 }, wantError: "capacities must not be negative and retained capacity must be positive"},
		{name: "retry cadence too short", mutate: func(cfg *epbscfg.Config, _ *RuntimeDependencies) {
			cfg.RetryInterval = minValidatedPreferencesRetryInterval - time.Nanosecond
		}, wantError: "retry interval is too short"},
		{name: "negative bid delay", mutate: func(cfg *epbscfg.Config, _ *RuntimeDependencies) {
			cfg.BidDelay = -time.Nanosecond
		}, wantError: "bid delay must be within the preceding slot"},
		{name: "negative publish lead", mutate: func(cfg *epbscfg.Config, _ *RuntimeDependencies) { cfg.BidPublishLead = -time.Nanosecond }, wantError: "bid publish lead must not be negative"},
		{name: "publish lead reaches slot duration", mutate: func(cfg *epbscfg.Config, deps *RuntimeDependencies) {
			cfg.BidPublishLead = time.Duration(deps.BeaconConfig.SecondsPerSlot) * time.Second
		}, wantError: "first bid attempt must precede the bid publish time"},
		{name: "first attempt reaches publish time", mutate: func(cfg *epbscfg.Config, deps *RuntimeDependencies) {
			cfg.BidDelay = time.Duration(deps.BeaconConfig.SecondsPerSlot)*time.Second - cfg.BidPublishLead
		}, wantError: "first bid attempt must precede the bid publish time"},
		{name: "first attempt passes publish time", mutate: func(cfg *epbscfg.Config, deps *RuntimeDependencies) {
			cfg.BidDelay = time.Duration(deps.BeaconConfig.SecondsPerSlot)*time.Second - cfg.BidPublishLead + time.Nanosecond
		}, wantError: "first bid attempt must precede the bid publish time"},
		{name: "bid delay reaches target slot", mutate: func(cfg *epbscfg.Config, deps *RuntimeDependencies) {
			cfg.BidDelay = time.Duration(deps.BeaconConfig.SecondsPerSlot) * time.Second
		}, wantError: "bid delay must be within the preceding slot"},
		{name: "bid delay leaves no retry cadence", mutate: func(cfg *epbscfg.Config, deps *RuntimeDependencies) {
			cfg.BidPublishLead = 0
			cfg.BidDelay = time.Duration(deps.BeaconConfig.SecondsPerSlot)*time.Second - cfg.RetryInterval
		}, wantError: "bid delay and retry cadence must fit within the preceding slot"},
		{name: "bid delay exceeds retry cadence boundary", mutate: func(cfg *epbscfg.Config, deps *RuntimeDependencies) {
			cfg.BidPublishLead = 0
			cfg.BidDelay = time.Duration(deps.BeaconConfig.SecondsPerSlot)*time.Second - cfg.RetryInterval + time.Nanosecond
		}, wantError: "bid delay and retry cadence must fit within the preceding slot"},
	} {
		t.Run(test.name, func(t *testing.T) {
			testCfg := valid
			testDeps := deps
			test.mutate(&testCfg, &testDeps)
			testDeps.PendingDirectory = filepath.Join(t.TempDir(), "pending")
			runtime, err := NewRuntime(testCfg, testDeps)
			require.ErrorContains(t, err, test.wantError)
			require.Nil(t, runtime)
		})
	}
}

func TestRuntimeAcceptsMaximumBidMarginBoundaries(t *testing.T) {
	beaconCfg := gloasCoordinatorConfig()
	keyPath := filepath.Join(t.TempDir(), "builder.key")
	privateKey, err := bls.GenerateKey()
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(keyPath, privateKey.Bytes(), 0o600))
	valid := epbscfg.DefaultConfig()
	valid.Enabled = true
	valid.KeyPath = keyPath

	for _, test := range []struct {
		name         string
		maxBidMargin float64
	}{
		{name: "equal to bid margin", maxBidMargin: valid.BidMargin},
		{name: "equal to one", maxBidMargin: 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			cfg := valid
			cfg.MaxBidMargin = test.maxBidMargin
			_, _, err := prepareRuntimeConfig(cfg, &beaconCfg)
			require.NoError(t, err)
		})
	}
}

func TestPrepareRuntimeConfigDerivesBidDelay(t *testing.T) {
	beaconCfg := gloasCoordinatorConfig()
	beaconCfg.SecondsPerSlot = 12
	maxSlotSeconds := uint64(math.MaxInt64 / int64(time.Second))
	keyPath := filepath.Join(t.TempDir(), "builder.key")
	privateKey, err := bls.GenerateKey()
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(keyPath, privateKey.Bytes(), 0o600))

	for _, test := range []struct {
		name        string
		slotSeconds uint64
		bidDelay    time.Duration
		publishLead time.Duration
		want        time.Duration
	}{
		{name: "default lead", publishLead: 400 * time.Millisecond, want: 8600 * time.Millisecond},
		{name: "shorter lead", publishLead: 200 * time.Millisecond, want: 8800 * time.Millisecond},
		{name: "zero lead", want: 9 * time.Second},
		{name: "lead reaches three quarters", publishLead: 9 * time.Second},
		{name: "explicit delay", bidDelay: 8500 * time.Millisecond, publishLead: 400 * time.Millisecond, want: 8500 * time.Millisecond},
		{name: "maximum slot duration", slotSeconds: maxSlotSeconds, publishLead: 400 * time.Millisecond, want: time.Duration(maxSlotSeconds)*time.Second/4*3 - 400*time.Millisecond},
	} {
		t.Run(test.name, func(t *testing.T) {
			testBeaconCfg := beaconCfg
			if test.slotSeconds != 0 {
				testBeaconCfg.SecondsPerSlot = test.slotSeconds
			}
			cfg := epbscfg.DefaultConfig()
			cfg.Enabled = true
			cfg.KeyPath = keyPath
			cfg.BidDelay = test.bidDelay
			cfg.BidPublishLead = test.publishLead

			resolved, _, err := prepareRuntimeConfig(cfg, &testBeaconCfg)
			require.NoError(t, err)
			require.Equal(t, test.want, resolved.BidDelay)
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
	cfg.BidPublishLead = 0
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
	cfg.BidPublishLead = 9 * time.Second
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
	runtimeCfg.MinProfitGwei = 16
	runtimeCfg.BidPublishLead = 0
	clock := eth_clock.NewMockEthereumClock(gomock.NewController(t))
	clock.EXPECT().GetCurrentSlot().Return(uint64(0)).AnyTimes()
	status := executionbuilder.NewEmbeddedBuilderStatus(true)
	highestBids := new(coordinatorHighestBidReader)
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
		HighestBids:      highestBids,
		Events:           beaconevents.NewEventEmitter(),
		Status:           status,
	}
	runtime, err := NewRuntime(runtimeCfg, deps)
	require.NoError(t, err)
	runtime.runner.observeOutcome(7, eladapter.ErrExecutionBusy)
	require.Equal(t, uint64(7), status.Snapshot().LastOutcomeSlot)
	require.Equal(t, executionbuilder.BuilderOutcomeExecutionBusy, status.Snapshot().LastOutcome)
	require.Equal(t, int(cfg.SlotsPerEpoch), runtime.runner.maxPending)
	require.Equal(t, runtimeCfg.BidDelay, runtime.runner.bidDelay)
	require.Equal(t, runtimeCfg.BidPublishLead, runtime.coordinator.bidPublishLead)
	require.Equal(t, runtimeCfg.MaxBidMargin, runtime.coordinator.maxBidMargin)
	require.Equal(t, runtimeCfg.MinProfitGwei, runtime.coordinator.minProfitGwei)
	require.Equal(t, highestBids, runtime.coordinator.highestBids)
	live, ok := runtime.runner.coordinator.(*LiveCoordinator)
	require.True(t, ok)
	require.Same(t, status, live.coordinator.status)
	require.Equal(t, runtimeCfg.CollateralWarningGwei, live.collateralWarningGwei)
	require.NotNil(t, runtime.coordinator.slotTime)

	derivedCfg := runtimeCfg
	derivedCfg.BidDelay = 0
	derivedCfg.BidPublishLead = 450 * time.Millisecond
	derivedCfg.RetryInterval = 250 * time.Millisecond
	deps.PendingDirectory = filepath.Join(t.TempDir(), "derived-pending")
	derivedRuntime, err := NewRuntime(derivedCfg, deps)
	require.NoError(t, err)
	require.Equal(t, 8550*time.Millisecond, derivedRuntime.runner.bidDelay)
	require.Equal(t, derivedCfg.BidPublishLead, derivedRuntime.coordinator.bidPublishLead)
	require.Equal(t, derivedCfg.RetryInterval, derivedRuntime.coordinator.retryInterval)
	require.Equal(t, derivedCfg.MaxBidMargin, derivedRuntime.coordinator.maxBidMargin)
	require.Equal(t, highestBids, derivedRuntime.coordinator.highestBids)
	wantSlotTime := time.Unix(123, 0)
	clock.EXPECT().GetSlotTime(uint64(17)).Return(wantSlotTime)
	require.Equal(t, wantSlotTime, derivedRuntime.coordinator.slotTime(17))

	blockedPath := filepath.Join(t.TempDir(), "not-a-directory")
	require.NoError(t, os.WriteFile(blockedPath, nil, 0o600))
	deps.PendingDirectory = blockedPath
	_, err = NewRuntime(runtimeCfg, deps)
	require.ErrorIs(t, err, ErrPendingPayloadStore)
}

func TestBuilderAttemptOutcomeClassifiesKnownFailures(t *testing.T) {
	for _, test := range []struct {
		name string
		err  error
		want string
	}{
		{name: "stale input", err: errors.Join(errors.New("resolve"), ErrSlotInputStale), want: executionbuilder.BuilderOutcomeStaleInput},
		{name: "input unavailable", err: ErrSlotInputUnavailable, want: executionbuilder.BuilderOutcomeInputUnavailable},
		{name: "execution busy", err: errors.Join(errors.New("assemble"), eladapter.ErrExecutionBusy), want: executionbuilder.BuilderOutcomeExecutionBusy},
		{name: "payload not ready", err: ErrPayloadNotReady, want: executionbuilder.BuilderOutcomePayloadNotReady},
		{name: "already tracked", err: ErrAuctionAlreadyTracked, want: executionbuilder.BuilderOutcomeAlreadyTracked},
		{name: "collateral exhausted", err: errors.Join(ErrSlotInputUnavailable, ErrBuilderCollateralExhausted), want: executionbuilder.BuilderOutcomeCollateralExhausted},
		{name: "outbid", err: ErrBidOutbid, want: executionbuilder.BuilderOutcomeOutbid},
		{name: "below min profit", err: ErrBidBelowMinProfit, want: executionbuilder.BuilderOutcomeBelowMinProfit},
		{name: "bid rejected", err: errLocalBidNotAccepted, want: executionbuilder.BuilderOutcomeBidRejected},
		{name: "no bid", err: errValidatedPreferencesAttemptNoBid, want: executionbuilder.BuilderOutcomeNoBid},
		{name: "unknown", err: errors.New("unknown"), want: executionbuilder.BuilderOutcomeFailed},
	} {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.want, builderAttemptOutcome(test.err))
		})
	}
}
