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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/builder/epbs/eladapter"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/gossip"
	"github.com/erigontech/erigon/cl/phase1/forkchoice"
	"github.com/erigontech/erigon/common"
)

type revealRunnerSigner struct{}

func (revealRunnerSigner) Pubkey() common.Bytes48 { return common.Bytes48{0: 1} }

func (revealRunnerSigner) SignBid(context.Context, common.Hash) (common.Bytes96, error) {
	return common.Bytes96{0: 2}, nil
}

func (revealRunnerSigner) SignEnvelope(context.Context, common.Hash) (common.Bytes96, error) {
	return common.Bytes96{0: 3}, nil
}

type revealBlockStore struct {
	mu            sync.Mutex
	blocks        map[common.Hash]*cltypes.SignedBeaconBlock
	envelopes     map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope
	getBlockCalls atomic.Int32
}

func (s *revealBlockStore) GetBlock(root common.Hash) (*cltypes.SignedBeaconBlock, bool) {
	s.getBlockCalls.Add(1)
	s.mu.Lock()
	defer s.mu.Unlock()
	block, ok := s.blocks[root]
	return block, ok
}

func (s *revealBlockStore) ReadEnvelopeFromDisk(root common.Hash) (*cltypes.SignedExecutionPayloadEnvelope, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	envelope := s.envelopes[root]
	if envelope == nil {
		return nil, errors.New("envelope not found")
	}
	return envelope.Clone().(*cltypes.SignedExecutionPayloadEnvelope), nil
}

func (s *revealBlockStore) storeEnvelope(envelope *cltypes.SignedExecutionPayloadEnvelope) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.envelopes[envelope.Message.BeaconBlockRoot] = envelope.Clone().(*cltypes.SignedExecutionPayloadEnvelope)
}

type persistedRevealProcessor struct {
	store    *revealBlockStore
	mismatch bool
	calls    atomic.Int32
}

func (p *persistedRevealProcessor) ProcessMessage(_ context.Context, _ *uint64, envelope *cltypes.SignedExecutionPayloadEnvelope) error {
	p.calls.Add(1)
	owned := envelope.Clone().(*cltypes.SignedExecutionPayloadEnvelope)
	if p.mismatch {
		owned.Signature[0] ^= 0xff
	}
	p.store.storeEnvelope(owned)
	return errors.New("index write failed after envelope persistence")
}

type recordingRevealPublisher struct {
	calls     atomic.Int32
	published chan struct{}
}

func (p *recordingRevealPublisher) Publish(context.Context, string, []byte) error {
	p.calls.Add(1)
	if p.published != nil {
		p.published <- struct{}{}
	}
	return nil
}

type blockingRevealProcessor struct {
	contextErr chan error
	started    chan struct{}
	startOnce  sync.Once
}

func (p *blockingRevealProcessor) ProcessMessage(ctx context.Context, _ *uint64, _ *cltypes.SignedExecutionPayloadEnvelope) error {
	if p.started != nil {
		p.startOnce.Do(func() { close(p.started) })
	}
	<-ctx.Done()
	p.contextErr <- ctx.Err()
	return ctx.Err()
}

type blockingEnvelopeSigner struct {
	revealRunnerSigner
	contextErr chan error
}

func (s *blockingEnvelopeSigner) SignEnvelope(ctx context.Context, _ common.Hash) (common.Bytes96, error) {
	<-ctx.Done()
	s.contextErr <- ctx.Err()
	return common.Bytes96{}, ctx.Err()
}

type failingRevealProcessor struct {
	calls atomic.Int32
}

func (p *failingRevealProcessor) ProcessMessage(context.Context, *uint64, *cltypes.SignedExecutionPayloadEnvelope) error {
	p.calls.Add(1)
	return errors.New("local processing failed")
}

type successfulRevealProcessor struct {
	calls atomic.Int32
}

func (p *successfulRevealProcessor) ProcessMessage(context.Context, *uint64, *cltypes.SignedExecutionPayloadEnvelope) error {
	p.calls.Add(1)
	return nil
}

type failingRevealPublisher struct {
	calls atomic.Int32
}

type revealHeadReader struct {
	mu    sync.Mutex
	node  forkchoice.ForkChoiceNode
	calls atomic.Int32
}

func (r *revealHeadReader) GetHeadNode() (forkchoice.ForkChoiceNode, error) {
	r.calls.Add(1)
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.node, nil
}

func (r *revealHeadReader) setRoot(root common.Hash) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.node.Root = root
}

type firstBlockingRevealProcessor struct {
	calls     atomic.Int32
	started   chan struct{}
	release   chan struct{}
	processed chan common.Hash
}

func (p *firstBlockingRevealProcessor) ProcessMessage(
	ctx context.Context,
	_ *uint64,
	envelope *cltypes.SignedExecutionPayloadEnvelope,
) error {
	call := p.calls.Add(1)
	p.processed <- envelope.Message.BeaconBlockRoot
	if call != 1 {
		return nil
	}
	close(p.started)
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-p.release:
		return nil
	}
}

func (p *failingRevealPublisher) Publish(context.Context, string, []byte) error {
	p.calls.Add(1)
	return errors.New("gossip failed")
}

func retainedRevealFixture(
	t *testing.T,
	clock LiveSlotClock,
	processor PayloadProcessor,
	publisher GossipPublisher,
	store *revealBlockStore,
	maxQueued int,
	retryInterval time.Duration,
	blobs ...*eladapter.BlobsBundle,
) (*revealRunner, revealRequest) {
	t.Helper()
	config := gloasCoordinatorConfig()
	input := validCoordinatorSlotInput(config)
	payload := validCoordinatorPayload(&config, input, big.NewInt(2_000_000_000))
	if len(blobs) != 0 {
		payload.BlobsBundle = blobs[0]
	}
	assembler := &coordinatorAssembler{
		payloadID: 1,
		payload:   payload,
	}
	coordinator := NewCoordinator(
		&config,
		revealRunnerSigner{},
		FixedMarginStrategy{Margin: 1},
		assembler,
		discardCoordinatorPublisher{},
		maxQueued,
	)
	signedBid, err := coordinator.RunSlot(t.Context(), input)
	require.NoError(t, err)
	signedBidRoot, err := signedBid.HashSSZ()
	require.NoError(t, err)
	block := cltypes.NewSignedBeaconBlock(&config, clparams.GloasVersion)
	block.Block.Slot = signedBid.Message.Slot
	block.Block.ParentRoot = signedBid.Message.ParentBlockRoot
	block.Block.Body.SignedExecutionPayloadBid = signedBid
	blockRoot, err := block.Block.HashSSZ()
	require.NoError(t, err)
	root := common.Hash(blockRoot)
	store.blocks[root] = block
	identity := PayloadIdentity{
		Slot: signedBid.Message.Slot, ParentBlockHash: signedBid.Message.ParentBlockHash,
		ParentBlockRoot: signedBid.Message.ParentBlockRoot, BlockHash: signedBid.Message.BlockHash,
	}
	runner := newRevealRunner(
		&config, clock, revealRunnerSigner{}, coordinator, store, processor, publisher, store, nil, retryInterval, maxQueued,
	)
	return runner, revealRequest{
		key:      revealKey{beaconBlockRoot: root, signedBidRoot: common.Hash(signedBidRoot)},
		identity: identity,
		slot:     signedBid.Message.Slot,
	}
}

type orderedBlobDataPreparer struct {
	order  *[]string
	bundle **eladapter.BlobsBundle
}

func (p orderedBlobDataPreparer) Prepare(
	_ context.Context,
	_ uint64,
	_ common.Hash,
	bundle *eladapter.BlobsBundle,
) (PreparedBlobData, error) {
	*p.bundle = bundle
	*p.order = append(*p.order, "prepare")
	return orderedPreparedBlobData{order: p.order}, nil
}

type orderedPreparedBlobData struct {
	order *[]string
}

func (p orderedPreparedBlobData) Store(context.Context) error {
	*p.order = append(*p.order, "store")
	return nil
}

func (p orderedPreparedBlobData) Publish(context.Context) error {
	*p.order = append(*p.order, "columns")
	return nil
}

type retryingBlobDataPreparer struct {
	prepareCalls atomic.Int32
	prepared     *retryingPreparedBlobData
}

func (p *retryingBlobDataPreparer) Prepare(context.Context, uint64, common.Hash, *eladapter.BlobsBundle) (PreparedBlobData, error) {
	p.prepareCalls.Add(1)
	return p.prepared, nil
}

type retryingPreparedBlobData struct {
	storeCalls    atomic.Int32
	publishCalls  atomic.Int32
	storeFailures int32
	pubFailures   int32
}

type blockingBlobDataPreparer struct {
	prepared *blockingPreparedBlobData
}

func (p *blockingBlobDataPreparer) Prepare(context.Context, uint64, common.Hash, *eladapter.BlobsBundle) (PreparedBlobData, error) {
	return p.prepared, nil
}

type blockingPreparedBlobData struct {
	started      chan struct{}
	contextErr   chan error
	storeOnce    sync.Once
	publishCalls atomic.Int32
}

func (p *blockingPreparedBlobData) Store(ctx context.Context) error {
	p.storeOnce.Do(func() { close(p.started) })
	<-ctx.Done()
	p.contextErr <- ctx.Err()
	return ctx.Err()
}

func (p *blockingPreparedBlobData) Publish(context.Context) error {
	p.publishCalls.Add(1)
	return nil
}

type rootRecordingBlobDataPreparer struct {
	mu    sync.Mutex
	roots []common.Hash
}

func (p *rootRecordingBlobDataPreparer) Prepare(
	_ context.Context,
	_ uint64,
	root common.Hash,
	_ *eladapter.BlobsBundle,
) (PreparedBlobData, error) {
	p.mu.Lock()
	p.roots = append(p.roots, root)
	p.mu.Unlock()
	return orderedPreparedBlobData{order: new([]string)}, nil
}

func (p *rootRecordingBlobDataPreparer) preparedRoots() []common.Hash {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]common.Hash(nil), p.roots...)
}

func (p *retryingPreparedBlobData) Store(context.Context) error {
	if p.storeCalls.Add(1) <= p.storeFailures {
		return errors.New("column store failed")
	}
	return nil
}

func (p *retryingPreparedBlobData) Publish(context.Context) error {
	if p.publishCalls.Add(1) <= p.pubFailures {
		return errors.New("column gossip failed")
	}
	return nil
}

type orderedRevealProcessor struct {
	order *[]string
}

func (p orderedRevealProcessor) ProcessMessage(context.Context, *uint64, *cltypes.SignedExecutionPayloadEnvelope) error {
	*p.order = append(*p.order, "process")
	return nil
}

type orderedRevealPublisher struct {
	order  *[]string
	topics *[]string
}

func (p orderedRevealPublisher) Publish(_ context.Context, topic string, _ []byte) error {
	*p.topics = append(*p.topics, topic)
	*p.order = append(*p.order, "envelope")
	return nil
}

func TestRevealRunnerPublishesEnvelopeBeforeBlobGossipAfterLocalAvailability(t *testing.T) {
	store := &revealBlockStore{
		blocks:    make(map[common.Hash]*cltypes.SignedBeaconBlock),
		envelopes: make(map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope),
	}
	order := make([]string, 0, 5)
	topics := make([]string, 0, 1)
	var preparedBundle *eladapter.BlobsBundle
	runner, request := retainedRevealFixture(
		t,
		revealTestClock{slot: 64},
		orderedRevealProcessor{order: &order},
		orderedRevealPublisher{order: &order, topics: &topics},
		store,
		1,
		time.Millisecond,
		validCoordinatorBlobsBundle(gloasCoordinatorConfig(), 1),
	)
	runner.blobData = orderedBlobDataPreparer{order: &order, bundle: &preparedBundle}
	runner.clock = fixedRevealDeadlineClock{revealTestClock: revealTestClock{slot: request.slot}, slotTime: time.Now()}

	require.NoError(t, runner.reveal(t.Context(), request))
	require.Equal(t, []string{"prepare", "store", "process", "envelope", "columns"}, order)
	require.NotNil(t, preparedBundle)
	require.Len(t, preparedBundle.Blobs, 1)
	require.Equal(t, []string{gossip.TopicNameExecutionPayload}, topics)
}

func TestRevealRunnerRetriesBlobSideEffectsWithoutRepreparingOrReprocessingEnvelope(t *testing.T) {
	store := &revealBlockStore{
		blocks:    make(map[common.Hash]*cltypes.SignedBeaconBlock),
		envelopes: make(map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope),
	}
	processor := new(successfulRevealProcessor)
	publisher := new(recordingRevealPublisher)
	runner, request := retainedRevealFixture(
		t,
		revealTestClock{slot: 64},
		processor,
		publisher,
		store,
		1,
		time.Millisecond,
		validCoordinatorBlobsBundle(gloasCoordinatorConfig(), 1),
	)
	prepared := &retryingPreparedBlobData{storeFailures: 1, pubFailures: 1}
	preparer := &retryingBlobDataPreparer{prepared: prepared}
	runner.blobData = preparer
	runner.clock = fixedRevealDeadlineClock{revealTestClock: revealTestClock{slot: request.slot}, slotTime: time.Now()}

	require.NoError(t, runner.reveal(t.Context(), request))
	require.Equal(t, int32(1), preparer.prepareCalls.Load())
	require.Equal(t, int32(2), prepared.storeCalls.Load())
	require.Equal(t, int32(1), processor.calls.Load())
	require.Equal(t, int32(2), prepared.publishCalls.Load())
	require.Equal(t, int32(1), publisher.calls.Load())
}

func TestRevealRunnerBlobStorageStopsAtPayloadDeadline(t *testing.T) {
	store := &revealBlockStore{
		blocks:    make(map[common.Hash]*cltypes.SignedBeaconBlock),
		envelopes: make(map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope),
	}
	processor := new(successfulRevealProcessor)
	publisher := new(recordingRevealPublisher)
	runner, request := retainedRevealFixture(
		t,
		revealTestClock{slot: 64},
		processor,
		publisher,
		store,
		1,
		time.Millisecond,
		validCoordinatorBlobsBundle(gloasCoordinatorConfig(), 1),
	)
	prepared := &blockingPreparedBlobData{started: make(chan struct{}), contextErr: make(chan error, 1)}
	runner.blobData = &blockingBlobDataPreparer{prepared: prepared}
	deadlineOffset := time.Duration(runner.beaconCfg.SecondsPerSlot) * time.Second *
		time.Duration(runner.beaconCfg.PayloadDueBps) / time.Duration(clparams.BpsFactor)
	runner.clock = fixedRevealDeadlineClock{
		revealTestClock: revealTestClock{slot: request.slot},
		slotTime:        time.Now().Add(-deadlineOffset + 20*time.Millisecond),
	}

	result := make(chan error, 1)
	go func() { result <- runner.reveal(t.Context(), request) }()
	<-prepared.started

	require.ErrorIs(t, <-result, ErrRevealExpired)
	require.ErrorIs(t, <-prepared.contextErr, context.DeadlineExceeded)
	require.Zero(t, processor.calls.Load())
	require.Zero(t, prepared.publishCalls.Load())
	require.Zero(t, publisher.calls.Load())
}

type revealTestClock struct {
	slot uint64
}

func (c revealTestClock) GetCurrentSlot() uint64 {
	return c.slot
}

func (revealTestClock) GetSlotTime(uint64) time.Time {
	return time.Time{}
}

func (revealTestClock) GenesisValidatorsRoot() common.Hash {
	return common.Hash{}
}

func TestRevealRunnerPrunesExpiredTrackingBeforeBlockLookup(t *testing.T) {
	runner := newRevealRunner(
		nil,
		revealTestClock{slot: 11},
		nil,
		nil,
		&runtimeAcceptedBlockReader{blocks: make(map[common.Hash]*cltypes.SignedBeaconBlock)},
		nil,
		nil,
		nil,
		nil,
		time.Second,
		1,
	)
	runner.tracked[revealKey{beaconBlockRoot: common.Hash{1}}] = revealTracking{slot: 10}

	require.False(t, runner.SubmitAcceptedBlock(common.Hash{2}))
	require.Empty(t, runner.tracked)
}

func TestRevealRunnerContinuesAfterExactEnvelopeWasPersisted(t *testing.T) {
	store := &revealBlockStore{
		blocks:    make(map[common.Hash]*cltypes.SignedBeaconBlock),
		envelopes: make(map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope),
	}
	processor := &persistedRevealProcessor{store: store}
	publisher := new(recordingRevealPublisher)
	runner, request := retainedRevealFixture(
		t,
		revealTestClock{slot: 64},
		processor,
		publisher,
		store,
		1,
		time.Millisecond,
	)
	runner.clock = fixedRevealDeadlineClock{revealTestClock: revealTestClock{slot: request.slot}, slotTime: time.Now()}
	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()

	require.NoError(t, runner.reveal(ctx, request))
	require.Equal(t, int32(1), processor.calls.Load())
	require.Equal(t, int32(1), publisher.calls.Load())
}

func TestRevealRunnerDoesNotReconcileMismatchedPersistedEnvelope(t *testing.T) {
	store := &revealBlockStore{
		blocks:    make(map[common.Hash]*cltypes.SignedBeaconBlock),
		envelopes: make(map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope),
	}
	processor := &persistedRevealProcessor{store: store, mismatch: true}
	publisher := new(recordingRevealPublisher)
	runner, request := retainedRevealFixture(
		t,
		revealTestClock{slot: 64},
		processor,
		publisher,
		store,
		1,
		time.Millisecond,
	)
	runner.clock = fixedRevealDeadlineClock{revealTestClock: revealTestClock{slot: request.slot}, slotTime: time.Now()}
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()

	require.ErrorIs(t, runner.reveal(ctx, request), context.DeadlineExceeded)
	require.Positive(t, processor.calls.Load())
	require.Zero(t, publisher.calls.Load())
}

func TestRevealRunnerDeadlineCancelsBlockingProcessorAndRunJoins(t *testing.T) {
	store := &revealBlockStore{
		blocks:    make(map[common.Hash]*cltypes.SignedBeaconBlock),
		envelopes: make(map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope),
	}
	processor := &blockingRevealProcessor{contextErr: make(chan error, 1)}
	runner, request := retainedRevealFixture(
		t,
		revealTestClock{slot: 64},
		processor,
		new(recordingRevealPublisher),
		store,
		1,
		time.Second,
	)
	runner.beaconCfg.SecondsPerSlot = 1
	runner.beaconCfg.PayloadDueBps = 200
	runner.clock = fixedRevealDeadlineClock{revealTestClock: revealTestClock{slot: request.slot}, slotTime: time.Now()}
	runner.requests <- request
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() {
		runner.Run(ctx)
		close(done)
	}()

	select {
	case err := <-processor.contextErr:
		require.ErrorIs(t, err, context.DeadlineExceeded)
	case <-time.After(500 * time.Millisecond):
		cancel()
		<-done
		t.Fatal("payload deadline did not cancel blocking processor")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("reveal runner did not join after cancellation")
	}
}

func TestRevealRunnerDoesNotRetryAfterDeadline(t *testing.T) {
	store := &revealBlockStore{
		blocks:    make(map[common.Hash]*cltypes.SignedBeaconBlock),
		envelopes: make(map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope),
	}
	processor := new(failingRevealProcessor)
	runner, request := retainedRevealFixture(
		t,
		revealTestClock{slot: 64},
		processor,
		new(recordingRevealPublisher),
		store,
		1,
		100*time.Millisecond,
	)
	runner.beaconCfg.SecondsPerSlot = 1
	runner.beaconCfg.PayloadDueBps = 200
	runner.clock = fixedRevealDeadlineClock{revealTestClock: revealTestClock{slot: request.slot}, slotTime: time.Now()}

	err := runner.reveal(t.Context(), request)
	require.ErrorIs(t, err, ErrRevealExpired)
	require.Equal(t, int32(1), processor.calls.Load())
}

func TestRevealRunnerGossipsImmediatelyAfterLocalSuccessWithoutPostDeadlineRetry(t *testing.T) {
	store := &revealBlockStore{
		blocks:    make(map[common.Hash]*cltypes.SignedBeaconBlock),
		envelopes: make(map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope),
	}
	processor := new(successfulRevealProcessor)
	publisher := new(failingRevealPublisher)
	runner, request := retainedRevealFixture(
		t,
		revealTestClock{slot: 64},
		processor,
		publisher,
		store,
		1,
		100*time.Millisecond,
	)
	runner.beaconCfg.SecondsPerSlot = 1
	runner.beaconCfg.PayloadDueBps = 200
	runner.clock = fixedRevealDeadlineClock{revealTestClock: revealTestClock{slot: request.slot}, slotTime: time.Now()}

	err := runner.reveal(t.Context(), request)
	require.ErrorIs(t, err, ErrRevealExpired)
	require.Equal(t, int32(1), processor.calls.Load())
	require.Equal(t, int32(1), publisher.calls.Load())
}

func TestRevealRunnerDeadlineOwnsEnvelopeSigning(t *testing.T) {
	store := &revealBlockStore{
		blocks:    make(map[common.Hash]*cltypes.SignedBeaconBlock),
		envelopes: make(map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope),
	}
	runner, request := retainedRevealFixture(
		t,
		revealTestClock{slot: 64},
		new(failingRevealProcessor),
		new(recordingRevealPublisher),
		store,
		1,
		time.Second,
	)
	runner.beaconCfg.SecondsPerSlot = 1
	runner.beaconCfg.PayloadDueBps = 200
	runner.clock = fixedRevealDeadlineClock{revealTestClock: revealTestClock{slot: request.slot}, slotTime: time.Now()}
	signer := &blockingEnvelopeSigner{contextErr: make(chan error, 1)}
	runner.signer = signer

	require.ErrorIs(t, runner.reveal(t.Context(), request), ErrRevealExpired)
	require.ErrorIs(t, <-signer.contextErr, context.DeadlineExceeded)
}

func TestRevealRunnerParentCancellationIsNotRevealExpiry(t *testing.T) {
	store := &revealBlockStore{
		blocks:    make(map[common.Hash]*cltypes.SignedBeaconBlock),
		envelopes: make(map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope),
	}
	processor := &blockingRevealProcessor{contextErr: make(chan error, 1), started: make(chan struct{})}
	runner, request := retainedRevealFixture(
		t,
		revealTestClock{slot: 64},
		processor,
		new(recordingRevealPublisher),
		store,
		1,
		time.Second,
	)
	runner.clock = fixedRevealDeadlineClock{revealTestClock: revealTestClock{slot: request.slot}, slotTime: time.Now()}
	ctx, cancel := context.WithCancel(t.Context())
	result := make(chan error, 1)
	go func() { result <- runner.reveal(ctx, request) }()
	<-processor.started
	cancel()

	err := <-result
	require.ErrorIs(t, err, context.Canceled)
	require.NotErrorIs(t, err, ErrRevealExpired)
}

type fixedRevealDeadlineClock struct {
	revealTestClock
	slotTime time.Time
}

func (c fixedRevealDeadlineClock) GetSlotTime(uint64) time.Time {
	return c.slotTime
}

type mutableRevealClock struct {
	slot atomic.Uint64
}

func (c *mutableRevealClock) GetCurrentSlot() uint64 { return c.slot.Load() }

func (*mutableRevealClock) GetSlotTime(uint64) time.Time { return time.Now() }

func (*mutableRevealClock) GenesisValidatorsRoot() common.Hash { return common.Hash{} }

func TestRevealRunnerSlotPruneDoesNotReleaseQueuedCapacity(t *testing.T) {
	clock := new(mutableRevealClock)
	clock.slot.Store(64)
	store := &revealBlockStore{
		blocks:    make(map[common.Hash]*cltypes.SignedBeaconBlock),
		envelopes: make(map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope),
	}
	runner, oldRequest := retainedRevealFixture(
		t,
		clock,
		new(failingRevealProcessor),
		new(recordingRevealPublisher),
		store,
		1,
		time.Second,
	)
	require.True(t, runner.SubmitAcceptedBlock(oldRequest.key.beaconBlockRoot))

	runner.coordinator.PruneExpiredBeforeSlot(oldRequest.slot + 1)
	nextInput := coordinatorInputAtSlot(validCoordinatorSlotInput(*runner.beaconCfg), oldRequest.slot+1)
	nextPayload := validCoordinatorPayload(runner.beaconCfg, nextInput, big.NewInt(2_000_000_000))
	runner.coordinator.assembler = &coordinatorAssembler{payloadID: 2, payload: nextPayload}
	nextBid, err := runner.coordinator.RunSlot(t.Context(), nextInput)
	require.NoError(t, err)
	nextBidRoot, err := nextBid.HashSSZ()
	require.NoError(t, err)
	nextBlock := cltypes.NewSignedBeaconBlock(runner.beaconCfg, clparams.GloasVersion)
	nextBlock.Block.Slot = nextBid.Message.Slot
	nextBlock.Block.ParentRoot = nextBid.Message.ParentBlockRoot
	nextBlock.Block.Body.SignedExecutionPayloadBid = nextBid
	nextBlockRoot, err := nextBlock.Block.HashSSZ()
	require.NoError(t, err)
	nextRoot := common.Hash(nextBlockRoot)
	store.blocks[nextRoot] = nextBlock
	clock.slot.Store(nextInput.Slot)

	result := make(chan bool, 1)
	go func() { result <- runner.SubmitAcceptedBlock(nextRoot) }()
	select {
	case accepted := <-result:
		require.False(t, accepted)
	case <-time.After(200 * time.Millisecond):
		t.Fatal("enqueue blocked after slot prune released tracking for queued work")
	}
	require.Contains(t, runner.tracked, oldRequest.key)
	nextKey := revealKey{beaconBlockRoot: nextRoot, signedBidRoot: common.Hash(nextBidRoot)}
	require.NotContains(t, runner.tracked, nextKey)
}

func TestRevealRunnerFullQueueRollsBackTracking(t *testing.T) {
	store := &revealBlockStore{
		blocks:    make(map[common.Hash]*cltypes.SignedBeaconBlock),
		envelopes: make(map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope),
	}
	runner, request := retainedRevealFixture(
		t,
		revealTestClock{slot: 64},
		new(failingRevealProcessor),
		new(recordingRevealPublisher),
		store,
		1,
		time.Second,
	)
	runner.requests <- revealRequest{}

	require.False(t, runner.SubmitAcceptedBlock(request.key.beaconBlockRoot))
	require.NotContains(t, runner.tracked, request.key)
	require.Zero(t, runner.activeCount)
}

func TestRevealRunnerCompletedSameSlotDedupeStaysBoundedUntilNextSlot(t *testing.T) {
	clock := new(mutableRevealClock)
	store := &revealBlockStore{
		blocks:    make(map[common.Hash]*cltypes.SignedBeaconBlock),
		envelopes: make(map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope),
	}
	runner, request := retainedRevealFixture(
		t,
		clock,
		new(successfulRevealProcessor),
		new(recordingRevealPublisher),
		store,
		1,
		time.Second,
	)
	clock.slot.Store(request.slot)
	require.True(t, runner.SubmitAcceptedBlock(request.key.beaconBlockRoot))
	queued := <-runner.requests
	runner.finishRequest(queued.key)
	require.Len(t, runner.tracked, 1)
	require.Zero(t, runner.activeCount)

	secondBlock := store.blocks[request.key.beaconBlockRoot]
	secondBlock.Block.StateRoot[0] ^= 1
	secondBlockRoot, err := secondBlock.Block.HashSSZ()
	require.NoError(t, err)
	secondRoot := common.Hash(secondBlockRoot)
	store.blocks[secondRoot] = secondBlock

	require.False(t, runner.SubmitAcceptedBlock(secondRoot))
	require.Len(t, runner.tracked, 1)
	require.Zero(t, runner.activeCount)

	clock.slot.Store(request.slot + 1)
	require.True(t, runner.SubmitAcceptedBlock(secondRoot))
	require.Len(t, runner.tracked, 1)
	require.Equal(t, 1, runner.activeCount)
}

func TestRevealRunnerReconcilesRejectedCanonicalHeadAfterCapacityRelease(t *testing.T) {
	clock := new(mutableRevealClock)
	store := &revealBlockStore{
		blocks:    make(map[common.Hash]*cltypes.SignedBeaconBlock),
		envelopes: make(map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope),
	}
	processor := &firstBlockingRevealProcessor{
		started:   make(chan struct{}),
		release:   make(chan struct{}),
		processed: make(chan common.Hash, 2),
	}
	publisher := &recordingRevealPublisher{published: make(chan struct{}, 2)}
	runner, firstRequest := retainedRevealFixture(
		t,
		clock,
		processor,
		publisher,
		store,
		1,
		5*time.Millisecond,
		validCoordinatorBlobsBundle(gloasCoordinatorConfig(), 1),
	)
	blobData := new(rootRecordingBlobDataPreparer)
	runner.blobData = blobData
	clock.slot.Store(firstRequest.slot)
	head := new(revealHeadReader)
	runner.head = head
	require.True(t, runner.SubmitAcceptedBlock(firstRequest.key.beaconBlockRoot))

	secondBlock := store.blocks[firstRequest.key.beaconBlockRoot]
	secondBlock.Block.StateRoot[0] ^= 1
	secondBlockRoot, err := secondBlock.Block.HashSSZ()
	require.NoError(t, err)
	secondRoot := common.Hash(secondBlockRoot)
	store.blocks[secondRoot] = secondBlock
	require.False(t, runner.SubmitAcceptedBlock(secondRoot))
	head.setRoot(secondRoot)

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() {
		runner.Run(ctx)
		close(done)
	}()
	defer func() {
		cancel()
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Error("reveal runner did not join")
		}
	}()

	select {
	case <-processor.started:
	case <-time.After(time.Second):
		t.Fatal("first reveal did not start")
	}
	require.Eventually(t, func() bool { return head.calls.Load() > 0 }, 100*time.Millisecond, time.Millisecond)
	require.Eventually(t, func() bool { return store.getBlockCalls.Load() > 2 }, 100*time.Millisecond, time.Millisecond)
	require.Equal(t, int32(1), processor.calls.Load())
	close(processor.release)

	for range 2 {
		select {
		case <-publisher.published:
		case <-time.After(time.Second):
			t.Fatal("canonical head was not gossiped after capacity release")
		}
	}
	require.Equal(t, firstRequest.key.beaconBlockRoot, <-processor.processed)
	require.Equal(t, secondRoot, <-processor.processed)
	require.Eventually(t, func() bool {
		runner.mu.Lock()
		defer runner.mu.Unlock()
		_, firstTracked := runner.tracked[firstRequest.key]
		secondKey := revealKey{beaconBlockRoot: secondRoot, signedBidRoot: firstRequest.key.signedBidRoot}
		tracking, secondTracked := runner.tracked[secondKey]
		return len(runner.tracked) == 1 && !firstTracked && secondTracked && !tracking.active && runner.activeCount == 0
	}, time.Second, time.Millisecond)

	headCalls := head.calls.Load()
	blockReads := store.getBlockCalls.Load()
	require.Eventually(t, func() bool { return head.calls.Load() >= headCalls+3 }, 100*time.Millisecond, time.Millisecond)
	require.Equal(t, int32(2), processor.calls.Load())
	require.Equal(t, int32(2), publisher.calls.Load())
	require.Equal(t, []common.Hash{firstRequest.key.beaconBlockRoot, secondRoot}, blobData.preparedRoots())
	require.Equal(t, blockReads, store.getBlockCalls.Load())

	reorgRoot := common.Hash{0: 0xff}
	head.setRoot(reorgRoot)
	require.Eventually(t, func() bool { return store.getBlockCalls.Load() > blockReads }, 100*time.Millisecond, time.Millisecond)
}
