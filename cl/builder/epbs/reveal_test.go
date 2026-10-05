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
	"math"
	"math/big"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/beacon/beaconevents"
	"github.com/erigontech/erigon/cl/builder/epbs/eladapter"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/gossip"
	"github.com/erigontech/erigon/cl/phase1/forkchoice"
	"github.com/erigontech/erigon/cl/phase1/forkchoice/mock_services"
	"github.com/erigontech/erigon/cl/phase1/network/services"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
)

type revealRunnerSigner struct{}

func (revealRunnerSigner) Pubkey() common.Bytes48 { return common.Bytes48{0: 1} }

func (revealRunnerSigner) SignBid(context.Context, common.Hash) (common.Bytes96, error) {
	return common.Bytes96{0: 2}, nil
}

func (revealRunnerSigner) SignEnvelope(context.Context, common.Hash) (common.Bytes96, error) {
	return common.Bytes96{0: 3}, nil
}

type countingEnvelopeSigner struct {
	revealRunnerSigner
	calls atomic.Int32
}

func (s *countingEnvelopeSigner) SignEnvelope(context.Context, common.Hash) (common.Bytes96, error) {
	s.calls.Add(1)
	return common.Bytes96{0: 3}, nil
}

type revealBlockStore struct {
	mu            sync.Mutex
	blocks        map[common.Hash]*cltypes.SignedBeaconBlock
	envelopes     map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope
	getBlockCalls atomic.Int32
	envelopeReads atomic.Int32
}

func newRevealBlockStore() *revealBlockStore {
	return &revealBlockStore{
		blocks:    make(map[common.Hash]*cltypes.SignedBeaconBlock),
		envelopes: make(map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope),
	}
}

func (s *revealBlockStore) GetBlock(root common.Hash) (*cltypes.SignedBeaconBlock, bool) {
	s.getBlockCalls.Add(1)
	s.mu.Lock()
	defer s.mu.Unlock()
	block, ok := s.blocks[root]
	return block, ok
}

func (s *revealBlockStore) ReadEnvelopeFromDisk(root common.Hash) (*cltypes.SignedExecutionPayloadEnvelope, error) {
	s.envelopeReads.Add(1)
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
	missing  bool
	err      error
	calls    atomic.Int32
}

func (p *persistedRevealProcessor) ProcessMessage(_ context.Context, _ *uint64, envelope *cltypes.SignedExecutionPayloadEnvelope) error {
	p.calls.Add(1)
	if !p.missing {
		owned := envelope.Clone().(*cltypes.SignedExecutionPayloadEnvelope)
		if p.mismatch {
			owned.Signature[0] ^= 0xff
		}
		p.store.storeEnvelope(owned)
	}
	if p.err != nil {
		return p.err
	}
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
	blockedFor chan time.Duration
	startOnce  sync.Once
}

func (p *blockingRevealProcessor) ProcessMessage(ctx context.Context, _ *uint64, _ *cltypes.SignedExecutionPayloadEnvelope) error {
	startedAt := time.Now()
	if p.started != nil {
		p.startOnce.Do(func() { close(p.started) })
	}
	<-ctx.Done()
	if p.blockedFor != nil {
		p.blockedFor <- time.Since(startedAt)
	}
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

type canceledRevealProcessor struct {
	calls atomic.Int32
}

func (p *canceledRevealProcessor) ProcessMessage(context.Context, *uint64, *cltypes.SignedExecutionPayloadEnvelope) error {
	p.calls.Add(1)
	return context.Canceled
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

func (r *revealHeadReader) GetHeadNode() (forkchoice.ForkChoiceNode, uint64, error) {
	r.calls.Add(1)
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.node, 0, nil
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

type releaseBlockedRevealProcessor struct {
	calls       atomic.Int32
	started     chan struct{}
	release     chan struct{}
	releaseOnce sync.Once
}

func (p *releaseBlockedRevealProcessor) ProcessMessage(context.Context, *uint64, *cltypes.SignedExecutionPayloadEnvelope) error {
	if p.calls.Add(1) == 1 {
		close(p.started)
		<-p.release
	}
	return nil
}

func (p *releaseBlockedRevealProcessor) unblock() {
	p.releaseOnce.Do(func() { close(p.release) })
}

type revealLogCaptureHandler struct {
	records chan<- *log.Record
}

func (h revealLogCaptureHandler) Log(record *log.Record) error {
	if !strings.HasPrefix(record.Msg, "Embedded builder payload") {
		return nil
	}
	select {
	case h.records <- record:
	default:
	}
	return nil
}

func (revealLogCaptureHandler) Enabled(context.Context, log.Lvl) bool { return true }

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

type selectiveBlockingBlobDataPreparer struct {
	blockedRoot common.Hash
	blocked     PreparedBlobData
	prepared    chan common.Hash
}

type heldPreparedBlobData struct {
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func (p *heldPreparedBlobData) Store(ctx context.Context) error {
	p.once.Do(func() { close(p.started) })
	<-p.release
	return ctx.Err()
}

func (*heldPreparedBlobData) Publish(context.Context) error { return nil }

func (p *selectiveBlockingBlobDataPreparer) Prepare(
	_ context.Context,
	_ uint64,
	root common.Hash,
	_ *eladapter.BlobsBundle,
) (PreparedBlobData, error) {
	p.prepared <- root
	if root == p.blockedRoot {
		return p.blocked, nil
	}
	return orderedPreparedBlobData{order: new([]string)}, nil
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

func TestRevealRunnerValidatesLocallyBeforeBlobGossipAndPersistence(t *testing.T) {
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
		validCoordinatorBlobsBundle(t, gloasCoordinatorConfig(), 1),
	)
	runner.blobData = orderedBlobDataPreparer{order: &order, bundle: &preparedBundle}
	runner.clock = fixedRevealDeadlineClock{revealTestClock: revealTestClock{slot: request.slot}, slotTime: time.Now()}

	require.NoError(t, runner.reveal(t.Context(), request))
	require.Equal(t, []string{"prepare", "process", "envelope", "columns", "store"}, order)
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
		validCoordinatorBlobsBundle(t, gloasCoordinatorConfig(), 1),
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

func TestRevealRunnerBlobStorageStopsAtPayloadDeadlineAfterNetworkReveal(t *testing.T) {
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
		validCoordinatorBlobsBundle(t, gloasCoordinatorConfig(), 1),
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
	require.Equal(t, int32(1), processor.calls.Load())
	require.Equal(t, int32(1), prepared.publishCalls.Load())
	require.Equal(t, int32(1), publisher.calls.Load())
}

func TestRevealRunnerDoesNotPublishBlobRevealBeforeLocalValidation(t *testing.T) {
	store := &revealBlockStore{
		blocks:    make(map[common.Hash]*cltypes.SignedBeaconBlock),
		envelopes: make(map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope),
	}
	processor := new(failingRevealProcessor)
	publisher := new(recordingRevealPublisher)
	runner, request := retainedRevealFixture(
		t,
		revealTestClock{slot: 64},
		processor,
		publisher,
		store,
		1,
		100*time.Millisecond,
		validCoordinatorBlobsBundle(t, gloasCoordinatorConfig(), 1),
	)
	prepared := new(retryingPreparedBlobData)
	runner.blobData = &retryingBlobDataPreparer{prepared: prepared}
	runner.beaconCfg.SecondsPerSlot = 1
	runner.beaconCfg.PayloadDueBps = 200
	runner.clock = fixedRevealDeadlineClock{revealTestClock: revealTestClock{slot: request.slot}, slotTime: time.Now()}

	require.ErrorIs(t, runner.reveal(t.Context(), request), ErrRevealExpired)
	require.Equal(t, int32(1), processor.calls.Load())
	require.Zero(t, publisher.calls.Load())
	require.Zero(t, prepared.publishCalls.Load())
	require.Zero(t, prepared.storeCalls.Load())
}

func TestRevealRunnerGossipsBlobRevealWhileColumnStoreFails(t *testing.T) {
	processor := new(successfulRevealProcessor)
	publisher := new(recordingRevealPublisher)
	runner, request := retainedRevealFixture(
		t,
		revealTestClock{slot: 64},
		processor,
		publisher,
		newRevealBlockStore(),
		1,
		10*time.Millisecond,
		validCoordinatorBlobsBundle(t, gloasCoordinatorConfig(), 1),
	)
	prepared := &retryingPreparedBlobData{storeFailures: math.MaxInt32}
	runner.blobData = &retryingBlobDataPreparer{prepared: prepared}
	runner.beaconCfg.SecondsPerSlot = 1
	runner.beaconCfg.PayloadDueBps = 2000
	runner.clock = fixedRevealDeadlineClock{revealTestClock: revealTestClock{slot: request.slot}, slotTime: time.Now()}

	err := runner.reveal(t.Context(), request)
	require.ErrorIs(t, err, ErrRevealExpired)
	require.ErrorContains(t, err, "column store failed")
	require.Equal(t, int32(1), processor.calls.Load())
	require.Equal(t, int32(1), publisher.calls.Load())
	require.Equal(t, int32(1), prepared.publishCalls.Load())
	require.Greater(t, prepared.storeCalls.Load(), int32(1))
}

// importGatedForkChoice hides the beacon block until it is imported and closes validated when an
// envelope admission finishes as seen.
type importGatedForkChoice struct {
	*mock_services.ForkChoiceStorageMock
	imported      atomic.Bool
	validated     chan struct{}
	validatedOnce sync.Once
}

func newImportGatedForkChoice(t *testing.T) *importGatedForkChoice {
	return &importGatedForkChoice{ForkChoiceStorageMock: mock_services.NewForkChoiceStorageMock(t), validated: make(chan struct{})}
}

func (f *importGatedForkChoice) GetBlock(root common.Hash) (*cltypes.SignedBeaconBlock, bool) {
	if !f.imported.Load() {
		return nil, false
	}
	return f.ForkChoiceStorageMock.GetBlock(root)
}

func (f *importGatedForkChoice) FinishExecutionPayloadEnvelopeForGossip(token forkchoice.ExecutionPayloadEnvelopeAdmissionToken, seen bool) {
	f.ForkChoiceStorageMock.FinishExecutionPayloadEnvelopeForGossip(token, seen)
	if seen {
		f.validatedOnce.Do(func() { close(f.validated) })
	}
}

// importAfterFirstSubmission imports the block right after the first local submission, as when
// the reveal starts on the block_gossip event before fork choice has imported the block. Later
// submissions wait for retryGate.
type importAfterFirstSubmission struct {
	service   PayloadProcessor
	fc        *importGatedForkChoice
	retryGate <-chan struct{}
	calls     atomic.Int32
	errs      []error
}

func (p *importAfterFirstSubmission) ProcessMessage(ctx context.Context, slot *uint64, envelope *cltypes.SignedExecutionPayloadEnvelope) error {
	if p.calls.Add(1) > 1 {
		select {
		case <-p.retryGate:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	err := p.service.ProcessMessage(ctx, slot, envelope)
	p.fc.imported.Store(true)
	p.errs = append(p.errs, err)
	return err
}

// payloadServiceRevealFixture reveals a one-blob payload through the execution payload service,
// with the block imported only after the first local submission.
func payloadServiceRevealFixture(
	t *testing.T,
	fc *importGatedForkChoice,
	retryGate <-chan struct{},
	prepared *retryingPreparedBlobData,
) (*revealRunner, revealRequest, *importAfterFirstSubmission, *recordingRevealPublisher) {
	t.Helper()
	cfg := gloasCoordinatorConfig()
	processor := &importAfterFirstSubmission{fc: fc, retryGate: retryGate}
	store := newRevealBlockStore()
	publisher := new(recordingRevealPublisher)
	runner, request := retainedRevealFixture(
		t,
		revealTestClock{slot: 64},
		processor,
		publisher,
		store,
		1,
		10*time.Millisecond,
		validCoordinatorBlobsBundle(t, cfg, 1),
	)
	fc.Blocks[request.key.beaconBlockRoot] = store.blocks[request.key.beaconBlockRoot]
	processor.service = services.NewExecutionPayloadService(t.Context(), fc, &cfg, beaconevents.NewEventEmitter())
	runner.persisted = fc
	runner.blobData = &retryingBlobDataPreparer{prepared: prepared}
	now := time.Now()
	runner.clock = fixedRevealDeadlineClock{revealTestClock: revealTestClock{slot: request.slot}, slotTime: now}
	deadline, ok := payloadRevealDeadline(runner.clock, runner.beaconCfg, request.slot)
	require.True(t, ok)
	runner.clock = fixedRevealDeadlineClock{
		revealTestClock: revealTestClock{slot: request.slot},
		slotTime:        now.Add(1500*time.Millisecond - deadline.Sub(now)),
	}
	return runner, request, processor, publisher
}

func TestRevealRunnerRevealsBlobPayloadSubmittedBeforeBlockImport(t *testing.T) {
	prepared := new(retryingPreparedBlobData)
	fc := newImportGatedForkChoice(t)
	var waitingForColumns atomic.Int32
	fc.OnExecutionPayloadAtFn = func(context.Context, *cltypes.SignedExecutionPayloadEnvelope, bool, bool, time.Time) error {
		if prepared.storeCalls.Load() == 0 {
			waitingForColumns.Add(1)
			return forkchoice.ErrEIP7594ColumnDataNotAvailable
		}
		return nil
	}
	runner, request, processor, publisher := payloadServiceRevealFixture(t, fc, fc.validated, prepared)

	require.NoError(t, runner.reveal(t.Context(), request))
	require.Equal(t, int32(2), processor.calls.Load())
	require.ErrorIs(t, processor.errs[1], services.ErrExecutionPayloadEnvelopeAlreadySeen)
	require.Equal(t, int32(1), waitingForColumns.Load())
	require.Equal(t, int32(1), publisher.calls.Load())
	require.Equal(t, int32(1), prepared.publishCalls.Load())
	require.Equal(t, int32(1), prepared.storeCalls.Load())
}

func TestRevealRunnerAcceptsEnvelopeSeenWhileWaitingForAdmission(t *testing.T) {
	prepared := new(retryingPreparedBlobData)
	fc := newImportGatedForkChoice(t)
	workerValidating := make(chan struct{})
	revealClaiming := make(chan struct{})
	var workerOnce, claimOnce sync.Once
	fc.OnExecutionPayloadAtFn = func(ctx context.Context, _ *cltypes.SignedExecutionPayloadEnvelope, _, _ bool, _ time.Time) error {
		workerOnce.Do(func() { close(workerValidating) })
		select {
		case <-revealClaiming:
		case <-ctx.Done():
			return ctx.Err()
		}
		return forkchoice.ErrEIP7594ColumnDataNotAvailable
	}
	fc.ClaimExecutionPayloadEnvelopeForGossipFunc = func(ctx context.Context, root common.Hash, builderIndex uint64) (forkchoice.ExecutionPayloadEnvelopeAdmissionToken, error) {
		claimOnce.Do(func() { close(revealClaiming) })
		return fc.EnvelopeGossipAdmissions.Claim(ctx, root, builderIndex)
	}
	runner, request, processor, publisher := payloadServiceRevealFixture(t, fc, workerValidating, prepared)

	require.NoError(t, runner.reveal(t.Context(), request))
	require.Equal(t, int32(2), processor.calls.Load())
	require.ErrorIs(t, processor.errs[1], forkchoice.ErrExecutionPayloadEnvelopeAlreadySeen)
	require.Equal(t, int32(1), publisher.calls.Load())
	require.Equal(t, int32(1), prepared.publishCalls.Load())
	require.Equal(t, int32(1), prepared.storeCalls.Load())
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

func TestRevealRunnerComparesPersistedEnvelopeWhenAdmissionNeedsLookup(t *testing.T) {
	for _, test := range []struct {
		name     string
		mismatch bool
		missing  bool
		accepted bool
	}{
		{name: "matching bytes", accepted: true},
		{name: "different bytes", mismatch: true},
		{name: "unreadable", missing: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			store := newRevealBlockStore()
			processor := &persistedRevealProcessor{
				store: store, mismatch: test.mismatch, missing: test.missing, err: forkchoice.ErrExecutionPayloadEnvelopeLookupRequired,
			}
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

			err := runner.reveal(ctx, request)
			if !test.accepted {
				require.ErrorIs(t, err, context.DeadlineExceeded)
				require.Positive(t, store.envelopeReads.Load())
				require.Zero(t, publisher.calls.Load())
				return
			}
			require.NoError(t, err)
			require.Equal(t, int32(1), processor.calls.Load())
			require.Equal(t, int32(1), store.envelopeReads.Load())
			require.Equal(t, int32(1), publisher.calls.Load())
		})
	}
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
	require.True(t, runner.SubmitAcceptedBlock(request.key.beaconBlockRoot))
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

func setRevealDeadlinePassed(t *testing.T, runner *revealRunner, slot uint64) (time.Time, time.Time) {
	t.Helper()
	slotStart := time.Now()
	runner.clock = fixedRevealDeadlineClock{revealTestClock: revealTestClock{slot: slot}, slotTime: slotStart}
	deadline, ok := payloadRevealDeadline(runner.clock, runner.beaconCfg, slot)
	require.True(t, ok)
	slotStart = time.Now().Add(-deadline.Sub(slotStart) - 10*time.Millisecond)
	runner.clock = fixedRevealDeadlineClock{revealTestClock: revealTestClock{slot: slot}, slotTime: slotStart}
	deadline, ok = payloadRevealDeadline(runner.clock, runner.beaconCfg, slot)
	require.True(t, ok)
	return slotStart, deadline
}

func activateRevealRequest(runner *revealRunner, request revealRequest, requestedAt time.Time, trigger revealTrigger) revealRequest {
	runner.mu.Lock()
	defer runner.mu.Unlock()
	request.requestedAt = requestedAt
	request.trigger = trigger
	request.generation = runner.newGenerationLocked()
	runner.tracked[request.key] = revealTracking{
		slot: request.slot, requestedAt: requestedAt, trigger: trigger, generation: request.generation, active: true,
	}
	runner.activeCount++
	return request
}

func observeRevealOutcomes(runner *revealRunner) chan revealOutcome {
	outcomes := make(chan revealOutcome, 4)
	runner.observeOutcome = func(outcome revealOutcome) { outcomes <- outcome }
	return outcomes
}

func captureRevealLogs(t *testing.T) <-chan *log.Record {
	t.Helper()
	records := make(chan *log.Record, 8)
	previous := log.Root().GetHandler()
	log.Root().SetHandler(revealLogCaptureHandler{records: records})
	t.Cleanup(func() { log.Root().SetHandler(previous) })
	return records
}

func requireSingleRevealOutcome(t *testing.T, outcomes chan revealOutcome) revealOutcome {
	t.Helper()
	outcome := receiveRevealTestValue(t, outcomes, "reveal outcome was not reported")
	select {
	case extra := <-outcomes:
		t.Fatalf("unexpected extra reveal outcome: %+v", extra)
	default:
	}
	return outcome
}

func receiveRevealTestValue[T any](t *testing.T, values <-chan T, failure string) T {
	t.Helper()
	select {
	case value := <-values:
		return value
	case <-time.After(time.Second):
		t.Fatal(failure)
		var zero T
		return zero
	}
}

func requireRevealCapacityReleased(t *testing.T, runner *revealRunner) {
	t.Helper()
	runner.mu.Lock()
	defer runner.mu.Unlock()
	require.Zero(t, runner.activeCount)
	require.Zero(t, runner.reservedActive)
}

func TestRevealRunnerReportsOutcomes(t *testing.T) {
	tests := []struct {
		name               string
		trigger            revealTrigger
		requestTime        func(time.Time, time.Time) time.Time
		deadlineExpired    bool
		dropPayload        bool
		mutateRequest      func(*revealRequest)
		wantKind           revealOutcomeKind
		wantErr            error
		wantErrText        string
		wantSignerCalls    int32
		wantProcessorCalls int32
		wantPublisherCalls int32
	}{
		{
			name: "revealed", trigger: revealTriggerGossip,
			requestTime: func(slotStart, _ time.Time) time.Time { return slotStart.Add(40 * time.Millisecond) },
			wantKind:    revealOutcomeRevealed, wantSignerCalls: 1, wantProcessorCalls: 1, wantPublisherCalls: 1,
		},
		{
			name: "revealed for a request created before slot start", trigger: revealTriggerGossip,
			requestTime: func(slotStart, _ time.Time) time.Time { return slotStart.Add(-500 * time.Millisecond) },
			wantKind:    revealOutcomeRevealed, wantSignerCalls: 1, wantProcessorCalls: 1, wantPublisherCalls: 1,
		},
		{
			name: "revealed despite a late request time", trigger: revealTriggerImported,
			requestTime: func(_ time.Time, deadline time.Time) time.Time { return deadline.Add(time.Millisecond) },
			wantKind:    revealOutcomeRevealed, wantSignerCalls: 1, wantProcessorCalls: 1, wantPublisherCalls: 1,
		},
		{
			name: "withheld", trigger: revealTriggerImported, deadlineExpired: true,
			requestTime: func(_ time.Time, deadline time.Time) time.Time { return deadline.Add(time.Millisecond) },
			wantKind:    revealOutcomeWithheld, wantErr: ErrRevealExpired,
		},
		{
			name: "withheld at the deadline", trigger: revealTriggerImported, deadlineExpired: true,
			requestTime: func(_ time.Time, deadline time.Time) time.Time { return deadline },
			wantKind:    revealOutcomeWithheld, wantErr: ErrRevealExpired,
		},
		{
			name: "failed because the payload is missing", trigger: revealTriggerImported, dropPayload: true,
			requestTime: func(slotStart, _ time.Time) time.Time { return slotStart },
			wantKind:    revealOutcomeFailed, wantErr: errRetainedPayloadMissing,
		},
		{
			name: "failed because the payload is missing at the deadline", trigger: revealTriggerCanonicalHead, dropPayload: true,
			requestTime: func(_ time.Time, deadline time.Time) time.Time { return deadline },
			wantKind:    revealOutcomeFailed, wantErr: errRetainedPayloadMissing,
		},
		{
			name: "failed because the bid root mismatches at the deadline", trigger: revealTriggerCanonicalHead,
			requestTime: func(_ time.Time, deadline time.Time) time.Time { return deadline },
			mutateRequest: func(request *revealRequest) {
				request.key.signedBidRoot[0] ^= 0xff
			},
			wantKind: revealOutcomeFailed, wantErrText: "epbs/reveal: retained bid root mismatch",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := newRevealBlockStore()
			processor := new(successfulRevealProcessor)
			publisher := new(recordingRevealPublisher)
			runner, request := retainedRevealFixture(
				t, revealTestClock{slot: 64}, processor, publisher, store, 1, time.Millisecond,
			)
			signer := new(countingEnvelopeSigner)
			runner.signer = signer
			slotStart := time.Now()
			runner.clock = fixedRevealDeadlineClock{
				revealTestClock: revealTestClock{slot: request.slot}, slotTime: slotStart,
			}
			deadline, ok := payloadRevealDeadline(runner.clock, runner.beaconCfg, request.slot)
			require.True(t, ok)
			if test.deadlineExpired {
				slotStart, deadline = setRevealDeadlinePassed(t, runner, request.slot)
			}
			if test.dropPayload {
				require.True(t, runner.coordinator.DropPayload(request.identity))
			}
			if test.mutateRequest != nil {
				test.mutateRequest(&request)
			}
			requestedAt := test.requestTime(slotStart, deadline)
			request = activateRevealRequest(runner, request, requestedAt, test.trigger)
			outcomes := observeRevealOutcomes(runner)

			before := time.Now()
			runner.runRequest(t.Context(), request)
			after := time.Now()

			outcome := requireSingleRevealOutcome(t, outcomes)
			require.Equal(t, test.wantKind, outcome.kind)
			require.Equal(t, request.slot, outcome.request.slot)
			require.Equal(t, request.key.beaconBlockRoot, outcome.request.key.beaconBlockRoot)
			require.Equal(t, request.identity.BlockHash, outcome.request.identity.BlockHash)
			require.Equal(t, test.trigger, outcome.request.trigger)
			require.Equal(t, requestedAt.Sub(slotStart), outcome.requestedAfterSlotStart)
			if test.wantErr == nil {
				if test.wantErrText == "" {
					require.NoError(t, outcome.err)
				} else {
					require.EqualError(t, outcome.err, test.wantErrText)
				}
			} else {
				require.ErrorIs(t, outcome.err, test.wantErr)
			}
			if test.wantKind == revealOutcomeRevealed {
				require.LessOrEqual(t, outcome.revealElapsed, after.Sub(before))
			}
			require.Equal(t, test.wantSignerCalls, signer.calls.Load())
			require.Equal(t, test.wantProcessorCalls, processor.calls.Load())
			require.Equal(t, test.wantPublisherCalls, publisher.calls.Load())
			requireRevealCapacityReleased(t, runner)
		})
	}
}

func TestLogRevealOutcomeContract(t *testing.T) {
	records := captureRevealLogs(t)
	outcome := revealOutcome{
		request: revealRequest{
			key:      revealKey{beaconBlockRoot: common.Hash{0: 1}},
			identity: PayloadIdentity{BlockHash: common.Hash{0: 2}},
			slot:     64,
			trigger:  revealTriggerGossip,
		},
		requestedAfterSlotStart: 20 * time.Millisecond,
		revealElapsed:           30 * time.Millisecond,
		err:                     errors.New("reveal failed"),
	}
	tests := []struct {
		name    string
		kind    revealOutcomeKind
		level   log.Lvl
		message string
		keys    []string
	}{
		{
			name: "revealed", kind: revealOutcomeRevealed, level: log.LvlInfo,
			message: "Embedded builder payload revealed",
			keys:    []string{"slot", "blockRoot", "blockHash", "trigger", "requestedAfterSlotStart", "revealElapsed"},
		},
		{
			name: "withheld", kind: revealOutcomeWithheld, level: log.LvlInfo,
			message: "Embedded builder payload withheld",
			keys:    []string{"slot", "blockRoot", "blockHash", "trigger", "requestedAfterSlotStart"},
		},
		{
			name: "failed", kind: revealOutcomeFailed, level: log.LvlWarn,
			message: "Embedded builder payload reveal failed",
			keys:    []string{"slot", "blockRoot", "blockHash", "trigger", "requestedAfterSlotStart", "revealElapsed", "err"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			outcome.kind = test.kind
			logRevealOutcome(outcome)
			record := receiveRevealTestValue(t, records, "reveal outcome was not logged")
			require.Equal(t, test.level, record.Lvl)
			require.Equal(t, test.message, record.Msg)
			require.Len(t, record.Ctx, len(test.keys)*2)
			fields := make(map[string]any, len(record.Ctx)/2)
			keys := make([]string, 0, len(record.Ctx)/2)
			for i := 0; i < len(record.Ctx); i += 2 {
				key, ok := record.Ctx[i].(string)
				require.True(t, ok)
				keys = append(keys, key)
				fields[key] = record.Ctx[i+1]
			}
			require.ElementsMatch(t, test.keys, keys)
			require.Equal(t, uint64(64), fields["slot"])
			require.Equal(t, common.Hash{0: 1}, fields["blockRoot"])
			require.Equal(t, common.Hash{0: 2}, fields["blockHash"])
			require.Equal(t, revealTriggerGossip, fields["trigger"])
			require.Equal(t, 20*time.Millisecond, fields["requestedAfterSlotStart"])
			if test.kind != revealOutcomeWithheld {
				require.Equal(t, 30*time.Millisecond, fields["revealElapsed"])
			}
			if test.kind == revealOutcomeFailed {
				require.Equal(t, outcome.err, fields["err"])
			}
			select {
			case extra := <-records:
				t.Fatalf("unexpected extra log record: %+v", extra)
			default:
			}
		})
	}
}

func TestRevealRunnerDefaultObserverLogsRevealedOutcome(t *testing.T) {
	store := newRevealBlockStore()
	runner, request := retainedRevealFixture(
		t,
		revealTestClock{slot: 64},
		new(successfulRevealProcessor),
		new(recordingRevealPublisher),
		store,
		1,
		time.Millisecond,
	)
	slotStart := time.Now()
	runner.clock = fixedRevealDeadlineClock{revealTestClock: revealTestClock{slot: request.slot}, slotTime: slotStart}
	request = activateRevealRequest(runner, request, slotStart, revealTriggerGossip)
	records := captureRevealLogs(t)

	runner.runRequest(t.Context(), request)

	record := receiveRevealTestValue(t, records, "default reveal observer did not log the outcome")
	require.Equal(t, log.LvlInfo, record.Lvl)
	require.Equal(t, "Embedded builder payload revealed", record.Msg)
	requireRevealCapacityReleased(t, runner)
}

func TestRevealRunnerReportsFailedWhenDeadlinePassesDuringReveal(t *testing.T) {
	store := newRevealBlockStore()
	processor := &blockingRevealProcessor{
		contextErr: make(chan error, 1), started: make(chan struct{}), blockedFor: make(chan time.Duration, 1),
	}
	runner, request := retainedRevealFixture(
		t, revealTestClock{slot: 64}, processor, new(recordingRevealPublisher), store, 1, time.Second,
	)
	runner.beaconCfg.SecondsPerSlot = 1
	runner.beaconCfg.PayloadDueBps = 2000
	slotStart := time.Now()
	runner.clock = fixedRevealDeadlineClock{revealTestClock: revealTestClock{slot: request.slot}, slotTime: slotStart}
	request = activateRevealRequest(runner, request, slotStart, revealTriggerGossip)
	outcomes := observeRevealOutcomes(runner)

	done := make(chan struct{})
	go func() {
		runner.runRequest(t.Context(), request)
		close(done)
	}()
	receiveRevealTestValue(t, done, "reveal did not finish after the payload deadline")

	select {
	case <-processor.started:
	default:
		t.Fatal("reveal processor did not start before the payload deadline")
	}
	select {
	case err := <-processor.contextErr:
		require.ErrorIs(t, err, context.DeadlineExceeded)
	default:
		t.Fatal("payload deadline did not cancel the reveal processor")
	}
	var blockedFor time.Duration
	select {
	case blockedFor = <-processor.blockedFor:
	default:
		t.Fatal("reveal processor did not record its blocked interval")
	}
	require.Positive(t, blockedFor)

	outcome := requireSingleRevealOutcome(t, outcomes)
	require.Equal(t, revealOutcomeFailed, outcome.kind)
	require.Equal(t, time.Duration(0), outcome.requestedAfterSlotStart)
	require.GreaterOrEqual(t, outcome.revealElapsed, blockedFor)
	require.ErrorIs(t, outcome.err, ErrRevealExpired)
	requireRevealCapacityReleased(t, runner)
}

func TestRevealRunnerDoesNotReportCanceledReveal(t *testing.T) {
	store := newRevealBlockStore()
	processor := &blockingRevealProcessor{contextErr: make(chan error, 1), started: make(chan struct{})}
	runner, request := retainedRevealFixture(
		t, revealTestClock{slot: 64}, processor, new(recordingRevealPublisher), store, 1, time.Millisecond,
	)
	slotStart := time.Now()
	runner.clock = fixedRevealDeadlineClock{revealTestClock: revealTestClock{slot: request.slot}, slotTime: slotStart}
	request = activateRevealRequest(runner, request, slotStart, revealTriggerImported)
	outcomes := observeRevealOutcomes(runner)
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() {
		runner.runRequest(ctx, request)
		close(done)
	}()
	receiveRevealTestValue(t, processor.started, "reveal processor did not start")
	cancel()
	receiveRevealTestValue(t, done, "canceled reveal did not stop")
	err := receiveRevealTestValue(t, processor.contextErr, "canceled reveal did not cancel the processor")
	require.ErrorIs(t, err, context.Canceled)
	require.Empty(t, outcomes)
	requireRevealCapacityReleased(t, runner)
}

func TestRevealRunnerReportsProcessorCancellationWithHealthyContext(t *testing.T) {
	store := newRevealBlockStore()
	processor := new(canceledRevealProcessor)
	runner, request := retainedRevealFixture(
		t, revealTestClock{slot: 64}, processor, new(recordingRevealPublisher), store, 1, time.Millisecond,
	)
	runner.beaconCfg.SecondsPerSlot = 1
	runner.beaconCfg.PayloadDueBps = 2000
	slotStart := time.Now()
	runner.clock = fixedRevealDeadlineClock{revealTestClock: revealTestClock{slot: request.slot}, slotTime: slotStart}
	request = activateRevealRequest(runner, request, slotStart, revealTriggerImported)
	outcomes := observeRevealOutcomes(runner)

	runner.runRequest(t.Context(), request)

	outcome := requireSingleRevealOutcome(t, outcomes)
	require.Equal(t, revealOutcomeFailed, outcome.kind)
	require.ErrorIs(t, outcome.err, context.Canceled)
	require.Positive(t, processor.calls.Load())
	requireRevealCapacityReleased(t, runner)
}

func TestRevealRunnerRecordsSubmissionTrigger(t *testing.T) {
	tests := []struct {
		name    string
		trigger revealTrigger
		submit  func(*testing.T, *revealRunner, *revealBlockStore, revealRequest) revealRequest
	}{
		{
			name:    "gossip",
			trigger: revealTriggerGossip,
			submit: func(t *testing.T, runner *revealRunner, store *revealBlockStore, request revealRequest) revealRequest {
				block := store.blocks[request.key.beaconBlockRoot]
				require.True(t, runner.SubmitGossipValidatedBlock(request.key.beaconBlockRoot, block))
				return receiveRevealTestValue(t, runner.requests, "gossip reveal request was not queued")
			},
		},
		{
			name:    "imported",
			trigger: revealTriggerImported,
			submit: func(t *testing.T, runner *revealRunner, _ *revealBlockStore, request revealRequest) revealRequest {
				require.True(t, runner.SubmitAcceptedBlock(request.key.beaconBlockRoot))
				return receiveRevealTestValue(t, runner.requests, "imported reveal request was not queued")
			},
		},
		{
			name:    "canonical head",
			trigger: revealTriggerCanonicalHead,
			submit: func(t *testing.T, runner *revealRunner, _ *revealBlockStore, request revealRequest) revealRequest {
				head := new(revealHeadReader)
				head.setRoot(request.key.beaconBlockRoot)
				runner.head = head
				runner.reconcileCanonicalHead(t.Context())
				canonicalRequest, ok := runner.nextCanonicalRequest()
				require.True(t, ok)
				return canonicalRequest
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := newRevealBlockStore()
			runner, request := retainedRevealFixture(
				t, revealTestClock{slot: 64}, new(successfulRevealProcessor), new(recordingRevealPublisher), store, 1, time.Millisecond,
			)
			runner.clock = fixedRevealDeadlineClock{
				revealTestClock: revealTestClock{slot: request.slot}, slotTime: time.Now().Add(-time.Second),
			}

			before := time.Now()
			queued := test.submit(t, runner, store, request)
			after := time.Now()
			require.Equal(t, test.trigger, queued.trigger)
			require.False(t, queued.requestedAt.Before(before))
			require.False(t, queued.requestedAt.After(after))
		})
	}
}

func TestRevealRunnerReportsWithheldForLateGossipSubmission(t *testing.T) {
	store := newRevealBlockStore()
	processor := new(successfulRevealProcessor)
	publisher := new(recordingRevealPublisher)
	runner, fixtureRequest := retainedRevealFixture(
		t, revealTestClock{slot: 64}, processor, publisher, store, 1, time.Millisecond,
	)
	signer := new(countingEnvelopeSigner)
	runner.signer = signer
	_, deadline := setRevealDeadlinePassed(t, runner, fixtureRequest.slot)
	block := store.blocks[fixtureRequest.key.beaconBlockRoot]
	require.True(t, runner.SubmitGossipValidatedBlock(fixtureRequest.key.beaconBlockRoot, block))
	request := receiveRevealTestValue(t, runner.requests, "late gossip reveal request was not queued")
	require.False(t, request.requestedAt.Before(deadline))
	outcomes := observeRevealOutcomes(runner)

	runner.runRequest(t.Context(), request)

	outcome := requireSingleRevealOutcome(t, outcomes)
	require.Equal(t, revealOutcomeWithheld, outcome.kind)
	require.Equal(t, revealTriggerGossip, outcome.request.trigger)
	require.ErrorIs(t, outcome.err, ErrRevealExpired)
	require.Zero(t, signer.calls.Load())
	require.Zero(t, processor.calls.Load())
	require.Zero(t, publisher.calls.Load())
	requireRevealCapacityReleased(t, runner)
}

func TestRevealRunnerCanonicalReplacementKeepsFirstRequestTime(t *testing.T) {
	store := newRevealBlockStore()
	processor := &firstBlockingRevealProcessor{
		started: make(chan struct{}), release: make(chan struct{}), processed: make(chan common.Hash, 2),
	}
	runner, fixtureRequest := retainedRevealFixture(
		t, revealTestClock{slot: 64}, processor, new(recordingRevealPublisher), store, 1, time.Millisecond,
	)
	slotStart := time.Now()
	runner.clock = fixedRevealDeadlineClock{revealTestClock: revealTestClock{slot: fixtureRequest.slot}, slotTime: slotStart}
	outcomes := observeRevealOutcomes(runner)
	block := store.blocks[fixtureRequest.key.beaconBlockRoot]
	require.True(t, runner.SubmitGossipValidatedBlock(fixtureRequest.key.beaconBlockRoot, block))
	request := receiveRevealTestValue(t, runner.requests, "gossip reveal request was not queued")
	firstRequestedAt := request.requestedAt
	firstDone := make(chan struct{})
	go func() {
		runner.runRequest(t.Context(), request)
		close(firstDone)
	}()
	receiveRevealTestValue(t, processor.started, "gossip reveal processor did not start")

	require.True(t, runner.submitAcceptedBlock(request.key.beaconBlockRoot, true))
	receiveRevealTestValue(t, firstDone, "superseded reveal did not stop")
	require.Empty(t, outcomes)
	requireRevealCapacityReleased(t, runner)
	canonicalRequest, ok := runner.nextCanonicalRequest()
	require.True(t, ok)
	require.False(t, firstRequestedAt.IsZero())
	require.Equal(t, firstRequestedAt, canonicalRequest.requestedAt)
	require.Equal(t, revealTriggerGossip, canonicalRequest.trigger)

	runner.runRequest(t.Context(), canonicalRequest)
	outcome := requireSingleRevealOutcome(t, outcomes)
	require.Equal(t, revealOutcomeRevealed, outcome.kind)
	require.Equal(t, revealTriggerGossip, outcome.request.trigger)
	require.Equal(t, firstRequestedAt.Sub(slotStart), outcome.requestedAfterSlotStart)
	requireRevealCapacityReleased(t, runner)
}

func TestRevealRunnerSupersededGenerationCannotFinishCanonicalReplacement(t *testing.T) {
	store := newRevealBlockStore()
	processor := &releaseBlockedRevealProcessor{started: make(chan struct{}), release: make(chan struct{})}
	t.Cleanup(processor.unblock)
	runner, fixtureRequest := retainedRevealFixture(
		t, revealTestClock{slot: 64}, processor, new(recordingRevealPublisher), store, 1, time.Millisecond,
	)
	runner.clock = fixedRevealDeadlineClock{
		revealTestClock: revealTestClock{slot: fixtureRequest.slot}, slotTime: time.Now(),
	}
	outcomes := observeRevealOutcomes(runner)
	block := store.blocks[fixtureRequest.key.beaconBlockRoot]
	require.True(t, runner.SubmitGossipValidatedBlock(fixtureRequest.key.beaconBlockRoot, block))
	gossipRequest := receiveRevealTestValue(t, runner.requests, "gossip reveal request was not queued")
	firstDone := make(chan struct{})
	go func() {
		runner.runRequest(t.Context(), gossipRequest)
		close(firstDone)
	}()
	receiveRevealTestValue(t, processor.started, "gossip reveal processor did not start")

	require.True(t, runner.submitAcceptedBlock(gossipRequest.key.beaconBlockRoot, true))
	canonicalRequest, ok := runner.nextCanonicalRequest()
	require.True(t, ok)
	processor.unblock()
	receiveRevealTestValue(t, firstDone, "superseded gossip reveal did not finish")
	require.Empty(t, outcomes)

	runner.mu.Lock()
	tracking, tracked := runner.tracked[canonicalRequest.key]
	activeCount := runner.activeCount
	reservedActive := runner.reservedActive
	runner.mu.Unlock()
	require.True(t, tracked)
	require.True(t, tracking.active)
	require.Equal(t, canonicalRequest.generation, tracking.generation)
	require.Equal(t, 1, activeCount)
	require.Equal(t, 1, reservedActive)

	runner.runRequest(t.Context(), canonicalRequest)
	outcome := requireSingleRevealOutcome(t, outcomes)
	require.Equal(t, revealOutcomeRevealed, outcome.kind)
	requireRevealCapacityReleased(t, runner)
}

func TestRevealRunnerDoesNotReportWhenCompletionClaimIsLost(t *testing.T) {
	store := newRevealBlockStore()
	processor := &firstBlockingRevealProcessor{
		started: make(chan struct{}), release: make(chan struct{}), processed: make(chan common.Hash, 1),
	}
	runner, request := retainedRevealFixture(
		t, revealTestClock{slot: 64}, processor, new(recordingRevealPublisher), store, 1, time.Millisecond,
	)
	slotStart := time.Now()
	runner.clock = fixedRevealDeadlineClock{revealTestClock: revealTestClock{slot: request.slot}, slotTime: slotStart}
	request = activateRevealRequest(runner, request, slotStart, revealTriggerGossip)
	outcomes := observeRevealOutcomes(runner)
	done := make(chan struct{})
	go func() {
		runner.runRequest(t.Context(), request)
		close(done)
	}()
	receiveRevealTestValue(t, processor.started, "reveal processor did not start")

	runner.mu.Lock()
	tracking := runner.tracked[request.key]
	stop := runner.retireTrackingLocked(request.key, tracking)
	runner.mu.Unlock()
	require.NotNil(t, stop)
	close(processor.release)
	receiveRevealTestValue(t, done, "reveal did not finish after losing its completion claim")

	require.Empty(t, outcomes)
	requireRevealCapacityReleased(t, runner)
}

func TestRevealRunnerCompletionClaimsRequestBeforeReporting(t *testing.T) {
	store := newRevealBlockStore()
	processor := new(successfulRevealProcessor)
	publisher := new(recordingRevealPublisher)
	runner, fixtureRequest := retainedRevealFixture(
		t, revealTestClock{slot: 64}, processor, publisher, store, 1, time.Millisecond,
	)
	runner.clock = fixedRevealDeadlineClock{
		revealTestClock: revealTestClock{slot: fixtureRequest.slot}, slotTime: time.Now(),
	}
	observerStarted := make(chan revealOutcome, 1)
	releaseObserver := make(chan struct{})
	runner.observeOutcome = func(outcome revealOutcome) {
		observerStarted <- outcome
		select {
		case <-releaseObserver:
		case <-time.After(time.Second):
			t.Error("outcome observer was not released")
		}
	}
	block := store.blocks[fixtureRequest.key.beaconBlockRoot]
	require.True(t, runner.SubmitGossipValidatedBlock(fixtureRequest.key.beaconBlockRoot, block))
	request := receiveRevealTestValue(t, runner.requests, "gossip reveal request was not queued")
	done := make(chan struct{})
	go func() {
		runner.runRequest(t.Context(), request)
		close(done)
	}()
	outcome := receiveRevealTestValue(t, observerStarted, "outcome observer did not start")

	replacementAccepted := runner.submitAcceptedBlock(request.key.beaconBlockRoot, true)
	close(releaseObserver)
	receiveRevealTestValue(t, done, "reveal did not finish after outcome reporting")

	require.False(t, replacementAccepted)
	require.Equal(t, revealOutcomeRevealed, outcome.kind)
	require.Equal(t, int32(1), processor.calls.Load())
	require.Equal(t, int32(1), publisher.calls.Load())
	requireRevealCapacityReleased(t, runner)
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
	runner.finishRequest(queued.key, queued.generation)
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

func TestRevealRunnerReconcilesCanonicalHeadWithReservedCapacity(t *testing.T) {
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
		validCoordinatorBlobsBundle(t, gloasCoordinatorConfig(), 1),
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
	head.setRoot(secondRoot)
	require.Eventually(t, func() bool { return head.calls.Load() > 0 }, time.Second, time.Millisecond)
	require.Eventually(t, func() bool { return store.getBlockCalls.Load() > 2 }, time.Second, time.Millisecond)
	require.Eventually(t, func() bool { return processor.calls.Load() == 2 }, time.Second, time.Millisecond)
	require.Equal(t, firstRequest.key.beaconBlockRoot, <-processor.processed)
	require.Equal(t, secondRoot, <-processor.processed)
	close(processor.release)

	for range 2 {
		select {
		case <-publisher.published:
		case <-time.After(time.Second):
			t.Fatal("canonical head was not gossiped after capacity release")
		}
	}
	require.Eventually(t, func() bool {
		runner.mu.Lock()
		defer runner.mu.Unlock()
		firstTracking, firstTracked := runner.tracked[firstRequest.key]
		secondKey := revealKey{beaconBlockRoot: secondRoot, signedBidRoot: firstRequest.key.signedBidRoot}
		secondTracking, secondTracked := runner.tracked[secondKey]
		return len(runner.tracked) == 2 && firstTracked && !firstTracking.active && secondTracked && !secondTracking.active && runner.activeCount == 0
	}, time.Second, time.Millisecond)

	headCalls := head.calls.Load()
	blockReads := store.getBlockCalls.Load()
	require.Eventually(t, func() bool { return head.calls.Load() >= headCalls+3 }, time.Second, time.Millisecond)
	require.Equal(t, int32(2), processor.calls.Load())
	require.Equal(t, int32(2), publisher.calls.Load())
	require.Equal(t, []common.Hash{firstRequest.key.beaconBlockRoot, secondRoot}, blobData.preparedRoots())
	require.Equal(t, blockReads, store.getBlockCalls.Load())

	reorgRoot := common.Hash{0: 0xff}
	head.setRoot(reorgRoot)
	require.Eventually(t, func() bool { return store.getBlockCalls.Load() > blockReads }, time.Second, time.Millisecond)
}

func TestRevealRunnerReservesWorkerAndCapacityForCanonicalHead(t *testing.T) {
	store := &revealBlockStore{
		blocks:    make(map[common.Hash]*cltypes.SignedBeaconBlock),
		envelopes: make(map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope),
	}
	runner, firstRequest := retainedRevealFixture(
		t,
		revealTestClock{slot: 64},
		new(successfulRevealProcessor),
		new(recordingRevealPublisher),
		store,
		1,
		5*time.Millisecond,
		validCoordinatorBlobsBundle(t, gloasCoordinatorConfig(), 1),
	)
	deadlineOffset := time.Duration(runner.beaconCfg.SecondsPerSlot) * time.Second *
		time.Duration(runner.beaconCfg.PayloadDueBps) / time.Duration(clparams.BpsFactor)
	runner.clock = fixedRevealDeadlineClock{
		revealTestClock: revealTestClock{slot: firstRequest.slot},
		slotTime:        time.Now().Add(-deadlineOffset + 500*time.Millisecond),
	}
	blocked := &blockingPreparedBlobData{started: make(chan struct{}), contextErr: make(chan error, 1)}
	preparer := &selectiveBlockingBlobDataPreparer{
		blockedRoot: firstRequest.key.beaconBlockRoot,
		blocked:     blocked,
		prepared:    make(chan common.Hash, 2),
	}
	runner.blobData = preparer
	head := new(revealHeadReader)
	runner.head = head
	require.True(t, runner.SubmitAcceptedBlock(firstRequest.key.beaconBlockRoot))

	secondBlock := store.blocks[firstRequest.key.beaconBlockRoot]
	secondBlock.Block.StateRoot[0] ^= 1
	secondBlockRoot, err := secondBlock.Block.HashSSZ()
	require.NoError(t, err)
	secondRoot := common.Hash(secondBlockRoot)
	store.blocks[secondRoot] = secondBlock
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
	<-blocked.started
	require.Equal(t, firstRequest.key.beaconBlockRoot, <-preparer.prepared)

	select {
	case root := <-preparer.prepared:
		require.Equal(t, secondRoot, root)
	case <-time.After(150 * time.Millisecond):
		t.Fatal("canonical reveal did not receive reserved execution capacity")
	}
}

func TestRevealRunnerCanonicalHeadUsesLatestRoot(t *testing.T) {
	store := &revealBlockStore{
		blocks:    make(map[common.Hash]*cltypes.SignedBeaconBlock),
		envelopes: make(map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope),
	}
	runner, firstRequest := retainedRevealFixture(
		t,
		revealTestClock{slot: 64},
		new(successfulRevealProcessor),
		new(recordingRevealPublisher),
		store,
		1,
		5*time.Millisecond,
		validCoordinatorBlobsBundle(t, gloasCoordinatorConfig(), 1),
	)
	deadlineOffset := time.Duration(runner.beaconCfg.SecondsPerSlot) * time.Second *
		time.Duration(runner.beaconCfg.PayloadDueBps) / time.Duration(clparams.BpsFactor)
	runner.clock = fixedRevealDeadlineClock{
		revealTestClock: revealTestClock{slot: firstRequest.slot},
		slotTime:        time.Now().Add(-deadlineOffset + 500*time.Millisecond),
	}
	blocked := &blockingPreparedBlobData{started: make(chan struct{}), contextErr: make(chan error, 1)}
	preparer := &selectiveBlockingBlobDataPreparer{
		blockedRoot: firstRequest.key.beaconBlockRoot,
		blocked:     blocked,
		prepared:    make(chan common.Hash, 2),
	}
	runner.blobData = preparer
	head := new(revealHeadReader)
	head.setRoot(firstRequest.key.beaconBlockRoot)
	runner.head = head

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
	require.Equal(t, firstRequest.key.beaconBlockRoot, <-preparer.prepared)
	<-blocked.started

	secondBlock := store.blocks[firstRequest.key.beaconBlockRoot]
	secondBlock.Block.StateRoot[0] ^= 1
	secondBlockRoot, err := secondBlock.Block.HashSSZ()
	require.NoError(t, err)
	secondRoot := common.Hash(secondBlockRoot)
	store.blocks[secondRoot] = secondBlock
	head.setRoot(secondRoot)

	select {
	case root := <-preparer.prepared:
		require.Equal(t, secondRoot, root)
	case <-time.After(150 * time.Millisecond):
		t.Fatal("canonical reveal did not switch to the latest head")
	}
}

func TestRevealRunnerPromotesQueuedCanonicalRoot(t *testing.T) {
	store := &revealBlockStore{
		blocks:    make(map[common.Hash]*cltypes.SignedBeaconBlock),
		envelopes: make(map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope),
	}
	runner, request := retainedRevealFixture(
		t,
		revealTestClock{slot: 64},
		new(successfulRevealProcessor),
		new(recordingRevealPublisher),
		store,
		1,
		5*time.Millisecond,
		validCoordinatorBlobsBundle(t, gloasCoordinatorConfig(), 1),
	)
	deadlineOffset := time.Duration(runner.beaconCfg.SecondsPerSlot) * time.Second *
		time.Duration(runner.beaconCfg.PayloadDueBps) / time.Duration(clparams.BpsFactor)
	runner.clock = fixedRevealDeadlineClock{
		revealTestClock: revealTestClock{slot: request.slot},
		slotTime:        time.Now().Add(-deadlineOffset + 500*time.Millisecond),
	}
	preparer := new(rootRecordingBlobDataPreparer)
	runner.blobData = preparer
	require.True(t, runner.SubmitAcceptedBlock(request.key.beaconBlockRoot))
	require.True(t, runner.submitAcceptedBlock(request.key.beaconBlockRoot, true))

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() {
		runner.Run(ctx)
		close(done)
	}()
	require.Eventually(t, func() bool { return len(preparer.preparedRoots()) == 1 }, 150*time.Millisecond, time.Millisecond)
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("reveal runner did not join")
	}
	require.Equal(t, []common.Hash{request.key.beaconBlockRoot}, preparer.preparedRoots())
}

func TestRevealRunnerCanonicalHeadReverseFlipDropsStalePendingRoot(t *testing.T) {
	store := &revealBlockStore{
		blocks:    make(map[common.Hash]*cltypes.SignedBeaconBlock),
		envelopes: make(map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope),
	}
	runner, firstRequest := retainedRevealFixture(
		t,
		revealTestClock{slot: 64},
		new(successfulRevealProcessor),
		new(recordingRevealPublisher),
		store,
		1,
		5*time.Millisecond,
		validCoordinatorBlobsBundle(t, gloasCoordinatorConfig(), 1),
	)
	deadlineOffset := time.Duration(runner.beaconCfg.SecondsPerSlot) * time.Second *
		time.Duration(runner.beaconCfg.PayloadDueBps) / time.Duration(clparams.BpsFactor)
	runner.clock = fixedRevealDeadlineClock{
		revealTestClock: revealTestClock{slot: firstRequest.slot},
		slotTime:        time.Now().Add(-deadlineOffset + 500*time.Millisecond),
	}
	blocked := &heldPreparedBlobData{started: make(chan struct{}), release: make(chan struct{})}
	preparer := &selectiveBlockingBlobDataPreparer{
		blockedRoot: firstRequest.key.beaconBlockRoot,
		blocked:     blocked,
		prepared:    make(chan common.Hash, 3),
	}
	runner.blobData = preparer

	firstBlock := store.blocks[firstRequest.key.beaconBlockRoot]
	secondBlock := cltypes.NewSignedBeaconBlock(runner.beaconCfg, clparams.GloasVersion)
	secondBlock.Block.Slot = firstBlock.Block.Slot
	secondBlock.Block.ParentRoot = firstBlock.Block.ParentRoot
	secondBlock.Block.StateRoot[0] = 1
	secondBlock.Block.Body.SignedExecutionPayloadBid = firstBlock.Block.Body.SignedExecutionPayloadBid
	secondBlockRoot, err := secondBlock.Block.HashSSZ()
	require.NoError(t, err)
	secondRoot := common.Hash(secondBlockRoot)
	store.blocks[secondRoot] = secondBlock

	require.True(t, runner.submitAcceptedBlock(firstRequest.key.beaconBlockRoot, true))
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
	require.Equal(t, firstRequest.key.beaconBlockRoot, <-preparer.prepared)
	<-blocked.started
	require.True(t, runner.submitAcceptedBlock(secondRoot, true))
	require.True(t, runner.submitAcceptedBlock(firstRequest.key.beaconBlockRoot, true))
	close(blocked.release)

	select {
	case root := <-preparer.prepared:
		require.Equal(t, firstRequest.key.beaconBlockRoot, root)
	case <-time.After(150 * time.Millisecond):
		t.Fatal("canonical reveal retained the stale intermediate head")
	}
}

func TestRevealRunnerCanonicalHeadReturnToCompletedRootCancelsNewerReveal(t *testing.T) {
	store := &revealBlockStore{
		blocks:    make(map[common.Hash]*cltypes.SignedBeaconBlock),
		envelopes: make(map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope),
	}
	runner, firstRequest := retainedRevealFixture(
		t,
		revealTestClock{slot: 64},
		new(successfulRevealProcessor),
		new(recordingRevealPublisher),
		store,
		4,
		5*time.Millisecond,
		validCoordinatorBlobsBundle(t, gloasCoordinatorConfig(), 1),
	)
	deadlineOffset := time.Duration(runner.beaconCfg.SecondsPerSlot) * time.Second *
		time.Duration(runner.beaconCfg.PayloadDueBps) / time.Duration(clparams.BpsFactor)
	runner.clock = fixedRevealDeadlineClock{
		revealTestClock: revealTestClock{slot: firstRequest.slot},
		slotTime:        time.Now().Add(-deadlineOffset + 5*time.Second),
	}

	firstBlock := store.blocks[firstRequest.key.beaconBlockRoot]
	secondBlock := cltypes.NewSignedBeaconBlock(runner.beaconCfg, clparams.GloasVersion)
	secondBlock.Block.Slot = firstBlock.Block.Slot
	secondBlock.Block.ParentRoot = firstBlock.Block.ParentRoot
	secondBlock.Block.StateRoot[0] = 1
	secondBlock.Block.Body.SignedExecutionPayloadBid = firstBlock.Block.Body.SignedExecutionPayloadBid
	secondBlockRoot, err := secondBlock.Block.HashSSZ()
	require.NoError(t, err)
	secondRoot := common.Hash(secondBlockRoot)
	store.blocks[secondRoot] = secondBlock
	blocked := &blockingPreparedBlobData{started: make(chan struct{}), contextErr: make(chan error, 1)}
	preparer := &selectiveBlockingBlobDataPreparer{
		blockedRoot: secondRoot,
		blocked:     blocked,
		prepared:    make(chan common.Hash, 3),
	}
	runner.blobData = preparer

	require.True(t, runner.submitAcceptedBlock(firstRequest.key.beaconBlockRoot, true))
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
	require.Equal(t, firstRequest.key.beaconBlockRoot, <-preparer.prepared)
	require.Eventually(t, func() bool {
		runner.mu.Lock()
		defer runner.mu.Unlock()
		tracking, ok := runner.tracked[firstRequest.key]
		return ok && !tracking.active
	}, 150*time.Millisecond, time.Millisecond)
	require.True(t, runner.submitAcceptedBlock(secondRoot, true))
	require.Equal(t, secondRoot, <-preparer.prepared)
	<-blocked.started
	runner.submitAcceptedBlock(firstRequest.key.beaconBlockRoot, true)

	select {
	case err := <-blocked.contextErr:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(150 * time.Millisecond):
		t.Fatal("stale canonical reveal was not cancelled")
	}
}
