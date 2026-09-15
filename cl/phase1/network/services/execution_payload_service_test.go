// Copyright 2024 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package services

import (
	"context"
	"encoding/binary"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/beacon/beaconevents"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/phase1/core/state/lru"
	"github.com/erigontech/erigon/cl/phase1/forkchoice"
	"github.com/erigontech/erigon/cl/phase1/forkchoice/mock_services"
	"github.com/erigontech/erigon/common"
)

func setupExecutionPayloadService(t *testing.T) (ExecutionPayloadService, *mock_services.ForkChoiceStorageMock) {
	cfg := &clparams.MainnetBeaconConfig
	forkchoiceMock := mock_services.NewForkChoiceStorageMock(t)
	service := NewExecutionPayloadService(canceledPendingQueueContext(t), forkchoiceMock, cfg, beaconevents.NewEventEmitter())
	service.(*executionPayloadService).pending.stopAndWait()
	return service, forkchoiceMock
}

func newTestSignedEnvelope(slot uint64, blockRoot common.Hash, builderIndex uint64) *cltypes.SignedExecutionPayloadEnvelope {
	envelope := cltypes.NewExecutionPayloadEnvelope(&clparams.MainnetBeaconConfig)
	envelope.BeaconBlockRoot = blockRoot
	envelope.BuilderIndex = builderIndex
	// Initialize Eth1Block fields needed for HashSSZ
	if envelope.Payload != nil {
		envelope.Payload.SlotNumber = slot
		envelope.Payload.Extra = solid.NewExtraData()
		envelope.Payload.Transactions = &solid.TransactionsSSZ{}
		envelope.Payload.Withdrawals = solid.NewStaticListSSZ[*cltypes.Withdrawal](int(clparams.MainnetBeaconConfig.MaxWithdrawalsPerPayload), 44)
		envelope.Payload.BlockAccessList = solid.NewByteListSSZ(clparams.MainnetBeaconConfig.MaxBytesPerTransaction)
	}
	return &cltypes.SignedExecutionPayloadEnvelope{
		Message:   envelope,
		Signature: common.Bytes96{},
	}
}

func newTestGloasBlock(slot, builderIndex uint64, stateRoot ...common.Hash) *cltypes.SignedBeaconBlock {
	block := &cltypes.SignedBeaconBlock{Block: &cltypes.BeaconBlock{
		Slot: slot,
		Body: &cltypes.BeaconBody{
			Version: clparams.GloasVersion,
			SignedExecutionPayloadBid: &cltypes.SignedExecutionPayloadBid{
				Message: &cltypes.ExecutionPayloadBid{BuilderIndex: builderIndex},
			},
		},
	}}
	if len(stateRoot) != 0 {
		block.Block.StateRoot = stateRoot[0]
	}
	return block
}

func TestExecutionPayloadServiceDecodeRejectsNonCanonicalOffsets(t *testing.T) {
	service, _ := setupExecutionPayloadService(t)
	encoded, err := newTestSignedEnvelope(100, common.Hash{1}, 1).EncodeSSZ(nil)
	require.NoError(t, err)
	const signedFixedSize = 100
	const envelopeFixedSize = 80
	nonCanonical := append([]byte(nil), encoded[:signedFixedSize+envelopeFixedSize]...)
	nonCanonical = append(nonCanonical, make([]byte, 4)...)
	nonCanonical = append(nonCanonical, encoded[signedFixedSize+envelopeFixedSize:]...)
	for offset := signedFixedSize; offset < signedFixedSize+8; offset += 4 {
		binary.LittleEndian.PutUint32(nonCanonical[offset:], binary.LittleEndian.Uint32(encoded[offset:])+4)
	}

	_, err = service.DecodeGossipMessage("peer123", nonCanonical, clparams.GloasVersion)
	require.Error(t, err)
}

func oversizedExtraDataEnvelopeSSZ(t *testing.T, envelope *cltypes.SignedExecutionPayloadEnvelope) []byte {
	t.Helper()
	encoded, err := envelope.EncodeSSZ(nil)
	require.NoError(t, err)

	const (
		signedMessageOffsetPosition          = 0
		messageRequestsOffsetPosition        = 4
		payloadExtraOffsetPosition           = 436
		payloadTransactionsOffsetPosition    = 504
		payloadWithdrawalsOffsetPosition     = 508
		payloadBlockAccessListOffsetPosition = 528
	)
	messageStart := int(binary.LittleEndian.Uint32(encoded[signedMessageOffsetPosition:]))
	payloadStart := messageStart + int(binary.LittleEndian.Uint32(encoded[messageStart:]))
	extraStart := payloadStart + int(binary.LittleEndian.Uint32(encoded[payloadStart+payloadExtraOffsetPosition:]))
	malformed := append([]byte{}, encoded[:extraStart]...)
	malformed = append(malformed, make([]byte, 33)...)
	malformed = append(malformed, encoded[extraStart:]...)

	for _, position := range []int{
		messageStart + messageRequestsOffsetPosition,
		payloadStart + payloadTransactionsOffsetPosition,
		payloadStart + payloadWithdrawalsOffsetPosition,
		payloadStart + payloadBlockAccessListOffsetPosition,
	} {
		offset := binary.LittleEndian.Uint32(malformed[position:])
		binary.LittleEndian.PutUint32(malformed[position:], offset+33)
	}
	return malformed
}

func TestExecutionPayloadServiceRejectsOversizedExtraDataSSZ(t *testing.T) {
	service, _ := setupExecutionPayloadService(t)
	envelope := newTestSignedEnvelope(100, common.HexToHash("0x1234"), 1)

	_, err := service.DecodeGossipMessage("", oversizedExtraDataEnvelopeSSZ(t, envelope), clparams.GloasVersion)
	require.Error(t, err)
}

func TestExecutionPayloadServiceRejectsUnsupportedEnvelopeVersions(t *testing.T) {
	service, _ := setupExecutionPayloadService(t)
	encoded, err := newTestSignedEnvelope(100, common.HexToHash("0x1234"), 1).EncodeSSZ(nil)
	require.NoError(t, err)

	for _, version := range []clparams.StateVersion{clparams.FuluVersion, clparams.StateVersion(255)} {
		_, err := service.DecodeGossipMessage("", encoded, version)
		require.ErrorContains(t, err, "unsupported execution payload envelope consensus version")
	}
	decoded, err := service.DecodeGossipMessage("", encoded, clparams.GloasVersion)
	require.NoError(t, err)
	require.NotNil(t, decoded)
}

func TestExecutionPayloadServiceRejectsMalformedEnvelopeBeforePendingHash(t *testing.T) {
	service, _ := setupExecutionPayloadService(t)
	envelope := newTestSignedEnvelope(100, common.HexToHash("0x1234"), 1)
	envelope.Message.Payload.Withdrawals.Append(nil)

	require.NotPanics(t, func() {
		err := service.ProcessMessage(context.Background(), nil, envelope)
		require.ErrorContains(t, err, "nil withdrawal at index 0")
	})
}

func TestExecutionPayloadServiceNilEnvelope(t *testing.T) {
	service, _ := setupExecutionPayloadService(t)

	// Test nil envelope
	err := service.ProcessMessage(context.Background(), nil, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "nil execution payload envelope")

	// Test envelope with nil message
	err = service.ProcessMessage(context.Background(), nil, &cltypes.SignedExecutionPayloadEnvelope{})
	require.Error(t, err)
	require.Contains(t, err.Error(), "nil execution payload envelope")
}

func TestExecutionPayloadServiceBlockNotFound(t *testing.T) {
	service, fcu := setupExecutionPayloadService(t)

	blockRoot := common.HexToHash("0x1234")
	envelope := newTestSignedEnvelope(100, blockRoot, 1)

	// Block not in forkchoice - should queue and return ErrIgnore
	err := service.ProcessMessage(context.Background(), nil, envelope)
	require.ErrorIs(t, err, ErrIgnore)

	// Verify envelope was queued (check internal state)
	impl := service.(*executionPayloadService)
	require.Equal(t, int32(1), impl.pending.count.Load())
	fcu.DeleteEnvelope(blockRoot)

	// Now add block to forkchoice
	fcu.Blocks[blockRoot] = newTestGloasBlock(100, 1)

	// Process same envelope again - should succeed now (block found)
	// Note: OnExecutionPayload mock returns nil by default
	err = service.ProcessMessage(context.Background(), nil, envelope)
	require.NoError(t, err)
}

func TestExecutionPayloadServiceDoesNotReportQueuedWhenPendingQueueFull(t *testing.T) {
	service, _ := setupExecutionPayloadService(t)
	impl := service.(*executionPayloadService)
	impl.pending.capacity = 1

	queuedRoot := common.HexToHash("0x1111")
	queued, err := impl.queuePendingEnvelope(queuedRoot, newTestSignedEnvelope(100, queuedRoot, 1), time.Now())
	require.NoError(t, err)
	require.True(t, queued)
	require.Equal(t, int32(1), impl.pending.count.Load())
	ownedBytes := impl.pendingBytes.Load()

	blockRoot := common.HexToHash("0x2222")
	output := captureServiceLogs(t)
	err = service.ProcessMessage(t.Context(), nil, newTestSignedEnvelope(100, blockRoot, 2))

	require.ErrorIs(t, err, ErrIgnore)
	require.Contains(t, err.Error(), "pending job queue full")
	require.NotContains(t, output.String(), "Queued execution payload envelope for later processing")
	require.Equal(t, int32(1), impl.pending.count.Load())
	require.Equal(t, ownedBytes, impl.pendingBytes.Load())
}

func TestExecutionPayloadServicePreservesIngressTimeWhileWaitingForBlock(t *testing.T) {
	cfg := &clparams.MainnetBeaconConfig
	forkchoiceMock := mock_services.NewForkChoiceStorageMock(t)
	service := NewExecutionPayloadService(canceledPendingQueueContext(t), forkchoiceMock, cfg, beaconevents.NewEventEmitter())
	service.(*executionPayloadService).pending.stopAndWait()
	impl := service.(*executionPayloadService)
	early := time.Unix(1_700_000_000, 250_000_000)
	late := early.Add(10 * time.Second)
	impl.now = func() time.Time { return early }
	blockRoot := common.Hash{1}
	envelope := newTestSignedEnvelope(100, blockRoot, 1)

	require.ErrorIs(t, service.ProcessMessage(t.Context(), nil, envelope), ErrIgnore)
	receivedAt := make(chan time.Time, 1)
	forkchoiceMock.OnExecutionPayloadAtFn = func(_ context.Context, got *cltypes.SignedExecutionPayloadEnvelope, checkData, validate bool, received time.Time) error {
		require.Same(t, envelope, got)
		require.True(t, checkData)
		require.True(t, validate)
		receivedAt <- received
		return nil
	}
	impl.now = func() time.Time { return late }
	forkchoiceMock.Blocks[blockRoot] = newTestGloasBlock(100, 1)
	impl.pending.processPending(t.Context())

	require.Equal(t, early, <-receivedAt)
}

func TestExecutionPayloadServiceIgnoresMalformedEnvelopeForUnknownBlockWithoutQueueing(t *testing.T) {
	service, _ := setupExecutionPayloadService(t)
	envelope := newTestSignedEnvelope(100, common.HexToHash("0x1234"), 1)
	envelope.Message.Payload.Withdrawals = nil

	err := service.ProcessMessage(t.Context(), nil, envelope)
	require.ErrorIs(t, err, ErrIgnore)
	impl := service.(*executionPayloadService)
	require.Zero(t, impl.pending.count.Load())
	require.Zero(t, impl.pendingBytes.Load())
}

func TestExecutionPayloadServiceRejectsMalformedEnvelopeForKnownBlock(t *testing.T) {
	service, fcu := setupExecutionPayloadService(t)
	blockRoot := common.HexToHash("0x1234")
	fcu.Blocks[blockRoot] = newTestGloasBlock(100, 1)
	envelope := newTestSignedEnvelope(100, blockRoot, 1)
	envelope.Message.Payload.Withdrawals = nil

	err := service.ProcessMessage(t.Context(), nil, envelope)
	require.ErrorContains(t, err, "missing payload withdrawals")
	require.NotErrorIs(t, err, ErrIgnore)
}

func TestExecutionPayloadServiceRejectsMismatchedBuilderBeforePersistedEnvelopeRead(t *testing.T) {
	service, fcu := setupExecutionPayloadService(t)
	blockRoot := common.HexToHash("0x1234")
	fcu.Blocks[blockRoot] = newTestGloasBlock(100, 1)
	persisted := newTestSignedEnvelope(100, blockRoot, 1)
	fcu.SetEnvelope(blockRoot, persisted)
	var reads atomic.Int32
	fcu.ReadEnvelopeFromDiskFunc = func(common.Hash) (*cltypes.SignedExecutionPayloadEnvelope, error) {
		reads.Add(1)
		return persisted, nil
	}

	err := service.ProcessMessage(t.Context(), nil, newTestSignedEnvelope(100, blockRoot, 2))

	require.ErrorContains(t, err, "does not match bid builder index")
	require.NotErrorIs(t, err, ErrIgnore)
	require.Zero(t, reads.Load())
}

func TestExecutionPayloadServiceEmitsGossipEvent(t *testing.T) {
	cfg := &clparams.MainnetBeaconConfig
	forkchoiceMock := mock_services.NewForkChoiceStorageMock(t)
	emitter := beaconevents.NewEventEmitter()
	service := NewExecutionPayloadService(canceledPendingQueueContext(t), forkchoiceMock, cfg, emitter)
	service.(*executionPayloadService).pending.stopAndWait()
	events := make(chan *beaconevents.EventStream, 1)
	subscription := emitter.Operation().Subscribe(events)
	defer subscription.Unsubscribe()

	blockRoot := common.Hash{1}
	stateRoot := common.Hash{2}
	forkchoiceMock.Blocks[blockRoot] = newTestGloasBlock(100, 7, stateRoot)
	var stateCopies atomic.Int32
	forkchoiceMock.GetStateAtBlockRootFn = func(common.Hash, bool) (*state.CachingBeaconState, error) {
		stateCopies.Add(1)
		return nil, errors.New("unexpected state copy")
	}
	forkchoiceMock.HeadVal = blockRoot
	forkchoiceMock.HeadSlotVal = 100
	envelope := newTestSignedEnvelope(100, blockRoot, 7)
	require.NoError(t, service.ProcessMessage(t.Context(), nil, envelope))

	require.Equal(t, beaconevents.OpExecutionPayloadGossip, (<-events).Event)
	require.Zero(t, stateCopies.Load())
}

func TestExecutionPayloadServiceAcceptsGossipWhenValidatedEnvelopeWaitsForColumns(t *testing.T) {
	cfg := &clparams.MainnetBeaconConfig
	forkchoiceMock := mock_services.NewForkChoiceStorageMock(t)
	emitter := beaconevents.NewEventEmitter()
	service := NewExecutionPayloadService(canceledPendingQueueContext(t), forkchoiceMock, cfg, emitter)
	service.(*executionPayloadService).pending.stopAndWait()
	events := make(chan *beaconevents.EventStream, 1)
	subscription := emitter.Operation().Subscribe(events)
	defer subscription.Unsubscribe()

	blockRoot := common.Hash{1}
	forkchoiceMock.Blocks[blockRoot] = newTestGloasBlock(100, 7)
	forkchoiceMock.OnExecutionPayloadErr = forkchoice.ErrEIP7594ColumnDataNotAvailable
	envelope := newTestSignedEnvelope(100, blockRoot, 7)

	require.NoError(t, service.ProcessMessage(t.Context(), nil, envelope))
	select {
	case event := <-events:
		require.Equal(t, beaconevents.OpExecutionPayloadGossip, event.Event)
		require.Equal(t, blockRoot, event.Data.(*beaconevents.ExecutionPayloadGossipData).BlockRoot)
	default:
		t.Fatal("validated gossip envelope did not emit execution_payload_gossip while waiting for columns")
	}
}

func TestExecutionPayloadServiceDoesNotEmitStaleHeadV2AfterReorg(t *testing.T) {
	cfg := &clparams.MainnetBeaconConfig
	forkchoiceMock := mock_services.NewForkChoiceStorageMock(t)
	emitter := beaconevents.NewEventEmitter()
	service := NewExecutionPayloadService(canceledPendingQueueContext(t), forkchoiceMock, cfg, emitter)
	service.(*executionPayloadService).pending.stopAndWait()
	stateEvents := make(chan *beaconevents.EventStream, 1)
	stateSubscription := emitter.State().Subscribe(stateEvents)
	defer stateSubscription.Unsubscribe()

	blockRoot := common.Hash{1}
	reorgRoot := common.Hash{9}
	forkchoiceMock.Blocks[blockRoot] = newTestGloasBlock(100, 7, common.Hash{2})
	headState := state.New(cfg)
	headState.SetVersion(clparams.GloasVersion)
	require.NoError(t, headState.SetSlot(100))
	require.NoError(t, headState.SetBlockRootAt(63, common.Hash{3}))
	require.NoError(t, headState.SetBlockRootAt(95, common.Hash{4}))
	forkchoiceMock.ViewStateAtBlockRootFn = func(_ common.Hash, fn func(*state.CachingBeaconState) error) error {
		err := fn(headState)
		forkchoiceMock.HeadVal = reorgRoot
		return err
	}
	forkchoiceMock.HeadVal = blockRoot
	forkchoiceMock.HeadSlotVal = 100

	require.NoError(t, service.ProcessMessage(t.Context(), nil, newTestSignedEnvelope(100, blockRoot, 7)))
	select {
	case event := <-stateEvents:
		t.Fatalf("emitted stale event after reorg: %#v", event)
	default:
	}
}

func TestExecutionPayloadServiceDoesNotEmitFullHeadV2AfterStatusChanges(t *testing.T) {
	cfg := &clparams.MainnetBeaconConfig
	forkchoiceMock := mock_services.NewForkChoiceStorageMock(t)
	emitter := beaconevents.NewEventEmitter()
	service := NewExecutionPayloadService(canceledPendingQueueContext(t), forkchoiceMock, cfg, emitter)
	service.(*executionPayloadService).pending.stopAndWait()
	stateEvents := make(chan *beaconevents.EventStream, 1)
	stateSubscription := emitter.State().Subscribe(stateEvents)
	defer stateSubscription.Unsubscribe()

	blockRoot := common.Hash{1}
	forkchoiceMock.Blocks[blockRoot] = newTestGloasBlock(100, 7, common.Hash{2})
	headState := state.New(cfg)
	headState.SetVersion(clparams.GloasVersion)
	require.NoError(t, headState.SetSlot(100))
	require.NoError(t, headState.SetBlockRootAt(63, common.Hash{3}))
	require.NoError(t, headState.SetBlockRootAt(95, common.Hash{4}))
	forkchoiceMock.ViewStateAtBlockRootFn = func(_ common.Hash, fn func(*state.CachingBeaconState) error) error {
		err := fn(headState)
		forkchoiceMock.HeadPayloadStatusVal = cltypes.PayloadStatusEmpty
		return err
	}
	forkchoiceMock.HeadVal = blockRoot
	forkchoiceMock.HeadSlotVal = 100
	forkchoiceMock.HeadPayloadStatusVal = cltypes.PayloadStatusFull

	require.NoError(t, service.ProcessMessage(t.Context(), nil, newTestSignedEnvelope(100, blockRoot, 7)))
	select {
	case event := <-stateEvents:
		t.Fatalf("emitted full head event after status changed: %#v", event)
	default:
	}
}

func TestExecutionPayloadServiceDoesNotEmitGossipWhenValidationFails(t *testing.T) {
	cfg := &clparams.MainnetBeaconConfig
	forkchoiceMock := mock_services.NewForkChoiceStorageMock(t)
	emitter := beaconevents.NewEventEmitter()
	service := NewExecutionPayloadService(canceledPendingQueueContext(t), forkchoiceMock, cfg, emitter)
	service.(*executionPayloadService).pending.stopAndWait()
	events := make(chan *beaconevents.EventStream, 1)
	subscription := emitter.Operation().Subscribe(events)
	defer subscription.Unsubscribe()

	blockRoot := common.Hash{1}
	forkchoiceMock.Blocks[blockRoot] = newTestGloasBlock(100, 7)
	forkchoiceMock.OnExecutionPayloadErr = errors.New("invalid envelope signature")

	require.Error(t, service.ProcessMessage(t.Context(), nil, newTestSignedEnvelope(100, blockRoot, 7)))
	select {
	case event := <-events:
		t.Fatalf("emitted gossip event for invalid envelope: %#v", event)
	default:
	}
}

func TestExecutionPayloadServiceProgressesWhileEventFeedIsBlocked(t *testing.T) {
	cfg := &clparams.MainnetBeaconConfig
	forkchoiceMock := mock_services.NewForkChoiceStorageMock(t)
	emitter := beaconevents.NewEventEmitter()
	service := NewExecutionPayloadService(canceledPendingQueueContext(t), forkchoiceMock, cfg, emitter)
	service.(*executionPayloadService).pending.stopAndWait()
	slow := make(chan *beaconevents.EventStream)
	slowSubscription := emitter.Operation().Subscribe(slow)
	defer slowSubscription.Unsubscribe()
	ready := make(chan *beaconevents.EventStream)
	readySubscription := emitter.Operation().Subscribe(ready)
	defer readySubscription.Unsubscribe()
	blockedSendDone := make(chan struct{})
	go func() {
		emitter.Operation().SendAttestation(&beaconevents.AttestationData{})
		close(blockedSendDone)
	}()
	<-ready

	blockRoot := common.Hash{1}
	forkchoiceMock.Blocks[blockRoot] = newTestGloasBlock(100, 7)
	processDone := make(chan error, 1)
	ctx := t.Context()
	go func() { processDone <- service.ProcessMessage(ctx, nil, newTestSignedEnvelope(100, blockRoot, 7)) }()
	select {
	case err := <-processDone:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("execution payload gossip processing blocked on the event feed")
	}

	slowSubscription.Unsubscribe()
	select {
	case <-blockedSendDone:
	case <-time.After(time.Second):
		t.Fatal("legacy event send remained blocked after unsubscribe")
	}
}

func TestExecutionPayloadServiceAlreadySeen(t *testing.T) {
	service, fcu := setupExecutionPayloadService(t)

	blockRoot := common.HexToHash("0x1234")
	envelope := newTestSignedEnvelope(100, blockRoot, 1)

	// Add block to forkchoice
	fcu.Blocks[blockRoot] = newTestGloasBlock(100, 1)

	// First call should succeed
	err := service.ProcessMessage(context.Background(), nil, envelope)
	require.NoError(t, err)

	// Second call with same (blockRoot, builderIndex) should be ignored
	err = service.ProcessMessage(context.Background(), nil, envelope)
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrIgnore))
	require.Contains(t, err.Error(), "already seen envelope")
}

func TestExecutionPayloadServiceSharesSeenEnvelopeAdmissionWithREST(t *testing.T) {
	service, fcu := setupExecutionPayloadService(t)
	blockRoot := common.HexToHash("0x1234")
	envelope := newTestSignedEnvelope(100, blockRoot, 1)
	fcu.Blocks[blockRoot] = newTestGloasBlock(100, 1)
	token, err := fcu.ClaimExecutionPayloadEnvelopeForGossip(t.Context(), blockRoot, 1)
	require.NoError(t, err)
	fcu.FinishExecutionPayloadEnvelopeForGossip(token, true)

	err = service.ProcessMessage(context.Background(), nil, envelope)

	require.ErrorIs(t, err, ErrIgnore)
	require.ErrorContains(t, err, "already seen")
	require.False(t, fcu.OnExecutionPayloadCalled)
}

func TestExecutionPayloadServiceQueuesEnvelopeWhileAdmissionIsBusy(t *testing.T) {
	service, fcu := setupExecutionPayloadService(t)
	tokens := make([]forkchoice.ExecutionPayloadEnvelopeAdmissionToken, 0, 1024)
	for i := range 1024 {
		token, err := fcu.ClaimExecutionPayloadEnvelopeForGossip(t.Context(), common.Hash{byte(i), byte(i >> 8)}, uint64(i))
		require.NoError(t, err)
		tokens = append(tokens, token)
	}

	blockRoot := common.HexToHash("0xffff")
	envelope := newTestSignedEnvelope(100, blockRoot, 7)
	fcu.Blocks[blockRoot] = newTestGloasBlock(100, 7)
	require.ErrorIs(t, service.ProcessMessage(t.Context(), nil, envelope), ErrIgnore)
	impl := service.(*executionPayloadService)
	require.Equal(t, int32(1), impl.pending.count.Load())
	impl.pending.processPending(t.Context())
	require.Equal(t, int32(1), impl.pending.count.Load())
	require.False(t, fcu.OnExecutionPayloadCalled)

	for _, token := range tokens {
		fcu.FinishExecutionPayloadEnvelopeForGossip(token, false)
	}
	impl.pending.processPending(t.Context())

	require.Zero(t, impl.pending.count.Load())
	require.True(t, fcu.OnExecutionPayloadCalled)
	require.True(t, impl.seenEnvelopesCache.Contains(seenEnvelopeKey{blockRoot, 7}))
}

func TestExecutionPayloadServicePendingBusyIdentityDoesNotBlockOtherJobs(t *testing.T) {
	for _, tc := range []struct {
		name   string
		aFirst bool
	}{
		{name: "busy queued first", aFirst: true},
		{name: "busy queued second"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			service, fcu := setupExecutionPayloadService(t)
			impl := service.(*executionPayloadService)
			rootA := common.Hash{1}
			rootB := common.Hash{2}
			envelopeA := newTestSignedEnvelope(100, rootA, 1)
			envelopeB := newTestSignedEnvelope(100, rootB, 2)
			fcu.Blocks[rootA] = newTestGloasBlock(100, 1)
			fcu.Blocks[rootB] = newTestGloasBlock(100, 2)
			owner, err := fcu.EnvelopeGossipAdmissions.Claim(t.Context(), rootA, 1)
			require.NoError(t, err)
			defer fcu.EnvelopeGossipAdmissions.Finish(owner, false)
			fcu.ClaimExecutionPayloadEnvelopeForGossipFunc = func(context.Context, common.Hash, uint64) (forkchoice.ExecutionPayloadEnvelopeAdmissionToken, error) {
				return forkchoice.ExecutionPayloadEnvelopeAdmissionToken{}, errors.New("blocking admission used by pending worker")
			}
			fcu.TryClaimExecutionPayloadEnvelopeForGossipFunc = func(root common.Hash, builderIndex uint64) (forkchoice.ExecutionPayloadEnvelopeAdmissionToken, error) {
				return fcu.EnvelopeGossipAdmissions.TryClaim(root, builderIndex)
			}
			if tc.aFirst {
				_, err = impl.queuePendingEnvelope(rootA, envelopeA, time.Now())
				require.NoError(t, err)
				_, err = impl.queuePendingEnvelope(rootB, envelopeB, time.Now())
			} else {
				_, err = impl.queuePendingEnvelope(rootB, envelopeB, time.Now())
				require.NoError(t, err)
				_, err = impl.queuePendingEnvelope(rootA, envelopeA, time.Now())
			}
			require.NoError(t, err)
			processed := make([]common.Hash, 0, 2)
			fcu.OnExecutionPayloadFn = func(_ context.Context, envelope *cltypes.SignedExecutionPayloadEnvelope, _, _ bool) error {
				processed = append(processed, envelope.Message.BeaconBlockRoot)
				return nil
			}

			impl.pending.processPending(t.Context())

			require.Equal(t, []common.Hash{rootB}, processed)
			require.Equal(t, int32(1), impl.pending.count.Load())
			hashA, err := envelopeA.HashSSZ()
			require.NoError(t, err)
			_, queuedA := impl.pending.jobs.Load(pendingEnvelopeKey{blockRoot: rootA, envelopeHash: hashA})
			require.True(t, queuedA)

			fcu.EnvelopeGossipAdmissions.Finish(owner, false)
			impl.pending.processPending(t.Context())
			require.Equal(t, []common.Hash{rootB, rootA}, processed)
			require.Zero(t, impl.pending.count.Load())
		})
	}
}

func TestExecutionPayloadServicePendingBusyEnvelopeDefersTerminalValidation(t *testing.T) {
	service, fcu := setupExecutionPayloadService(t)
	impl := service.(*executionPayloadService)
	blockRoot := common.HexToHash("0x1234")
	envelope := newTestSignedEnvelope(100, blockRoot, 1)
	fcu.Blocks[blockRoot] = newTestGloasBlock(100, 1)
	fcu.OnExecutionPayloadErr = forkchoice.ErrInvalidExecutionPayloadEnvelope
	owner, err := fcu.EnvelopeGossipAdmissions.Claim(t.Context(), blockRoot, 1)
	require.NoError(t, err)
	defer fcu.EnvelopeGossipAdmissions.Finish(owner, false)
	queued, err := impl.queuePendingEnvelope(blockRoot, envelope, time.Now())
	require.NoError(t, err)
	require.True(t, queued)

	for range 2 {
		impl.pending.processPending(t.Context())
		require.Equal(t, int32(1), impl.pending.count.Load())
		require.False(t, fcu.OnExecutionPayloadCalled)
	}

	fcu.EnvelopeGossipAdmissions.Finish(owner, false)
	impl.pending.processPending(t.Context())

	require.Zero(t, impl.pending.count.Load())
	require.True(t, fcu.OnExecutionPayloadCalled)
	token, err := fcu.EnvelopeGossipAdmissions.TryClaim(blockRoot, 1)
	require.NoError(t, err)
	fcu.EnvelopeGossipAdmissions.Finish(token, false)
}

func TestExecutionPayloadServicePendingRejectsBuilderMismatchBeforeAdmission(t *testing.T) {
	for _, tc := range []struct {
		name  string
		block func() *cltypes.SignedBeaconBlock
	}{
		{name: "builder mismatch", block: func() *cltypes.SignedBeaconBlock { return newTestGloasBlock(100, 2) }},
		{name: "incomplete block", block: func() *cltypes.SignedBeaconBlock { return &cltypes.SignedBeaconBlock{} }},
		{name: "missing bid", block: func() *cltypes.SignedBeaconBlock {
			block := newTestGloasBlock(100, 1)
			block.Block.Body.SignedExecutionPayloadBid = nil
			return block
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			service, fcu := setupExecutionPayloadService(t)
			impl := service.(*executionPayloadService)
			blockRoot := common.HexToHash("0x1234")
			envelope := newTestSignedEnvelope(100, blockRoot, 1)

			require.ErrorIs(t, service.ProcessMessage(t.Context(), nil, envelope), ErrIgnore)
			require.Equal(t, int32(1), impl.pending.count.Load())
			fcu.Blocks[blockRoot] = tc.block()
			var tryClaims atomic.Int32
			fcu.TryClaimExecutionPayloadEnvelopeForGossipFunc = func(root common.Hash, builderIndex uint64) (forkchoice.ExecutionPayloadEnvelopeAdmissionToken, error) {
				tryClaims.Add(1)
				return fcu.EnvelopeGossipAdmissions.TryClaim(root, builderIndex)
			}

			impl.pending.processPending(t.Context())

			require.Zero(t, impl.pending.count.Load())
			require.Zero(t, tryClaims.Load())
			require.False(t, fcu.OnExecutionPayloadCalled)
			require.False(t, impl.seenEnvelopesCache.Contains(seenEnvelopeKey{blockRoot, 1}))
			token, err := fcu.EnvelopeGossipAdmissions.TryClaim(blockRoot, 1)
			require.NoError(t, err)
			fcu.EnvelopeGossipAdmissions.Finish(token, false)
		})
	}
}

func TestExecutionPayloadServiceIgnoresSeenEnvelopeWhenPersistenceIsUnavailable(t *testing.T) {
	service, fcu := setupExecutionPayloadService(t)
	blockRoot := common.HexToHash("0x1234")
	envelope := newTestSignedEnvelope(100, blockRoot, 1)
	fcu.Blocks[blockRoot] = newTestGloasBlock(100, 1)
	service.(*executionPayloadService).seenEnvelopesCache.Add(seenEnvelopeKey{blockRoot, 1}, struct{}{})

	require.ErrorIs(t, service.ProcessMessage(context.Background(), nil, envelope), ErrIgnore)
}

func TestExecutionPayloadServiceSlotBelowFinalized(t *testing.T) {
	service, fcu := setupExecutionPayloadService(t)

	blockRoot := common.HexToHash("0x1234")
	envelope := newTestSignedEnvelope(50, blockRoot, 1) // slot 50

	// Add block to forkchoice
	fcu.Blocks[blockRoot] = newTestGloasBlock(51, 1)

	fcu.FinalizedCheckpointVal = solid.Checkpoint{Epoch: 2}

	err := service.ProcessMessage(context.Background(), nil, envelope)
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrIgnore))
	require.Contains(t, err.Error(), "envelope slot 50 < finalized slot 64")
}

func TestExecutionPayloadServiceRejectsFinalizedUnknownBlockBeforeQueue(t *testing.T) {
	service, fcu := setupExecutionPayloadService(t)
	fcu.FinalizedCheckpointVal = solid.Checkpoint{Epoch: 2}
	envelope := newTestSignedEnvelope(63, common.HexToHash("0x1234"), 1)

	err := service.ProcessMessage(context.Background(), nil, envelope)
	require.ErrorIs(t, err, ErrIgnore)
	require.Contains(t, err.Error(), "envelope slot 63 < finalized slot 64")
	impl := service.(*executionPayloadService)
	require.Zero(t, impl.pending.count.Load())
	require.Zero(t, impl.pendingBytes.Load())
}

func TestExecutionPayloadServiceRejectsKnownFinalizedBlockWithForgedEnvelopeSlot(t *testing.T) {
	service, fcu := setupExecutionPayloadService(t)
	blockRoot := common.HexToHash("0x1234")
	fcu.Blocks[blockRoot] = newTestGloasBlock(63, 1)
	fcu.FinalizedCheckpointVal = solid.Checkpoint{Epoch: 2}
	envelope := newTestSignedEnvelope(100, blockRoot, 1)
	validationCalled := false
	fcu.OnExecutionPayloadFn = func(_ context.Context, got *cltypes.SignedExecutionPayloadEnvelope, checkBlobData, validatePayload bool) error {
		validationCalled = true
		require.Same(t, envelope, got)
		require.True(t, checkBlobData)
		require.True(t, validatePayload)
		return errors.New("block slot 63 != envelope.payload.slot_number 100")
	}

	err := service.ProcessMessage(t.Context(), nil, envelope)

	require.Error(t, err)
	require.NotErrorIs(t, err, ErrIgnore)
	require.Contains(t, err.Error(), "block slot 63 != envelope.payload.slot_number 100")
	require.True(t, validationCalled)
}

func TestExecutionPayloadServiceUsesFinalizedEpochStartBoundary(t *testing.T) {
	for _, tc := range []struct {
		name    string
		slot    uint64
		ignored bool
	}{
		{name: "below", slot: 63, ignored: true},
		{name: "exact", slot: 64},
		{name: "above", slot: 65},
	} {
		t.Run(tc.name, func(t *testing.T) {
			service, fcu := setupExecutionPayloadService(t)
			root := common.Hash{byte(tc.slot)}
			fcu.FinalizedCheckpointVal = solid.Checkpoint{Epoch: 2}
			fcu.FinalizedSlotVal = 95
			fcu.Blocks[root] = newTestGloasBlock(tc.slot, 1)

			err := service.ProcessMessage(context.Background(), nil, newTestSignedEnvelope(tc.slot, root, 1))
			if tc.ignored {
				require.ErrorIs(t, err, ErrIgnore)
				return
			}
			require.NoError(t, err)
		})
	}
}

func TestExecutionPayloadServiceRechecksFinalizedBoundaryAfterApply(t *testing.T) {
	for _, tc := range []struct {
		name       string
		processErr error
	}{
		{name: "success"},
		{name: "indices pending", processErr: forkchoice.ErrExecutionPayloadEnvelopeIndicesPending},
		{name: "persistence failed", processErr: forkchoice.ErrExecutionPayloadEnvelopePersistenceFailed},
	} {
		t.Run(tc.name, func(t *testing.T) {
			service, fcu := setupExecutionPayloadService(t)
			impl := service.(*executionPayloadService)
			events := make(chan *beaconevents.EventStream, 1)
			subscription := impl.emitters.Operation().Subscribe(events)
			defer subscription.Unsubscribe()
			blockRoot := common.HexToHash("0x1234")
			envelope := newTestSignedEnvelope(100, blockRoot, 1)
			fcu.Blocks[blockRoot] = newTestGloasBlock(100, 1)
			fcu.FinalizedCheckpointVal = solid.Checkpoint{Epoch: 3}
			fcu.OnExecutionPayloadAtFn = func(context.Context, *cltypes.SignedExecutionPayloadEnvelope, bool, bool, time.Time) error {
				fcu.FinalizedCheckpointVal = solid.Checkpoint{Epoch: 4}
				return tc.processErr
			}

			err := service.ProcessMessage(context.Background(), nil, envelope)

			require.ErrorIs(t, err, ErrIgnore)
			require.ErrorContains(t, err, "envelope slot 100 < finalized slot 128")
			seenKey := seenEnvelopeKey{beaconBlockRoot: blockRoot, builderIndex: 1}
			require.False(t, impl.seenEnvelopesCache.Contains(seenKey))
			select {
			case event := <-events:
				t.Fatalf("finalized envelope emitted event %s", event.Event)
			default:
			}
			token, claimErr := fcu.ClaimExecutionPayloadEnvelopeForGossip(t.Context(), blockRoot, 1)
			require.NoError(t, claimErr)
			fcu.FinishExecutionPayloadEnvelopeForGossip(token, false)
		})
	}
}

func TestExecutionPayloadServiceSuccess(t *testing.T) {
	service, fcu := setupExecutionPayloadService(t)

	blockRoot := common.HexToHash("0x1234")
	envelope := newTestSignedEnvelope(100, blockRoot, 1)

	// Add block to forkchoice
	fcu.Blocks[blockRoot] = newTestGloasBlock(100, 1)
	fcu.FinalizedSlotVal = 50

	// Process should succeed
	err := service.ProcessMessage(context.Background(), nil, envelope)
	require.NoError(t, err)

	// Verify envelope was marked as seen
	impl := service.(*executionPayloadService)
	seenKey := seenEnvelopeKey{
		beaconBlockRoot: blockRoot,
		builderIndex:    1,
	}
	require.True(t, impl.seenEnvelopesCache.Contains(seenKey))
}

func TestExecutionPayloadServiceIgnoresLocalCancellation(t *testing.T) {
	for _, processErr := range []error{context.Canceled, context.DeadlineExceeded} {
		t.Run(processErr.Error(), func(t *testing.T) {
			service, fcu := setupExecutionPayloadService(t)
			blockRoot := common.HexToHash("0x1234")
			fcu.Blocks[blockRoot] = newTestGloasBlock(100, 1)
			fcu.FinalizedSlotVal = 50
			fcu.OnExecutionPayloadErr = processErr

			err := service.ProcessMessage(context.Background(), nil, newTestSignedEnvelope(100, blockRoot, 1))
			require.ErrorIs(t, err, ErrIgnore)
		})
	}
}

func TestExecutionPayloadServiceAcceptsValidatedEnvelopeWithIndicesPending(t *testing.T) {
	cfg := &clparams.MainnetBeaconConfig
	fcu := mock_services.NewForkChoiceStorageMock(t)
	emitter := beaconevents.NewEventEmitter()
	service := NewExecutionPayloadService(canceledPendingQueueContext(t), fcu, cfg, emitter)
	service.(*executionPayloadService).pending.stopAndWait()
	events := make(chan *beaconevents.EventStream, 1)
	subscription := emitter.Operation().Subscribe(events)
	defer subscription.Unsubscribe()

	blockRoot := common.HexToHash("0x1234")
	fcu.Blocks[blockRoot] = newTestGloasBlock(100, 1)
	var calls atomic.Int32
	var validations atomic.Int32
	fcu.OnExecutionPayloadFn = func(context.Context, *cltypes.SignedExecutionPayloadEnvelope, bool, bool) error {
		calls.Add(1)
		return forkchoice.ErrExecutionPayloadEnvelopeIndicesPending
	}
	fcu.ValidateExecutionPayloadEnvelopeForGossipFunc = func(*cltypes.SignedExecutionPayloadEnvelope) error {
		validations.Add(1)
		return nil
	}
	envelope := newTestSignedEnvelope(100, blockRoot, 1)

	require.NoError(t, service.ProcessMessage(t.Context(), nil, envelope))
	require.Equal(t, beaconevents.OpExecutionPayloadGossip, (<-events).Event)
	impl := service.(*executionPayloadService)
	seenKey := seenEnvelopeKey{blockRoot, 1}
	require.True(t, impl.seenEnvelopesCache.Contains(seenKey))
	impl.seenEnvelopesCache.Remove(seenKey)
	require.ErrorIs(t, service.ProcessMessage(t.Context(), nil, envelope), ErrIgnore)
	require.Equal(t, int32(1), calls.Load())
	require.Equal(t, int32(1), validations.Load())
}

func TestExecutionPayloadServiceRejectsUnvalidatedEnvelopeWithIndicesPending(t *testing.T) {
	cfg := &clparams.MainnetBeaconConfig
	fcu := mock_services.NewForkChoiceStorageMock(t)
	emitter := beaconevents.NewEventEmitter()
	service := NewExecutionPayloadService(canceledPendingQueueContext(t), fcu, cfg, emitter)
	service.(*executionPayloadService).pending.stopAndWait()
	events := make(chan *beaconevents.EventStream, 1)
	subscription := emitter.Operation().Subscribe(events)
	defer subscription.Unsubscribe()

	blockRoot := common.HexToHash("0x1234")
	fcu.Blocks[blockRoot] = newTestGloasBlock(100, 1)
	fcu.OnExecutionPayloadErr = forkchoice.ErrExecutionPayloadEnvelopeIndicesPending
	fcu.ValidateExecutionPayloadEnvelopeForGossipFunc = func(*cltypes.SignedExecutionPayloadEnvelope) error {
		return errors.New("forged signature")
	}
	envelope := newTestSignedEnvelope(100, blockRoot, 1)

	err := service.ProcessMessage(t.Context(), nil, envelope)

	require.ErrorContains(t, err, "forged signature")
	impl := service.(*executionPayloadService)
	require.False(t, impl.seenEnvelopesCache.Contains(seenEnvelopeKey{blockRoot, 1}))
	select {
	case event := <-events:
		t.Fatalf("unvalidated envelope emitted event %s", event.Event)
	default:
	}
	token, claimErr := fcu.ClaimExecutionPayloadEnvelopeForGossip(t.Context(), blockRoot, 1)
	require.NoError(t, claimErr)
	fcu.FinishExecutionPayloadEnvelopeForGossip(token, false)
}

func TestExecutionPayloadServiceAcceptsPersistenceFailureWithoutMarkingSeen(t *testing.T) {
	cfg := &clparams.MainnetBeaconConfig
	fcu := mock_services.NewForkChoiceStorageMock(t)
	emitter := beaconevents.NewEventEmitter()
	service := NewExecutionPayloadService(canceledPendingQueueContext(t), fcu, cfg, emitter)
	service.(*executionPayloadService).pending.stopAndWait()
	events := make(chan *beaconevents.EventStream, 2)
	subscription := emitter.Operation().Subscribe(events)
	defer subscription.Unsubscribe()

	blockRoot := common.HexToHash("0x1234")
	fcu.Blocks[blockRoot] = newTestGloasBlock(100, 1)
	var calls atomic.Int32
	fcu.OnExecutionPayloadFn = func(context.Context, *cltypes.SignedExecutionPayloadEnvelope, bool, bool) error {
		calls.Add(1)
		return forkchoice.ErrExecutionPayloadEnvelopePersistenceFailed
	}
	envelope := newTestSignedEnvelope(100, blockRoot, 1)
	impl := service.(*executionPayloadService)

	require.NoError(t, service.ProcessMessage(t.Context(), nil, envelope))
	require.Equal(t, beaconevents.OpExecutionPayloadGossip, (<-events).Event)
	seenKey := seenEnvelopeKey{blockRoot, 1}
	require.False(t, impl.seenEnvelopesCache.Contains(seenKey))
	require.NoError(t, service.ProcessMessage(t.Context(), nil, envelope))
	require.Equal(t, beaconevents.OpExecutionPayloadGossip, (<-events).Event)
	require.False(t, impl.seenEnvelopesCache.Contains(seenKey))
	require.Equal(t, int32(2), calls.Load())
	token, err := fcu.ClaimExecutionPayloadEnvelopeForGossip(t.Context(), blockRoot, 1)
	require.NoError(t, err)
	fcu.FinishExecutionPayloadEnvelopeForGossip(token, false)
}

func TestExecutionPayloadServiceRejectsBuilderDifferentFromBlockBid(t *testing.T) {
	service, fcu := setupExecutionPayloadService(t)

	blockRoot := common.HexToHash("0x1234")
	envelope1 := newTestSignedEnvelope(100, blockRoot, 1) // builder 1
	envelope2 := newTestSignedEnvelope(100, blockRoot, 2) // builder 2

	fcu.Blocks[blockRoot] = newTestGloasBlock(100, 1)
	fcu.FinalizedSlotVal = 50

	err := service.ProcessMessage(context.Background(), nil, envelope1)
	require.NoError(t, err)

	err = service.ProcessMessage(context.Background(), nil, envelope2)
	require.ErrorIs(t, err, forkchoice.ErrInvalidExecutionPayloadEnvelope)

	impl := service.(*executionPayloadService)
	require.True(t, impl.seenEnvelopesCache.Contains(seenEnvelopeKey{blockRoot, 1}))
	require.False(t, impl.seenEnvelopesCache.Contains(seenEnvelopeKey{blockRoot, 2}))
}

func TestExecutionPayloadServicePendingEnvelopeExpiry(t *testing.T) {
	cfg := &clparams.MainnetBeaconConfig
	forkchoiceMock := mock_services.NewForkChoiceStorageMock(t)
	ctx := t.Context()

	// Use a stopped queue so expiry is processed explicitly.
	impl := &executionPayloadService{
		forkchoiceStore: forkchoiceMock,
		beaconCfg:       cfg,
		emitters:        beaconevents.NewEventEmitter(),
	}
	impl.pending = impl.newPendingQueue(canceledPendingQueueContext(t))
	impl.pending.stopAndWait()
	seenCache, err := lru.New[seenEnvelopeKey, struct{}]("seen_envelopes", seenEnvelopeCacheSize)
	require.NoError(t, err)
	impl.seenEnvelopesCache = seenCache

	blockRoot := common.HexToHash("0x1234")
	envelope := newTestSignedEnvelope(100, blockRoot, 1)
	envelopeHash, err := envelope.HashSSZ()
	require.NoError(t, err)

	// Add expired job directly
	key := pendingEnvelopeKey{
		blockRoot:    blockRoot,
		envelopeHash: envelopeHash,
	}
	ownedBytes := uint64(envelope.EncodingSizeSSZ())
	storePendingJob(t, impl.pending, key, &pendingEnvelopeJob{envelope: envelope, ownedBytes: ownedBytes}, time.Now().Add(-(pendingEnvelopeExpiry + time.Second)))
	impl.pendingBytes.Store(ownedBytes)

	// Process pending - should remove expired
	impl.pending.processPending(ctx)

	require.Equal(t, int32(0), impl.pending.count.Load())
	require.Zero(t, impl.pendingBytes.Load())
	_, exists := impl.pending.jobs.Load(key)
	require.False(t, exists)
}

func TestExecutionPayloadServicePendingEnvelopeProcessing(t *testing.T) {
	cfg := &clparams.MainnetBeaconConfig
	forkchoiceMock := mock_services.NewForkChoiceStorageMock(t)
	ctx := t.Context()

	// Use a stopped queue so pending jobs are processed explicitly.
	impl := &executionPayloadService{
		forkchoiceStore: forkchoiceMock,
		beaconCfg:       cfg,
		emitters:        beaconevents.NewEventEmitter(),
	}
	impl.pending = impl.newPendingQueue(canceledPendingQueueContext(t))
	impl.pending.stopAndWait()
	seenCache, err := lru.New[seenEnvelopeKey, struct{}]("seen_envelopes", seenEnvelopeCacheSize)
	require.NoError(t, err)
	impl.seenEnvelopesCache = seenCache

	blockRoot := common.HexToHash("0x1234")
	envelope := newTestSignedEnvelope(100, blockRoot, 1)
	envelopeHash, err := envelope.HashSSZ()
	require.NoError(t, err)

	// Add pending job
	key := pendingEnvelopeKey{
		blockRoot:    blockRoot,
		envelopeHash: envelopeHash,
	}
	ownedBytes := uint64(envelope.EncodingSizeSSZ())
	storePendingJob(t, impl.pending, key, &pendingEnvelopeJob{envelope: envelope, ownedBytes: ownedBytes}, time.Now())
	impl.pendingBytes.Store(ownedBytes)

	// Block not yet available - should keep pending
	impl.pending.processPending(ctx)
	require.Equal(t, int32(1), impl.pending.count.Load())

	// Now add block
	forkchoiceMock.Blocks[blockRoot] = newTestGloasBlock(100, 1)

	// Process again - should process and remove
	impl.pending.processPending(ctx)
	require.Equal(t, int32(0), impl.pending.count.Load())
	require.Zero(t, impl.pendingBytes.Load())
	_, exists := impl.pending.jobs.Load(key)
	require.False(t, exists)

	// Envelope should be marked as seen
	require.True(t, impl.seenEnvelopesCache.Contains(seenEnvelopeKey{blockRoot, 1}))
}

func TestExecutionPayloadServiceMultiplePendingForSameBlock(t *testing.T) {
	cfg := &clparams.MainnetBeaconConfig
	forkchoiceMock := mock_services.NewForkChoiceStorageMock(t)
	ctx := t.Context()

	impl := &executionPayloadService{
		forkchoiceStore: forkchoiceMock,
		beaconCfg:       cfg,
		emitters:        beaconevents.NewEventEmitter(),
	}
	impl.pending = impl.newPendingQueue(canceledPendingQueueContext(t))
	impl.pending.stopAndWait()
	seenCache, err := lru.New[seenEnvelopeKey, struct{}]("seen_envelopes", seenEnvelopeCacheSize)
	require.NoError(t, err)
	impl.seenEnvelopesCache = seenCache

	blockRoot := common.HexToHash("0x1234")

	// Create two different envelopes for the same block (different builders)
	envelope1 := newTestSignedEnvelope(100, blockRoot, 1)
	envelope2 := newTestSignedEnvelope(100, blockRoot, 2)

	hash1, _ := envelope1.HashSSZ()
	hash2, _ := envelope2.HashSSZ()

	// Add both as pending
	ownedBytes1 := uint64(envelope1.EncodingSizeSSZ())
	ownedBytes2 := uint64(envelope2.EncodingSizeSSZ())
	storePendingJob(t, impl.pending, pendingEnvelopeKey{blockRoot, hash1}, &pendingEnvelopeJob{envelope: envelope1, ownedBytes: ownedBytes1}, time.Now())
	storePendingJob(t, impl.pending, pendingEnvelopeKey{blockRoot, hash2}, &pendingEnvelopeJob{envelope: envelope2, ownedBytes: ownedBytes2}, time.Now())
	impl.pendingBytes.Store(ownedBytes1 + ownedBytes2)

	forkchoiceMock.Blocks[blockRoot] = newTestGloasBlock(100, 1)

	// Process - both should be processed
	impl.pending.processPending(ctx)

	require.Equal(t, int32(0), impl.pending.count.Load())
	require.Zero(t, impl.pendingBytes.Load())
	require.True(t, impl.seenEnvelopesCache.Contains(seenEnvelopeKey{blockRoot, 1}))
	require.False(t, impl.seenEnvelopesCache.Contains(seenEnvelopeKey{blockRoot, 2}))
}

func TestExecutionPayloadServicePendingQueueCap(t *testing.T) {
	cfg := &clparams.MainnetBeaconConfig
	forkchoiceMock := mock_services.NewForkChoiceStorageMock(t)

	seenCache, err := lru.New[seenEnvelopeKey, struct{}]("seen_envelopes", seenEnvelopeCacheSize)
	require.NoError(t, err)
	impl := &executionPayloadService{
		forkchoiceStore:    forkchoiceMock,
		beaconCfg:          cfg,
		emitters:           beaconevents.NewEventEmitter(),
		seenEnvelopesCache: seenCache,
	}
	impl.pending = impl.newPendingQueue(canceledPendingQueueContext(t))
	impl.pending.stopAndWait()

	impl.pending.count.Store(maxPendingEnvelopes)

	blockRoot := common.HexToHash("0xffff")
	envelope := newTestSignedEnvelope(100, blockRoot, 999)

	queued, err := impl.queuePendingEnvelope(blockRoot, envelope, time.Now())
	require.ErrorIs(t, err, errPendingJobQueueFull)
	require.False(t, queued)

	require.Equal(t, int32(maxPendingEnvelopes), impl.pending.count.Load())
	envelopeHash, err := envelope.HashSSZ()
	require.NoError(t, err)
	_, exists := impl.pending.jobs.Load(pendingEnvelopeKey{blockRoot, envelopeHash})
	require.False(t, exists)
}

func TestExecutionPayloadServicePendingQueueCapConcurrent(t *testing.T) {
	cfg := &clparams.MainnetBeaconConfig
	forkchoiceMock := mock_services.NewForkChoiceStorageMock(t)

	seenCache, err := lru.New[seenEnvelopeKey, struct{}]("seen_envelopes", seenEnvelopeCacheSize)
	require.NoError(t, err)
	impl := &executionPayloadService{
		forkchoiceStore:    forkchoiceMock,
		beaconCfg:          cfg,
		emitters:           beaconevents.NewEventEmitter(),
		seenEnvelopesCache: seenCache,
	}
	impl.pending = impl.newPendingQueue(canceledPendingQueueContext(t))
	impl.pending.stopAndWait()

	impl.pending.count.Store(maxPendingEnvelopes - 5)

	var wg sync.WaitGroup
	for i := range 100 {
		wg.Go(func() {
			blockRoot := common.Hash{byte(i), byte(i >> 8)}
			envelope := newTestSignedEnvelope(100, blockRoot, uint64(10000+i))
			_, _ = impl.queuePendingEnvelope(blockRoot, envelope, time.Now())
		})
	}
	wg.Wait()

	require.Equal(t, int32(maxPendingEnvelopes), impl.pending.count.Load())
	stored := 0
	impl.pending.jobs.Range(func(_, _ any) bool {
		stored++
		return true
	})
	require.Equal(t, 5, stored)
}

func TestExecutionPayloadServicePendingQueueOwnsBoundedBytes(t *testing.T) {
	service, forkchoiceMock := setupExecutionPayloadService(t)
	var forkchoiceAdmissions atomic.Int32
	forkchoiceMock.OnExecutionPayloadFn = func(context.Context, *cltypes.SignedExecutionPayloadEnvelope, bool, bool) error {
		forkchoiceAdmissions.Add(1)
		return nil
	}

	largeTransaction := make([]byte, int(clparams.MaxChunkSize)-1024)
	for i := range 5 {
		root := common.Hash{byte(i + 1)}
		envelope := newTestSignedEnvelope(100, root, uint64(i+1))
		envelope.Message.Payload.Transactions = solid.NewTransactionsSSZFromTransactions([][]byte{largeTransaction})
		err := service.ProcessMessage(t.Context(), nil, envelope)
		require.ErrorIs(t, err, ErrIgnore)
		if i == 4 {
			require.ErrorContains(t, err, "capacity reached")
		}
	}

	impl := service.(*executionPayloadService)
	require.Equal(t, int32(4), impl.pending.count.Load())
	require.Zero(t, forkchoiceAdmissions.Load())
}

func TestExecutionPayloadServiceProcessesEnvelopeWhenBlockArrivesAfterAdmission(t *testing.T) {
	service, forkchoiceMock := setupExecutionPayloadService(t)
	blockRoot := common.Hash{1}
	envelope := newTestSignedEnvelope(100, blockRoot, 1)
	type call struct {
		checkBlobData   bool
		validatePayload bool
	}
	calls := make(chan call, 2)
	forkchoiceMock.OnExecutionPayloadFn = func(_ context.Context, got *cltypes.SignedExecutionPayloadEnvelope, checkBlobData, validatePayload bool) error {
		require.Same(t, envelope, got)
		calls <- call{checkBlobData: checkBlobData, validatePayload: validatePayload}
		return nil
	}

	require.ErrorIs(t, service.ProcessMessage(t.Context(), nil, envelope), ErrIgnore)
	require.Empty(t, calls)
	forkchoiceMock.Blocks[blockRoot] = newTestGloasBlock(100, 1)
	service.(*executionPayloadService).pending.processPending(t.Context())

	require.Equal(t, call{checkBlobData: true, validatePayload: true}, <-calls)
	require.Empty(t, calls)
	require.Zero(t, service.(*executionPayloadService).pending.count.Load())
	require.Zero(t, service.(*executionPayloadService).pendingBytes.Load())
}

func TestExecutionPayloadServiceProcessesEnvelopeWhenBlockArrivesBeforeAdmission(t *testing.T) {
	service, forkchoiceMock := setupExecutionPayloadService(t)
	blockRoot := common.Hash{1}
	envelope := newTestSignedEnvelope(100, blockRoot, 1)
	forkchoiceMock.Blocks[blockRoot] = newTestGloasBlock(100, 1)
	var calls atomic.Int32
	forkchoiceMock.OnExecutionPayloadFn = func(_ context.Context, got *cltypes.SignedExecutionPayloadEnvelope, checkBlobData, validatePayload bool) error {
		require.Same(t, envelope, got)
		require.True(t, checkBlobData)
		require.True(t, validatePayload)
		calls.Add(1)
		return nil
	}

	require.NoError(t, service.ProcessMessage(t.Context(), nil, envelope))
	require.Equal(t, int32(1), calls.Load())
	require.Zero(t, service.(*executionPayloadService).pending.count.Load())
	require.Zero(t, service.(*executionPayloadService).pendingBytes.Load())
}

func TestExecutionPayloadServicePendingByteAdmissionConcurrent(t *testing.T) {
	cfg := &clparams.MainnetBeaconConfig
	forkchoiceMock := mock_services.NewForkChoiceStorageMock(t)
	impl := &executionPayloadService{
		forkchoiceStore: forkchoiceMock,
		beaconCfg:       cfg,
		emitters:        beaconevents.NewEventEmitter(),
	}
	impl.pending = impl.newPendingQueue(canceledPendingQueueContext(t))
	impl.pending.stopAndWait()
	envelopeSize := uint64(newTestSignedEnvelope(100, common.Hash{1}, 1).EncodingSizeSSZ())
	impl.pendingBytes.Store(maxPendingEnvelopeBytes - 5*envelopeSize)

	type result struct {
		queued bool
		err    error
	}
	results := make(chan result, 100)
	var wg sync.WaitGroup
	for i := range 100 {
		wg.Go(func() {
			queued, err := impl.queuePendingEnvelope(
				common.Hash{byte(i), byte(i >> 8)},
				newTestSignedEnvelope(100, common.Hash{byte(i), byte(i >> 8)}, uint64(i+1)),
				time.Now(),
			)
			results <- result{queued: queued, err: err}
		})
	}
	wg.Wait()
	close(results)

	admitted := 0
	for result := range results {
		if result.queued {
			require.NoError(t, result.err)
			admitted++
			continue
		}
		require.ErrorContains(t, result.err, "capacity reached")
	}
	require.Equal(t, 5, admitted)
	require.Equal(t, int32(5), impl.pending.count.Load())
	require.Equal(t, maxPendingEnvelopeBytes, impl.pendingBytes.Load())
}

func TestExecutionPayloadServiceDuplicateAtByteCapacityDoesNotReadmitForkchoice(t *testing.T) {
	service, forkchoiceMock := setupExecutionPayloadService(t)
	var forkchoiceAdmissions atomic.Int32
	forkchoiceMock.OnExecutionPayloadFn = func(context.Context, *cltypes.SignedExecutionPayloadEnvelope, bool, bool) error {
		forkchoiceAdmissions.Add(1)
		return nil
	}
	envelope := newTestSignedEnvelope(100, common.Hash{1}, 1)
	require.ErrorIs(t, service.ProcessMessage(t.Context(), nil, envelope), ErrIgnore)

	impl := service.(*executionPayloadService)
	impl.pendingBytes.Store(maxPendingEnvelopeBytes)
	require.ErrorIs(t, service.ProcessMessage(t.Context(), nil, envelope), ErrIgnore)
	require.Equal(t, int32(1), impl.pending.count.Load())
	require.Zero(t, forkchoiceAdmissions.Load())
}

func TestExecutionPayloadServiceConcurrentDuplicateRemovalConservesOwnership(t *testing.T) {
	service, forkchoiceMock := setupExecutionPayloadService(t)
	impl := service.(*executionPayloadService)
	blockRoot := common.Hash{1}
	envelope := newTestSignedEnvelope(100, blockRoot, 1)
	queued, err := impl.queuePendingEnvelope(blockRoot, envelope, time.Now())
	require.NoError(t, err)
	require.True(t, queued)
	forkchoiceMock.Blocks[blockRoot] = newTestGloasBlock(100, 1)

	results := make(chan error, 50)
	var wg sync.WaitGroup
	wg.Go(func() {
		for range 50 {
			impl.pending.processPending(t.Context())
		}
	})
	for range 50 {
		wg.Go(func() {
			_, err := impl.queuePendingEnvelope(blockRoot, envelope, time.Now())
			results <- err
		})
	}
	wg.Wait()
	close(results)
	for err := range results {
		require.NoError(t, err)
	}
	impl.pending.processPending(t.Context())

	stored := 0
	ownedBytes := uint64(0)
	impl.pending.jobs.Range(func(_, value any) bool {
		stored++
		ownedBytes += value.(*pendingJob[*pendingEnvelopeJob]).msg.ownedBytes
		return true
	})
	require.Equal(t, int32(stored), impl.pending.count.Load())
	require.Equal(t, ownedBytes, impl.pendingBytes.Load())
}

func TestExecutionPayloadServiceNames(t *testing.T) {
	service, _ := setupExecutionPayloadService(t)
	impl := service.(*executionPayloadService)

	names := impl.Names()
	require.Len(t, names, 1)
	require.Equal(t, "execution_payload", names[0])

	require.True(t, impl.IsMyGossipMessage("execution_payload"))
	require.False(t, impl.IsMyGossipMessage("beacon_block"))
}

func TestValidateEnvelopeLimitsDoesNotApplyLegacyDepositRequestMaximum(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	cfg.MaxDepositRequestsPerPayload = 1
	envelope := cltypes.NewExecutionPayloadEnvelope(&cfg)
	envelope.ExecutionRequests.Deposits.Append(&solid.DepositRequest{})
	envelope.ExecutionRequests.Deposits.Append(&solid.DepositRequest{})
	envelope.Payload.Withdrawals = solid.NewStaticListSSZ[*cltypes.Withdrawal](int(cfg.MaxWithdrawalsPerPayload), 44)
	require.NoError(t, validateEnvelopeLimits(&cfg, envelope))
}

func TestExecutionPayloadServiceDecodesProgressiveDepositRequestsAboveLegacyGuard(t *testing.T) {
	service, _ := setupExecutionPayloadService(t)
	envelope := newTestSignedEnvelope(100, common.Hash{1}, 1)
	const depositCount = 16_385
	for range depositCount {
		envelope.Message.ExecutionRequests.Deposits.Append(&solid.DepositRequest{})
	}
	encoded, err := envelope.EncodeSSZ(nil)
	require.NoError(t, err)
	require.Less(t, uint64(len(encoded)), clparams.MaxChunkSize)

	decoded, err := service.DecodeGossipMessage("peer123", encoded, clparams.GloasVersion)
	require.NoError(t, err)
	require.Equal(t, depositCount, decoded.Message.ExecutionRequests.Deposits.Len())
}

func TestValidateEnvelopeLimitsRejectsOversizedRequestsAndWithdrawals(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	cfg.MaxWithdrawalsPerPayload = 1
	envelope := cltypes.NewExecutionPayloadEnvelope(&cfg)
	envelope.Payload.Withdrawals = solid.NewStaticListSSZ[*cltypes.Withdrawal](16, 44)
	envelope.Payload.Withdrawals.Append(&cltypes.Withdrawal{})
	envelope.Payload.Withdrawals.Append(&cltypes.Withdrawal{})
	require.Error(t, validateEnvelopeLimits(&cfg, envelope))
}

func TestValidateEnvelopeLimitsRequiresWithdrawalsList(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	envelope := cltypes.NewExecutionPayloadEnvelope(&cfg)
	require.ErrorContains(t, validateEnvelopeLimits(&cfg, envelope), "missing payload withdrawals")

	envelope.Payload.Withdrawals = solid.NewStaticListSSZ[*cltypes.Withdrawal](int(cfg.MaxWithdrawalsPerPayload), 44)
	require.NoError(t, validateEnvelopeLimits(&cfg, envelope))
}
