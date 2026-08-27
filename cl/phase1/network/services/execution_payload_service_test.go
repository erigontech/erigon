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
	service := NewExecutionPayloadService(t.Context(), forkchoiceMock, cfg, beaconevents.NewEventEmitter())
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
	}
	return &cltypes.SignedExecutionPayloadEnvelope{
		Message:   envelope,
		Signature: common.Bytes96{},
	}
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

	// Now add block to forkchoice
	fcu.Blocks[blockRoot] = &cltypes.SignedBeaconBlock{
		Block: &cltypes.BeaconBlock{
			Slot: 100,
		},
	}

	// Process same envelope again - should succeed now (block found)
	// Note: OnExecutionPayload mock returns nil by default
	err = service.ProcessMessage(context.Background(), nil, envelope)
	require.NoError(t, err)
}

func TestExecutionPayloadServiceEmitsGossipAndImportedEvents(t *testing.T) {
	cfg := &clparams.MainnetBeaconConfig
	forkchoiceMock := mock_services.NewForkChoiceStorageMock(t)
	emitter := beaconevents.NewEventEmitter()
	service := NewExecutionPayloadService(t.Context(), forkchoiceMock, cfg, emitter)
	events := make(chan *beaconevents.EventStream, 3)
	subscription := emitter.Operation().Subscribe(events)
	defer subscription.Unsubscribe()
	stateEvents := make(chan *beaconevents.EventStream, 1)
	stateSubscription := emitter.State().Subscribe(stateEvents)
	defer stateSubscription.Unsubscribe()

	blockRoot := common.Hash{1}
	stateRoot := common.Hash{2}
	forkchoiceMock.Blocks[blockRoot] = &cltypes.SignedBeaconBlock{Block: &cltypes.BeaconBlock{Slot: 100, StateRoot: stateRoot}}
	headState := state.New(cfg)
	headState.SetVersion(clparams.GloasVersion)
	require.NoError(t, headState.SetSlot(100))
	require.NoError(t, headState.SetBlockRootAt(63, common.Hash{3}))
	require.NoError(t, headState.SetBlockRootAt(95, common.Hash{4}))
	forkchoiceMock.GetStateAtBlockRootFn = func(root common.Hash, alwaysCopy bool) (*state.CachingBeaconState, error) {
		require.Equal(t, blockRoot, root)
		require.True(t, alwaysCopy)
		return headState, nil
	}
	forkchoiceMock.HeadVal = blockRoot
	forkchoiceMock.HeadSlotVal = 100
	envelope := newTestSignedEnvelope(100, blockRoot, 7)
	require.NoError(t, service.ProcessMessage(t.Context(), nil, envelope))

	require.Equal(t, beaconevents.OpExecutionPayloadGossip, (<-events).Event)
	require.Equal(t, beaconevents.OpExecutionPayload, (<-events).Event)
	require.Equal(t, beaconevents.OpExecutionPayloadAvailable, (<-events).Event)
	headEvent := <-stateEvents
	require.Equal(t, beaconevents.StateHeadV2, headEvent.Event)
	require.Equal(t, "full", headEvent.Data.(*beaconevents.HeadV2Data).Data.PayloadStatus)
	require.Equal(t, blockRoot, headEvent.Data.(*beaconevents.HeadV2Data).Data.Block)
}

func TestExecutionPayloadServiceEmitsGossipWhenValidatedEnvelopeWaitsForColumns(t *testing.T) {
	cfg := &clparams.MainnetBeaconConfig
	forkchoiceMock := mock_services.NewForkChoiceStorageMock(t)
	emitter := beaconevents.NewEventEmitter()
	service := NewExecutionPayloadService(t.Context(), forkchoiceMock, cfg, emitter)
	events := make(chan *beaconevents.EventStream, 1)
	subscription := emitter.Operation().Subscribe(events)
	defer subscription.Unsubscribe()

	blockRoot := common.Hash{1}
	forkchoiceMock.Blocks[blockRoot] = &cltypes.SignedBeaconBlock{Block: &cltypes.BeaconBlock{Slot: 100}}
	forkchoiceMock.OnExecutionPayloadErr = forkchoice.ErrEIP7594ColumnDataNotAvailable
	envelope := newTestSignedEnvelope(100, blockRoot, 7)

	require.ErrorIs(t, service.ProcessMessage(t.Context(), nil, envelope), ErrIgnore)
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
	service := NewExecutionPayloadService(t.Context(), forkchoiceMock, cfg, emitter)
	stateEvents := make(chan *beaconevents.EventStream, 1)
	stateSubscription := emitter.State().Subscribe(stateEvents)
	defer stateSubscription.Unsubscribe()

	blockRoot := common.Hash{1}
	reorgRoot := common.Hash{9}
	forkchoiceMock.Blocks[blockRoot] = &cltypes.SignedBeaconBlock{Block: &cltypes.BeaconBlock{Slot: 100, StateRoot: common.Hash{2}}}
	headState := state.New(cfg)
	headState.SetVersion(clparams.GloasVersion)
	require.NoError(t, headState.SetSlot(100))
	require.NoError(t, headState.SetBlockRootAt(63, common.Hash{3}))
	require.NoError(t, headState.SetBlockRootAt(95, common.Hash{4}))
	forkchoiceMock.GetStateAtBlockRootFn = func(root common.Hash, alwaysCopy bool) (*state.CachingBeaconState, error) {
		require.Equal(t, blockRoot, root)
		require.True(t, alwaysCopy)
		forkchoiceMock.HeadVal = reorgRoot
		return headState, nil
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
	service := NewExecutionPayloadService(t.Context(), forkchoiceMock, cfg, emitter)
	stateEvents := make(chan *beaconevents.EventStream, 1)
	stateSubscription := emitter.State().Subscribe(stateEvents)
	defer stateSubscription.Unsubscribe()

	blockRoot := common.Hash{1}
	forkchoiceMock.Blocks[blockRoot] = &cltypes.SignedBeaconBlock{Block: &cltypes.BeaconBlock{Slot: 100, StateRoot: common.Hash{2}}}
	headState := state.New(cfg)
	headState.SetVersion(clparams.GloasVersion)
	require.NoError(t, headState.SetSlot(100))
	require.NoError(t, headState.SetBlockRootAt(63, common.Hash{3}))
	require.NoError(t, headState.SetBlockRootAt(95, common.Hash{4}))
	forkchoiceMock.GetStateAtBlockRootFn = func(common.Hash, bool) (*state.CachingBeaconState, error) {
		forkchoiceMock.HeadPayloadStatusVal = cltypes.PayloadStatusEmpty
		return headState, nil
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
	service := NewExecutionPayloadService(t.Context(), forkchoiceMock, cfg, emitter)
	events := make(chan *beaconevents.EventStream, 1)
	subscription := emitter.Operation().Subscribe(events)
	defer subscription.Unsubscribe()

	blockRoot := common.Hash{1}
	forkchoiceMock.Blocks[blockRoot] = &cltypes.SignedBeaconBlock{Block: &cltypes.BeaconBlock{Slot: 100}}
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
	service := NewExecutionPayloadService(t.Context(), forkchoiceMock, cfg, emitter)
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
	forkchoiceMock.Blocks[blockRoot] = &cltypes.SignedBeaconBlock{Block: &cltypes.BeaconBlock{Slot: 100}}
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
	fcu.Blocks[blockRoot] = &cltypes.SignedBeaconBlock{
		Block: &cltypes.BeaconBlock{
			Slot: 100,
		},
	}

	// First call should succeed
	err := service.ProcessMessage(context.Background(), nil, envelope)
	require.NoError(t, err)

	// Second call with same (blockRoot, builderIndex) should be ignored
	err = service.ProcessMessage(context.Background(), nil, envelope)
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrIgnore))
	require.Contains(t, err.Error(), "already seen envelope")
}

func TestExecutionPayloadServiceSlotBelowFinalized(t *testing.T) {
	service, fcu := setupExecutionPayloadService(t)

	blockRoot := common.HexToHash("0x1234")
	envelope := newTestSignedEnvelope(50, blockRoot, 1) // slot 50

	// Add block to forkchoice
	fcu.Blocks[blockRoot] = &cltypes.SignedBeaconBlock{
		Block: &cltypes.BeaconBlock{
			Slot: 51,
		},
	}

	// Set finalized slot higher than envelope slot
	fcu.FinalizedSlotVal = 100
	fcu.FinalizedCheckpointVal = solid.Checkpoint{Epoch: 2}

	err := service.ProcessMessage(context.Background(), nil, envelope)
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrIgnore))
	require.Contains(t, err.Error(), "envelope slot 50 < finalized slot 64")
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
			fcu.Blocks[root] = &cltypes.SignedBeaconBlock{Block: &cltypes.BeaconBlock{Slot: tc.slot}}

			err := service.ProcessMessage(context.Background(), nil, newTestSignedEnvelope(tc.slot, root, 1))
			if tc.ignored {
				require.ErrorIs(t, err, ErrIgnore)
				return
			}
			require.NoError(t, err)
		})
	}
}

func TestExecutionPayloadServiceSuccess(t *testing.T) {
	service, fcu := setupExecutionPayloadService(t)

	blockRoot := common.HexToHash("0x1234")
	envelope := newTestSignedEnvelope(100, blockRoot, 1)

	// Add block to forkchoice
	fcu.Blocks[blockRoot] = &cltypes.SignedBeaconBlock{
		Block: &cltypes.BeaconBlock{
			Slot: 100,
		},
	}
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
			fcu.Blocks[blockRoot] = &cltypes.SignedBeaconBlock{Block: &cltypes.BeaconBlock{Slot: 100}}
			fcu.FinalizedSlotVal = 50
			fcu.OnExecutionPayloadErr = processErr

			err := service.ProcessMessage(context.Background(), nil, newTestSignedEnvelope(100, blockRoot, 1))
			require.ErrorIs(t, err, ErrIgnore)
		})
	}
}

func TestExecutionPayloadServiceDifferentBuildersSameBlock(t *testing.T) {
	service, fcu := setupExecutionPayloadService(t)

	blockRoot := common.HexToHash("0x1234")
	envelope1 := newTestSignedEnvelope(100, blockRoot, 1) // builder 1
	envelope2 := newTestSignedEnvelope(100, blockRoot, 2) // builder 2

	// Add block to forkchoice
	fcu.Blocks[blockRoot] = &cltypes.SignedBeaconBlock{
		Block: &cltypes.BeaconBlock{
			Slot: 100,
		},
	}
	fcu.FinalizedSlotVal = 50

	// Both envelopes should be accepted (different builders)
	err := service.ProcessMessage(context.Background(), nil, envelope1)
	require.NoError(t, err)

	err = service.ProcessMessage(context.Background(), nil, envelope2)
	require.NoError(t, err)

	// Verify both are marked as seen
	impl := service.(*executionPayloadService)
	require.True(t, impl.seenEnvelopesCache.Contains(seenEnvelopeKey{blockRoot, 1}))
	require.True(t, impl.seenEnvelopesCache.Contains(seenEnvelopeKey{blockRoot, 2}))
}

func TestExecutionPayloadServicePendingEnvelopeExpiry(t *testing.T) {
	cfg := &clparams.MainnetBeaconConfig
	forkchoiceMock := mock_services.NewForkChoiceStorageMock(t)
	ctx := t.Context()

	// Create service directly to access internals; the background loop is not started
	impl := &executionPayloadService{
		forkchoiceStore: forkchoiceMock,
		beaconCfg:       cfg,
		emitters:        beaconevents.NewEventEmitter(),
	}
	impl.pending = impl.newPendingQueue()
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
	impl.pending.jobs.Store(key, &pendingJob[*pendingEnvelopeJob]{
		msg:          &pendingEnvelopeJob{envelope: envelope, ownedBytes: ownedBytes},
		creationTime: time.Now().Add(-pendingEnvelopeExpiry - time.Second), // expired
	})
	impl.pending.count.Store(1)
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

	// Create service directly to access internals; the background loop is not started
	impl := &executionPayloadService{
		forkchoiceStore: forkchoiceMock,
		beaconCfg:       cfg,
		emitters:        beaconevents.NewEventEmitter(),
	}
	impl.pending = impl.newPendingQueue()
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
	impl.pending.jobs.Store(key, &pendingJob[*pendingEnvelopeJob]{
		msg:          &pendingEnvelopeJob{envelope: envelope, ownedBytes: ownedBytes},
		creationTime: time.Now(),
	})
	impl.pending.count.Store(1)
	impl.pendingBytes.Store(ownedBytes)

	// Block not yet available - should keep pending
	impl.pending.processPending(ctx)
	require.Equal(t, int32(1), impl.pending.count.Load())

	// Now add block
	forkchoiceMock.Blocks[blockRoot] = &cltypes.SignedBeaconBlock{
		Block: &cltypes.BeaconBlock{
			Slot: 100,
		},
	}

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
	impl.pending = impl.newPendingQueue()
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
	impl.pending.jobs.Store(pendingEnvelopeKey{blockRoot, hash1}, &pendingJob[*pendingEnvelopeJob]{
		msg:          &pendingEnvelopeJob{envelope: envelope1, ownedBytes: ownedBytes1},
		creationTime: time.Now(),
	})
	impl.pending.jobs.Store(pendingEnvelopeKey{blockRoot, hash2}, &pendingJob[*pendingEnvelopeJob]{
		msg:          &pendingEnvelopeJob{envelope: envelope2, ownedBytes: ownedBytes2},
		creationTime: time.Now(),
	})
	impl.pending.count.Store(2)
	impl.pendingBytes.Store(ownedBytes1 + ownedBytes2)

	// Add block
	forkchoiceMock.Blocks[blockRoot] = &cltypes.SignedBeaconBlock{
		Block: &cltypes.BeaconBlock{
			Slot: 100,
		},
	}

	// Process - both should be processed
	impl.pending.processPending(ctx)

	require.Equal(t, int32(0), impl.pending.count.Load())
	require.Zero(t, impl.pendingBytes.Load())
	require.True(t, impl.seenEnvelopesCache.Contains(seenEnvelopeKey{blockRoot, 1}))
	require.True(t, impl.seenEnvelopesCache.Contains(seenEnvelopeKey{blockRoot, 2}))
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
	impl.pending = impl.newPendingQueue()

	impl.pending.count.Store(maxPendingEnvelopes)

	blockRoot := common.HexToHash("0xffff")
	envelope := newTestSignedEnvelope(100, blockRoot, 999)

	impl.queuePendingEnvelope(blockRoot, envelope)

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
	impl.pending = impl.newPendingQueue()

	impl.pending.count.Store(maxPendingEnvelopes - 5)

	var wg sync.WaitGroup
	for i := range 100 {
		wg.Go(func() {
			blockRoot := common.Hash{byte(i), byte(i >> 8)}
			envelope := newTestSignedEnvelope(100, blockRoot, uint64(10000+i))
			impl.queuePendingEnvelope(blockRoot, envelope)
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
	forkchoiceMock.Blocks[blockRoot] = &cltypes.SignedBeaconBlock{Block: &cltypes.BeaconBlock{Slot: 100}}
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
	forkchoiceMock.Blocks[blockRoot] = &cltypes.SignedBeaconBlock{Block: &cltypes.BeaconBlock{Slot: 100}}
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
	impl.pending = impl.newPendingQueue()
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
	queued, err := impl.queuePendingEnvelope(blockRoot, envelope)
	require.NoError(t, err)
	require.True(t, queued)
	forkchoiceMock.Blocks[blockRoot] = &cltypes.SignedBeaconBlock{Block: &cltypes.BeaconBlock{Slot: 100}}

	results := make(chan error, 50)
	var wg sync.WaitGroup
	wg.Go(func() {
		for range 50 {
			impl.pending.processPending(t.Context())
		}
	})
	for range 50 {
		wg.Go(func() {
			_, err := impl.queuePendingEnvelope(blockRoot, envelope)
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
