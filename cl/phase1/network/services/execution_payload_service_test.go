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
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/beacon/beaconevents"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
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

func setupExecutionPayloadServiceWithoutLoop(t *testing.T) (*executionPayloadService, *mock_services.ForkChoiceStorageMock) {
	cfg := &clparams.MainnetBeaconConfig
	forkchoiceMock := mock_services.NewForkChoiceStorageMock(t)
	seenCache, err := lru.New[seenEnvelopeKey, struct{}]("seen_envelopes", seenEnvelopeCacheSize)
	require.NoError(t, err)
	return &executionPayloadService{
		forkchoiceStore:    forkchoiceMock,
		beaconCfg:          cfg,
		emitters:           beaconevents.NewEventEmitter(),
		seenEnvelopesCache: seenCache,
		pendingCond:        sync.NewCond(&sync.Mutex{}),
	}, forkchoiceMock
}

func newTestSignedEnvelope(slot uint64, blockRoot common.Hash, builderIndex uint64) *cltypes.SignedExecutionPayloadEnvelope {
	envelope := cltypes.NewExecutionPayloadEnvelope(&clparams.MainnetBeaconConfig)
	envelope.BeaconBlockRoot = blockRoot
	envelope.BuilderIndex = builderIndex
	// Initialize Eth1Block fields needed for HashSSZ
	if envelope.Payload != nil {
		envelope.Payload.Extra = solid.NewExtraData()
		envelope.Payload.Transactions = &solid.TransactionsSSZ{}
	}
	return &cltypes.SignedExecutionPayloadEnvelope{
		Message:   envelope,
		Signature: common.Bytes96{},
	}
}

func newTestSignedBlockWithBuilder(_ common.Hash, slot, builderIndex uint64) *cltypes.SignedBeaconBlock {
	block := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.GloasVersion)
	block.Block.Slot = slot
	block.Block.Body.SignedExecutionPayloadBid.Message.BuilderIndex = builderIndex
	return block
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
	require.Equal(t, int32(1), impl.pendingCount.Load())

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

func TestExecutionPayloadServiceAccountsEnvelopeAppliedWhenBlockArrives(t *testing.T) {
	impl, fcu := setupExecutionPayloadServiceWithoutLoop(t)
	blockRoot := common.HexToHash("0x1234")
	envelope := newTestSignedEnvelope(100, blockRoot, 1)
	events := make(chan *beaconevents.EventStream, 1)
	sub := impl.emitters.Operation().Subscribe(events)
	defer sub.Unsubscribe()

	require.ErrorIs(t, impl.ProcessMessage(t.Context(), nil, envelope), ErrIgnore)
	require.Equal(t, int32(1), impl.pendingCount.Load())
	fcu.Blocks[blockRoot] = &cltypes.SignedBeaconBlock{Block: &cltypes.BeaconBlock{Slot: 100}}
	fcu.Envelopes[blockRoot] = envelope
	impl.processPendingEnvelopes(t.Context())

	require.Equal(t, int32(0), impl.pendingCount.Load())
	require.True(t, impl.seenEnvelopesCache.Contains(seenEnvelopeKey{blockRoot, 1}))
	require.Equal(t, beaconevents.OpExecutionPayloadAvailable, (<-events).Event)
}

func TestExecutionPayloadServiceNilBeaconBlock(t *testing.T) {
	impl, fcu := setupExecutionPayloadServiceWithoutLoop(t)
	blockRoot := common.HexToHash("0x1234")
	fcu.Blocks[blockRoot] = new(cltypes.SignedBeaconBlock)

	err := impl.ProcessMessage(t.Context(), nil, newTestSignedEnvelope(100, blockRoot, 1))
	require.Error(t, err)
	require.Contains(t, err.Error(), "nil beacon block")
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
			Slot: 50,
		},
	}

	// Set finalized slot higher than envelope slot
	fcu.FinalizedSlotVal = 100

	err := service.ProcessMessage(context.Background(), nil, envelope)
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrIgnore))
	require.Contains(t, err.Error(), "envelope slot 50 < finalized slot 100")
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

	// Create service directly to access internals
	impl := &executionPayloadService{
		forkchoiceStore: forkchoiceMock,
		beaconCfg:       cfg,
		emitters:        beaconevents.NewEventEmitter(),
		pendingCond:     nil, // Don't start background loop
	}
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
	impl.pendingEnvelopes.Store(key, &envelopeJob{
		envelope:     envelope,
		creationTime: time.Now().Add(-pendingEnvelopeExpiry - time.Second), // expired
	})
	impl.pendingCount.Store(1)

	// Process pending - should remove expired
	impl.processPendingEnvelopes(ctx)

	require.Equal(t, int32(0), impl.pendingCount.Load())
	_, exists := impl.pendingEnvelopes.Load(key)
	require.False(t, exists)
}

func TestExecutionPayloadServicePendingEnvelopeProcessing(t *testing.T) {
	cfg := &clparams.MainnetBeaconConfig
	forkchoiceMock := mock_services.NewForkChoiceStorageMock(t)
	ctx := t.Context()

	// Create service directly to access internals
	impl := &executionPayloadService{
		forkchoiceStore: forkchoiceMock,
		beaconCfg:       cfg,
		emitters:        beaconevents.NewEventEmitter(),
		pendingCond:     nil,
	}
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
	job := &envelopeJob{
		envelope:     envelope,
		creationTime: time.Now(),
	}
	job.validate.Store(true)
	impl.pendingEnvelopes.Store(key, job)
	impl.pendingCount.Store(1)

	// Block not yet available - should keep pending
	impl.processPendingEnvelopes(ctx)
	require.Equal(t, int32(1), impl.pendingCount.Load())

	// Now add block
	forkchoiceMock.Blocks[blockRoot] = &cltypes.SignedBeaconBlock{
		Block: &cltypes.BeaconBlock{
			Slot: 100,
		},
	}

	// Process again - should process and remove
	impl.processPendingEnvelopes(ctx)
	require.Equal(t, int32(0), impl.pendingCount.Load())
	_, exists := impl.pendingEnvelopes.Load(key)
	require.False(t, exists)

	// Envelope should be marked as seen
	require.True(t, impl.seenEnvelopesCache.Contains(seenEnvelopeKey{blockRoot, 1}))
}

func TestExecutionPayloadServiceRetriesEnvelopeUntilColumnDataAvailable(t *testing.T) {
	impl, fcu := setupExecutionPayloadServiceWithoutLoop(t)
	blockRoot := common.HexToHash("0x1234")
	envelope := newTestSignedEnvelope(100, blockRoot, 1)
	fcu.Blocks[blockRoot] = newTestSignedBlockWithBuilder(blockRoot, 100, 1)
	fcu.OnExecutionPayloadErr = forkchoice.ErrEIP7594ColumnDataNotAvailable

	err := impl.ProcessMessage(t.Context(), nil, envelope)
	require.ErrorIs(t, err, ErrIgnore)
	require.Equal(t, int32(1), impl.pendingCount.Load())

	impl.pendingEnvelopes.Range(func(_, value any) bool {
		value.(*envelopeJob).nextAttempt = time.Time{}
		return true
	})
	impl.processPendingEnvelopes(t.Context())
	require.Equal(t, int32(1), impl.pendingCount.Load())
	impl.pendingEnvelopes.Range(func(_, value any) bool {
		job := value.(*envelopeJob)
		require.Equal(t, 2*time.Second, job.retryDelay)
		require.True(t, job.nextAttempt.After(time.Now()))
		return true
	})

	fcu.OnExecutionPayloadErr = nil
	impl.pendingEnvelopes.Range(func(_, value any) bool {
		value.(*envelopeJob).nextAttempt = time.Time{}
		return true
	})
	impl.processPendingEnvelopes(t.Context())
	require.Equal(t, int32(0), impl.pendingCount.Load())
	require.True(t, impl.seenEnvelopesCache.Contains(seenEnvelopeKey{blockRoot, 1}))
}

func TestExecutionPayloadServiceRetriesRecoveredEnvelope(t *testing.T) {
	impl, fcu := setupExecutionPayloadServiceWithoutLoop(t)
	blockRoot := common.HexToHash("0x1234")
	envelope := newTestSignedEnvelope(100, blockRoot, 1)
	fcu.Blocks[blockRoot] = newTestSignedBlockWithBuilder(blockRoot, 100, 1)
	fcu.OnExecutionPayloadErr = forkchoice.ErrEIP7594ColumnDataNotAvailable

	err := impl.ProcessRecoveredEnvelope(t.Context(), envelope, true)
	require.ErrorIs(t, err, ErrIgnore)
	require.ErrorIs(t, err, forkchoice.ErrEIP7594ColumnDataNotAvailable)
	require.Equal(t, int32(1), impl.pendingCount.Load())

	fcu.OnExecutionPayloadErr = nil
	impl.pendingEnvelopes.Range(func(_, value any) bool {
		value.(*envelopeJob).nextAttempt = time.Time{}
		return true
	})
	impl.processPendingEnvelopes(t.Context())
	require.Equal(t, int32(0), impl.pendingCount.Load())
	require.True(t, impl.seenEnvelopesCache.Contains(seenEnvelopeKey{blockRoot, 1}))
}

func TestExecutionPayloadServiceDataAvailabilityRetriesDeduplicateByRoot(t *testing.T) {
	impl, fcu := setupExecutionPayloadServiceWithoutLoop(t)
	blockRoot := common.HexToHash("0x1234")
	fcu.Blocks[blockRoot] = newTestSignedBlockWithBuilder(blockRoot, 100, 1)
	fcu.OnExecutionPayloadErr = forkchoice.ErrEIP7594ColumnDataNotAvailable

	require.Error(t, impl.ProcessRecoveredEnvelope(t.Context(), newTestSignedEnvelope(100, blockRoot, 1), true))
	require.Error(t, impl.ProcessRecoveredEnvelope(t.Context(), newTestSignedEnvelope(100, blockRoot, 2), true))
	require.Equal(t, int32(1), impl.pendingCount.Load())
}

func TestExecutionPayloadServiceFreshDataAvailabilityRetryRefreshesExpiry(t *testing.T) {
	impl, _ := setupExecutionPayloadServiceWithoutLoop(t)
	blockRoot := common.HexToHash("0x1234")
	envelope := newTestSignedEnvelope(100, blockRoot, 1)
	impl.queuePendingEnvelopeWithOptions(blockRoot, envelope, false, true, true)

	key := pendingEnvelopeKey{blockRoot: blockRoot, dataAvailability: true}
	value, ok := impl.pendingEnvelopes.Load(key)
	require.True(t, ok)
	value.(*envelopeJob).creationTime = time.Now().Add(-pendingDataAvailabilityExpiry - time.Second)

	impl.queuePendingEnvelopeWithOptions(blockRoot, envelope, true, true, true)
	impl.processPendingEnvelopes(t.Context())

	value, ok = impl.pendingEnvelopes.Load(key)
	require.True(t, ok)
	require.True(t, value.(*envelopeJob).recovered.Load())
	require.Equal(t, int32(1), impl.pendingCount.Load())
}

func TestExecutionPayloadServicePendingExpiryCoversDeferredColumnSync(t *testing.T) {
	require.Equal(t, 30*time.Second, pendingEnvelopeExpiry)
	require.Greater(t, pendingDataAvailabilityExpiry, time.Minute)
}

func TestExecutionPayloadServiceRejectsGossipAfterRecoveredEnvelope(t *testing.T) {
	impl, fcu := setupExecutionPayloadServiceWithoutLoop(t)
	blockRoot := common.HexToHash("0x1234")
	valid := newTestSignedEnvelope(100, blockRoot, 1)
	fcu.Blocks[blockRoot] = newTestSignedBlockWithBuilder(blockRoot, 100, 1)
	require.NoError(t, impl.ProcessRecoveredEnvelope(t.Context(), valid, true))
	fcu.Envelopes[blockRoot] = valid

	forged := newTestSignedEnvelope(100, blockRoot, 2)
	err := impl.ProcessMessage(t.Context(), nil, forged)
	require.Error(t, err)
	require.NotErrorIs(t, err, ErrIgnore)
	require.False(t, impl.seenEnvelopesCache.Contains(seenEnvelopeKey{blockRoot, 2}))
}

func TestExecutionPayloadServiceRejectsConcurrentAlreadyStoredGossip(t *testing.T) {
	impl, fcu := setupExecutionPayloadServiceWithoutLoop(t)
	blockRoot := common.HexToHash("0x1234")
	fcu.Blocks[blockRoot] = newTestSignedBlockWithBuilder(blockRoot, 100, 1)
	fcu.Envelopes[blockRoot] = newTestSignedEnvelope(100, blockRoot, 1)
	hasEnvelope := false
	fcu.HasEnvelopeOverride = &hasEnvelope
	fcu.OnExecutionPayloadErr = forkchoice.ErrExecutionPayloadAlreadyStored

	err := impl.ProcessMessage(t.Context(), nil, newTestSignedEnvelope(100, blockRoot, 2))
	require.Error(t, err)
	require.NotErrorIs(t, err, ErrIgnore)
	require.Equal(t, int32(0), impl.pendingCount.Load())
	require.False(t, impl.seenEnvelopesCache.Contains(seenEnvelopeKey{blockRoot, 2}))
}

func TestExecutionPayloadServiceIgnoresSameBuilderWithoutStoredEnvelopeRead(t *testing.T) {
	impl, fcu := setupExecutionPayloadServiceWithoutLoop(t)
	blockRoot := common.HexToHash("0x1234")
	fcu.Blocks[blockRoot] = newTestSignedBlockWithBuilder(blockRoot, 100, 1)
	fcu.Envelopes[blockRoot] = newTestSignedEnvelope(100, blockRoot, 1)

	err := impl.ProcessMessage(t.Context(), nil, newTestSignedEnvelope(100, blockRoot, 1))
	require.ErrorIs(t, err, ErrIgnore)
	require.Zero(t, fcu.ReadEnvelopeCalls.Load())
}

func TestExecutionPayloadServiceUsesCommittedBuilderForDuplicateGossip(t *testing.T) {
	impl, fcu := setupExecutionPayloadServiceWithoutLoop(t)
	blockRoot := common.HexToHash("0x1234")
	fcu.Blocks[blockRoot] = newTestSignedBlockWithBuilder(blockRoot, 100, 1)
	fcu.Envelopes[blockRoot] = newTestSignedEnvelope(100, blockRoot, 1)

	for builderIndex := uint64(2); builderIndex <= 3; builderIndex++ {
		err := impl.ProcessMessage(t.Context(), nil, newTestSignedEnvelope(100, blockRoot, builderIndex))
		require.Error(t, err)
		require.NotErrorIs(t, err, ErrIgnore)
	}
	require.Zero(t, fcu.ReadEnvelopeCalls.Load())
}

func TestExecutionPayloadServiceFinalizedDuplicateAvoidsStoredEnvelopeRead(t *testing.T) {
	impl, fcu := setupExecutionPayloadServiceWithoutLoop(t)
	blockRoot := common.HexToHash("0x1234")
	fcu.Blocks[blockRoot] = newTestSignedBlockWithBuilder(blockRoot, 99, 1)
	fcu.Envelopes[blockRoot] = newTestSignedEnvelope(99, blockRoot, 1)
	fcu.FinalizedSlotVal = 100

	err := impl.ProcessMessage(t.Context(), nil, newTestSignedEnvelope(99, blockRoot, 2))
	require.ErrorIs(t, err, ErrIgnore)
	require.Zero(t, fcu.ReadEnvelopeCalls.Load())
}

func TestExecutionPayloadServiceEmitsAvailabilityOnceAcrossProvenance(t *testing.T) {
	for _, tt := range []struct {
		name           string
		firstRecovered bool
	}{
		{name: "recovery then gossip", firstRecovered: true},
		{name: "gossip then recovery"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			impl, fcu := setupExecutionPayloadServiceWithoutLoop(t)
			blockRoot := common.HexToHash("0x1234")
			envelope := newTestSignedEnvelope(100, blockRoot, 1)
			fcu.Blocks[blockRoot] = &cltypes.SignedBeaconBlock{Block: &cltypes.BeaconBlock{Slot: 100}}

			events := make(chan *beaconevents.EventStream, 2)
			sub := impl.emitters.Operation().Subscribe(events)
			defer sub.Unsubscribe()

			if tt.firstRecovered {
				require.NoError(t, impl.ProcessRecoveredEnvelope(t.Context(), envelope, true))
			} else {
				require.NoError(t, impl.ProcessMessage(t.Context(), nil, envelope))
			}
			fcu.Envelopes[blockRoot] = envelope
			fcu.OnExecutionPayloadErr = forkchoice.ErrExecutionPayloadAlreadyStored
			if tt.firstRecovered {
				require.ErrorIs(t, impl.ProcessMessage(t.Context(), nil, envelope), ErrIgnore)
			} else {
				require.ErrorIs(t, impl.ProcessRecoveredEnvelope(t.Context(), envelope, true), ErrIgnore)
			}

			event := <-events
			require.Equal(t, beaconevents.OpExecutionPayloadAvailable, event.Event)
			select {
			case duplicate := <-events:
				t.Fatalf("unexpected duplicate event: %v", duplicate)
			default:
			}
		})
	}
}

func TestExecutionPayloadServiceMultiplePendingForSameBlock(t *testing.T) {
	cfg := &clparams.MainnetBeaconConfig
	forkchoiceMock := mock_services.NewForkChoiceStorageMock(t)
	ctx := t.Context()

	impl := &executionPayloadService{
		forkchoiceStore: forkchoiceMock,
		beaconCfg:       cfg,
		emitters:        beaconevents.NewEventEmitter(),
		pendingCond:     nil,
	}
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
	job1 := &envelopeJob{
		envelope:     envelope1,
		creationTime: time.Now(),
	}
	job1.validate.Store(true)
	impl.pendingEnvelopes.Store(pendingEnvelopeKey{blockRoot, hash1, false}, job1)
	job2 := &envelopeJob{
		envelope:     envelope2,
		creationTime: time.Now(),
	}
	job2.validate.Store(true)
	impl.pendingEnvelopes.Store(pendingEnvelopeKey{blockRoot, hash2, false}, job2)
	impl.pendingCount.Store(2)

	// Add block
	forkchoiceMock.Blocks[blockRoot] = &cltypes.SignedBeaconBlock{
		Block: &cltypes.BeaconBlock{
			Slot: 100,
		},
	}

	// Process - both should be processed
	impl.processPendingEnvelopes(ctx)

	require.Equal(t, int32(0), impl.pendingCount.Load())
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
		pendingCond:        sync.NewCond(&sync.Mutex{}),
	}

	impl.pendingCount.Store(maxPendingEnvelopes)

	blockRoot := common.HexToHash("0xffff")
	envelope := newTestSignedEnvelope(100, blockRoot, 999)

	impl.queuePendingEnvelope(blockRoot, envelope)

	require.Equal(t, int32(maxPendingEnvelopes), impl.pendingCount.Load())
	envelopeHash, err := envelope.HashSSZ()
	require.NoError(t, err)
	_, exists := impl.pendingEnvelopes.Load(pendingEnvelopeKey{blockRoot, envelopeHash, false})
	require.False(t, exists)
}

func TestExecutionPayloadServicePendingQueueCapRejectsUntrustedBeforeHashing(t *testing.T) {
	impl, _ := setupExecutionPayloadServiceWithoutLoop(t)
	impl.pendingCount.Store(maxPendingEnvelopes)

	require.NotPanics(t, func() {
		impl.queuePendingEnvelope(common.HexToHash("0x1234"), &cltypes.SignedExecutionPayloadEnvelope{})
	})
	require.Equal(t, int32(maxPendingEnvelopes), impl.pendingCount.Load())
}

func TestExecutionPayloadServicePendingQueueUpgradesDataAvailabilityDuplicateAtCap(t *testing.T) {
	impl, _ := setupExecutionPayloadServiceWithoutLoop(t)
	blockRoot := common.HexToHash("0x1234")
	envelope := newTestSignedEnvelope(100, blockRoot, 1)
	impl.queuePendingEnvelopeWithOptions(blockRoot, envelope, false, true, true)
	impl.pendingCount.Store(maxPendingEnvelopes)

	impl.queuePendingEnvelopeWithOptions(blockRoot, envelope, true, true, true)

	value, ok := impl.pendingEnvelopes.Load(pendingEnvelopeKey{blockRoot: blockRoot, dataAvailability: true})
	require.True(t, ok)
	require.True(t, value.(*envelopeJob).recovered.Load())
	require.Equal(t, int32(maxPendingEnvelopes), impl.pendingCount.Load())
}

func TestExecutionPayloadServicePromotesValidatedRetryAtCap(t *testing.T) {
	impl, _ := setupExecutionPayloadServiceWithoutLoop(t)
	blockRoot := common.HexToHash("0x1234")
	envelope := newTestSignedEnvelope(100, blockRoot, 1)
	envelopeHash, err := envelope.HashSSZ()
	require.NoError(t, err)
	oldKey := pendingEnvelopeKey{blockRoot: blockRoot, envelopeHash: envelopeHash}
	job := &envelopeJob{envelope: envelope, creationTime: time.Now()}
	job.validate.Store(true)
	impl.pendingEnvelopes.Store(oldKey, job)
	impl.pendingCount.Store(maxPendingEnvelopes)

	impl.promoteDataAvailabilityRetry(oldKey, job)

	_, oldExists := impl.pendingEnvelopes.Load(oldKey)
	value, newExists := impl.pendingEnvelopes.Load(pendingEnvelopeKey{blockRoot: blockRoot, dataAvailability: true})
	require.False(t, oldExists)
	require.True(t, newExists)
	require.Same(t, job, value)
	require.Equal(t, int32(maxPendingEnvelopes), impl.pendingCount.Load())
}

func TestExecutionPayloadServiceDataAvailabilityRetryEvictsUnvalidatedAtCap(t *testing.T) {
	impl, _ := setupExecutionPayloadServiceWithoutLoop(t)
	unvalidatedRoot := common.HexToHash("0x1111")
	unvalidated := newTestSignedEnvelope(100, unvalidatedRoot, 1)
	unvalidatedHash, err := unvalidated.HashSSZ()
	require.NoError(t, err)
	unvalidatedKey := pendingEnvelopeKey{blockRoot: unvalidatedRoot, envelopeHash: unvalidatedHash}
	impl.pendingEnvelopes.Store(unvalidatedKey, &envelopeJob{envelope: unvalidated, creationTime: time.Now()})
	impl.pendingCount.Store(maxPendingEnvelopes)

	validatedRoot := common.HexToHash("0x2222")
	validated := newTestSignedEnvelope(100, validatedRoot, 2)
	impl.queuePendingEnvelopeWithOptions(validatedRoot, validated, true, true, true)

	_, unvalidatedExists := impl.pendingEnvelopes.Load(unvalidatedKey)
	_, validatedExists := impl.pendingEnvelopes.Load(pendingEnvelopeKey{blockRoot: validatedRoot, dataAvailability: true})
	require.False(t, unvalidatedExists)
	require.True(t, validatedExists)
	require.Equal(t, int32(maxPendingEnvelopes), impl.pendingCount.Load())
}

func TestExecutionPayloadServiceDataAvailabilityRetryDoesNotEvictProcessingJob(t *testing.T) {
	impl, _ := setupExecutionPayloadServiceWithoutLoop(t)
	processingRoot := common.HexToHash("0x1111")
	processingEnvelope := newTestSignedEnvelope(100, processingRoot, 1)
	processingHash, err := processingEnvelope.HashSSZ()
	require.NoError(t, err)
	processingKey := pendingEnvelopeKey{blockRoot: processingRoot, envelopeHash: processingHash}
	impl.pendingEnvelopes.Store(processingKey, &envelopeJob{envelope: processingEnvelope, creationTime: time.Now(), processing: true})

	impl.pendingCount.Store(1)
	impl.pendingMu.Lock()
	impl.evictUnvalidatedPendingEnvelope()
	impl.pendingMu.Unlock()

	_, processingExists := impl.pendingEnvelopes.Load(processingKey)
	require.True(t, processingExists)
	require.Equal(t, int32(1), impl.pendingCount.Load())
}

func TestExecutionPayloadServiceInFlightPromotionSurvivesPriorityEviction(t *testing.T) {
	impl, fcu := setupExecutionPayloadServiceWithoutLoop(t)
	processingRoot := common.HexToHash("0x1111")
	processingEnvelope := newTestSignedEnvelope(100, processingRoot, 1)
	processingHash, err := processingEnvelope.HashSSZ()
	require.NoError(t, err)
	processingKey := pendingEnvelopeKey{blockRoot: processingRoot, envelopeHash: processingHash}
	processingJob := &envelopeJob{envelope: processingEnvelope, creationTime: time.Now()}
	processingJob.validate.Store(true)
	impl.pendingEnvelopes.Store(processingKey, processingJob)
	fcu.Blocks[processingRoot] = newTestSignedBlockWithBuilder(processingRoot, 100, 1)

	evictableRoot := common.HexToHash("0x2222")
	evictableEnvelope := newTestSignedEnvelope(100, evictableRoot, 2)
	evictableHash, err := evictableEnvelope.HashSSZ()
	require.NoError(t, err)
	impl.pendingEnvelopes.Store(
		pendingEnvelopeKey{blockRoot: evictableRoot, envelopeHash: evictableHash},
		&envelopeJob{envelope: evictableEnvelope, creationTime: time.Now()},
	)
	impl.pendingCount.Store(maxPendingEnvelopes)

	entered := make(chan struct{})
	release := make(chan struct{})
	fcu.OnExecutionPayloadFunc = func(context.Context, *cltypes.SignedExecutionPayloadEnvelope, bool, bool) error {
		close(entered)
		<-release
		return forkchoice.ErrEIP7594ColumnDataNotAvailable
	}
	done := make(chan struct{})
	go func() {
		impl.processPendingEnvelopes(t.Context())
		close(done)
	}()
	<-entered

	priorityRoot := common.HexToHash("0x3333")
	impl.queuePendingEnvelopeWithOptions(priorityRoot, newTestSignedEnvelope(100, priorityRoot, 3), true, true, true)
	close(release)
	<-done

	_, promoted := impl.pendingEnvelopes.Load(pendingEnvelopeKey{blockRoot: processingRoot, dataAvailability: true})
	_, priority := impl.pendingEnvelopes.Load(pendingEnvelopeKey{blockRoot: priorityRoot, dataAvailability: true})
	require.True(t, promoted)
	require.True(t, priority)
}

func TestExecutionPayloadServicePendingDuplicateUpgradesAtCap(t *testing.T) {
	impl, _ := setupExecutionPayloadServiceWithoutLoop(t)
	blockRoot := common.HexToHash("0x1234")
	envelope := newTestSignedEnvelope(100, blockRoot, 1)
	impl.queuePendingEnvelopeWithOptions(blockRoot, envelope, false, false, false)
	impl.pendingCount.Store(maxPendingEnvelopes)

	impl.queuePendingEnvelopeWithOptions(blockRoot, envelope, true, true, false)

	hash, err := envelope.HashSSZ()
	require.NoError(t, err)
	value, ok := impl.pendingEnvelopes.Load(pendingEnvelopeKey{blockRoot: blockRoot, envelopeHash: hash})
	require.True(t, ok)
	require.True(t, value.(*envelopeJob).recovered.Load())
	require.True(t, value.(*envelopeJob).validate.Load())
}

func TestExecutionPayloadServicePendingQueuePreservesRecoveredEnvelope(t *testing.T) {
	impl, _ := setupExecutionPayloadServiceWithoutLoop(t)
	blockRoot := common.HexToHash("0x1234")
	envelope := newTestSignedEnvelope(100, blockRoot, 1)
	envelopeHash, err := envelope.HashSSZ()
	require.NoError(t, err)

	impl.queuePendingEnvelopeWithOptions(blockRoot, envelope, true, false, false)
	impl.queuePendingEnvelope(blockRoot, envelope)

	value, ok := impl.pendingEnvelopes.Load(pendingEnvelopeKey{blockRoot, envelopeHash, false})
	require.True(t, ok)
	job := value.(*envelopeJob)
	require.True(t, job.recovered.Load())
	require.True(t, job.validate.Load())
	require.Equal(t, int32(1), impl.pendingCount.Load())
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
		pendingCond:        sync.NewCond(&sync.Mutex{}),
	}

	impl.pendingCount.Store(maxPendingEnvelopes - 5)

	var wg sync.WaitGroup
	for i := range 100 {
		wg.Go(func() {
			blockRoot := common.Hash{byte(i), byte(i >> 8)}
			envelope := newTestSignedEnvelope(100, blockRoot, uint64(10000+i))
			impl.queuePendingEnvelope(blockRoot, envelope)
		})
	}
	wg.Wait()

	require.Equal(t, int32(maxPendingEnvelopes), impl.pendingCount.Load())
	stored := 0
	impl.pendingEnvelopes.Range(func(_, _ any) bool {
		stored++
		return true
	})
	require.Equal(t, 5, stored)
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
