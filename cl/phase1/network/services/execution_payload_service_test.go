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
	"fmt"
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

func TestExecutionPayloadServiceRetainsEnvelopeAcrossColumnSyncInterval(t *testing.T) {
	cfg := &clparams.MainnetBeaconConfig
	forkchoiceMock := mock_services.NewForkChoiceStorageMock(t)
	impl := &executionPayloadService{
		forkchoiceStore: forkchoiceMock,
		beaconCfg:       cfg,
		emitters:        beaconevents.NewEventEmitter(),
	}
	seenCache, err := lru.New[seenEnvelopeKey, struct{}]("seen_envelopes", seenEnvelopeCacheSize)
	require.NoError(t, err)
	impl.seenEnvelopesCache = seenCache

	blockRoot := common.HexToHash("0x1234")
	envelope := newTestSignedEnvelope(100, blockRoot, 1)
	envelopeHash, err := envelope.HashSSZ()
	require.NoError(t, err)
	key := pendingEnvelopeKey{blockRoot: blockRoot, envelopeHash: envelopeHash}
	impl.pendingEnvelopes.Store(key, &envelopeJob{
		envelope:     envelope,
		creationTime: time.Now().Add(-time.Minute),
	})
	impl.pendingCount.Store(1)

	impl.processPendingEnvelopes(t.Context())

	require.Equal(t, int32(1), impl.pendingCount.Load())
	_, exists := impl.pendingEnvelopes.Load(key)
	require.True(t, exists)
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
	impl.pendingEnvelopes.Store(key, &envelopeJob{
		envelope:     envelope,
		creationTime: time.Now(),
	})
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

func TestExecutionPayloadServiceQueuesEnvelopeUntilDataAvailable(t *testing.T) {
	service, fcu := setupExecutionPayloadService(t)
	impl := service.(*executionPayloadService)
	blockRoot := common.HexToHash("0x1234")
	envelope := newTestSignedEnvelope(100, blockRoot, 1)
	fcu.Blocks[blockRoot] = &cltypes.SignedBeaconBlock{
		Block: &cltypes.BeaconBlock{Slot: 100},
	}
	fcu.OnExecutionPayloadErr = forkchoice.ErrEIP7594ColumnDataNotAvailable

	err := service.ProcessMessage(t.Context(), nil, envelope)
	require.ErrorIs(t, err, forkchoice.ErrEIP7594ColumnDataNotAvailable)
	require.Equal(t, int32(1), impl.pendingCount.Load())

	fcu.OnExecutionPayloadErr = nil
	impl.processPendingEnvelopes(t.Context())

	require.Equal(t, int32(0), impl.pendingCount.Load())
	require.True(t, impl.seenEnvelopesCache.Contains(seenEnvelopeKey{blockRoot, 1}))
}

func TestExecutionPayloadServiceQueuesInitialTemporaryELFailure(t *testing.T) {
	service, fcu := setupExecutionPayloadService(t)
	impl := service.(*executionPayloadService)
	blockRoot := common.HexToHash("0x1234")
	envelope := newTestSignedEnvelope(100, blockRoot, 1)
	fcu.Blocks[blockRoot] = &cltypes.SignedBeaconBlock{Block: &cltypes.BeaconBlock{Slot: 100}}
	fcu.OnExecutionPayloadErr = fmt.Errorf("%w: timeout", forkchoice.ErrELPayloadValidationUnavailable)

	err := service.ProcessMessage(t.Context(), nil, envelope)
	require.ErrorIs(t, err, forkchoice.ErrELPayloadValidationUnavailable)
	require.Equal(t, int32(1), impl.pendingCount.Load())

	envelopeHash, err := envelope.HashSSZ()
	require.NoError(t, err)
	_, exists := impl.pendingEnvelopes.Load(pendingEnvelopeKey{blockRoot: blockRoot, envelopeHash: envelopeHash})
	require.True(t, exists)

	fcu.OnExecutionPayloadErr = nil
	impl.processPendingEnvelopes(t.Context())
	require.Zero(t, impl.pendingCount.Load())
	require.True(t, impl.seenEnvelopesCache.Contains(seenEnvelopeKey{blockRoot, 1}))
}

func TestExecutionPayloadServiceDoesNotExpireRetryableKnownBlock(t *testing.T) {
	cfg := &clparams.MainnetBeaconConfig
	forkchoiceMock := mock_services.NewForkChoiceStorageMock(t)
	impl := &executionPayloadService{
		forkchoiceStore: forkchoiceMock,
		beaconCfg:       cfg,
		emitters:        beaconevents.NewEventEmitter(),
	}
	seenCache, err := lru.New[seenEnvelopeKey, struct{}]("seen_envelopes", seenEnvelopeCacheSize)
	require.NoError(t, err)
	impl.seenEnvelopesCache = seenCache

	blockRoot := common.HexToHash("0x1234")
	envelope := newTestSignedEnvelope(100, blockRoot, 1)
	envelopeHash, err := envelope.HashSSZ()
	require.NoError(t, err)
	key := pendingEnvelopeKey{blockRoot: blockRoot, envelopeHash: envelopeHash}
	impl.pendingEnvelopes.Store(key, &envelopeJob{
		envelope:     envelope,
		creationTime: time.Now().Add(-pendingEnvelopeExpiry - time.Second),
	})
	impl.pendingCount.Store(1)
	forkchoiceMock.Blocks[blockRoot] = &cltypes.SignedBeaconBlock{Block: &cltypes.BeaconBlock{Slot: 100}}
	forkchoiceMock.OnExecutionPayloadErr = fmt.Errorf("%w: timeout", forkchoice.ErrELPayloadValidationUnavailable)

	impl.processPendingEnvelopes(t.Context())

	require.Equal(t, int32(1), impl.pendingCount.Load())
	value, exists := impl.pendingEnvelopes.Load(key)
	require.True(t, exists)
	require.True(t, value.(*envelopeJob).nextAttempt.After(time.Now()))
}

func TestExecutionPayloadServiceRetainsPendingEnvelopeUntilDataAvailable(t *testing.T) {
	service, fcu := setupExecutionPayloadService(t)
	impl := service.(*executionPayloadService)
	blockRoot := common.HexToHash("0x1234")
	envelope := newTestSignedEnvelope(100, blockRoot, 1)

	require.ErrorIs(t, service.ProcessMessage(t.Context(), nil, envelope), ErrIgnore)
	envelopeHash, err := envelope.HashSSZ()
	require.NoError(t, err)
	key := pendingEnvelopeKey{blockRoot: blockRoot, envelopeHash: envelopeHash}
	value, ok := impl.pendingEnvelopes.Load(key)
	require.True(t, ok)
	creationTime := value.(*envelopeJob).creationTime

	fcu.Blocks[blockRoot] = &cltypes.SignedBeaconBlock{
		Block: &cltypes.BeaconBlock{Slot: 100},
	}
	fcu.OnExecutionPayloadErr = forkchoice.ErrEIP7594ColumnDataNotAvailable

	impl.processPendingEnvelopes(t.Context())
	require.Equal(t, int32(1), impl.pendingCount.Load())
	value, ok = impl.pendingEnvelopes.Load(key)
	require.True(t, ok)
	require.Equal(t, creationTime, value.(*envelopeJob).creationTime)
	require.True(t, value.(*envelopeJob).nextAttempt.After(time.Now()))

	fcu.OnExecutionPayloadErr = nil
	value.(*envelopeJob).nextAttempt = time.Time{}
	impl.processPendingEnvelopes(t.Context())

	require.Equal(t, int32(0), impl.pendingCount.Load())
	require.True(t, impl.seenEnvelopesCache.Contains(seenEnvelopeKey{blockRoot, 1}))
}

func TestExecutionPayloadServiceDropsPendingEnvelopeAfterValidationFailure(t *testing.T) {
	service, fcu := setupExecutionPayloadService(t)
	impl := service.(*executionPayloadService)
	blockRoot := common.HexToHash("0x1234")
	envelope := newTestSignedEnvelope(100, blockRoot, 1)

	require.ErrorIs(t, service.ProcessMessage(t.Context(), nil, envelope), ErrIgnore)
	fcu.Blocks[blockRoot] = &cltypes.SignedBeaconBlock{
		Block: &cltypes.BeaconBlock{Slot: 100},
	}
	fcu.OnExecutionPayloadErr = errors.New("invalid envelope")

	impl.processPendingEnvelopes(t.Context())

	require.Equal(t, int32(0), impl.pendingCount.Load())
	require.False(t, impl.seenEnvelopesCache.Contains(seenEnvelopeKey{blockRoot, 1}))
}

func TestExecutionPayloadServiceRetainsPendingEnvelopeAfterTemporaryELFailure(t *testing.T) {
	service, fcu := setupExecutionPayloadService(t)
	impl := service.(*executionPayloadService)
	blockRoot := common.HexToHash("0x1234")
	envelope := newTestSignedEnvelope(100, blockRoot, 1)

	require.ErrorIs(t, service.ProcessMessage(t.Context(), nil, envelope), ErrIgnore)
	fcu.Blocks[blockRoot] = &cltypes.SignedBeaconBlock{Block: &cltypes.BeaconBlock{Slot: 100}}
	fcu.OnExecutionPayloadErr = fmt.Errorf("%w: timeout", forkchoice.ErrELPayloadValidationUnavailable)

	impl.processPendingEnvelopes(t.Context())

	require.Equal(t, int32(1), impl.pendingCount.Load())
	envelopeHash, err := envelope.HashSSZ()
	require.NoError(t, err)
	_, exists := impl.pendingEnvelopes.Load(pendingEnvelopeKey{blockRoot: blockRoot, envelopeHash: envelopeHash})
	require.True(t, exists)
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
	impl.pendingEnvelopes.Store(pendingEnvelopeKey{blockRoot, hash1}, &envelopeJob{
		envelope:     envelope1,
		creationTime: time.Now(),
	})
	impl.pendingEnvelopes.Store(pendingEnvelopeKey{blockRoot, hash2}, &envelopeJob{
		envelope:     envelope2,
		creationTime: time.Now(),
	})
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

	impl.queuePendingEnvelope(blockRoot, envelope, false)

	require.Equal(t, int32(maxPendingEnvelopes), impl.pendingCount.Load())
	envelopeHash, err := envelope.HashSSZ()
	require.NoError(t, err)
	_, exists := impl.pendingEnvelopes.Load(pendingEnvelopeKey{blockRoot, envelopeHash})
	require.False(t, exists)
}

func TestExecutionPayloadServicePendingQueueRejectsUnknownWorkWhenAllJobsAreKnown(t *testing.T) {
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

	var oldestKey pendingEnvelopeKey
	for i := range maxPendingEnvelopes {
		blockRoot := common.Hash{byte(i), byte(i >> 8)}
		envelope := newTestSignedEnvelope(100, blockRoot, uint64(i))
		envelopeHash, hashErr := envelope.HashSSZ()
		require.NoError(t, hashErr)
		key := pendingEnvelopeKey{blockRoot, envelopeHash}
		if i == 0 {
			oldestKey = key
		}
		job := &envelopeJob{
			envelope:     envelope,
			creationTime: time.Now().Add(-time.Hour + time.Duration(i)),
		}
		job.blockSeen.Store(true)
		impl.pendingEnvelopes.Store(key, job)
	}
	impl.pendingCount.Store(maxPendingEnvelopes)

	blockRoot := common.HexToHash("0xffff")
	envelope := newTestSignedEnvelope(100, blockRoot, 9999)
	impl.queuePendingEnvelope(blockRoot, envelope, false)

	envelopeHash, err := envelope.HashSSZ()
	require.NoError(t, err)
	_, exists := impl.pendingEnvelopes.Load(pendingEnvelopeKey{blockRoot, envelopeHash})
	require.False(t, exists)
	_, exists = impl.pendingEnvelopes.Load(oldestKey)
	require.True(t, exists)
	require.Equal(t, int32(maxPendingEnvelopes), impl.pendingCount.Load())
}

func TestExecutionPayloadServicePendingQueueEvictsUnknownBeforeOlderKnownWork(t *testing.T) {
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

	var oldestKnownKey, unknownKey pendingEnvelopeKey
	for i := range maxPendingEnvelopes {
		blockRoot := common.Hash{byte(i), byte(i >> 8)}
		envelope := newTestSignedEnvelope(100, blockRoot, uint64(i))
		envelopeHash, hashErr := envelope.HashSSZ()
		require.NoError(t, hashErr)
		key := pendingEnvelopeKey{blockRoot, envelopeHash}
		job := &envelopeJob{
			envelope:     envelope,
			creationTime: time.Now(),
		}
		job.blockSeen.Store(true)
		switch i {
		case 0:
			oldestKnownKey = key
			job.creationTime = time.Now().Add(-2 * time.Hour)
		case 1:
			unknownKey = key
			job.creationTime = time.Now().Add(-time.Hour)
			job.blockSeen.Store(false)
		}
		impl.pendingEnvelopes.Store(key, job)
	}
	impl.pendingCount.Store(maxPendingEnvelopes)

	blockRoot := common.HexToHash("0xffff")
	envelope := newTestSignedEnvelope(100, blockRoot, 9999)
	impl.queuePendingEnvelope(blockRoot, envelope, false)

	envelopeHash, err := envelope.HashSSZ()
	require.NoError(t, err)
	_, exists := impl.pendingEnvelopes.Load(pendingEnvelopeKey{blockRoot, envelopeHash})
	require.True(t, exists)
	_, exists = impl.pendingEnvelopes.Load(oldestKnownKey)
	require.True(t, exists)
	_, exists = impl.pendingEnvelopes.Load(unknownKey)
	require.False(t, exists)
	require.Equal(t, int32(maxPendingEnvelopes), impl.pendingCount.Load())
}

func TestExecutionPayloadServicePendingQueueAdmitsKnownWorkWhenAllJobsAreKnown(t *testing.T) {
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

	var oldestKey pendingEnvelopeKey
	for i := range maxPendingEnvelopes {
		blockRoot := common.Hash{byte(i), byte(i >> 8)}
		envelope := newTestSignedEnvelope(100, blockRoot, uint64(i))
		envelopeHash, hashErr := envelope.HashSSZ()
		require.NoError(t, hashErr)
		key := pendingEnvelopeKey{blockRoot, envelopeHash}
		if i == 0 {
			oldestKey = key
		}
		job := &envelopeJob{envelope: envelope, creationTime: time.Now().Add(-time.Hour + time.Duration(i))}
		job.blockSeen.Store(true)
		impl.pendingEnvelopes.Store(key, job)
	}
	impl.pendingCount.Store(maxPendingEnvelopes)

	blockRoot := common.HexToHash("0xffff")
	envelope := newTestSignedEnvelope(100, blockRoot, 9999)
	impl.queuePendingEnvelope(blockRoot, envelope, true)

	envelopeHash, err := envelope.HashSSZ()
	require.NoError(t, err)
	_, exists := impl.pendingEnvelopes.Load(pendingEnvelopeKey{blockRoot, envelopeHash})
	require.True(t, exists)
	_, exists = impl.pendingEnvelopes.Load(oldestKey)
	require.False(t, exists)
	require.Equal(t, int32(maxPendingEnvelopes), impl.pendingCount.Load())
}

func TestExecutionPayloadServicePendingQueueDoesNotEvictResolvingWork(t *testing.T) {
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

	var resolvingKey, unknownKey pendingEnvelopeKey
	for i := range maxPendingEnvelopes {
		blockRoot := common.Hash{byte(i), byte(i >> 8)}
		envelope := newTestSignedEnvelope(100, blockRoot, uint64(i))
		envelopeHash, hashErr := envelope.HashSSZ()
		require.NoError(t, hashErr)
		key := pendingEnvelopeKey{blockRoot, envelopeHash}
		job := &envelopeJob{envelope: envelope, creationTime: time.Now()}
		job.blockSeen.Store(true)
		switch i {
		case 0:
			resolvingKey = key
			job.creationTime = time.Now().Add(-2 * time.Hour)
			job.blockSeen.Store(false)
			job.resolving.Store(true)
		case 1:
			unknownKey = key
			job.creationTime = time.Now().Add(-time.Hour)
			job.blockSeen.Store(false)
		}
		impl.pendingEnvelopes.Store(key, job)
	}
	impl.pendingCount.Store(maxPendingEnvelopes)

	blockRoot := common.HexToHash("0xffff")
	envelope := newTestSignedEnvelope(100, blockRoot, 9999)
	impl.queuePendingEnvelope(blockRoot, envelope, false)

	_, exists := impl.pendingEnvelopes.Load(resolvingKey)
	require.True(t, exists)
	_, exists = impl.pendingEnvelopes.Load(unknownKey)
	require.False(t, exists)
}

func TestExecutionPayloadServiceStaleCompletionDoesNotDeleteReplacement(t *testing.T) {
	impl := &executionPayloadService{}
	key := pendingEnvelopeKey{blockRoot: common.HexToHash("0x1234")}
	stale := &envelopeJob{}
	replacement := &envelopeJob{}
	impl.pendingEnvelopes.Store(key, replacement)
	impl.pendingCount.Store(1)

	impl.finishPendingEnvelopeAttempt(key, stale, nil)

	value, exists := impl.pendingEnvelopes.Load(key)
	require.True(t, exists)
	require.Same(t, replacement, value)
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
			impl.queuePendingEnvelope(blockRoot, envelope, false)
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
