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
	"fmt"
	"sync"
	"sync/atomic"
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
	service := NewExecutionPayloadService(t.Context(), forkchoiceMock, cfg, beaconevents.NewEventEmitter(), nil)
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

func pendingEnvelopeKeyForTest(t *testing.T, envelope *cltypes.SignedExecutionPayloadEnvelope) pendingEnvelopeKey {
	t.Helper()
	messageHash, err := envelope.Message.HashSSZ()
	require.NoError(t, err)
	return pendingEnvelopeKey{blockRoot: envelope.Message.BeaconBlockRoot, messageHash: messageHash}
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

func TestExecutionPayloadServiceRejectsNonCanonicalSSZ(t *testing.T) {
	service := &executionPayloadService{beaconCfg: &clparams.MainnetBeaconConfig}
	original := newTestSignedEnvelope(100, common.HexToHash("0x1234"), 1)
	encoded, err := original.EncodeSSZ(nil)
	require.NoError(t, err)

	t.Run("trailing bytes", func(t *testing.T) {
		mutated := append(append([]byte(nil), encoded...), 0)
		_, decodeErr := service.DecodeGossipMessage("peer123", mutated, clparams.GloasVersion)
		require.Error(t, decodeErr)
	})

	t.Run("dynamic offset gap", func(t *testing.T) {
		const signedEnvelopeFixedSize = 4 + 96
		mutated := make([]byte, 0, len(encoded)+1)
		mutated = append(mutated, encoded[:signedEnvelopeFixedSize]...)
		mutated = append(mutated, 0)
		mutated = append(mutated, encoded[signedEnvelopeFixedSize:]...)
		binary.LittleEndian.PutUint32(mutated[:4], signedEnvelopeFixedSize+1)
		lossy := &cltypes.SignedExecutionPayloadEnvelope{Message: cltypes.NewExecutionPayloadEnvelope(&clparams.MainnetBeaconConfig)}
		require.NoError(t, lossy.DecodeSSZ(mutated, int(clparams.GloasVersion)))
		_, decodeErr := service.DecodeGossipMessage("peer123", mutated, clparams.GloasVersion)
		require.ErrorContains(t, decodeErr, "non-canonical SSZ")
	})
}

func TestExecutionPayloadServiceBlockNotFound(t *testing.T) {
	service, fcu := setupExecutionPayloadService(t)

	blockRoot := common.HexToHash("0x1234")
	envelope := newTestSignedEnvelope(100, blockRoot, 1)

	// Block not in forkchoice - should not retain unauthenticated input.
	err := service.ProcessMessage(context.Background(), nil, envelope)
	require.ErrorIs(t, err, ErrIgnore)

	// Unknown-block envelopes are attacker-controlled and must not consume memory.
	impl := service.(*executionPayloadService)
	require.Zero(t, impl.pendingCount.Load())

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

	// Add expired job directly
	key := pendingEnvelopeKeyForTest(t, envelope)
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
	key := pendingEnvelopeKeyForTest(t, envelope)
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

	// Add pending job
	key := pendingEnvelopeKeyForTest(t, envelope)
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

	_, exists := impl.pendingEnvelopes.Load(pendingEnvelopeKeyForTest(t, envelope))
	require.True(t, exists)

	fcu.OnExecutionPayloadErr = nil
	impl.processPendingEnvelopes(t.Context())
	require.Zero(t, impl.pendingCount.Load())
	require.True(t, impl.seenEnvelopesCache.Contains(seenEnvelopeKey{blockRoot, 1}))
}

func TestExecutionPayloadServiceExpiresRetryableKnownBlock(t *testing.T) {
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
	key := pendingEnvelopeKeyForTest(t, envelope)
	impl.pendingEnvelopes.Store(key, &envelopeJob{
		envelope:     envelope,
		creationTime: time.Now().Add(-pendingEnvelopeExpiry - time.Second),
	})
	impl.pendingCount.Store(1)
	forkchoiceMock.Blocks[blockRoot] = &cltypes.SignedBeaconBlock{Block: &cltypes.BeaconBlock{Slot: 100}}
	forkchoiceMock.OnExecutionPayloadErr = fmt.Errorf("%w: timeout", forkchoice.ErrELPayloadValidationUnavailable)

	impl.processPendingEnvelopes(t.Context())

	require.Zero(t, impl.pendingCount.Load())
	_, exists := impl.pendingEnvelopes.Load(key)
	require.False(t, exists)
}

func TestExecutionPayloadServiceRetainsPendingEnvelopeUntilDataAvailable(t *testing.T) {
	service, fcu := setupExecutionPayloadService(t)
	impl := service.(*executionPayloadService)
	blockRoot := common.HexToHash("0x1234")
	envelope := newTestSignedEnvelope(100, blockRoot, 1)
	fcu.Blocks[blockRoot] = &cltypes.SignedBeaconBlock{
		Block: &cltypes.BeaconBlock{Slot: 100},
	}
	fcu.OnExecutionPayloadErr = forkchoice.ErrEIP7594ColumnDataNotAvailable

	require.ErrorIs(t, service.ProcessMessage(t.Context(), nil, envelope), ErrIgnore)
	key := pendingEnvelopeKeyForTest(t, envelope)
	value, ok := impl.pendingEnvelopes.Load(key)
	require.True(t, ok)
	creationTime := value.(*envelopeJob).creationTime

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
	fcu.Blocks[blockRoot] = &cltypes.SignedBeaconBlock{
		Block: &cltypes.BeaconBlock{Slot: 100},
	}
	fcu.OnExecutionPayloadErr = errors.New("invalid envelope")

	require.Error(t, service.ProcessMessage(t.Context(), nil, envelope))

	require.Equal(t, int32(0), impl.pendingCount.Load())
	require.False(t, impl.seenEnvelopesCache.Contains(seenEnvelopeKey{blockRoot, 1}))
}

func TestExecutionPayloadServiceRetainsPendingEnvelopeAfterTemporaryELFailure(t *testing.T) {
	service, fcu := setupExecutionPayloadService(t)
	impl := service.(*executionPayloadService)
	blockRoot := common.HexToHash("0x1234")
	envelope := newTestSignedEnvelope(100, blockRoot, 1)
	fcu.Blocks[blockRoot] = &cltypes.SignedBeaconBlock{Block: &cltypes.BeaconBlock{Slot: 100}}
	fcu.OnExecutionPayloadErr = fmt.Errorf("%w: timeout", forkchoice.ErrELPayloadValidationUnavailable)

	require.ErrorIs(t, service.ProcessMessage(t.Context(), nil, envelope), ErrIgnore)

	require.Equal(t, int32(1), impl.pendingCount.Load())
	_, exists := impl.pendingEnvelopes.Load(pendingEnvelopeKeyForTest(t, envelope))
	require.True(t, exists)
}

func TestExecutionPayloadServiceSignatureFloodUsesOnePendingRoot(t *testing.T) {
	cfg := &clparams.MainnetBeaconConfig
	forkchoiceMock := mock_services.NewForkChoiceStorageMock(t)
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
	first := newTestSignedEnvelope(100, blockRoot, 1)
	require.True(t, impl.queuePendingEnvelope(blockRoot, first, false, false))
	for i := range maxPendingEnvelopes * 2 {
		variant := newTestSignedEnvelope(100, blockRoot, 1)
		variant.Signature[0] = byte(i)
		variant.Signature[1] = byte(i >> 8)
		require.False(t, impl.queuePendingEnvelope(blockRoot, variant, false, false))
	}

	require.Equal(t, int32(1), impl.pendingCount.Load())
	value, exists := impl.pendingEnvelopes.Load(pendingEnvelopeKeyForTest(t, first))
	require.True(t, exists)
	require.Same(t, first, value.(*envelopeJob).envelope)
}

func TestExecutionPayloadServiceForgedThenValidBeforeBlockProcessesValid(t *testing.T) {
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
	blockRoot := common.HexToHash("0x1234")
	forged := newTestSignedEnvelope(100, blockRoot, 1)
	valid := newTestSignedEnvelope(100, blockRoot, 2)
	var validationCalls atomic.Int32
	forkchoiceMock.OnExecutionPayloadFunc = func(candidate *cltypes.SignedExecutionPayloadEnvelope) error {
		validationCalls.Add(1)
		if candidate == forged {
			return errors.New("forged envelope")
		}
		return nil
	}

	require.ErrorIs(t, impl.ProcessMessage(t.Context(), nil, forged), ErrIgnore)
	require.ErrorIs(t, impl.ProcessMessage(t.Context(), nil, valid), ErrIgnore)
	require.Zero(t, validationCalls.Load())
	require.Zero(t, impl.pendingCount.Load())

	forkchoiceMock.Blocks[blockRoot] = &cltypes.SignedBeaconBlock{Block: &cltypes.BeaconBlock{Slot: 100}}
	require.Error(t, impl.ProcessMessage(t.Context(), nil, forged))
	require.NoError(t, impl.ProcessMessage(t.Context(), nil, valid))

	require.Zero(t, impl.pendingCount.Load())
	require.NotContains(t, impl.pendingRootCounts, blockRoot)
	require.True(t, impl.seenEnvelopesCache.Contains(seenEnvelopeKey{blockRoot, 2}))
	require.Equal(t, int32(2), validationCalls.Load())
}

func TestExecutionPayloadServiceSemanticCandidateFloodIsPerRootBounded(t *testing.T) {
	impl := &executionPayloadService{}
	blockRoot := common.HexToHash("0x1234")
	for i := range maxPendingEnvelopes * 2 {
		envelope := newTestSignedEnvelope(100, blockRoot, uint64(i))
		require.True(t, impl.queuePendingEnvelope(blockRoot, envelope, false, false))
	}
	require.Equal(t, int32(maxPendingCandidatesPerRoot), impl.pendingCount.Load())
}

func TestExecutionPayloadServiceValidCandidateAtPerRootCapReplacesOldestUnknown(t *testing.T) {
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
	blockRoot := common.HexToHash("0x1234")
	first := newTestSignedEnvelope(100, blockRoot, 1)
	require.True(t, impl.queuePendingEnvelope(blockRoot, first, false, false))
	firstKey := pendingEnvelopeKeyForTest(t, first)
	value, exists := impl.pendingEnvelopes.Load(firstKey)
	require.True(t, exists)
	value.(*envelopeJob).creationTime = time.Now().Add(-time.Hour)
	for i := 1; i < maxPendingCandidatesPerRoot; i++ {
		candidate := newTestSignedEnvelope(100, blockRoot, uint64(i+1))
		require.True(t, impl.queuePendingEnvelope(blockRoot, candidate, false, false))
	}

	newest := newTestSignedEnvelope(100, blockRoot, 999)
	require.True(t, impl.queuePendingEnvelope(blockRoot, newest, false, false))
	require.Equal(t, int32(maxPendingCandidatesPerRoot), impl.pendingCount.Load())
	_, exists = impl.pendingEnvelopes.Load(firstKey)
	require.False(t, exists)
	_, exists = impl.pendingEnvelopes.Load(pendingEnvelopeKeyForTest(t, newest))
	require.True(t, exists)
	forkchoiceMock.Blocks[blockRoot] = &cltypes.SignedBeaconBlock{Block: &cltypes.BeaconBlock{Slot: 100}}
	forkchoiceMock.OnExecutionPayloadFunc = func(candidate *cltypes.SignedExecutionPayloadEnvelope) error {
		if candidate != newest {
			return errors.New("forged envelope")
		}
		return nil
	}
	impl.processPendingEnvelopes(t.Context())
	require.Zero(t, impl.pendingCount.Load())
	require.NotContains(t, impl.pendingRootCounts, blockRoot)
	require.True(t, impl.seenEnvelopesCache.Contains(seenEnvelopeKey{blockRoot, 999}))
}

func TestExecutionPayloadServiceValidatedCandidateReplacesUnknownCandidate(t *testing.T) {
	impl := &executionPayloadService{pendingCond: sync.NewCond(&sync.Mutex{})}
	blockRoot := common.HexToHash("0x1234")
	unknown := newTestSignedEnvelope(100, blockRoot, 1)
	require.True(t, impl.queuePendingEnvelope(blockRoot, unknown, false, false))
	key := pendingEnvelopeKeyForTest(t, unknown)
	value, exists := impl.pendingEnvelopes.Load(key)
	require.True(t, exists)
	originalCreationTime := value.(*envelopeJob).creationTime

	validated := newTestSignedEnvelope(100, blockRoot, 1)
	validated.Signature[0] = 1
	require.True(t, impl.queuePendingEnvelope(blockRoot, validated, true, true))
	value, exists = impl.pendingEnvelopes.Load(key)
	require.True(t, exists)
	job := value.(*envelopeJob)
	require.Same(t, validated, job.envelope)
	require.True(t, job.validated)
	require.True(t, job.blockSeen.Load())
	require.Equal(t, originalCreationTime, job.creationTime)
	require.Equal(t, int32(1), impl.pendingCount.Load())

	for i := range maxPendingEnvelopes * 2 {
		forged := newTestSignedEnvelope(100, blockRoot, 1)
		forged.Signature[0] = byte(i)
		forged.Signature[1] = byte(i >> 8)
		require.False(t, impl.queuePendingEnvelope(blockRoot, forged, true, false))
	}
	value, exists = impl.pendingEnvelopes.Load(key)
	require.True(t, exists)
	require.Same(t, validated, value.(*envelopeJob).envelope)
}

func TestExecutionPayloadServiceCanceledCandidateDoesNotReplaceValidatedCandidate(t *testing.T) {
	service, fcu := setupExecutionPayloadService(t)
	impl := service.(*executionPayloadService)
	blockRoot := common.HexToHash("0x1234")
	validated := newTestSignedEnvelope(100, blockRoot, 1)
	require.True(t, impl.queuePendingEnvelope(blockRoot, validated, true, true))
	fcu.Blocks[blockRoot] = &cltypes.SignedBeaconBlock{Block: &cltypes.BeaconBlock{Slot: 100}}
	fcu.OnExecutionPayloadErr = context.Canceled

	canceled := newTestSignedEnvelope(100, blockRoot, 1)
	canceled.Signature[0] = 2
	err := service.ProcessMessage(t.Context(), nil, canceled)
	require.ErrorIs(t, err, context.Canceled)
	value, exists := impl.pendingEnvelopes.Load(pendingEnvelopeKeyForTest(t, validated))
	require.True(t, exists)
	require.Same(t, validated, value.(*envelopeJob).envelope)
	require.Equal(t, int32(1), impl.pendingCount.Load())
}

func TestExecutionPayloadServiceDifferentRootsRetainIndependentCapacity(t *testing.T) {
	impl := &executionPayloadService{pendingCond: sync.NewCond(&sync.Mutex{})}
	const roots = 128
	for i := range roots {
		blockRoot := common.Hash{byte(i), byte(i >> 8)}
		envelope := newTestSignedEnvelope(100, blockRoot, uint64(i))
		require.True(t, impl.queuePendingEnvelope(blockRoot, envelope, false, false))
	}
	require.Equal(t, int32(roots), impl.pendingCount.Load())
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

	impl.queuePendingEnvelope(blockRoot, envelope, false, false)

	require.Equal(t, int32(maxPendingEnvelopes), impl.pendingCount.Load())
	_, exists := impl.pendingEnvelopes.Load(pendingEnvelopeKeyForTest(t, envelope))
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
		key := pendingEnvelopeKey{blockRoot: blockRoot}
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
	impl.queuePendingEnvelope(blockRoot, envelope, false, false)

	_, exists := impl.pendingEnvelopes.Load(pendingEnvelopeKeyForTest(t, envelope))
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
		key := pendingEnvelopeKey{blockRoot: blockRoot}
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
	impl.queuePendingEnvelope(blockRoot, envelope, false, false)

	_, exists := impl.pendingEnvelopes.Load(pendingEnvelopeKeyForTest(t, envelope))
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
		key := pendingEnvelopeKey{blockRoot: blockRoot}
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
	impl.queuePendingEnvelope(blockRoot, envelope, true, true)

	_, exists := impl.pendingEnvelopes.Load(pendingEnvelopeKeyForTest(t, envelope))
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
		key := pendingEnvelopeKey{blockRoot: blockRoot}
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
	impl.queuePendingEnvelope(blockRoot, envelope, false, false)

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
			impl.queuePendingEnvelope(blockRoot, envelope, false, false)
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
