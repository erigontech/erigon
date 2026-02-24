// Copyright 2026 The Erigon Authors
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
	"go.uber.org/mock/gomock"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/phase1/core/state/lru"
	"github.com/erigontech/erigon/cl/phase1/forkchoice/mock_services"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
	"github.com/erigontech/erigon/common"
)

func setupPayloadAttestationService(t *testing.T, ctrl *gomock.Controller) (*payloadAttestationService, *mock_services.ForkChoiceStorageMock, *eth_clock.MockEthereumClock) {
	forkchoiceMock := mock_services.NewForkChoiceStorageMock(t)
	ethClockMock := eth_clock.NewMockEthereumClock(ctrl)

	seenCache, err := lru.New[seenPayloadAttestationKey, struct{}]("seen_payload_attestations", seenPayloadAttestationCacheSize)
	require.NoError(t, err)

	service := &payloadAttestationService{
		forkchoiceStore:       forkchoiceMock,
		ethClock:              ethClockMock,
		netCfg:                nil, // Not used in current implementation
		seenAttestationsCache: seenCache,
		pendingCond:           sync.NewCond(&sync.Mutex{}), // Needed for queuePendingAttestation
	}

	return service, forkchoiceMock, ethClockMock
}

func newTestPayloadAttestationMessage(slot uint64, validatorIndex uint64, blockRoot common.Hash) *cltypes.PayloadAttestationMessage {
	return &cltypes.PayloadAttestationMessage{
		ValidatorIndex: validatorIndex,
		Data: &cltypes.PayloadAttestationData{
			BeaconBlockRoot:   blockRoot,
			Slot:              slot,
			PayloadPresent:    true,
			BlobDataAvailable: true,
		},
		Signature: common.Bytes96{},
	}
}

func TestPayloadAttestationServiceNilMessage(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	service, _, _ := setupPayloadAttestationService(t, ctrl)

	// Test nil message
	err := service.ProcessMessage(context.Background(), nil, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "nil payload attestation message")

	// Test message with nil data
	err = service.ProcessMessage(context.Background(), nil, &cltypes.PayloadAttestationMessage{})
	require.Error(t, err)
	require.Contains(t, err.Error(), "nil payload attestation message")
}

func TestPayloadAttestationServiceSlotMismatch(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	service, _, ethClockMock := setupPayloadAttestationService(t, ctrl)

	blockRoot := common.HexToHash("0x1234")
	msg := newTestPayloadAttestationMessage(100, 1, blockRoot)

	// Mock: slot 100 is NOT current slot (with disparity)
	ethClockMock.EXPECT().IsSlotCurrentSlotWithMaximumClockDisparity(uint64(100)).Return(false)

	err := service.ProcessMessage(context.Background(), nil, msg)
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrIgnore))
	require.Contains(t, err.Error(), "not current slot")
}

func TestPayloadAttestationServiceDuplicateValidator(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	service, fcu, ethClockMock := setupPayloadAttestationService(t, ctrl)

	blockRoot := common.HexToHash("0x1234")
	msg := newTestPayloadAttestationMessage(100, 42, blockRoot)

	// Add block header to forkchoice
	fcu.Headers[blockRoot] = &cltypes.BeaconBlockHeader{
		Slot: 100,
	}

	// First call setup
	ethClockMock.EXPECT().IsSlotCurrentSlotWithMaximumClockDisparity(uint64(100)).Return(true)

	// First call should succeed
	err := service.ProcessMessage(context.Background(), nil, msg)
	require.NoError(t, err)

	// Second call setup
	ethClockMock.EXPECT().IsSlotCurrentSlotWithMaximumClockDisparity(uint64(100)).Return(true)

	// Second call with same (slot, validatorIndex) should be ignored
	err = service.ProcessMessage(context.Background(), nil, msg)
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrIgnore))
	require.Contains(t, err.Error(), "already seen payload attestation")
}

func TestPayloadAttestationServiceBlockNotFound(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	service, _, ethClockMock := setupPayloadAttestationService(t, ctrl)

	blockRoot := common.HexToHash("0x1234")
	msg := newTestPayloadAttestationMessage(100, 1, blockRoot)

	// Mock: slot is current
	ethClockMock.EXPECT().IsSlotCurrentSlotWithMaximumClockDisparity(uint64(100)).Return(true)

	// Block not in forkchoice - should queue and return nil
	err := service.ProcessMessage(context.Background(), nil, msg)
	require.NoError(t, err)

	// Verify attestation was queued
	require.Equal(t, int32(1), service.pendingCount.Load())

	// Verify the pending key
	key := pendingPayloadAttestationKey{
		blockRoot:      blockRoot,
		validatorIndex: 1,
	}
	_, exists := service.pendingAttestations.Load(key)
	require.True(t, exists)
}

func TestPayloadAttestationServiceSuccess(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	service, fcu, ethClockMock := setupPayloadAttestationService(t, ctrl)

	blockRoot := common.HexToHash("0x1234")
	msg := newTestPayloadAttestationMessage(100, 42, blockRoot)

	// Add block header to forkchoice
	fcu.Headers[blockRoot] = &cltypes.BeaconBlockHeader{
		Slot: 100,
	}

	// Mock expectations
	ethClockMock.EXPECT().IsSlotCurrentSlotWithMaximumClockDisparity(uint64(100)).Return(true)

	// Process should succeed
	err := service.ProcessMessage(context.Background(), nil, msg)
	require.NoError(t, err)

	// Verify attestation was marked as seen
	seenKey := seenPayloadAttestationKey{
		slot:           100,
		validatorIndex: 42,
	}
	require.True(t, service.seenAttestationsCache.Contains(seenKey))
}

func TestPayloadAttestationServiceDifferentValidatorsSameBlock(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	service, fcu, ethClockMock := setupPayloadAttestationService(t, ctrl)

	blockRoot := common.HexToHash("0x1234")
	msg1 := newTestPayloadAttestationMessage(100, 1, blockRoot)
	msg2 := newTestPayloadAttestationMessage(100, 2, blockRoot)

	// Add block header to forkchoice
	fcu.Headers[blockRoot] = &cltypes.BeaconBlockHeader{
		Slot: 100,
	}

	// Both should be processed (different validators)
	ethClockMock.EXPECT().IsSlotCurrentSlotWithMaximumClockDisparity(uint64(100)).Return(true).Times(2)

	err := service.ProcessMessage(context.Background(), nil, msg1)
	require.NoError(t, err)

	err = service.ProcessMessage(context.Background(), nil, msg2)
	require.NoError(t, err)

	// Verify both are marked as seen
	require.True(t, service.seenAttestationsCache.Contains(seenPayloadAttestationKey{100, 1}))
	require.True(t, service.seenAttestationsCache.Contains(seenPayloadAttestationKey{100, 2}))
}

func TestPayloadAttestationServicePendingExpiry(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	service, _, _ := setupPayloadAttestationService(t, ctrl)

	blockRoot := common.HexToHash("0x1234")
	msg := newTestPayloadAttestationMessage(100, 1, blockRoot)

	// Add expired job directly
	key := pendingPayloadAttestationKey{
		blockRoot:      blockRoot,
		validatorIndex: 1,
	}
	service.pendingAttestations.Store(key, &pendingPayloadAttestationJob{
		msg:          msg,
		creationTime: time.Now().Add(-pendingPayloadAttestationExpiry - time.Second), // expired
	})
	service.pendingCount.Store(1)

	// Process pending - should remove expired
	service.processPendingAttestations(context.Background())

	require.Equal(t, int32(0), service.pendingCount.Load())
	_, exists := service.pendingAttestations.Load(key)
	require.False(t, exists)
}

func TestPayloadAttestationServicePendingSlotMismatch(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	service, _, ethClockMock := setupPayloadAttestationService(t, ctrl)

	blockRoot := common.HexToHash("0x1234")
	msg := newTestPayloadAttestationMessage(100, 1, blockRoot)

	// Add pending job
	key := pendingPayloadAttestationKey{
		blockRoot:      blockRoot,
		validatorIndex: 1,
	}
	service.pendingAttestations.Store(key, &pendingPayloadAttestationJob{
		msg:          msg,
		creationTime: time.Now(),
	})
	service.pendingCount.Store(1)

	// Mock: slot 100 is no longer current
	ethClockMock.EXPECT().IsSlotCurrentSlotWithMaximumClockDisparity(uint64(100)).Return(false)

	// Process pending - should remove due to slot mismatch
	service.processPendingAttestations(context.Background())

	require.Equal(t, int32(0), service.pendingCount.Load())
	_, exists := service.pendingAttestations.Load(key)
	require.False(t, exists)
}

func TestPayloadAttestationServicePendingProcessing(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	service, fcu, ethClockMock := setupPayloadAttestationService(t, ctrl)

	blockRoot := common.HexToHash("0x1234")
	msg := newTestPayloadAttestationMessage(100, 42, blockRoot)

	// Add pending job
	key := pendingPayloadAttestationKey{
		blockRoot:      blockRoot,
		validatorIndex: 42,
	}
	service.pendingAttestations.Store(key, &pendingPayloadAttestationJob{
		msg:          msg,
		creationTime: time.Now(),
	})
	service.pendingCount.Store(1)

	// First process: slot ok, but block not available
	ethClockMock.EXPECT().IsSlotCurrentSlotWithMaximumClockDisparity(uint64(100)).Return(true)
	service.processPendingAttestations(context.Background())
	require.Equal(t, int32(1), service.pendingCount.Load()) // Still pending

	// Now add block header
	fcu.Headers[blockRoot] = &cltypes.BeaconBlockHeader{
		Slot: 100,
	}

	// Second process: slot ok, block available -> should process
	// ProcessMessage will be called, which calls IsSlotCurrentSlotWithMaximumClockDisparity again
	ethClockMock.EXPECT().IsSlotCurrentSlotWithMaximumClockDisparity(uint64(100)).Return(true).Times(2)
	service.processPendingAttestations(context.Background())

	require.Equal(t, int32(0), service.pendingCount.Load())
	_, exists := service.pendingAttestations.Load(key)
	require.False(t, exists)

	// Attestation should be marked as seen
	require.True(t, service.seenAttestationsCache.Contains(seenPayloadAttestationKey{100, 42}))
}

func TestPayloadAttestationServiceMultiplePendingForSameBlock(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	service, fcu, ethClockMock := setupPayloadAttestationService(t, ctrl)

	blockRoot := common.HexToHash("0x1234")

	// Create two different attestations for the same block (different validators)
	msg1 := newTestPayloadAttestationMessage(100, 1, blockRoot)
	msg2 := newTestPayloadAttestationMessage(100, 2, blockRoot)

	// Add both as pending
	service.pendingAttestations.Store(pendingPayloadAttestationKey{blockRoot, 1}, &pendingPayloadAttestationJob{
		msg:          msg1,
		creationTime: time.Now(),
	})
	service.pendingAttestations.Store(pendingPayloadAttestationKey{blockRoot, 2}, &pendingPayloadAttestationJob{
		msg:          msg2,
		creationTime: time.Now(),
	})
	service.pendingCount.Store(2)

	// Add block header
	fcu.Headers[blockRoot] = &cltypes.BeaconBlockHeader{
		Slot: 100,
	}

	// Expect IsSlotCurrentSlotWithMaximumClockDisparity to be called for each pending + each ProcessMessage
	ethClockMock.EXPECT().IsSlotCurrentSlotWithMaximumClockDisparity(uint64(100)).Return(true).Times(4)

	// Process - both should be processed
	service.processPendingAttestations(context.Background())

	require.Equal(t, int32(0), service.pendingCount.Load())
	require.True(t, service.seenAttestationsCache.Contains(seenPayloadAttestationKey{100, 1}))
	require.True(t, service.seenAttestationsCache.Contains(seenPayloadAttestationKey{100, 2}))
}

func TestPayloadAttestationServiceNames(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	service, _, _ := setupPayloadAttestationService(t, ctrl)

	names := service.Names()
	require.Len(t, names, 1)
	require.Equal(t, "payload_attestation_message", names[0])
}

func TestPayloadAttestationServiceDecodeGossipMessage(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	service, _, _ := setupPayloadAttestationService(t, ctrl)

	// Create a valid message and encode it
	original := newTestPayloadAttestationMessage(100, 42, common.HexToHash("0x1234"))
	encoded, err := original.EncodeSSZ(nil)
	require.NoError(t, err)

	// Decode it
	decoded, err := service.DecodeGossipMessage("peer123", encoded, clparams.GloasVersion)
	require.NoError(t, err)
	require.NotNil(t, decoded)
	require.Equal(t, original.ValidatorIndex, decoded.ValidatorIndex)
	require.Equal(t, original.Data.Slot, decoded.Data.Slot)
	require.Equal(t, original.Data.BeaconBlockRoot, decoded.Data.BeaconBlockRoot)
	require.Equal(t, original.Data.PayloadPresent, decoded.Data.PayloadPresent)
	require.Equal(t, original.Data.BlobDataAvailable, decoded.Data.BlobDataAvailable)
}

func TestPayloadAttestationServiceDecodeGossipMessageInvalid(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	service, _, _ := setupPayloadAttestationService(t, ctrl)

	// Try to decode invalid data
	_, err := service.DecodeGossipMessage("peer123", []byte{0x00, 0x01, 0x02}, clparams.GloasVersion)
	require.Error(t, err)
}
