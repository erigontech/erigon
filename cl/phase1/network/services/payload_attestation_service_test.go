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
	"math"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/erigontech/erigon/cl/beacon/beaconevents"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/phase1/core/state/lru"
	"github.com/erigontech/erigon/cl/phase1/forkchoice"
	"github.com/erigontech/erigon/cl/phase1/forkchoice/mock_services"
	"github.com/erigontech/erigon/cl/pool"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
	"github.com/erigontech/erigon/common"
)

type blockingPayloadAttestationForkchoice struct {
	forkchoice.ForkChoiceStorage
	active  atomic.Int32
	max     atomic.Int32
	started chan struct{}
	release chan struct{}
}

type panickingPayloadAttestationForkchoice struct {
	forkchoice.ForkChoiceStorage
}

func (*panickingPayloadAttestationForkchoice) OnPayloadAttestationMessage(context.Context, *cltypes.PayloadAttestationMessage, bool) error {
	panic("validation panic")
}

func (f *blockingPayloadAttestationForkchoice) OnPayloadAttestationMessage(ctx context.Context, _ *cltypes.PayloadAttestationMessage, _ bool) error {
	active := f.active.Add(1)
	defer f.active.Add(-1)
	for {
		maxActive := f.max.Load()
		if active <= maxActive || f.max.CompareAndSwap(maxActive, active) {
			break
		}
	}
	f.started <- struct{}{}
	select {
	case <-f.release:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

type retryPayloadAttestationForkchoice struct {
	forkchoice.ForkChoiceStorage
	calls        atomic.Int32
	firstStarted chan struct{}
	releaseFirst chan struct{}
}

type retryablePayloadAttestationForkchoice struct {
	forkchoice.ForkChoiceStorage
	first error
	calls atomic.Int32
}

func (f *retryablePayloadAttestationForkchoice) OnPayloadAttestationMessage(context.Context, *cltypes.PayloadAttestationMessage, bool) error {
	if f.calls.Add(1) == 1 {
		return f.first
	}
	return nil
}

type candidatePayloadAttestationForkchoice struct {
	forkchoice.ForkChoiceStorage
	started chan byte
	release chan struct{}
}

func (f *candidatePayloadAttestationForkchoice) OnPayloadAttestationMessage(_ context.Context, msg *cltypes.PayloadAttestationMessage, _ bool) error {
	candidate := msg.Signature[0]
	f.started <- candidate
	<-f.release
	if candidate != 3 {
		return errors.New("invalid signature")
	}
	return nil
}

func (f *retryPayloadAttestationForkchoice) OnPayloadAttestationMessage(context.Context, *cltypes.PayloadAttestationMessage, bool) error {
	if f.calls.Add(1) == 1 {
		close(f.firstStarted)
		<-f.releaseFirst
		return errors.New("invalid signature")
	}
	return nil
}

func setupPayloadAttestationService(t *testing.T, ctrl *gomock.Controller) (*payloadAttestationService, *mock_services.ForkChoiceStorageMock, *eth_clock.MockEthereumClock) {
	forkchoiceMock := mock_services.NewForkChoiceStorageMock(t)
	ethClockMock := eth_clock.NewMockEthereumClock(ctrl)
	ethClockMock.EXPECT().GenesisTime().Return(uint64(0)).AnyTimes()
	ethClockMock.EXPECT().GetSlotTime(gomock.Any()).DoAndReturn(func(slot uint64) time.Time {
		return time.Unix(int64(slot*12), 0)
	}).AnyTimes()

	seenCache, err := lru.New[seenPayloadAttestationKey, struct{}]("seen_payload_attestations", seenPayloadAttestationCacheSize)
	require.NoError(t, err)

	service := &payloadAttestationService{
		forkchoiceStore:       forkchoiceMock,
		ethClock:              ethClockMock,
		netCfg:                nil, // Not used in current implementation
		seenAttestationsCache: seenCache,
		emitters:              beaconevents.NewEventEmitter(),
		epbsPool:              pool.NewEpbsPool(),
		validationAdmission:   make(chan struct{}, maxConcurrentPayloadAttestationValidations),
		now:                   func() time.Time { return time.Unix(100*12+6, 0) },
	}
	service.pending = service.newPendingQueue()

	return service, forkchoiceMock, ethClockMock
}

func TestPayloadAttestationServiceBoundsKnownBlockValidation(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	service, fcu, _ := setupPayloadAttestationService(t, ctrl)
	service.validationAdmission = make(chan struct{}, 1)
	blockRoot := common.HexToHash("0x1234")
	fcu.Headers[blockRoot] = &cltypes.BeaconBlockHeader{Slot: 100}
	blockingForkchoice := &blockingPayloadAttestationForkchoice{
		ForkChoiceStorage: fcu,
		started:           make(chan struct{}, 1),
		release:           make(chan struct{}),
	}
	service.forkchoiceStore = blockingForkchoice
	firstDone := make(chan error, 1)
	go func() {
		firstDone <- service.ProcessMessage(context.Background(), nil, &cltypes.PayloadAttestationMessage{
			ValidatorIndex: 1,
			Data:           &cltypes.PayloadAttestationData{Slot: 100, BeaconBlockRoot: blockRoot},
		})
	}()
	<-blockingForkchoice.started

	secondCtx, cancelSecond := context.WithCancel(context.Background())
	cancelSecond()
	err := service.ProcessMessage(secondCtx, nil, &cltypes.PayloadAttestationMessage{
		ValidatorIndex: 2,
		Data:           &cltypes.PayloadAttestationData{Slot: 100, BeaconBlockRoot: blockRoot},
	})
	require.ErrorIs(t, err, ErrIgnore)
	close(blockingForkchoice.release)
	require.NoError(t, <-firstDone)
}

func TestPayloadAttestationServiceReleasesAdmissionAfterValidationPanic(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	service, fcu, _ := setupPayloadAttestationService(t, ctrl)
	service.validationAdmission = make(chan struct{}, 1)
	blockRoot := common.HexToHash("0x1234")
	fcu.Headers[blockRoot] = &cltypes.BeaconBlockHeader{Slot: 100}
	service.forkchoiceStore = &panickingPayloadAttestationForkchoice{ForkChoiceStorage: fcu}
	msg := newTestPayloadAttestationMessage(100, 1, blockRoot)

	var recovered any
	func() {
		defer func() { recovered = recover() }()
		_ = service.ProcessMessage(context.Background(), nil, msg)
	}()

	require.Equal(t, "validation panic", recovered)
	require.Empty(t, service.validationAdmission)
	service.forkchoiceStore = fcu
	require.NoError(t, service.ProcessMessage(context.Background(), nil, msg))
}

func TestPayloadAttestationServiceBackpressuresInsteadOfDroppingValidCandidate(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	service, fcu, _ := setupPayloadAttestationService(t, ctrl)
	service.validationAdmission = make(chan struct{}, 1)
	blockRoot := common.HexToHash("0x1234")
	fcu.Headers[blockRoot] = &cltypes.BeaconBlockHeader{Slot: 100}
	validations := &candidatePayloadAttestationForkchoice{
		ForkChoiceStorage: fcu,
		started:           make(chan byte, 2),
		release:           make(chan struct{}),
	}
	service.forkchoiceStore = validations
	invalid := newTestPayloadAttestationMessage(100, 1, blockRoot)
	invalid.Signature[0] = 1
	firstResult := make(chan error, 1)
	go func() { firstResult <- service.ProcessMessage(context.Background(), nil, invalid) }()
	require.Equal(t, byte(1), <-validations.started)

	valid := newTestPayloadAttestationMessage(100, 1, blockRoot)
	valid.Signature[0] = 3
	secondResult := make(chan error, 1)
	go func() { secondResult <- service.ProcessMessage(context.Background(), nil, valid) }()
	select {
	case err := <-secondResult:
		close(validations.release)
		t.Fatalf("valid candidate returned before admission was released: %v", err)
	case <-time.After(20 * time.Millisecond):
	}

	close(validations.release)
	require.Error(t, <-firstResult)
	require.NoError(t, <-secondResult)
}

func TestPayloadAttestationServiceDoesNotDropValidCandidateBehindInvalidCandidates(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	service, fcu, _ := setupPayloadAttestationService(t, ctrl)
	blockRoot := common.HexToHash("0x1234")
	fcu.Headers[blockRoot] = &cltypes.BeaconBlockHeader{Slot: 100}
	blockingForkchoice := &candidatePayloadAttestationForkchoice{
		ForkChoiceStorage: fcu,
		started:           make(chan byte, 3),
		release:           make(chan struct{}),
	}
	service.forkchoiceStore = blockingForkchoice
	results := make(chan error, 3)
	for candidate := byte(1); candidate <= 3; candidate++ {
		msg := newTestPayloadAttestationMessage(100, 1, blockRoot)
		msg.Signature[0] = candidate
		go func() { results <- service.ProcessMessage(context.Background(), nil, msg) }()
		if candidate == 1 {
			require.Equal(t, byte(1), <-blockingForkchoice.started)
		}
	}
	for range 2 {
		select {
		case <-blockingForkchoice.started:
		case <-time.After(time.Second):
			close(blockingForkchoice.release)
			t.Fatal("candidate was dropped before validation")
		}
	}
	close(blockingForkchoice.release)

	var successes int
	for range 3 {
		if <-results == nil {
			successes++
		}
	}
	require.Equal(t, 1, successes)
}

func TestPayloadAttestationServiceAllowsConcurrentValidationForDifferentValidators(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	service, fcu, _ := setupPayloadAttestationService(t, ctrl)
	blockRoot := common.HexToHash("0x1234")
	fcu.Headers[blockRoot] = &cltypes.BeaconBlockHeader{Slot: 100}

	blockingForkchoice := &blockingPayloadAttestationForkchoice{
		ForkChoiceStorage: fcu,
		started:           make(chan struct{}, 8),
		release:           make(chan struct{}),
	}
	service.forkchoiceStore = blockingForkchoice
	var wg sync.WaitGroup
	results := make(chan error, 8)
	for i := range 8 {
		wg.Go(func() {
			msg := newTestPayloadAttestationMessage(100, uint64(i), blockRoot)
			results <- service.ProcessMessage(context.Background(), nil, msg)
		})
	}

	for range 8 {
		select {
		case <-blockingForkchoice.started:
		case <-time.After(time.Second):
			close(blockingForkchoice.release)
			wg.Wait()
			require.FailNow(t, "validation was throttled")
		}
	}
	close(blockingForkchoice.release)
	wg.Wait()
	close(results)
	for err := range results {
		require.NoError(t, err)
	}
	require.Equal(t, int32(8), blockingForkchoice.max.Load())
}

func TestPayloadAttestationServiceAllowsConcurrentValidationForSameValidator(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	service, fcu, _ := setupPayloadAttestationService(t, ctrl)
	blockRoot := common.HexToHash("0x1234")
	fcu.Headers[blockRoot] = &cltypes.BeaconBlockHeader{Slot: 100}

	blockingForkchoice := &blockingPayloadAttestationForkchoice{
		ForkChoiceStorage: fcu,
		started:           make(chan struct{}, 2),
		release:           make(chan struct{}),
	}
	service.forkchoiceStore = blockingForkchoice
	results := make(chan error, 2)
	for range 2 {
		go func() {
			results <- service.ProcessMessage(context.Background(), nil, newTestPayloadAttestationMessage(100, 42, blockRoot))
		}()
	}

	<-blockingForkchoice.started
	select {
	case <-blockingForkchoice.started:
	case <-time.After(time.Second):
		close(blockingForkchoice.release)
		t.Fatal("second candidate did not reach validation")
	}
	close(blockingForkchoice.release)

	require.NoError(t, <-results)
	require.NoError(t, <-results)
	require.Equal(t, int32(2), blockingForkchoice.max.Load())
}

func TestPayloadAttestationServiceRetriesAfterInvalidDuplicate(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	service, fcu, _ := setupPayloadAttestationService(t, ctrl)
	blockRoot := common.HexToHash("0x1234")
	fcu.Headers[blockRoot] = &cltypes.BeaconBlockHeader{Slot: 100}

	retryForkchoice := &retryPayloadAttestationForkchoice{
		ForkChoiceStorage: fcu,
		firstStarted:      make(chan struct{}),
		releaseFirst:      make(chan struct{}),
	}
	service.forkchoiceStore = retryForkchoice
	firstResult := make(chan error, 1)
	go func() {
		firstResult <- service.ProcessMessage(context.Background(), nil, newTestPayloadAttestationMessage(100, 42, blockRoot))
	}()
	<-retryForkchoice.firstStarted

	secondResult := make(chan error, 1)
	go func() {
		second := newTestPayloadAttestationMessage(100, 42, blockRoot)
		second.Signature[0] = 1
		secondResult <- service.ProcessMessage(context.Background(), nil, second)
	}()

	require.Eventually(t, func() bool { return retryForkchoice.calls.Load() == 2 }, time.Second, time.Millisecond)
	close(retryForkchoice.releaseFirst)

	require.ErrorContains(t, <-firstResult, "invalid signature")
	require.NoError(t, <-secondResult)
	require.Equal(t, int32(2), retryForkchoice.calls.Load())
}

func TestPayloadAttestationServiceIgnoresCanceledDuplicateWaiter(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	service, fcu, _ := setupPayloadAttestationService(t, ctrl)
	blockRoot := common.HexToHash("0x1234")
	fcu.Headers[blockRoot] = &cltypes.BeaconBlockHeader{Slot: 100}
	blockingForkchoice := &blockingPayloadAttestationForkchoice{
		ForkChoiceStorage: fcu,
		started:           make(chan struct{}, 1),
		release:           make(chan struct{}),
	}
	service.forkchoiceStore = blockingForkchoice
	firstResult := make(chan error, 1)
	go func() {
		firstResult <- service.ProcessMessage(context.Background(), nil, newTestPayloadAttestationMessage(100, 42, blockRoot))
	}()
	<-blockingForkchoice.started

	waiterCtx, cancel := context.WithCancel(context.Background())
	cancel()
	err := service.ProcessMessage(waiterCtx, nil, newTestPayloadAttestationMessage(100, 42, blockRoot))
	require.ErrorIs(t, err, ErrIgnore)

	close(blockingForkchoice.release)
	require.NoError(t, <-firstResult)
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

	service, _, _ := setupPayloadAttestationService(t, ctrl)

	blockRoot := common.HexToHash("0x1234")
	msg := newTestPayloadAttestationMessage(100, 1, blockRoot)

	service.now = func() time.Time { return time.Unix(100*12, 0).Add(-gloasMaximumClockDisparity - time.Millisecond) }

	err := service.ProcessMessage(context.Background(), nil, msg)
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrIgnore))
	require.Contains(t, err.Error(), "not current slot")
}

func TestPayloadAttestationServiceRejectsTooEarlyNextSlot(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	service, _, _ := setupPayloadAttestationService(t, ctrl)
	service.now = func() time.Time { return time.Unix(100*12, 0).Add(-gloasMaximumClockDisparity - time.Millisecond) }

	err := service.ProcessMessage(context.Background(), nil, newTestPayloadAttestationMessage(100, 1, common.HexToHash("0x1234")))
	require.ErrorIs(t, err, ErrIgnore)
	require.NotErrorIs(t, err, ErrAttestationQueued)
}

func TestPayloadAttestationSlotCurrentBoundaries(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	service, _, _ := setupPayloadAttestationService(t, ctrl)
	slotStart := service.ethClock.GetSlotTime(100)
	nextSlotStart := service.ethClock.GetSlotTime(101)
	for _, tc := range []struct {
		name string
		now  time.Time
		slot uint64
		want bool
	}{
		{name: "too early next slot", now: slotStart.Add(-gloasMaximumClockDisparity - time.Millisecond), slot: 100, want: false},
		{name: "exact lower boundary", now: slotStart.Add(-gloasMaximumClockDisparity), slot: 100, want: true},
		{name: "current slot interior", now: slotStart.Add(6 * time.Second), slot: 100, want: true},
		{name: "exact upper boundary", now: nextSlotStart.Add(gloasMaximumClockDisparity), slot: 100, want: true},
		{name: "too late previous slot", now: nextSlotStart.Add(gloasMaximumClockDisparity + time.Millisecond), slot: 100, want: false},
		{name: "slot time overflow", now: slotStart, slot: uint64(math.MaxInt64/12) + 1, want: false},
		{name: "maximum slot", now: slotStart, slot: math.MaxUint64, want: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, isPayloadAttestationSlotCurrent(service.ethClock, tc.now, tc.slot))
		})
	}
}

func TestPayloadAttestationServiceDuplicateValidator(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	service, fcu, _ := setupPayloadAttestationService(t, ctrl)

	blockRoot := common.HexToHash("0x1234")
	msg := newTestPayloadAttestationMessage(100, 42, blockRoot)

	// Add block header to forkchoice
	fcu.Headers[blockRoot] = &cltypes.BeaconBlockHeader{
		Slot: 100,
	}

	// First call should succeed
	err := service.ProcessMessage(context.Background(), nil, msg)
	require.NoError(t, err)

	// Second call with same (slot, validatorIndex) should be ignored
	err = service.ProcessMessage(context.Background(), nil, msg)
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrIgnore))
	require.Contains(t, err.Error(), "already seen payload attestation")
}

func TestPayloadAttestationServiceBlockNotFound(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	service, _, _ := setupPayloadAttestationService(t, ctrl)

	blockRoot := common.HexToHash("0x1234")
	msg := newTestPayloadAttestationMessage(100, 1, blockRoot)

	// Block not in forkchoice - should queue and report queued.
	err := service.ProcessMessage(context.Background(), nil, msg)
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrIgnore))
	require.True(t, errors.Is(err, ErrAttestationQueued))

	// Verify attestation was queued
	require.Equal(t, int32(1), service.pending.count.Load())

	// Verify the pending key
	key := pendingPayloadAttestationKeyFor(blockRoot, msg)
	_, exists := service.pending.jobs.Load(key)
	require.True(t, exists)
}

func TestPayloadAttestationServiceReportsCapacityWhenMissingBlockQueueIsFull(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	service, _, _ := setupPayloadAttestationService(t, ctrl)
	for i := range maxPendingAttestations {
		key := pendingPayloadAttestationKey{
			blockRoot:      common.Hash{byte(i), byte(i >> 8)},
			validatorIndex: uint64(i),
			messageRoot:    common.Hash{byte(i >> 8), byte(i)},
		}
		service.pending.jobs.Store(key, &pendingJob[*cltypes.PayloadAttestationMessage]{})
	}
	service.pending.count.Store(maxPendingAttestations)

	blockRoot := common.HexToHash("0x1234")
	msg := newTestPayloadAttestationMessage(100, 1, blockRoot)
	err := service.ProcessMessage(context.Background(), nil, msg)
	require.ErrorIs(t, err, ErrIgnore)
	require.ErrorIs(t, err, ErrAttestationCapacity)
	require.NotErrorIs(t, err, ErrAttestationQueued)
	require.Equal(t, int32(maxPendingAttestations), service.pending.count.Load())
	_, exists := service.pending.jobs.Load(pendingPayloadAttestationKeyFor(blockRoot, msg))
	require.False(t, exists)
}

func TestPayloadAttestationServiceReportsQueuedWhenExactWorkExistsAtCapacity(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	service, _, _ := setupPayloadAttestationService(t, ctrl)
	blockRoot := common.HexToHash("0x1234")
	msg := newTestPayloadAttestationMessage(100, 1, blockRoot)
	service.pending.jobs.Store(pendingPayloadAttestationKeyFor(blockRoot, msg), &pendingJob[*cltypes.PayloadAttestationMessage]{msg: msg})
	for i := range maxPendingAttestations - 1 {
		key := pendingPayloadAttestationKey{
			blockRoot:      common.Hash{byte(i), byte(i >> 8)},
			validatorIndex: uint64(i + 2),
			messageRoot:    common.Hash{byte(i >> 8), byte(i)},
		}
		service.pending.jobs.Store(key, &pendingJob[*cltypes.PayloadAttestationMessage]{})
	}
	service.pending.count.Store(maxPendingAttestations)
	err := service.ProcessMessage(context.Background(), nil, msg)
	require.ErrorIs(t, err, ErrIgnore)
	require.ErrorIs(t, err, ErrAttestationQueued)
	require.NotErrorIs(t, err, ErrAttestationCapacity)
	require.Equal(t, int32(maxPendingAttestations), service.pending.count.Load())
}

func TestPayloadAttestationServicePendingQueueKeepsDistinctSameValidatorBlock(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	service, _, _ := setupPayloadAttestationService(t, ctrl)

	blockRoot := common.HexToHash("0x1234")
	first := newTestPayloadAttestationMessage(100, 1, blockRoot)
	second := newTestPayloadAttestationMessage(100, 1, blockRoot)
	second.Signature[0] = 1

	service.queuePendingAttestation(blockRoot, first)
	service.queuePendingAttestation(blockRoot, first)
	service.queuePendingAttestation(blockRoot, second)

	require.Equal(t, int32(2), service.pending.count.Load())
	_, firstExists := service.pending.jobs.Load(pendingPayloadAttestationKeyFor(blockRoot, first))
	require.True(t, firstExists)
	_, secondExists := service.pending.jobs.Load(pendingPayloadAttestationKeyFor(blockRoot, second))
	require.True(t, secondExists)
}

func TestPayloadAttestationServiceReferencedBlockSlotMismatch(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	service, fcu, _ := setupPayloadAttestationService(t, ctrl)

	blockRoot := common.HexToHash("0x1234")
	msg := newTestPayloadAttestationMessage(100, 1, blockRoot)

	fcu.Headers[blockRoot] = &cltypes.BeaconBlockHeader{
		Slot: 99,
	}

	err := service.ProcessMessage(context.Background(), nil, msg)
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrIgnore))
	require.Contains(t, err.Error(), "does not match referenced block slot")
	require.False(t, service.seenAttestationsCache.Contains(seenPayloadAttestationKey{100, 1}))
}

func TestPayloadAttestationServiceSuccess(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	service, fcu, _ := setupPayloadAttestationService(t, ctrl)

	blockRoot := common.HexToHash("0x1234")
	msg := newTestPayloadAttestationMessage(100, 42, blockRoot)

	// Add block header to forkchoice
	fcu.Headers[blockRoot] = &cltypes.BeaconBlockHeader{
		Slot: 100,
	}

	// Process should succeed
	err := service.ProcessMessage(context.Background(), nil, msg)
	require.NoError(t, err)

	// Verify attestation was marked as seen
	seenKey := seenPayloadAttestationKey{
		slot:           100,
		validatorIndex: 42,
	}
	require.True(t, service.seenAttestationsCache.Contains(seenKey))
	pooled, ok := service.epbsPool.PayloadAttestations.Get(pool.PayloadAttestationKey{Slot: 100, ValidatorIndex: 42})
	require.True(t, ok)
	require.Same(t, msg, pooled)
}

func TestPayloadAttestationServiceRESTCommitsOnlyAfterPublish(t *testing.T) {
	ctrl := gomock.NewController(t)
	service, fcu, _ := setupPayloadAttestationService(t, ctrl)
	root := common.HexToHash("0x1234")
	msg := newTestPayloadAttestationMessage(100, 42, root)
	fcu.Headers[root] = &cltypes.BeaconBlockHeader{Slot: 100}
	require.Error(t, service.ProcessRESTMessage(t.Context(), msg, func() error { return errors.New("publish failed") }))
	require.False(t, service.seenAttestationsCache.Contains(seenPayloadAttestationKey{100, 42}))
	_, ok := service.epbsPool.PayloadAttestations.Get(pool.PayloadAttestationKey{Slot: 100, ValidatorIndex: 42})
	require.False(t, ok)
	require.NoError(t, service.ProcessRESTMessage(t.Context(), msg, func() error { return nil }))
	require.True(t, service.seenAttestationsCache.Contains(seenPayloadAttestationKey{100, 42}))
}

type consumeOncePayloadAttestationForkchoice struct {
	forkchoice.ForkChoiceStorage
	calls atomic.Int32
}

type coordinatedPayloadAttestationForkchoice struct {
	forkchoice.ForkChoiceStorage
	calls        atomic.Int32
	firstStarted chan struct{}
	releaseFirst chan struct{}
	firstErr     error
}

func (f *coordinatedPayloadAttestationForkchoice) OnPayloadAttestationMessage(context.Context, *cltypes.PayloadAttestationMessage, bool) error {
	if f.calls.Add(1) == 1 {
		close(f.firstStarted)
		<-f.releaseFirst
		return f.firstErr
	}
	return nil
}

func (f *consumeOncePayloadAttestationForkchoice) OnPayloadAttestationMessage(context.Context, *cltypes.PayloadAttestationMessage, bool) error {
	if f.calls.Add(1) == 1 {
		return nil
	}
	return forkchoice.ErrIgnore
}

func TestPayloadAttestationServiceRESTRetriesPublishWithoutRevalidating(t *testing.T) {
	ctrl := gomock.NewController(t)
	service, fcu, _ := setupPayloadAttestationService(t, ctrl)
	root := common.HexToHash("0x1234")
	msg := newTestPayloadAttestationMessage(100, 42, root)
	fcu.Headers[root] = &cltypes.BeaconBlockHeader{Slot: 100}
	consumeOnce := &consumeOncePayloadAttestationForkchoice{ForkChoiceStorage: fcu}
	service.forkchoiceStore = consumeOnce
	require.Error(t, service.ProcessRESTMessage(t.Context(), msg, func() error { return errors.New("publish failed") }))
	require.NoError(t, service.ProcessRESTMessage(t.Context(), msg, func() error { return nil }))
	require.Equal(t, int32(1), consumeOnce.calls.Load())
	require.True(t, service.seenAttestationsCache.Contains(seenPayloadAttestationKey{100, 42}))
}

func TestPayloadAttestationServiceP2PSatisfiesPendingRESTPublish(t *testing.T) {
	ctrl := gomock.NewController(t)
	service, fcu, _ := setupPayloadAttestationService(t, ctrl)
	root := common.HexToHash("0x1234")
	msg := newTestPayloadAttestationMessage(100, 42, root)
	fcu.Headers[root] = &cltypes.BeaconBlockHeader{Slot: 100}
	consumeOnce := &consumeOncePayloadAttestationForkchoice{ForkChoiceStorage: fcu}
	service.forkchoiceStore = consumeOnce
	require.Error(t, service.ProcessRESTMessage(t.Context(), msg, func() error { return errors.New("publish failed") }))
	require.NoError(t, service.ProcessMessage(t.Context(), nil, msg))
	require.Equal(t, int32(1), consumeOnce.calls.Load())
	require.True(t, service.seenAttestationsCache.Contains(seenPayloadAttestationKey{100, 42}))
}

func TestPayloadAttestationServiceMismatchedP2PValidatesBehindPendingREST(t *testing.T) {
	ctrl := gomock.NewController(t)
	service, fcu, _ := setupPayloadAttestationService(t, ctrl)
	root := common.HexToHash("0x1234")
	restMsg := newTestPayloadAttestationMessage(100, 42, root)
	p2pMsg := newTestPayloadAttestationMessage(100, 42, root)
	p2pMsg.Signature[0] = 1
	fcu.Headers[root] = &cltypes.BeaconBlockHeader{Slot: 100}
	consumeOnce := &consumeOncePayloadAttestationForkchoice{ForkChoiceStorage: fcu}
	service.forkchoiceStore = consumeOnce
	require.Error(t, service.ProcessRESTMessage(t.Context(), restMsg, func() error { return errors.New("publish failed") }))
	require.ErrorIs(t, service.ProcessMessage(t.Context(), nil, p2pMsg), ErrAttestationRetryable)
	require.Equal(t, int32(2), consumeOnce.calls.Load())
}

func TestPayloadAttestationServiceExactP2PJoinsRESTValidationBeforePublishFailure(t *testing.T) {
	ctrl := gomock.NewController(t)
	service, fcu, _ := setupPayloadAttestationService(t, ctrl)
	root := common.HexToHash("0x1234")
	msg := newTestPayloadAttestationMessage(100, 42, root)
	fcu.Headers[root] = &cltypes.BeaconBlockHeader{Slot: 100}
	coordinated := &coordinatedPayloadAttestationForkchoice{
		ForkChoiceStorage: fcu,
		firstStarted:      make(chan struct{}),
		releaseFirst:      make(chan struct{}),
	}
	service.forkchoiceStore = coordinated
	restResult := make(chan error, 1)
	go func() {
		restResult <- service.ProcessRESTMessage(t.Context(), msg, func() error { return errors.New("publish failed") })
	}()
	<-coordinated.firstStarted
	p2pResult := make(chan error, 1)
	go func() { p2pResult <- service.ProcessMessage(t.Context(), nil, msg) }()
	waitForValidatedRESTCoordinatorRefs(t, service, seenPayloadAttestationKey{100, 42}, 2)
	close(coordinated.releaseFirst)
	require.Error(t, <-restResult)
	require.NoError(t, <-p2pResult)
	require.Equal(t, int32(1), coordinated.calls.Load())
	require.True(t, service.seenAttestationsCache.Contains(seenPayloadAttestationKey{100, 42}))
}

func TestPayloadAttestationServiceP2PValidatesAfterRESTValidationFailure(t *testing.T) {
	ctrl := gomock.NewController(t)
	service, fcu, _ := setupPayloadAttestationService(t, ctrl)
	root := common.HexToHash("0x1234")
	msg := newTestPayloadAttestationMessage(100, 42, root)
	fcu.Headers[root] = &cltypes.BeaconBlockHeader{Slot: 100}
	coordinated := &coordinatedPayloadAttestationForkchoice{
		ForkChoiceStorage: fcu,
		firstStarted:      make(chan struct{}),
		releaseFirst:      make(chan struct{}),
		firstErr:          errors.New("invalid signature"),
	}
	service.forkchoiceStore = coordinated
	restResult := make(chan error, 1)
	var publishCalled atomic.Bool
	go func() {
		restResult <- service.ProcessRESTMessage(t.Context(), msg, func() error {
			publishCalled.Store(true)
			return nil
		})
	}()
	<-coordinated.firstStarted
	p2pResult := make(chan error, 1)
	go func() { p2pResult <- service.ProcessMessage(t.Context(), nil, msg) }()
	waitForValidatedRESTCoordinatorRefs(t, service, seenPayloadAttestationKey{100, 42}, 2)
	close(coordinated.releaseFirst)
	require.Error(t, <-restResult)
	require.NoError(t, <-p2pResult)
	require.Equal(t, int32(2), coordinated.calls.Load())
	require.False(t, publishCalled.Load())
}

func waitForValidatedRESTCoordinatorRefs(t *testing.T, service *payloadAttestationService, key seenPayloadAttestationKey, refs int) {
	t.Helper()
	require.Eventually(t, func() bool {
		service.validatedRESTMu.Lock()
		defer service.validatedRESTMu.Unlock()
		entry := service.validatedREST[key]
		return entry != nil && entry.refs == refs
	}, time.Second, time.Millisecond)
}

func TestPayloadAttestationServiceRESTPendingRejectsDifferentIdentity(t *testing.T) {
	ctrl := gomock.NewController(t)
	service, fcu, _ := setupPayloadAttestationService(t, ctrl)
	root := common.HexToHash("0x1234")
	first := newTestPayloadAttestationMessage(100, 42, root)
	second := newTestPayloadAttestationMessage(100, 42, root)
	second.Signature[0] = 1
	fcu.Headers[root] = &cltypes.BeaconBlockHeader{Slot: 100}
	require.Error(t, service.ProcessRESTMessage(t.Context(), first, func() error { return errors.New("publish failed") }))
	err := service.ProcessRESTMessage(t.Context(), second, func() error { t.Fatal("different identity published"); return nil })
	require.ErrorIs(t, err, ErrAttestationConflict)
}

func TestPayloadAttestationServiceConcurrentRESTRetryCoalescesValidation(t *testing.T) {
	ctrl := gomock.NewController(t)
	service, fcu, _ := setupPayloadAttestationService(t, ctrl)
	root := common.HexToHash("0x1234")
	msg := newTestPayloadAttestationMessage(100, 42, root)
	fcu.Headers[root] = &cltypes.BeaconBlockHeader{Slot: 100}
	consumeOnce := &consumeOncePayloadAttestationForkchoice{ForkChoiceStorage: fcu}
	service.forkchoiceStore = consumeOnce
	firstPublishing := make(chan struct{})
	releaseFirst := make(chan struct{})
	results := make(chan error, 2)
	go func() {
		results <- service.ProcessRESTMessage(t.Context(), msg, func() error {
			close(firstPublishing)
			<-releaseFirst
			return errors.New("publish failed")
		})
	}()
	<-firstPublishing
	go func() {
		results <- service.ProcessRESTMessage(t.Context(), msg, func() error { return nil })
	}()
	close(releaseFirst)

	firstErr, secondErr := <-results, <-results
	require.True(t, (firstErr == nil) != (secondErr == nil))
	require.Equal(t, int32(1), consumeOnce.calls.Load())
	require.True(t, service.seenAttestationsCache.Contains(seenPayloadAttestationKey{100, 42}))
}

func TestValidatedRESTPayloadAttestationCoordinatorExpiresAndBoundsEntries(t *testing.T) {
	service := &payloadAttestationService{validatedREST: make(map[seenPayloadAttestationKey]*validatedRESTPayloadAttestation)}
	service.validatedREST[seenPayloadAttestationKey{1, 1}] = &validatedRESTPayloadAttestation{
		creationTime: time.Now().Add(-pendingPayloadAttestationExpiry - time.Second),
	}
	entry, err := service.acquireValidatedRESTAttestation(seenPayloadAttestationKey{2, 2}, common.Hash{2})
	require.NoError(t, err)
	require.Len(t, service.validatedREST, 1)
	service.releaseValidatedRESTAttestation(seenPayloadAttestationKey{2, 2}, entry, true)
	for i := range maxPendingAttestations - 1 {
		service.validatedREST[seenPayloadAttestationKey{slot: uint64(i + 3), validatorIndex: uint64(i + 3)}] = &validatedRESTPayloadAttestation{creationTime: time.Now()}
	}
	_, err = service.acquireValidatedRESTAttestation(seenPayloadAttestationKey{9999, 9999}, common.Hash{9})
	require.ErrorIs(t, err, ErrAttestationCapacity)
}

func TestPayloadAttestationServiceDifferentValidatorsSameBlock(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	service, fcu, _ := setupPayloadAttestationService(t, ctrl)

	blockRoot := common.HexToHash("0x1234")
	msg1 := newTestPayloadAttestationMessage(100, 1, blockRoot)
	msg2 := newTestPayloadAttestationMessage(100, 2, blockRoot)

	// Add block header to forkchoice
	fcu.Headers[blockRoot] = &cltypes.BeaconBlockHeader{
		Slot: 100,
	}

	// Both should be processed (different validators)
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
	key := pendingPayloadAttestationKeyFor(blockRoot, msg)
	service.pending.jobs.Store(key, &pendingJob[*cltypes.PayloadAttestationMessage]{
		msg:          msg,
		creationTime: time.Now().Add(-pendingPayloadAttestationExpiry - time.Second), // expired
	})
	service.pending.count.Store(1)

	// Process pending - should remove expired
	service.pending.processPending(context.Background())

	require.Equal(t, int32(0), service.pending.count.Load())
	_, exists := service.pending.jobs.Load(key)
	require.False(t, exists)
}

func TestPayloadAttestationServicePendingSlotMismatch(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	service, _, _ := setupPayloadAttestationService(t, ctrl)

	blockRoot := common.HexToHash("0x1234")
	msg := newTestPayloadAttestationMessage(100, 1, blockRoot)

	// Add pending job
	key := pendingPayloadAttestationKeyFor(blockRoot, msg)
	service.pending.jobs.Store(key, &pendingJob[*cltypes.PayloadAttestationMessage]{
		msg:          msg,
		creationTime: time.Now(),
	})
	service.pending.count.Store(1)

	service.now = func() time.Time { return time.Unix(101*12, 0).Add(gloasMaximumClockDisparity + time.Millisecond) }

	// Process pending - should remove due to slot mismatch
	service.pending.processPending(context.Background())

	require.Equal(t, int32(0), service.pending.count.Load())
	_, exists := service.pending.jobs.Load(key)
	require.False(t, exists)
}

func TestPayloadAttestationServicePendingProcessing(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	service, fcu, _ := setupPayloadAttestationService(t, ctrl)

	blockRoot := common.HexToHash("0x1234")
	msg := newTestPayloadAttestationMessage(100, 42, blockRoot)

	// Add pending job
	key := pendingPayloadAttestationKeyFor(blockRoot, msg)
	service.pending.jobs.Store(key, &pendingJob[*cltypes.PayloadAttestationMessage]{
		msg:          msg,
		creationTime: time.Now(),
	})
	service.pending.count.Store(1)

	// First process: slot ok, but block not available
	service.pending.processPending(context.Background())
	require.Equal(t, int32(1), service.pending.count.Load()) // Still pending

	// Now add block header
	fcu.Headers[blockRoot] = &cltypes.BeaconBlockHeader{
		Slot: 100,
	}

	// Second process: slot ok, block available -> should process
	service.pending.processPending(context.Background())

	require.Equal(t, int32(0), service.pending.count.Load())
	_, exists := service.pending.jobs.Load(key)
	require.False(t, exists)

	// Attestation should be marked as seen
	require.True(t, service.seenAttestationsCache.Contains(seenPayloadAttestationKey{100, 42}))
}

func TestPayloadAttestationServiceMultiplePendingForSameBlock(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	service, fcu, _ := setupPayloadAttestationService(t, ctrl)

	blockRoot := common.HexToHash("0x1234")

	// Create two different attestations for the same block (different validators)
	msg1 := newTestPayloadAttestationMessage(100, 1, blockRoot)
	msg2 := newTestPayloadAttestationMessage(100, 2, blockRoot)

	// Add both as pending
	service.pending.jobs.Store(pendingPayloadAttestationKeyFor(blockRoot, msg1), &pendingJob[*cltypes.PayloadAttestationMessage]{
		msg:          msg1,
		creationTime: time.Now(),
	})
	service.pending.jobs.Store(pendingPayloadAttestationKeyFor(blockRoot, msg2), &pendingJob[*cltypes.PayloadAttestationMessage]{
		msg:          msg2,
		creationTime: time.Now(),
	})
	service.pending.count.Store(2)

	// Add block header
	fcu.Headers[blockRoot] = &cltypes.BeaconBlockHeader{
		Slot: 100,
	}

	// Process - both should be processed
	service.pending.processPending(context.Background())

	require.Equal(t, int32(0), service.pending.count.Load())
	require.True(t, service.seenAttestationsCache.Contains(seenPayloadAttestationKey{100, 1}))
	require.True(t, service.seenAttestationsCache.Contains(seenPayloadAttestationKey{100, 2}))
}

func TestPayloadAttestationServiceRetainsPendingRetryableValidation(t *testing.T) {
	for _, firstErr := range []error{forkchoice.ErrIgnore, context.Canceled} {
		t.Run(firstErr.Error(), func(t *testing.T) {
			ctrl := gomock.NewController(t)
			service, fcu, _ := setupPayloadAttestationService(t, ctrl)
			root := common.HexToHash("0x1234")
			msg := newTestPayloadAttestationMessage(100, 42, root)
			fcu.Headers[root] = &cltypes.BeaconBlockHeader{Slot: 100}
			service.forkchoiceStore = &retryablePayloadAttestationForkchoice{ForkChoiceStorage: fcu, first: firstErr}
			service.queuePendingAttestation(root, msg)
			service.pending.processPending(t.Context())
			require.Equal(t, int32(1), service.pending.count.Load())
			service.pending.processPending(t.Context())
			require.Zero(t, service.pending.count.Load())
		})
	}
}

func TestPayloadAttestationServicePendingQueueCap(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	service, _, _ := setupPayloadAttestationService(t, ctrl)

	// Fill the queue to the cap
	service.pending.count.Store(maxPendingAttestations)

	blockRoot := common.HexToHash("0xffff")
	msg := newTestPayloadAttestationMessage(100, 999, blockRoot)

	service.queuePendingAttestation(blockRoot, msg)

	// Should still be at cap — new item was rejected
	require.Equal(t, int32(maxPendingAttestations), service.pending.count.Load())
	key := pendingPayloadAttestationKeyFor(blockRoot, msg)
	_, exists := service.pending.jobs.Load(key)
	require.False(t, exists)
}

func TestPayloadAttestationServicePendingQueueCapConcurrent(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	service, _, _ := setupPayloadAttestationService(t, ctrl)

	// Start near cap so only a few slots remain
	service.pending.count.Store(maxPendingAttestations - 5)

	var wg sync.WaitGroup
	for i := range 100 {
		wg.Go(func() {
			blockRoot := common.Hash{byte(i), byte(i >> 8)}
			msg := newTestPayloadAttestationMessage(100, uint64(10000+i), blockRoot)
			service.queuePendingAttestation(blockRoot, msg)
		})
	}
	wg.Wait()

	require.Equal(t, int32(maxPendingAttestations), service.pending.count.Load())
	stored := 0
	service.pending.jobs.Range(func(_, _ any) bool {
		stored++
		return true
	})
	require.Equal(t, 5, stored)
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

func TestPayloadAttestationServiceDecodeGossipMessageStrict(t *testing.T) {
	ctrl := gomock.NewController(t)
	service, _, _ := setupPayloadAttestationService(t, ctrl)
	encoded, err := newTestPayloadAttestationMessage(100, 42, common.Hash{1}).EncodeSSZ(nil)
	require.NoError(t, err)

	_, err = service.DecodeGossipMessage("peer123", append(encoded, 0), clparams.GloasVersion)
	require.Error(t, err)

	nonCanonicalBool := append([]byte(nil), encoded...)
	const payloadPresentOffset = 8 + 32 + 8
	nonCanonicalBool[payloadPresentOffset] = 2
	_, err = service.DecodeGossipMessage("peer123", nonCanonicalBool, clparams.GloasVersion)
	require.Error(t, err)
}
