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
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/erigontech/erigon/cl/beacon/beaconevents"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/gossip"
	"github.com/erigontech/erigon/cl/phase1/core/state/lru"
	"github.com/erigontech/erigon/cl/phase1/forkchoice"
	"github.com/erigontech/erigon/cl/phase1/forkchoice/mock_services"
	gossipmock "github.com/erigontech/erigon/cl/phase1/network/gossip/mock_services"
	"github.com/erigontech/erigon/common"
)

func newPendingEnvelopeTestService(t *testing.T) (*executionPayloadService, *mock_services.ForkChoiceStorageMock) {
	t.Helper()
	cache, err := lru.New[seenEnvelopeKey, struct{}]("seen_envelopes", seenEnvelopeCacheSize)
	require.NoError(t, err)
	fcu := mock_services.NewForkChoiceStorageMock(t)
	service := &executionPayloadService{ctx: t.Context(), forkchoiceStore: fcu, beaconCfg: &clparams.MainnetBeaconConfig, emitters: beaconevents.NewEventEmitter(), seenEnvelopesCache: cache}
	service.pending = service.newPendingQueue(t.Context())
	service.pending.stopAndWait()
	return service, fcu
}

func TestExecutionPayloadServicePendingRetriesLocalFailure(t *testing.T) {
	for _, localErr := range []error{errors.New("state read temporarily unavailable"), context.Canceled, context.DeadlineExceeded} {
		t.Run(localErr.Error(), func(t *testing.T) {
			service, fcu := newPendingEnvelopeTestService(t)
			events := make(chan *beaconevents.EventStream, 3)
			subscription := service.emitters.Operation().Subscribe(events)
			defer subscription.Unsubscribe()
			root := common.Hash{1}
			envelope := newTestSignedEnvelope(100, root, 1)
			var calls atomic.Int32
			fcu.OnExecutionPayloadFn = func(_ context.Context, got *cltypes.SignedExecutionPayloadEnvelope, checkData, validate bool) error {
				require.Same(t, envelope, got)
				require.True(t, checkData)
				require.True(t, validate)
				call := calls.Add(1)
				if call <= 2 {
					return localErr
				}
				return nil
			}
			require.ErrorIs(t, service.ProcessMessage(t.Context(), nil, envelope), ErrIgnore)
			service.pending.processPending(t.Context())
			require.Zero(t, calls.Load())
			fcu.Blocks[root] = newTestGloasBlock(100, 1)
			ownedBytes := uint64(envelope.EncodingSizeSSZ())
			for attempt := int32(1); attempt <= 2; attempt++ {
				service.pending.processPending(t.Context())
				require.Equal(t, attempt, calls.Load())
				require.Equal(t, int32(1), service.pending.count.Load())
				require.Equal(t, ownedBytes, service.pendingBytes.Load())
			}
			service.pending.processPending(t.Context())
			require.Equal(t, int32(3), calls.Load())
			require.Len(t, events, 1)
			require.Equal(t, beaconevents.OpExecutionPayloadGossip, (<-events).Event)
			require.Zero(t, service.pending.count.Load())
			require.Zero(t, service.pendingBytes.Load())
			service.pending.processPending(t.Context())
			require.Equal(t, int32(3), calls.Load())
		})
	}
}

func TestExecutionPayloadServicePendingParksEnvelopeOnlyWhenGossipStateIsUnavailable(t *testing.T) {
	for _, tc := range []struct {
		name       string
		validation error
		parked     bool
	}{
		{name: "state unavailable", validation: forkchoice.ErrExecutionPayloadEnvelopeStateUnavailable, parked: true},
		{name: "replay failed", validation: fmt.Errorf("%w: state replay failed", forkchoice.ErrIgnore)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			service, fcu := newPendingEnvelopeTestService(t)
			root := common.Hash{1}
			envelope := newTestSignedEnvelope(100, root, 1)
			require.ErrorIs(t, service.ProcessMessage(t.Context(), nil, envelope), ErrIgnore)
			fcu.Blocks[root] = newTestGloasBlock(100, 1)
			var validations atomic.Int32
			fcu.ValidateExecutionPayloadEnvelopeForGossipFunc = func(*cltypes.SignedExecutionPayloadEnvelope) error {
				validations.Add(1)
				return tc.validation
			}
			var imports atomic.Int32
			fcu.OnExecutionPayloadFn = func(context.Context, *cltypes.SignedExecutionPayloadEnvelope, bool, bool) error {
				imports.Add(1)
				return nil
			}

			service.pending.processPending(t.Context())
			require.Equal(t, int32(1), validations.Load())
			require.Zero(t, imports.Load())
			require.Zero(t, service.pending.count.Load())
			require.Zero(t, service.pendingBytes.Load())
			require.Equal(t, tc.parked, fcu.HasPendingExecutionPayloadEnvelope(root))

			for range 3 {
				service.pending.processPending(t.Context())
			}
			require.Equal(t, int32(1), validations.Load())
			require.Zero(t, imports.Load())
		})
	}
}

func TestExecutionPayloadServicePendingDropsTerminalResults(t *testing.T) {
	for _, tc := range []struct {
		name          string
		importErr     error
		wantSeen      bool
		wantPublished bool
	}{
		{name: "invalid", importErr: fmt.Errorf("%w: invalid signature", forkchoice.ErrInvalidExecutionPayloadEnvelope), wantSeen: true, wantPublished: true},
		{name: "data unavailable", importErr: forkchoice.ErrEIP7594ColumnDataNotAvailable, wantSeen: true, wantPublished: true},
		{name: "persistence failure", importErr: forkchoice.ErrExecutionPayloadEnvelopePersistenceFailed, wantPublished: true},
		{name: "ignore", importErr: fmt.Errorf("%w: state unavailable", forkchoice.ErrIgnore)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			service, fcu := newPendingEnvelopeTestService(t)
			gossipMock := gossipmock.NewMockGossip(gomock.NewController(t))
			service.gossipManager = gossipMock
			root := common.Hash{1}
			envelope := newTestSignedEnvelope(100, root, 1)
			encoded, err := envelope.EncodeSSZ(nil)
			require.NoError(t, err)
			require.ErrorIs(t, service.ProcessMessage(t.Context(), nil, envelope), ErrIgnore)
			fcu.Blocks[root] = newTestGloasBlock(100, 1)
			var calls atomic.Int32
			fcu.OnExecutionPayloadFn = func(context.Context, *cltypes.SignedExecutionPayloadEnvelope, bool, bool) error {
				calls.Add(1)
				return tc.importErr
			}
			if tc.wantPublished {
				gossipMock.EXPECT().PublishBackground(gossip.TopicNameExecutionPayload, encoded, gomock.Any()).Return(nil).Times(1)
			}
			service.pending.processPending(t.Context())
			require.Zero(t, service.pending.count.Load())
			require.Zero(t, service.pendingBytes.Load())
			service.pending.processPending(t.Context())
			require.Equal(t, int32(1), calls.Load())
			if tc.wantSeen {
				_, err := fcu.TryClaimExecutionPayloadEnvelopeForGossip(root, 1)
				require.ErrorIs(t, err, forkchoice.ErrExecutionPayloadEnvelopeAlreadySeen)
				require.True(t, service.seenEnvelopesCache.Contains(seenEnvelopeKey{root, 1}))
			} else {
				token, err := fcu.TryClaimExecutionPayloadEnvelopeForGossip(root, 1)
				require.NoError(t, err)
				fcu.FinishExecutionPayloadEnvelopeForGossip(token, false)
				require.False(t, service.seenEnvelopesCache.Contains(seenEnvelopeKey{root, 1}))
			}
		})
	}
}

func TestExecutionPayloadServicePendingRetryPreservesExpiry(t *testing.T) {
	service, fcu := newPendingEnvelopeTestService(t)
	root := common.Hash{1}
	envelope := newTestSignedEnvelope(100, root, 1)
	require.ErrorIs(t, service.ProcessMessage(t.Context(), nil, envelope), ErrIgnore)
	hash, err := envelope.HashSSZ()
	require.NoError(t, err)
	key := pendingEnvelopeKey{root, hash}
	stored, ok := service.pending.jobs.Load(key)
	require.True(t, ok)
	job := stored.(*pendingJob[*pendingEnvelopeJob])
	created := job.creationTime
	fcu.Blocks[root] = newTestGloasBlock(100, 1)
	calls := 0
	fcu.OnExecutionPayloadFn = func(context.Context, *cltypes.SignedExecutionPayloadEnvelope, bool, bool) error {
		calls++
		return errors.New("disk unavailable")
	}
	for range 2 {
		service.pending.processPending(t.Context())
		queued, err := service.queuePendingEnvelope(root, envelope, time.Now(), true)
		require.NoError(t, err)
		require.False(t, queued)
		require.Equal(t, created, job.creationTime)
	}
	job.creationTime = time.Now().Add(-pendingEnvelopeExpiry - time.Second)
	service.pending.processPending(t.Context())
	require.Equal(t, 2, calls)
	require.Zero(t, service.pending.count.Load())
	require.Zero(t, service.pendingBytes.Load())
}

func TestExecutionPayloadServicePendingAdmissionBusyPreservesExpiry(t *testing.T) {
	service, fcu := newPendingEnvelopeTestService(t)
	root := common.Hash{1}
	envelope := newTestSignedEnvelope(100, root, 1)
	require.ErrorIs(t, service.ProcessMessage(t.Context(), nil, envelope), ErrIgnore)
	hash, err := envelope.HashSSZ()
	require.NoError(t, err)
	key := pendingEnvelopeKey{root, hash}
	stored, ok := service.pending.jobs.Load(key)
	require.True(t, ok)
	job := stored.(*pendingJob[*pendingEnvelopeJob])
	creationTime := job.creationTime
	fcu.Blocks[root] = newTestGloasBlock(100, 1)
	token, err := fcu.TryClaimExecutionPayloadEnvelopeForGossip(root, 1)
	require.NoError(t, err)
	service.pending.processPending(t.Context())
	stored, ok = service.pending.jobs.Load(key)
	require.True(t, ok)
	require.Same(t, job, stored)
	require.Equal(t, creationTime, job.creationTime)
	require.Equal(t, int32(1), service.pending.count.Load())
	require.Equal(t, uint64(envelope.EncodingSizeSSZ()), service.pendingBytes.Load())
	_, _, available := fcu.TryBeginPayloadPreparation(t.Context(), 100, 100)
	require.False(t, available)

	job.creationTime = time.Now().Add(-pendingEnvelopeExpiry - time.Second)
	service.pending.processPending(t.Context())
	require.Zero(t, service.pending.count.Load())
	require.Zero(t, service.pendingBytes.Load())
	fcu.FinishExecutionPayloadEnvelopeForGossip(token, false)
	_, finish, available := fcu.TryBeginPayloadPreparation(t.Context(), 100, 100)
	require.True(t, available)
	finish()
}

func TestExecutionPayloadServicePendingSerializesRetries(t *testing.T) {
	service, fcu := newPendingEnvelopeTestService(t)
	root := common.Hash{1}
	envelope := newTestSignedEnvelope(100, root, 1)
	require.ErrorIs(t, service.ProcessMessage(t.Context(), nil, envelope), ErrIgnore)
	fcu.Blocks[root] = newTestGloasBlock(100, 1)
	entered, resume, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
	release := sync.OnceFunc(func() { close(resume) })
	defer release()
	var calls atomic.Int32
	fcu.OnExecutionPayloadFn = func(context.Context, *cltypes.SignedExecutionPayloadEnvelope, bool, bool) error {
		if calls.Add(1) == 1 {
			close(entered)
			<-resume
			return errors.New("disk unavailable")
		}
		return nil
	}
	go func() { service.pending.processPending(t.Context()); close(done) }()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("pending envelope was not processed")
	}
	var wg sync.WaitGroup
	for range 16 {
		wg.Go(func() { service.pending.processPending(t.Context()) })
	}
	wg.Wait()
	require.Equal(t, int32(1), calls.Load())
	queued, err := service.queuePendingEnvelope(root, envelope, time.Now(), true)
	require.NoError(t, err)
	require.False(t, queued)
	require.Equal(t, int32(1), service.pending.count.Load())
	require.Equal(t, uint64(envelope.EncodingSizeSSZ()), service.pendingBytes.Load())
	release()
	receiveWithTimeout(t, done, "pending retry did not finish")
	require.Equal(t, int32(1), service.pending.count.Load())
	require.Equal(t, uint64(envelope.EncodingSizeSSZ()), service.pendingBytes.Load())
	service.pending.processPending(t.Context())
	require.Equal(t, int32(2), calls.Load())
	require.Zero(t, service.pending.count.Load())
	require.Zero(t, service.pendingBytes.Load())
}

func TestExecutionPayloadServiceExpiredAttemptCannotRemoveReplacement(t *testing.T) {
	for _, oldErr := range []error{nil, errors.New("disk unavailable")} {
		t.Run(fmt.Sprint(oldErr), func(t *testing.T) {
			service, fcu := newPendingEnvelopeTestService(t)
			root := common.Hash{1}
			envelope := newTestSignedEnvelope(100, root, 1)
			require.ErrorIs(t, service.ProcessMessage(t.Context(), nil, envelope), ErrIgnore)
			hash, err := envelope.HashSSZ()
			require.NoError(t, err)
			key := pendingEnvelopeKey{root, hash}
			stored, ok := service.pending.jobs.Load(key)
			require.True(t, ok)
			job := stored.(*pendingJob[*pendingEnvelopeJob])
			fcu.Blocks[root] = newTestGloasBlock(100, 1)
			entered, resume, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
			release := sync.OnceFunc(func() { close(resume) })
			defer release()
			var calls atomic.Int32
			fcu.OnExecutionPayloadFn = func(context.Context, *cltypes.SignedExecutionPayloadEnvelope, bool, bool) error {
				if calls.Add(1) == 1 {
					close(entered)
					<-resume
					return oldErr
				}
				return nil
			}
			go func() { service.pending.processPending(t.Context()); close(done) }()
			select {
			case <-entered:
			case <-time.After(5 * time.Second):
				t.Fatal("pending envelope was not processed")
			}
			job.creationTime = time.Now().Add(-pendingEnvelopeExpiry - time.Second)
			service.pending.processPending(t.Context())
			require.Zero(t, service.pending.count.Load())
			require.Zero(t, service.pendingBytes.Load())
			queued, err := service.queuePendingEnvelope(root, envelope, time.Now(), true)
			require.NoError(t, err)
			require.True(t, queued)
			release()
			receiveWithTimeout(t, done, "expired pending attempt did not finish")
			require.Equal(t, int32(1), service.pending.count.Load())
			require.Equal(t, uint64(envelope.EncodingSizeSSZ()), service.pendingBytes.Load())
			service.pending.processPending(t.Context())
			require.Zero(t, service.pending.count.Load())
			require.Zero(t, service.pendingBytes.Load())
		})
	}
}

type disappearingEnvelopeBlockStore struct {
	forkchoice.ForkChoiceStorage
	reads int
}

func (s *disappearingEnvelopeBlockStore) GetBlock(root common.Hash) (*cltypes.SignedBeaconBlock, bool) {
	s.reads++
	if s.reads == 2 {
		return nil, false
	}
	return s.ForkChoiceStorage.GetBlock(root)
}

func TestExecutionPayloadServicePendingRetainsDisappearingBlock(t *testing.T) {
	service, fcu := newPendingEnvelopeTestService(t)
	root := common.Hash{1}
	envelope := newTestSignedEnvelope(100, root, 1)
	require.ErrorIs(t, service.ProcessMessage(t.Context(), nil, envelope), ErrIgnore)
	fcu.Blocks[root] = newTestGloasBlock(100, 1)
	service.forkchoiceStore = &disappearingEnvelopeBlockStore{ForkChoiceStorage: fcu}
	var calls atomic.Int32
	imported := make(chan struct{})
	fcu.OnExecutionPayloadFn = func(context.Context, *cltypes.SignedExecutionPayloadEnvelope, bool, bool) error {
		calls.Add(1)
		close(imported)
		return nil
	}
	service.pending.processPending(t.Context())
	require.Zero(t, calls.Load())
	require.Equal(t, int32(1), service.pending.count.Load())
	require.Equal(t, uint64(envelope.EncodingSizeSSZ()), service.pendingBytes.Load())
	service.pending.processPending(t.Context())
	receiveWithTimeout(t, imported, "pending envelope was not imported")
	require.Equal(t, int32(1), calls.Load())
	require.Zero(t, service.pending.count.Load())
	require.Zero(t, service.pendingBytes.Load())
}
