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

package forkchoice

import (
	"fmt"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/phase1/execution_client"
	"github.com/erigontech/erigon/common"
)

func TestRejectedEnvelopeRedeliverySkipsValidation(t *testing.T) {
	for _, mode := range []struct {
		name            string
		withEngine      bool
		validatePayload bool
	}{
		{name: "engine validation", withEngine: true, validatePayload: true},
		{name: "deferred validation", withEngine: true},
		{name: "without engine", validatePayload: true},
	} {
		t.Run(mode.name, func(t *testing.T) {
			cfg, blockState, block, envelope := validBlobEnvelopeFixture(t)
			block.Block.Body.SignedExecutionPayloadBid.Message.BlobKzgCommitments.Append(&cltypes.KZGCommitment{1})
			rebindAdmissionBlockRoot(t, blockState, block, envelope)
			resignAdmissionEnvelope(t, cfg, blockState, envelope)
			store, _ := newEnvelopeApplicationTestStore(t, cfg, blockState, block)
			if mode.withEngine {
				store.engine = execution_client.NewMockExecutionEngine(gomock.NewController(t))
			}
			store.RejectEnvelope(envelope.Message.BeaconBlockRoot)
			encoded, err := envelope.EncodeSSZ(nil)
			require.NoError(t, err)
			redelivered := &cltypes.SignedExecutionPayloadEnvelope{Message: cltypes.NewExecutionPayloadEnvelope(cfg)}
			require.NoError(t, redelivered.DecodeSSZ(encoded, int(clparams.GloasVersion)))
			verifyCalls := stubBLSVerify(t)

			err = store.OnExecutionPayload(t.Context(), redelivered, false, mode.validatePayload)

			require.ErrorIs(t, err, ErrInvalidExecutionPayloadEnvelope)
			require.ErrorContains(t, err, "envelope previously rejected")
			require.Zero(t, *verifyCalls)
			require.False(t, progressiveRootCached(t, redelivered.Message.Payload.Transactions))
			require.False(t, progressiveRootCached(t, redelivered.Message.Payload.BlockAccessList))
		})
	}
}

func TestRejectEnvelopePreservesHeadCacheOnRepeat(t *testing.T) {
	cfg, blockState, block, envelope := validAdmissionCancellationFixture(t)
	store, _ := newEnvelopeApplicationTestStore(t, cfg, blockState, block)
	root := envelope.Message.BeaconBlockRoot
	store.headHash = root
	store.headPayloadStatus = cltypes.PayloadStatusFull
	store.selectedHead.Store(&ForkChoiceNode{Root: root, PayloadStatus: cltypes.PayloadStatusFull})

	store.RejectEnvelope(root)

	require.True(t, store.IsEnvelopeRejected(root))
	require.Zero(t, store.headHash)
	require.Equal(t, cltypes.PayloadStatusPending, store.headPayloadStatus)
	require.Nil(t, store.selectedHead.Load())
	head := ForkChoiceNode{Root: common.Hash{1}, PayloadStatus: cltypes.PayloadStatusEmpty}
	store.headHash = head.Root
	store.headPayloadStatus = head.PayloadStatus
	store.selectedHead.Store(&head)

	store.RejectEnvelope(root)

	require.Equal(t, head.Root, store.headHash)
	require.Equal(t, head.PayloadStatus, store.headPayloadStatus)
	require.Same(t, &head, store.selectedHead.Load())
}

func TestApplyEnvelopeWarmsPayloadRootCachesBeforeLock(t *testing.T) {
	for _, validatePayload := range []bool{true, false} {
		t.Run(fmt.Sprintf("validatePayload=%t", validatePayload), func(t *testing.T) {
			cfg, blockState, block, envelope := validAdmissionCancellationFixture(t)
			store, _ := newEnvelopeApplicationTestStore(t, cfg, blockState, block)
			envelope.Message.Payload.Transactions = solid.NewProgressiveTransactionsSSZFromTransactions([][]byte{{1, 2, 3}})
			require.NoError(t, envelope.Message.Payload.BlockAccessList.SetBytes([]byte{4, 5, 6}))
			require.False(t, progressiveRootCached(t, envelope.Message.Payload.Transactions))
			require.False(t, progressiveRootCached(t, envelope.Message.Payload.BlockAccessList))

			ctx := t.Context()
			store.mu.Lock()
			done := make(chan struct{})
			go func() {
				_, _ = store.applyEnvelope(ctx, envelope, false, validatePayload, true, retryQueuedEnvelope, time.Now())
				close(done)
			}()

			cachedBeforeLock := assert.Eventually(t, func() bool {
				return progressiveRootCached(t, envelope.Message.Payload.Transactions) &&
					progressiveRootCached(t, envelope.Message.Payload.BlockAccessList)
			}, lockScopeTimeout, time.Millisecond)
			store.mu.Unlock()
			awaitSignal(t, done, "applyEnvelope to finish")
			require.True(t, cachedBeforeLock)
		})
	}
}

func TestApplyEnvelopeSkipsPayloadRootCacheWarmupForStoredEnvelope(t *testing.T) {
	cfg, blockState, block, envelope := validAdmissionCancellationFixture(t)
	store, graph := newEnvelopeApplicationTestStore(t, cfg, blockState, block)
	graph.envelope = envelope
	envelope.Message.Payload.Transactions = solid.NewProgressiveTransactionsSSZFromTransactions([][]byte{{1, 2, 3}})
	require.NoError(t, envelope.Message.Payload.BlockAccessList.SetBytes([]byte{4, 5, 6}))

	applied, err := store.applyEnvelope(t.Context(), envelope, false, true, true, retryQueuedEnvelope, time.Now())
	require.NoError(t, err)
	require.False(t, applied)
	require.False(t, progressiveRootCached(t, envelope.Message.Payload.Transactions))
	require.False(t, progressiveRootCached(t, envelope.Message.Payload.BlockAccessList))
}

func progressiveRootCached(t *testing.T, value any) bool {
	t.Helper()
	field := reflect.ValueOf(value).Elem().FieldByName("progressiveRoot")
	if !assert.True(t, field.IsValid(), "progressiveRoot field is missing") {
		return false
	}
	return !field.IsNil() && (*atomic.Pointer[[32]byte])(field.UnsafePointer()).Load() != nil
}
