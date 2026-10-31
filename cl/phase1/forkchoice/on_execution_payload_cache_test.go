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

	"github.com/erigontech/erigon/cl/cltypes/solid"
)

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
