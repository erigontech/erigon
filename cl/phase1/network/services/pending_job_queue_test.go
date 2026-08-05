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
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func newTestPendingJobQueue() *pendingJobQueue[int, string] {
	return newPendingJobQueue(pendingJobQueueOptions{
		capacity:      1,
		expiry:        time.Minute,
		checkInterval: time.Millisecond,
	},
		func(context.Context, int, string) (func(), bool) {
			return nil, false
		},
		func(int) {},
	)
}

func TestNewPendingJobQueueRejectsNilTryProcess(t *testing.T) {
	require.Panics(t, func() {
		newPendingJobQueue[int, string](
			pendingJobQueueOptions{
				capacity:      1,
				expiry:        time.Minute,
				checkInterval: time.Millisecond,
			},
			nil,
			func(int) {},
		)
	})
}

func TestNewPendingJobQueueRejectsNilOnExpired(t *testing.T) {
	require.Panics(t, func() {
		newPendingJobQueue[int, string](
			pendingJobQueueOptions{
				capacity:      1,
				expiry:        time.Minute,
				checkInterval: time.Millisecond,
			},
			func(context.Context, int, string) (func(), bool) {
				return nil, false
			},
			nil,
		)
	})
}

func TestPendingJobQueueEnqueueSkipsKeyBuildAtCapacity(t *testing.T) {
	queue := newTestPendingJobQueue()
	queue.count.Store(queue.capacity)

	keyBuilt := false
	err := queue.enqueue("message", func() (int, error) {
		keyBuilt = true
		return 1, nil
	})

	require.NoError(t, err)
	require.False(t, keyBuilt)
	require.Equal(t, queue.capacity, queue.count.Load())
}

func TestPendingJobQueueEnqueueReleasesReservationOnKeyBuildPanic(t *testing.T) {
	queue := newTestPendingJobQueue()

	require.Panics(t, func() {
		_ = queue.enqueue("message", func() (int, error) {
			panic("key build failed")
		})
	})
	require.Zero(t, queue.count.Load())
}

func TestPendingJobQueueAfterRemoveCanEnqueueSameKey(t *testing.T) {
	var queue *pendingJobQueue[int, string]
	afterRemoveCalled := false

	queue = newPendingJobQueue(pendingJobQueueOptions{
		capacity:      1,
		expiry:        time.Minute,
		checkInterval: time.Millisecond,
	},
		func(_ context.Context, key int, _ string) (func(), bool) {
			return func() {
				afterRemoveCalled = true
				_, exists := queue.jobs.Load(key)
				require.False(t, exists)
				require.NoError(t, queue.enqueue("replacement", func() (int, error) {
					return key, nil
				}))
			}, true
		},
		func(int) {},
	)

	require.NoError(t, queue.enqueue("original", func() (int, error) {
		return 1, nil
	}))

	queue.processPending(t.Context())

	require.True(t, afterRemoveCalled)
	require.Equal(t, int32(1), queue.count.Load())
	stored, exists := queue.jobs.Load(1)
	require.True(t, exists)
	require.Equal(t, "replacement", stored.(*pendingJob[string]).msg)
}
