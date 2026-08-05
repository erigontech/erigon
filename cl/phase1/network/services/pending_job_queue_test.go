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
	return newPendingJobQueue(1, time.Minute, time.Millisecond,
		func(context.Context, int, string) (func(), bool) {
			return nil, false
		},
		func(int) {},
	)
}

func TestNewPendingJobQueueRejectsNilTryProcess(t *testing.T) {
	require.Panics(t, func() {
		newPendingJobQueue[int, string](
			1,
			time.Minute,
			time.Millisecond,
			nil,
			func(int) {},
		)
	})
}

func TestNewPendingJobQueueRejectsNilOnExpired(t *testing.T) {
	require.Panics(t, func() {
		newPendingJobQueue[int, string](
			1,
			time.Minute,
			time.Millisecond,
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
