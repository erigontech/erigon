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

package das

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
)

func TestDeferredColumnSyncDue(t *testing.T) {
	slotStart := time.Unix(1000, 0)
	delay := 2 * time.Second
	require.False(t, deferredColumnSyncDue(slotStart, slotStart, slotStart, delay))
	require.False(t, deferredColumnSyncDue(slotStart.Add(1999*time.Millisecond), slotStart, slotStart, delay))
	require.True(t, deferredColumnSyncDue(slotStart.Add(2*time.Second), slotStart, slotStart, delay))
	require.True(t, deferredColumnSyncDue(slotStart.Add(time.Minute), slotStart, slotStart, delay))
}

func TestDeferredColumnSyncQueueGrowsTheBackoffUpToAnEpoch(t *testing.T) {
	const slotsPerEpoch = 8
	queue := newDeferredColumnSyncQueue()
	root := common.Hash{1}
	now := time.Unix(1000, 0)
	slot := 12 * time.Second

	require.True(t, queue.ready(root, now))
	queue.start([]common.Hash{root})
	require.False(t, queue.ready(root, now.Add(time.Hour)), "a root in a round is not picked again")

	for attempt := 1; attempt <= slotsPerEpoch+4; attempt++ {
		queue.failed(root, now, slot, slotsPerEpoch)
		wait := slot * time.Duration(min(attempt, slotsPerEpoch))
		require.False(t, queue.ready(root, now.Add(wait-time.Millisecond)), "attempt %d", attempt)
		require.True(t, queue.ready(root, now.Add(wait)), "attempt %d", attempt)
	}

	queue.done(root)
	require.True(t, queue.ready(root, now))
}

// A round that ends after its root was dropped must not bring the root back.
func TestDeferredColumnSyncQueueFailedAfterDoneLeavesNoEntry(t *testing.T) {
	queue := newDeferredColumnSyncQueue()
	root := common.Hash{1}
	now := time.Unix(1000, 0)
	queue.start([]common.Hash{root})
	queue.done(root)

	queue.failed(root, now, 12*time.Second, 32)

	require.Empty(t, queue.entries)
	require.True(t, queue.ready(root, now))
}

// Gossip gets the grace period from the later of the block's slot start and the moment the
// root was queued: a Gloas root is queued mid-slot, when its columns are still arriving.
func TestDeferredColumnSyncDueCountsTheGraceFromEnqueue(t *testing.T) {
	slotStart := time.Unix(1000, 0)
	queuedAt := slotStart.Add(5 * time.Second)
	delay := 2 * time.Second
	require.False(t, deferredColumnSyncDue(slotStart.Add(3*time.Second), slotStart, queuedAt, delay))
	require.False(t, deferredColumnSyncDue(queuedAt.Add(delay-time.Millisecond), slotStart, queuedAt, delay))
	require.True(t, deferredColumnSyncDue(queuedAt.Add(delay), slotStart, queuedAt, delay))
	// A root queued before its slot started waits from the slot start.
	require.True(t, deferredColumnSyncDue(slotStart.Add(delay), slotStart, slotStart.Add(-time.Second), delay))
}
