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
	require.False(t, deferredColumnSyncDue(slotStart, slotStart, delay))
	require.False(t, deferredColumnSyncDue(slotStart.Add(1999*time.Millisecond), slotStart, delay))
	require.True(t, deferredColumnSyncDue(slotStart.Add(2*time.Second), slotStart, delay))
	require.True(t, deferredColumnSyncDue(slotStart.Add(time.Minute), slotStart, delay))
}

func TestDeferredColumnSyncQueueGrowsTheBackoffAndKeepsTheRoot(t *testing.T) {
	queue := newDeferredColumnSyncQueue()
	root := common.Hash{1}
	now := time.Unix(1000, 0)
	slot := 12 * time.Second

	require.True(t, queue.ready(root, now))
	queue.start([]common.Hash{root})
	require.False(t, queue.ready(root, now.Add(time.Hour)), "a root in a round is not picked again")

	for attempt := 1; attempt <= deferredColumnSyncMaxBackoffSlots+4; attempt++ {
		queue.failed(root, now, slot)
		wait := slot * time.Duration(min(attempt, deferredColumnSyncMaxBackoffSlots))
		require.False(t, queue.ready(root, now.Add(wait-time.Millisecond)), "attempt %d", attempt)
		require.True(t, queue.ready(root, now.Add(wait)), "attempt %d", attempt)
	}

	queue.postpone(root, now, slot)
	require.False(t, queue.ready(root, now.Add(slot-time.Millisecond)))
	require.True(t, queue.ready(root, now.Add(slot)))

	queue.done(root)
	require.True(t, queue.ready(root, now))
}
