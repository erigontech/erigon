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

package v4

import (
	"bytes"
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/execution/commitment"
)

func TestOrderedStorageTasks(t *testing.T) {
	tasks := []storageTask{
		{entries: make([]storageEntry, 1)},
		{entries: make([]storageEntry, 4)},
		{entries: make([]storageEntry, 2)},
	}

	require.Equal(t, []int{1, 4, 2}, storageTaskLengths(orderedStorageTasks(tasks, orderSequential)))
	require.Equal(t, []int{4, 2, 1}, storageTaskLengths(orderedStorageTasks(tasks, orderLongestFirst)))
	require.Equal(t, []int{2, 4, 1}, storageTaskLengths(orderedStorageTasks(tasks, orderReversed)))
	require.Equal(t, []int{1, 4, 2}, storageTaskLengths(tasks))
}

func TestSharedScheduleBoundAcrossPhases(t *testing.T) {
	stats := new(scheduleStats)
	schedule := newWorkerSchedule(2, stats)
	release := make(chan struct{})
	entered := make(chan struct{}, 8)
	errors := make(chan error, 8)
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			errors <- schedule.run(context.Background(), func() error {
				entered <- struct{}{}
				<-release
				return nil
			})
		})
	}
	<-entered
	<-entered
	close(release)
	wg.Wait()
	for range 8 {
		require.NoError(t, <-errors)
	}

	require.Equal(t, int64(0), stats.inFlight.Load())
	require.Equal(t, int64(2), stats.max.Load())
}

func TestSchedulePoliciesPreserveRoot(t *testing.T) {
	entries := make([]parityUpdate, 0, 48)
	for i := range 16 {
		address := parityAddress(i)
		entries = append(entries, parityUpdate{key: address, update: accountParityUpdate(i)}, parityUpdate{
			key:    append(append([]byte(nil), address...), paritySlot(i)...),
			update: storageParityUpdate(i),
		})
	}

	roots := make([][]byte, 0, 3)
	for _, order := range []scheduleOrder{orderSequential, orderReversed, orderLongestFirst} {
		ctx := newParityContext()
		trie := &Trie{scheduleOrder: order, scheduleWorkers: 2}
		trie.ResetContext(ctx)
		root, err := trie.Process(context.Background(), makeParityUpdates(t, commitment.ModeUpdate, entries), "", nil, commitment.WarmupConfig{})
		require.NoError(t, err)
		roots = append(roots, root)
		trie.Release()
	}
	require.True(t, bytes.Equal(roots[0], roots[1]))
	require.True(t, bytes.Equal(roots[0], roots[2]))
}

func TestScheduleWorkerBoundDuringTrieProcess(t *testing.T) {
	entries := make([]parityUpdate, 0, 96)
	for i := range 32 {
		address := parityAddress(i)
		entries = append(entries, parityUpdate{key: address, update: accountParityUpdate(i)}, parityUpdate{
			key:    append(append([]byte(nil), address...), paritySlot(i)...),
			update: storageParityUpdate(i),
		})
	}
	stats := new(scheduleStats)
	trie := &Trie{scheduleOrder: orderLongestFirst, scheduleWorkers: 2, scheduleStats: stats}
	trie.ResetContext(newParityContext())
	_, err := trie.Process(context.Background(), makeParityUpdates(t, commitment.ModeUpdate, entries), "", nil, commitment.WarmupConfig{})
	require.NoError(t, err)
	require.LessOrEqual(t, stats.max.Load(), int64(2))
	require.Equal(t, int64(0), stats.inFlight.Load())
	trie.Release()
}

func storageTaskLengths(tasks []storageTask) []int {
	lengths := make([]int, len(tasks))
	for i := range tasks {
		lengths[i] = len(tasks[i].entries)
	}
	return lengths
}
