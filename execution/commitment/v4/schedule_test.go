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
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/execution/commitment"
)

func TestScheduleStatsTrackPeakInFlight(t *testing.T) {
	stats := new(scheduleStats)
	release := make(chan struct{})
	var wg sync.WaitGroup
	for range 4 {
		wg.Go(func() {
			stats.enter()
			<-release
			stats.leave()
		})
	}
	require.Eventually(t, func() bool { return stats.inFlight.Load() == 4 }, time.Second, time.Millisecond)
	close(release)
	wg.Wait()

	require.Equal(t, int64(0), stats.inFlight.Load())
	require.Equal(t, int64(4), stats.max.Load())
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
	trie := &Trie{scheduleWorkers: 2, scheduleStats: stats}
	trie.ResetContext(newParityContext())
	_, err := trie.Process(context.Background(), makeParityUpdates(t, commitment.ModeCollect, entries), "", nil, commitment.WarmupConfig{})
	require.NoError(t, err)
	require.LessOrEqual(t, stats.max.Load(), int64(2))
	require.Equal(t, int64(0), stats.inFlight.Load())
	trie.Release()
}

func TestRunStoragePhaseUsesConfiguredWorkers(t *testing.T) {
	storage := make([]storageTask, 64)
	for i := range storage {
		storage[i].wipe = true
		storage[i].addrHash[0] = byte(i)
	}
	roots := make([][32]byte, len(storage))
	var factoryCalls atomic.Int32
	factory := func(context.Context) (commitment.PatriciaContext, func()) {
		factoryCalls.Add(1)
		return newMockContext(), nil
	}

	err := runStoragePhase(context.Background(), newMockContext(), factory, storage, roots, make([]deltaParts, len(storage)), 4, new(scheduleStats))
	require.NoError(t, err)
	require.Equal(t, int32(4), factoryCalls.Load())
}

func TestScheduleSerialAndParallelRootsAgreeForManyContracts(t *testing.T) {
	entries := make([]parityUpdate, 0, 64)
	for i := range 32 {
		address := parityAddress(i)
		entries = append(entries,
			parityUpdate{key: address, update: accountParityUpdate(i)},
			parityUpdate{
				key:    append(append([]byte(nil), address...), paritySlot(i)...),
				update: storageParityUpdate(i),
			},
		)
	}

	serial := &Trie{scheduleWorkers: 1}
	serialCtx := newParityContext()
	serial.ResetContext(serialCtx)
	serialRoot, err := serial.Process(context.Background(), makeParityUpdates(t, commitment.ModeCollect, entries), "", nil, commitment.WarmupConfig{})
	require.NoError(t, err)
	serial.Release()

	parallel := &Trie{scheduleWorkers: 4}
	parallelCtx := newParityContext()
	parallel.ResetContext(parallelCtx)
	parallel.SetTrieContextFactory(parallelCtx.factory)
	parallelRoot, err := parallel.Process(context.Background(), makeParityUpdates(t, commitment.ModeCollect, entries), "", nil, commitment.WarmupConfig{})
	require.NoError(t, err)
	parallel.Release()

	require.Equal(t, serialRoot, parallelRoot)
}

func TestScheduledFieldUpdatePreservesStorageRoot(t *testing.T) {
	address := parityAddress(3)
	ctx := newParityContext()
	trie := &Trie{scheduleWorkers: 1}
	trie.ResetContext(ctx)
	initial := []parityUpdate{
		{key: address, update: accountParityUpdate(3)},
		{key: append(append([]byte(nil), address...), paritySlot(3)...), update: storageParityUpdate(3)},
	}
	_, err := trie.Process(context.Background(), makeParityUpdates(t, commitment.ModeCollect, initial), "", nil, commitment.WarmupConfig{})
	require.NoError(t, err)
	path := commitment.KeyToHexNibbleHash(address)
	_, _, _, before, err := decodeAccountLeaf(accountLeafFromParityContext(t, ctx, path))
	require.NoError(t, err)

	partial := &commitment.Update{Flags: commitment.BalanceUpdate, Balance: *uint256.NewInt(99)}
	_, err = trie.Process(context.Background(), makeParityUpdates(t, commitment.ModeCollect, []parityUpdate{{key: address, update: partial}}), "", nil, commitment.WarmupConfig{})
	require.NoError(t, err)
	_, _, _, after, err := decodeAccountLeaf(accountLeafFromParityContext(t, ctx, path))
	require.NoError(t, err)
	require.Equal(t, before, after)
}

func storageTaskLengths(tasks []storageTask) []int {
	lengths := make([]int, len(tasks))
	for i := range tasks {
		lengths[i] = len(tasks[i].entries)
	}
	return lengths
}

func accountLeafFromParityContext(t *testing.T, ctx *parityContext, path []byte) []byte {
	t.Helper()
	root, err := unfold(ctx, nil, planeAccount, nil)
	require.NoError(t, err)
	value, ok := accountLeafAt(root, path)
	require.True(t, ok)
	return value
}
