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
	"sync/atomic"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/execution/commitment"
)

func TestScheduleWorkerBoundDuringTrieProcess(t *testing.T) {
	first := commitment.KeyToHexNibbleHash(parityAddress(0))[0]
	entries := make([]parityUpdate, 0, 16)
	for i := 0; i < 256 && len(entries) < 16; i++ {
		address := parityAddress(i)
		if commitment.KeyToHexNibbleHash(address)[0] != first {
			continue
		}
		entries = append(entries, parityUpdate{key: address, update: accountParityUpdate(i)}, parityUpdate{
			key:    append(append([]byte(nil), address...), paritySlot(i)...),
			update: storageParityUpdate(i),
		})
	}
	require.Len(t, entries, 16)
	ctx := newParityContext()
	var factoryCalls atomic.Int32
	trie := &Trie{scheduleWorkers: 2}
	trie.ResetContext(ctx)
	trie.SetTrieContextFactory(func(c context.Context) (commitment.PatriciaContext, func()) {
		factoryCalls.Add(1)
		return ctx.factory(c)
	})
	_, err := trie.Process(context.Background(), makeParityUpdates(t, commitment.ModeCollect, entries), "", nil, commitment.WarmupConfig{})
	require.NoError(t, err)
	require.Equal(t, int32(2), factoryCalls.Load())
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

	err := runStoragePhase(context.Background(), newMockContext(), factory, storage, roots, make([]deltaParts, len(storage)), 4, 0)
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

func TestRunStoragePhaseHonorsFanOutMin(t *testing.T) {
	factoryCalls := func(fanOutMin int) int32 {
		task := storageTask{addrHash: [32]byte{1}}
		for i := range 64 {
			path := make([]byte, 64)
			path[0], path[1] = byte(i%16), byte(i/16)
			task.entries = append(task.entries, storageEntry{path: path, value: []byte{byte(i + 1)}, op: storagePut})
		}
		var calls atomic.Int32
		factory := func(context.Context) (commitment.PatriciaContext, func()) {
			calls.Add(1)
			return newMockContext(), nil
		}
		roots := make([][32]byte, 1)
		require.NoError(t, runStoragePhase(context.Background(), newMockContext(), factory, []storageTask{task}, roots, make([]deltaParts, 1), 2, fanOutMin))
		return calls.Load()
	}
	require.Equal(t, int32(1), factoryCalls(1<<20))
	require.Greater(t, factoryCalls(16), int32(1))
}
