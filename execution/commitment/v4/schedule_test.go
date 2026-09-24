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
	"encoding/binary"
	"errors"
	"maps"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/db/kv"
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

	err := runStoragePhase(context.Background(), newMockContext(), factory, storage, roots, make([]deltaParts, len(storage)), 4, 0, newStorageGate(storage))
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
	value, ok, _ := accountLeafAt(root, path)
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
		tasks := []storageTask{task}
		require.NoError(t, runStoragePhase(context.Background(), newMockContext(), factory, tasks, roots, make([]deltaParts, 1), 2, fanOutMin, newStorageGate(tasks)))
		return calls.Load()
	}
	require.Equal(t, int32(1), factoryCalls(1<<20))
	require.Greater(t, factoryCalls(16), int32(1))
}

func TestPipelinedAccountGroupsMatchSerial(t *testing.T) {
	var sparse, dense []int
	for i := 0; len(dense) < 1500 || len(sparse) < 2; i++ {
		h := commitment.KeyToHexNibbleHash(parityAddress(i))
		switch {
		case h[0] != 0:
			if len(dense) < 1500 {
				dense = append(dense, i)
			}
		case len(sparse) == 0:
			sparse = append(sparse, i)
		case len(sparse) == 1 && h[1] != commitment.KeyToHexNibbleHash(parityAddress(sparse[0]))[1]:
			sparse = append(sparse, i)
		}
	}
	deleted := &commitment.Update{Flags: commitment.DeleteUpdate}
	seed := make([]parityUpdate, 0, 2*len(dense))
	for _, i := range append(append([]int(nil), dense...), sparse...) {
		address := parityAddress(i)
		seed = append(seed, parityUpdate{key: address, update: accountParityUpdate(i)})
		if i%3 == 0 {
			seed = append(seed, parityUpdate{key: append(bytes.Clone(address), paritySlot(i)...), update: storageParityUpdate(i)})
		}
	}
	base := newParityContext()
	seeder := &Trie{scheduleWorkers: 1}
	seeder.ResetContext(base)
	_, err := seeder.Process(context.Background(), makeParityUpdates(t, commitment.ModeCollect, seed), "", nil, commitment.WarmupConfig{})
	require.NoError(t, err)

	next := []parityUpdate{{key: parityAddress(sparse[0]), update: deleted}, {key: parityAddress(dense[3]), update: deleted}}
	for k, i := range dense[10:400] {
		address := parityAddress(i)
		next = append(next, parityUpdate{key: address, update: accountParityUpdate(i + 1)})
		if k%4 == 0 {
			next = append(next, parityUpdate{key: append(bytes.Clone(address), paritySlot(i+7)...), update: storageParityUpdate(i + 2)})
		}
	}
	for i := 100000; i < 100200; i++ {
		if commitment.KeyToHexNibbleHash(parityAddress(i))[0] != 0 {
			next = append(next, parityUpdate{key: parityAddress(i), update: accountParityUpdate(i)})
		}
	}
	run := func(workers int) ([]byte, map[string][]byte) {
		ctx := newParityContext()
		maps.Copy(ctx.branches, base.branches)
		trie := &Trie{scheduleWorkers: workers}
		trie.ResetContext(ctx)
		if workers > 1 {
			trie.SetTrieContextFactory(ctx.factory)
		}
		root, err := trie.Process(context.Background(), makeParityUpdates(t, commitment.ModeCollect, next), "", nil, commitment.WarmupConfig{})
		require.NoError(t, err)
		return root, ctx.branches
	}
	serialRoot, serialBranches := run(1)
	parallelRoot, parallelBranches := run(4)
	require.Equal(t, serialRoot, parallelRoot)
	require.Equal(t, serialBranches, parallelBranches)
}

func TestStorageGateReleasesEachNibbleWhenItsTasksFinish(t *testing.T) {
	storage := []storageTask{{addrHash: [32]byte{0x01}}, {addrHash: [32]byte{0x0f}}, {addrHash: [32]byte{0xf0}}}
	gate := newStorageGate(storage)
	released := func(nib int) bool {
		select {
		case <-gate.ready[nib]:
			return true
		default:
			return false
		}
	}
	require.True(t, released(5))
	require.False(t, released(0))
	gate.done(&storage[0])
	require.False(t, released(0))
	gate.done(&storage[2])
	require.True(t, released(15))
	require.False(t, released(0))
	gate.done(&storage[1])
	require.True(t, released(0))
}

func TestRunStoragePhaseReleasesEveryTask(t *testing.T) {
	storage := make([]storageTask, 64)
	for i := range storage {
		storage[i].wipe = true
		storage[i].addrHash[0] = byte(i * 4)
	}
	factory := func(context.Context) (commitment.PatriciaContext, func()) { return newMockContext(), nil }
	for _, workers := range []int{1, 4} {
		gate := newStorageGate(storage)
		require.NoError(t, runStoragePhase(context.Background(), newMockContext(), factory, storage, make([][32]byte, len(storage)), make([]deltaParts, len(storage)), workers, 0, gate))
		for nib := range gate.ready {
			select {
			case <-gate.ready[nib]:
			default:
				t.Fatalf("workers=%d: nibble %d not released", workers, nib)
			}
		}
	}
}

type chainGatedContext struct {
	*parityContext
	known     map[string][]byte
	blocked   [32]byte
	chainRead chan struct{}
	once      sync.Once
}

func (c *chainGatedContext) Branch(key []byte) ([]byte, kv.Step, error) {
	switch {
	case key[0] == tagStorageNode && bytes.Equal(key[1:33], c.blocked[:]):
		select {
		case <-c.chainRead:
		case <-time.After(5 * time.Second):
			return nil, 0, errors.New("storage task waited for another nibble's account chain")
		}
	case key[0] == tagAccountNode:
		if _, ok := c.known[string(key)]; !ok {
			c.once.Do(func() { close(c.chainRead) })
		}
	}
	return c.parityContext.Branch(key)
}

func (c *chainGatedContext) factory(context.Context) (commitment.PatriciaContext, func()) {
	return c, nil
}

func TestAccountChainDoesNotWaitForOtherNibblesStorage(t *testing.T) {
	address := func(i int) []byte {
		a := make([]byte, 20)
		binary.BigEndian.PutUint64(a[12:], uint64(i))
		return a
	}
	var zero []int
	contract := -1
	for i := 0; len(zero) < 400 || contract < 0; i++ {
		switch commitment.KeyToHexNibbleHash(address(i))[0] {
		case 0:
			if len(zero) < 400 {
				zero = append(zero, i)
			}
		case 15:
			if contract < 0 {
				contract = i
			}
		}
	}
	seed := []parityUpdate{{key: address(contract), update: accountParityUpdate(contract)}}
	for k := range 8 {
		seed = append(seed, parityUpdate{key: append(address(contract), paritySlot(k)...), update: storageParityUpdate(k)})
	}
	for _, i := range zero[:300] {
		seed = append(seed, parityUpdate{key: address(i), update: accountParityUpdate(i)})
	}
	base := newParityContext()
	seeder := &Trie{scheduleWorkers: 1}
	seeder.ResetContext(base)
	_, err := seeder.Process(context.Background(), makeParityUpdates(t, commitment.ModeCollect, seed), "", nil, commitment.WarmupConfig{})
	require.NoError(t, err)

	next := []parityUpdate{{key: append(address(contract), paritySlot(100)...), update: storageParityUpdate(100)}}
	for _, i := range zero[300:] {
		next = append(next, parityUpdate{key: address(i), update: accountParityUpdate(i)})
	}
	ctx := &chainGatedContext{parityContext: newParityContext(), known: base.branches, chainRead: make(chan struct{})}
	maps.Copy(ctx.branches, base.branches)
	ctx.blocked = hashAddressPath(commitment.KeyToHexNibbleHash(address(contract))[:64])
	trie := &Trie{scheduleWorkers: 4}
	trie.ResetContext(ctx)
	trie.SetTrieContextFactory(ctx.factory)
	_, err = trie.Process(context.Background(), makeParityUpdates(t, commitment.ModeCollect, next), "", nil, commitment.WarmupConfig{})
	require.NoError(t, err)
}
