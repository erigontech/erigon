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

package pbt

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"slices"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/commitment/eip8297"
)

func TestTrieParallelPhaseAStartsBucketTasksConcurrently(t *testing.T) {
	ctx := newTrieTestContext()
	addressA := bytes.Repeat([]byte{0x51}, 20)
	addressB := bytes.Repeat([]byte{0x61}, 20)
	ops := []Op{
		{Key: eip8297.TreeKeyStorage(addressA, storageSlot(64)), Value: testTrieValue(1)},
		{Key: eip8297.TreeKeyStorage(addressB, storageSlot(64)), Value: testTrieValue(2)},
	}
	sort.Slice(ops, func(i, j int) bool { return bytes.Compare(ops[i].Key, ops[j].Key) < 0 })
	started := make(chan struct{})
	var count atomic.Int32
	trie := NewTrie(ctx)
	trie.SetTrieContextFactory(func(context.Context) (commitment.PatriciaContext, func()) { return ctx, func() {} })
	trie.SetPhaseHook(func(task phaseTask, op *Op) error {
		if task.kind != phaseBucket || op != nil {
			return nil
		}
		if count.Add(1) == 2 {
			close(started)
		}
		select {
		case <-started:
			return nil
		case <-time.After(time.Second):
			return fmt.Errorf("bucket tasks did not start concurrently")
		}
	})
	_, err := trie.ProcessParallelContext(t.Context(), ops, 2)
	require.NoError(t, err)
	require.Equal(t, int32(2), count.Load())
}

func TestTrieParallelPhaseAAttributesEveryOpOnce(t *testing.T) {
	ctx := newTrieTestContext()
	addressA := bytes.Repeat([]byte{0x71}, 20)
	addressB := bytes.Repeat([]byte{0x81}, 20)
	account := eip8297.TreeKeyAccount(addressA, eip8297.BasicDataLeafKey)
	code := eip8297.TreeKeyCodeChunk([32]byte{3}, 0)
	storageA := eip8297.TreeKeyStorage(addressA, storageSlot(64))
	storageB := eip8297.TreeKeyStorage(addressB, storageSlot(64))
	ops := []Op{
		{Key: account, Value: testTrieValue(1)},
		{Key: code, Value: testTrieValue(2)},
		{Key: storageA, Value: testTrieValue(3)},
		{Key: storageB, Value: testTrieValue(4)},
	}
	sort.Slice(ops, func(i, j int) bool { return bytes.Compare(ops[i].Key, ops[j].Key) < 0 })
	counts := make(map[string]int)
	var mu sync.Mutex
	trie := NewTrie(ctx)
	trie.SetTrieContextFactory(func(context.Context) (commitment.PatriciaContext, func()) { return ctx, func() {} })
	trie.SetPhaseHook(func(task phaseTask, op *Op) error {
		if op == nil {
			return nil
		}
		if task.kind == phaseBucket && op.Key[0] != eip8297.StorageZone {
			return fmt.Errorf("non-storage op owned by bucket")
		}
		if task.kind != phaseBucket && op.Key[0] == eip8297.StorageZone {
			return fmt.Errorf("storage op reached phase B")
		}
		mu.Lock()
		counts[string(op.Key)]++
		mu.Unlock()
		return nil
	})
	_, err := trie.ProcessParallelContext(t.Context(), ops, 2)
	require.NoError(t, err)
	for _, op := range ops {
		require.Equal(t, 1, counts[string(op.Key)], "%x", op.Key)
	}
}

func TestTrieParallelParityWorkerCounts(t *testing.T) {
	addressA := bytes.Repeat([]byte{0x91}, 20)
	addressB := bytes.Repeat([]byte{0xa1}, 20)
	initial := []Op{
		{Key: eip8297.TreeKeyAccount(addressA, eip8297.BasicDataLeafKey), Value: testTrieValue(1)},
		{Key: eip8297.TreeKeyStorage(addressA, storageSlot(64)), Value: testTrieValue(2)},
		{Key: eip8297.TreeKeyStorage(addressB, storageSlot(64)), Value: testTrieValue(3)},
	}
	batch := []Op{
		{Key: eip8297.TreeKeyAccount(addressA, eip8297.BasicDataLeafKey), Value: testTrieValue(4)},
		{Key: eip8297.TreeKeyStorage(addressA, storageSlot256()), Value: testTrieValue(5)},
		{Key: eip8297.TreeKeyStorage(addressB, storageSlot(65)), Value: testTrieValue(6)},
	}
	want := []Op{
		{Key: eip8297.TreeKeyAccount(addressA, eip8297.BasicDataLeafKey), Value: testTrieValue(4)},
		{Key: eip8297.TreeKeyStorage(addressA, storageSlot(64)), Value: testTrieValue(2)},
		{Key: eip8297.TreeKeyStorage(addressA, storageSlot256()), Value: testTrieValue(5)},
		{Key: eip8297.TreeKeyStorage(addressB, storageSlot(64)), Value: testTrieValue(3)},
		{Key: eip8297.TreeKeyStorage(addressB, storageSlot(65)), Value: testTrieValue(6)},
	}
	sort.Slice(initial, func(i, j int) bool { return bytes.Compare(initial[i].Key, initial[j].Key) < 0 })
	sort.Slice(batch, func(i, j int) bool { return bytes.Compare(batch[i].Key, batch[j].Key) < 0 })
	sort.Slice(want, func(i, j int) bool { return bytes.Compare(want[i].Key, want[j].Key) < 0 })
	for _, workers := range []int{1, 2, 8} {
		t.Run(fmt.Sprintf("workers-%d", workers), func(t *testing.T) {
			serialContext := newTrieTestContext()
			parallelContext := newTrieTestContext()
			requireProcess(t, serialContext, initial)
			requireProcess(t, parallelContext, initial)
			trie := NewTrie(parallelContext)
			trie.SetTrieContextFactory(func(context.Context) (commitment.PatriciaContext, func()) { return parallelContext, func() {} })
			serialRoot, err := NewTrie(serialContext).Process(batch)
			require.NoError(t, err)
			require.NoError(t, NewTrie(serialContext).Verify())
			parallelRoot, err := trie.ProcessParallel(batch, workers)
			require.NoError(t, err)
			require.Equal(t, serialRoot, parallelRoot)
			require.Equal(t, serialContext.records, parallelContext.records)
			assertPersistedTrie(t, parallelContext, want)
		})
	}
}

func TestBuildPhasePlanOwnsChainsAndBucketDependencies(t *testing.T) {
	addressA := bytes.Repeat([]byte{0x11}, 20)
	addressB := bytes.Repeat([]byte{0x22}, 20)
	keyA := eip8297.TreeKeyStorage(addressA, storageSlot(64))
	for byteValue := byte(0x23); keyA[1]>>4 == eip8297.TreeKeyStorage(bytes.Repeat([]byte{byteValue}, 20), storageSlot(64))[1]>>4; byteValue++ {
		addressB = bytes.Repeat([]byte{byteValue}, 20)
	}
	keyB := eip8297.TreeKeyStorage(addressB, storageSlot(64))
	account := eip8297.TreeKeyAccount(addressA, eip8297.BasicDataLeafKey)
	code := eip8297.TreeKeyCodeChunk([32]byte{1}, 0)
	ops := []Op{
		{Key: account, Value: testTrieValue(1)},
		{Key: code, Value: testTrieValue(2)},
		{Key: keyA, Value: testTrieValue(3)},
		{Key: keyB, Value: testTrieValue(4)},
	}
	sort.Slice(ops, func(i, j int) bool { return bytes.Compare(ops[i].Key, ops[j].Key) < 0 })

	plan, err := buildPhasePlan(ops)
	require.NoError(t, err)
	buckets := make(map[string]int)
	chains := make(map[[2]byte]int)
	for i, task := range plan.tasks {
		switch task.kind {
		case phaseBucket:
			buckets[task.key] = i
		case phaseChain:
			chains[[2]byte{task.zone, task.nibble}] = i
		}
	}
	require.Len(t, buckets, 2)
	require.NotEmpty(t, chains)
	for _, task := range plan.tasks {
		if task.kind != phaseChain {
			continue
		}
		if task.zone == eip8297.StorageZone {
			var want []int
			for key, index := range buckets {
				if []byte(key)[1]>>4 == task.nibble {
					want = append(want, index)
				}
			}
			sort.Ints(want)
			require.Equal(t, want, task.dependencies)
		} else {
			require.Empty(t, task.dependencies)
		}
	}
}

func TestRunPhasePlanWaitsForStorageBuckets(t *testing.T) {
	release := make(chan struct{})
	started := make(chan struct{})
	var bucketDone atomic.Bool
	var chainBeforeBucket atomic.Bool
	plan := phasePlan{tasks: []phaseTask{
		{kind: phaseBucket, key: "bucket"},
		{kind: phaseChain, zone: eip8297.StorageZone, nibble: 1, dependencies: []int{0}},
		{kind: phaseJoin, dependencies: []int{1}},
	}}
	done := make(chan error, 1)
	go func() {
		done <- runPhasePlan(t.Context(), 2, plan, func(task phaseTask) error {
			switch task.kind {
			case phaseBucket:
				close(started)
				<-release
				bucketDone.Store(true)
			case phaseChain:
				if !bucketDone.Load() {
					chainBeforeBucket.Store(true)
				}
			}
			return nil
		})
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("phase A task did not start")
	}
	close(release)
	require.NoError(t, <-done)
	require.True(t, bucketDone.Load())
	require.False(t, chainBeforeBucket.Load())
}

func TestBuiltPlanStorageChainWaitsForItsBucket(t *testing.T) {
	address := bytes.Repeat([]byte{0x41}, 20)
	key := eip8297.TreeKeyStorage(address, storageSlot(64))
	plan, err := buildPhasePlan([]Op{{Key: key, Value: testTrieValue(1)}})
	require.NoError(t, err)
	release := make(chan struct{})
	started := make(chan struct{})
	var bucketDone atomic.Bool
	var chainBeforeBucket atomic.Bool
	done := make(chan error, 1)
	go func() {
		done <- runPhasePlan(t.Context(), 2, plan, func(task phaseTask) error {
			switch task.kind {
			case phaseBucket:
				close(started)
				<-release
				bucketDone.Store(true)
			case phaseChain:
				if !bucketDone.Load() {
					chainBeforeBucket.Store(true)
				}
			}
			return nil
		})
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("phase A task did not start")
	}
	close(release)
	require.NoError(t, <-done)
	require.True(t, bucketDone.Load())
	require.False(t, chainBeforeBucket.Load())
}

func TestTrieParallelBucketTaskReadsRootOnce(t *testing.T) {
	address := bytes.Repeat([]byte{0x42}, 20)
	key := eip8297.TreeKeyStorage(address, storageSlot(64))
	bucketKey, err := bucketKeyForStorage(key)
	require.NoError(t, err)
	ctx := newTrieTestContext()
	_, err = NewTrie(ctx).ProcessParallel([]Op{{Key: key, Value: testTrieValue(1)}}, 2)
	require.NoError(t, err)
	reads := 0
	for _, read := range ctx.reads {
		if bytes.Equal(read, bucketKey) {
			reads++
		}
	}
	require.Equal(t, 1, reads)
}

func TestTrieParallelParityWithWhaleBuckets(t *testing.T) {
	address := bytes.Repeat([]byte{0x31}, 20)
	initial := []Op{{Key: eip8297.TreeKeyAccount(address, eip8297.BasicDataLeafKey), Value: testTrieValue(1)}}
	for slot := byte(64); slot < 104; slot++ {
		initial = append(initial, Op{Key: eip8297.TreeKeyStorage(address, storageSlot(slot)), Value: testTrieValue(slot)})
	}
	sort.Slice(initial, func(i, j int) bool { return bytes.Compare(initial[i].Key, initial[j].Key) < 0 })
	serialContext := newTrieTestContext()
	parallelContext := newTrieTestContext()
	requireProcess(t, serialContext, initial)
	requireProcess(t, parallelContext, initial)

	batch := []Op{
		{Key: initial[0].Key, Value: testTrieValue(7)},
		{Key: eip8297.TreeKeyStorage(address, storageSlot(110)), Value: testTrieValue(8)},
	}
	sort.Slice(batch, func(i, j int) bool { return bytes.Compare(batch[i].Key, batch[j].Key) < 0 })
	serialRoot, err := NewTrie(serialContext).Process(batch)
	require.NoError(t, err)
	parallelRoot, err := NewTrie(parallelContext).ProcessParallel(batch, 4)
	require.NoError(t, err)
	require.Equal(t, serialRoot, parallelRoot)
	require.Equal(t, serialContext.records, parallelContext.records)
	require.NoError(t, NewTrie(parallelContext).Verify())
}

func TestTrieParallelWhaleDeltasHaveRoundStartPrev(t *testing.T) {
	address := bytes.Repeat([]byte{0x32}, 20)
	initial := []Op{
		{Key: eip8297.TreeKeyStorage(address, storageSlot(64)), Value: testTrieValue(1)},
		{Key: eip8297.TreeKeyStorage(address, storageSlot(65)), Value: testTrieValue(2)},
	}
	ctx := newTrieTestContext()
	requireProcess(t, ctx, initial)
	start := cloneRecords(ctx.records)
	trie := NewTrie(ctx)
	batch := []Op{
		{Key: initial[0].Key, Value: testTrieValue(3)},
		{Key: eip8297.TreeKeyStorage(address, storageSlot256()), Value: testTrieValue(4)},
	}
	sort.Slice(batch, func(i, j int) bool { return bytes.Compare(batch[i].Key, batch[j].Key) < 0 })
	_, err := trie.ProcessParallel(batch, 4)
	require.NoError(t, err)
	deltas := trie.TakeDeltas()
	seen := make(map[string]struct{}, len(deltas))
	for _, delta := range deltas {
		_, duplicate := seen[string(delta.Key)]
		require.False(t, duplicate, "duplicate delta %x", delta.Key)
		seen[string(delta.Key)] = struct{}{}
		require.Equal(t, start[string(delta.Key)], delta.Prev, "delta %x", delta.Key)
	}
	for _, delta := range slices.Backward(deltas) {
		require.NoError(t, ctx.PutBranch(delta.Key, delta.Prev, delta.Data))
	}
	require.Equal(t, start, ctx.records)
}

func TestTrieParallelWipeAndRewriteAcrossPhases(t *testing.T) {
	address := bytes.Repeat([]byte{0x33}, 20)
	account := eip8297.TreeKeyAccount(address, eip8297.BasicDataLeafKey)
	keyA := eip8297.TreeKeyStorage(address, storageSlot(64))
	keyB := eip8297.TreeKeyStorage(address, storageSlot(65))
	prefix := bytes.Clone(keyA[:33])
	initial := []Op{{Key: account, Value: testTrieValue(1)}, {Key: keyA, Value: testTrieValue(2)}, {Key: keyB, Value: testTrieValue(3)}}
	ctx := newTrieTestContext()
	requireProcess(t, ctx, initial)
	batch := []Op{{Key: account, Value: testTrieValue(4)}, Drop(prefix), {Key: keyA, Value: testTrieValue(5)}, {Key: keyB, Value: testTrieValue(6)}}
	root, err := NewTrie(ctx).ProcessParallel(batch, 4)
	require.NoError(t, err)
	want := []Op{{Key: account, Value: testTrieValue(4)}, {Key: keyA, Value: testTrieValue(5)}, {Key: keyB, Value: testTrieValue(6)}}
	require.Equal(t, eip8297.StateRoot(entriesFromOps(want)), root)
	assertPersistedTrie(t, ctx, want)
}

func TestTrieParallelHeaderAndOverflowShareOneBatch(t *testing.T) {
	address := bytes.Repeat([]byte{0x34}, 20)
	entries := []Op{
		{Key: eip8297.TreeKeyAccount(address, eip8297.BasicDataLeafKey), Value: testTrieValue(1)},
		{Key: eip8297.TreeKeyAccount(address, eip8297.HeaderStorageSlots+63), Value: testTrieValue(2)},
		{Key: eip8297.TreeKeyStorage(address, storageSlot(64)), Value: testTrieValue(3)},
	}
	sort.Slice(entries, func(i, j int) bool { return bytes.Compare(entries[i].Key, entries[j].Key) < 0 })
	ctx := newTrieTestContext()
	root, err := NewTrie(ctx).ProcessParallel(entries, 4)
	require.NoError(t, err)
	require.Equal(t, eip8297.StateRoot(entriesFromOps(entries)), root)
	require.NoError(t, NewTrie(ctx).Verify())
	assertPersistedTrie(t, ctx, entries)
}

func TestTrieParallelRandomParity(t *testing.T) {
	for _, seed := range []int64{0x11, 0x22, 0x33} {
		t.Run(fmt.Sprintf("seed-%d", seed), func(t *testing.T) {
			rng := rand.New(rand.NewSource(seed))
			address := bytes.Repeat([]byte{0x45}, 20)
			storagePrefix := eip8297.TreeKeyStorage(address, storageSlot(64))[:33]
			pool := make([][]byte, 0, 56)
			initial := make([]Op, 0, 56)
			state := make(map[string]Op)
			for slot := byte(64); slot < 112; slot++ {
				key := eip8297.TreeKeyStorage(address, storageSlot(slot))
				value := testTrieValue(slot)
				pool = append(pool, key)
				initial = append(initial, Op{Key: key, Value: value})
				state[string(key)] = Op{Key: key, Value: value}
			}
			for i := range 8 {
				key := trieCodeKey(byte(i), 0, byte(i+1))
				value := testTrieValue(byte(i + 120))
				pool = append(pool, key)
				initial = append(initial, Op{Key: key, Value: value})
				state[string(key)] = Op{Key: key, Value: value}
			}
			sort.Slice(initial, func(i, j int) bool { return bytes.Compare(initial[i].Key, initial[j].Key) < 0 })
			serialContext := newTrieTestContext()
			parallelContext := newTrieTestContext()
			requireProcess(t, serialContext, initial)
			_, err := NewTrie(parallelContext).ProcessParallel(initial, 4)
			require.NoError(t, err)
			for batch := range 80 {
				var ops []Op
				if batch%17 == 0 {
					ops = append(ops, Drop(bytes.Clone(storagePrefix)))
					for key := range state {
						if bytes.HasPrefix([]byte(key), storagePrefix) {
							delete(state, key)
						}
					}
					for i := 0; i < 1+rng.Intn(4); i++ {
						slot := byte(64 + rng.Intn(48))
						key := eip8297.TreeKeyStorage(address, storageSlot(slot))
						value := testTrieValue(byte(batch + i + 1))
						op := Op{Key: key, Value: value}
						if _, exists := state[string(key)]; !exists {
							state[string(key)] = op
							ops = append(ops, op)
						}
					}
					sort.Slice(ops[1:], func(i, j int) bool { return bytes.Compare(ops[i+1].Key, ops[j+1].Key) < 0 })
				} else {
					chosen := make(map[int]struct{})
					for len(chosen) < 1+rng.Intn(6) {
						chosen[rng.Intn(len(pool))] = struct{}{}
					}
					for index := range chosen {
						key := pool[index]
						value := testTrieValue(byte(batch + index + 1))
						if rng.Intn(4) == 0 {
							value = [32]byte{}
							delete(state, string(key))
						} else {
							state[string(key)] = Op{Key: key, Value: value}
						}
						ops = append(ops, Op{Key: key, Value: value})
					}
					sort.Slice(ops, func(i, j int) bool { return bytes.Compare(ops[i].Key, ops[j].Key) < 0 })
				}
				serialRoot, err := NewTrie(serialContext).Process(ops)
				require.NoError(t, err, "seed=%d batch=%d", seed, batch)
				parallelRoot, err := NewTrie(parallelContext).ProcessParallel(ops, 4)
				require.NoError(t, err, "seed=%d batch=%d", seed, batch)
				require.Equal(t, serialRoot, parallelRoot, "seed=%d batch=%d", seed, batch)
				require.Equal(t, serialContext.records, parallelContext.records, "seed=%d batch=%d", seed, batch)
				entries := make([]Op, 0, len(state))
				for _, op := range state {
					entries = append(entries, op)
				}
				sort.Slice(entries, func(i, j int) bool { return bytes.Compare(entries[i].Key, entries[j].Key) < 0 })
				assertPersistedTrie(t, parallelContext, entries)
			}
		})
	}
}
