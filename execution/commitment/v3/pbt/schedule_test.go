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

func TestTrieParallelAccountChainRunsWithBucketTask(t *testing.T) {
	address := bytes.Repeat([]byte{0x75}, 20)
	ops := []Op{
		{Key: eip8297.TreeKeyAccount(address, eip8297.BasicDataLeafKey), Value: testTrieValue(1)},
		{Key: eip8297.TreeKeyStorage(address, storageSlot(64)), Value: testTrieValue(2)},
	}
	sort.Slice(ops, func(i, j int) bool { return bytes.Compare(ops[i].Key, ops[j].Key) < 0 })
	ctx := newTrieTestContext()
	trie := NewTrie(ctx)
	trie.SetTrieContextFactory(func(context.Context) (commitment.PatriciaContext, func()) { return ctx, func() {} })
	bucketStarted := make(chan struct{})
	accountStarted := make(chan struct{})
	trie.SetPhaseHook(func(task phaseTask, op *Op) error {
		if task.kind == phaseBucket && op == nil {
			select {
			case <-bucketStarted:
			default:
				close(bucketStarted)
			}
			select {
			case <-accountStarted:
			case <-time.After(time.Second):
				return fmt.Errorf("account chain did not start while bucket task ran")
			}
		}
		if task.kind == phaseChain && task.zone == eip8297.AccountZone && op != nil {
			select {
			case <-accountStarted:
			default:
				close(accountStarted)
			}
		}
		return nil
	})
	_, err := trie.ProcessParallelContext(t.Context(), ops, 2)
	require.NoError(t, err)
}

func TestTrieParallelStorageChainWaitsForBucketResult(t *testing.T) {
	address := bytes.Repeat([]byte{0x76}, 20)
	ops := []Op{
		{Key: eip8297.TreeKeyStorage(address, storageSlot(64)), Value: testTrieValue(1)},
		{Key: eip8297.TreeKeyStorage(address, storageSlot(65)), Value: testTrieValue(2)},
	}
	sort.Slice(ops, func(i, j int) bool { return bytes.Compare(ops[i].Key, ops[j].Key) < 0 })
	ctx := newTrieTestContext()
	trie := NewTrie(ctx)
	trie.SetTrieContextFactory(func(context.Context) (commitment.PatriciaContext, func()) { return ctx, func() {} })
	bucketFinished := make(chan struct{})
	chainBeforeBucket := atomic.Bool{}
	trie.SetPhaseHook(func(task phaseTask, op *Op) error {
		if task.kind == phaseBucket && op != nil && op.Key[len(op.Key)-1] == 65 {
			select {
			case <-bucketFinished:
			default:
				close(bucketFinished)
			}
		}
		if task.kind == phaseChain && task.zone == eip8297.StorageZone && op == nil {
			select {
			case <-bucketFinished:
			default:
				chainBeforeBucket.Store(true)
			}
		}
		return nil
	})
	_, err := trie.ProcessParallelContext(t.Context(), ops, 2)
	require.NoError(t, err)
	require.False(t, chainBeforeBucket.Load())
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

func TestTrieParallelCoreAttribution(t *testing.T) {
	ctx := newTrieTestContext()
	address := bytes.Repeat([]byte{0x71}, 20)
	accountKey := eip8297.TreeKeyAccount(address, eip8297.BasicDataLeafKey)
	codeKey := eip8297.TreeKeyCodeChunk([32]byte{3}, 0)
	storageKey := eip8297.TreeKeyStorage(address, storageSlot(64))
	initial := []Op{{Key: accountKey, Value: testTrieValue(1)}, {Key: codeKey, Value: testTrieValue(2)}}
	sort.Slice(initial, func(i, j int) bool { return bytes.Compare(initial[i].Key, initial[j].Key) < 0 })
	requireProcess(t, ctx, initial)
	ops := []Op{
		{Key: accountKey, Value: testTrieValue(3)},
		{Key: storageKey, Value: testTrieValue(3)},
	}
	sort.Slice(ops, func(i, j int) bool { return bytes.Compare(ops[i].Key, ops[j].Key) < 0 })
	applied := make(map[string]int)
	encoded := make(map[string]phaseTaskKind)
	var mu sync.Mutex
	trie := NewTrie(ctx)
	trie.SetTrieContextFactory(func(context.Context) (commitment.PatriciaContext, func()) { return ctx, func() {} })
	trie.SetCoreHooks(func(task phaseTask, op *Op) error {
		if op == nil {
			return nil
		}
		mu.Lock()
		defer mu.Unlock()
		applied[string(op.Key)]++
		if op.Key[0] == eip8297.StorageZone {
			require.Contains(t, []phaseTaskKind{phaseBucket, phaseBucketSubtask}, task.kind)
		} else {
			require.Equal(t, phaseChain, task.kind)
		}
		return nil
	}, func(task phaseTask, key []byte) error {
		mu.Lock()
		defer mu.Unlock()
		if _, exists := encoded[string(key)]; exists {
			return fmt.Errorf("record %x encoded twice", key)
		}
		encoded[string(key)] = task.kind
		return nil
	})
	_, err := trie.ProcessParallel(ops, 4)
	require.NoError(t, err)
	for _, op := range ops {
		require.Equal(t, 1, applied[string(op.Key)])
	}
	require.Equal(t, phaseJoin, encoded[string(GlobalRootKey())])
}

func TestTrieParallelChainMutationRunsConcurrently(t *testing.T) {
	addressA := bytes.Repeat([]byte{0x01}, 20)
	addressB := bytes.Repeat([]byte{0x02}, 20)
	for addressB[0] = 0; addressB[0] < 255 && eip8297.TreeKeyAccount(addressA, eip8297.BasicDataLeafKey)[1]>>4 == eip8297.TreeKeyAccount(addressB, eip8297.BasicDataLeafKey)[1]>>4; addressB[0]++ {
	}
	ops := []Op{
		{Key: eip8297.TreeKeyAccount(addressA, eip8297.BasicDataLeafKey), Value: testTrieValue(1)},
		{Key: eip8297.TreeKeyAccount(addressB, eip8297.BasicDataLeafKey), Value: testTrieValue(2)},
	}
	sort.Slice(ops, func(i, j int) bool { return bytes.Compare(ops[i].Key, ops[j].Key) < 0 })
	ctx := newTrieTestContext()
	var started atomic.Int32
	seen := make(map[string]struct{})
	var seenMu sync.Mutex
	bothStarted := make(chan struct{})
	trie := NewTrie(ctx)
	trie.SetTrieContextFactory(func(context.Context) (commitment.PatriciaContext, func()) { return ctx, func() {} })
	trie.SetCoreHooks(func(task phaseTask, op *Op) error {
		if task.kind != phaseChain || op == nil {
			return nil
		}
		name := string(eip8297.AppendBitPath(nil, &task.prefix))
		seenMu.Lock()
		if _, ok := seen[name]; !ok {
			seen[name] = struct{}{}
			if started.Add(1) == 2 {
				close(bothStarted)
			}
		}
		seenMu.Unlock()
		select {
		case <-bothStarted:
			return nil
		case <-time.After(time.Second):
			return fmt.Errorf("chain mutation did not run concurrently")
		}
	}, nil)
	_, err := trie.ProcessParallel(ops, 2)
	require.NoError(t, err)
	require.Equal(t, int32(2), started.Load())
}

func TestTrieParallelWhaleMutationRunsConcurrently(t *testing.T) {
	address := bytes.Repeat([]byte{0x77}, 20)
	stem := eip8297.TreeKeyStorage(address, storageSlot(64))[:33]
	ops := make([]Op, 0, 16)
	for slot := range 8 {
		ops = append(ops,
			Op{Key: storageKeyWithSuffix(stem, 0x20, byte(slot)), Value: testTrieValue(byte(slot))},
			Op{Key: storageKeyWithSuffix(stem, 0x40, byte(slot)), Value: testTrieValue(byte(slot + 8))},
		)
	}
	sort.Slice(ops, func(i, j int) bool { return bytes.Compare(ops[i].Key, ops[j].Key) < 0 })
	ctx := newTrieTestContext()
	var started atomic.Int32
	seen := make(map[string]struct{})
	var seenMu sync.Mutex
	bothStarted := make(chan struct{})
	trie := NewTrie(ctx)
	trie.SetTrieContextFactory(func(context.Context) (commitment.PatriciaContext, func()) { return ctx, func() {} })
	trie.SetCoreHooks(func(task phaseTask, op *Op) error {
		if task.kind != phaseBucketSubtask || op == nil {
			return nil
		}
		name := string(eip8297.AppendBitPath(nil, &task.prefix))
		seenMu.Lock()
		if _, ok := seen[name]; !ok {
			seen[name] = struct{}{}
			if started.Add(1) == 2 {
				close(bothStarted)
			}
		}
		seenMu.Unlock()
		select {
		case <-bothStarted:
			return nil
		case <-time.After(time.Second):
			return fmt.Errorf("whale mutation did not run concurrently")
		}
	}, nil)
	_, err := trie.ProcessParallelWithThreshold(ops, 2, 2)
	require.NoError(t, err)
	require.Equal(t, int32(2), started.Load())
}

func TestSubtreeTaskRejectsForeignOperations(t *testing.T) {
	address := bytes.Repeat([]byte{0x46}, 20)
	foreignAddress := bytes.Repeat([]byte{0x47}, 20)
	key := eip8297.TreeKeyStorage(address, storageSlot(64))
	bucketKey, err := bucketKeyForStorage(key)
	require.NoError(t, err)
	foreignKey := eip8297.TreeKeyStorage(foreignAddress, storageSlot(64))
	ctx := newTrieTestContext()
	requireProcess(t, ctx, []Op{{Key: key, Value: testTrieValue(1)}})
	for name, op := range map[string]Op{
		"insert": {Key: foreignKey, Value: testTrieValue(2)},
		"delete": {Key: foreignKey},
		"drop":   Drop(foreignKey[:33]),
	} {
		t.Run(name, func(t *testing.T) {
			err := callSubtreeTask(t, ctx, phaseTask{kind: phaseBucket, key: string(bucketKey), ops: []Op{op}})
			require.ErrorContains(t, err, "outside subtree prefix")
		})
	}
}

func callSubtreeTask(t *testing.T, ctx commitment.PatriciaContext, task phaseTask) (err error) {
	t.Helper()
	trie := NewTrie(ctx)
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("subtree task panicked: %v", recovered)
		}
	}()
	_, err = trie.runSubtreeTask(context.Background(), ctx, task)
	return err
}

func TestTrieParallelRoundFailureLeavesContextUnchanged(t *testing.T) {
	address := bytes.Repeat([]byte{0x72}, 20)
	account := eip8297.TreeKeyAccount(address, eip8297.BasicDataLeafKey)
	storage := eip8297.TreeKeyStorage(address, storageSlot(64))
	ctx := newTrieTestContext()
	initial := []Op{{Key: account, Value: testTrieValue(1)}}
	requireProcess(t, ctx, initial)
	start := cloneRecords(ctx.records)
	ops := []Op{{Key: account, Value: testTrieValue(2)}, {Key: storage, Value: testTrieValue(3)}}
	sort.Slice(ops, func(i, j int) bool { return bytes.Compare(ops[i].Key, ops[j].Key) < 0 })
	trie := NewTrie(ctx)
	trie.SetPhaseHook(func(task phaseTask, op *Op) error {
		if task.kind == phaseJoin && op == nil {
			return fmt.Errorf("phase B failure")
		}
		return nil
	})
	_, err := trie.ProcessParallel(ops, 2)
	require.EqualError(t, err, "phase B failure")
	require.Equal(t, start, ctx.records)
}

func TestTrieParallelReopenUpdatesOverflowStorage(t *testing.T) {
	address := bytes.Repeat([]byte{0x73}, 20)
	initial := []Op{
		{Key: eip8297.TreeKeyAccount(address, eip8297.BasicDataLeafKey), Value: testTrieValue(1)},
		{Key: eip8297.TreeKeyCodeChunk([32]byte{0x44}, 0), Value: testTrieValue(2)},
		{Key: eip8297.TreeKeyStorage(address, storageSlot(64)), Value: testTrieValue(3)},
	}
	batch := []Op{{Key: initial[2].Key, Value: testTrieValue(4)}}
	assertParallelReopenUpdate(t, initial, batch)
}

func TestTrieParallelReopenUpdatesAccountOverflowStorage(t *testing.T) {
	address := bytes.Repeat([]byte{0x74}, 20)
	initial := []Op{
		{Key: eip8297.TreeKeyAccount(address, eip8297.BasicDataLeafKey), Value: testTrieValue(1)},
		{Key: eip8297.TreeKeyAccount(address, eip8297.HeaderStorageSlots+63), Value: testTrieValue(2)},
		{Key: eip8297.TreeKeyStorage(address, storageSlot(64)), Value: testTrieValue(3)},
	}
	batch := []Op{{Key: initial[2].Key, Value: testTrieValue(4)}}
	assertParallelReopenUpdate(t, initial, batch)
}

func assertParallelReopenUpdate(t *testing.T, initial, batch []Op) {
	t.Helper()
	sort.Slice(initial, func(i, j int) bool { return bytes.Compare(initial[i].Key, initial[j].Key) < 0 })
	sort.Slice(batch, func(i, j int) bool { return bytes.Compare(batch[i].Key, batch[j].Key) < 0 })
	serialContext := newTrieTestContext()
	parallelContext := newTrieTestContext()
	parallelContext.rejectNilPrev = true
	requireProcess(t, serialContext, initial)
	requireProcess(t, parallelContext, initial)
	start := cloneRecords(parallelContext.records)
	serialRoot, err := NewTrie(serialContext).Process(batch)
	require.NoError(t, err)
	parallelTrie := NewTrie(parallelContext)
	parallelRoot, err := parallelTrie.ProcessParallel(batch, 2)
	require.NoError(t, err)
	require.Equal(t, serialRoot, parallelRoot)
	require.Equal(t, serialContext.records, parallelContext.records)
	require.NoError(t, NewTrie(parallelContext).Verify())
	want := append(append([]Op(nil), initial...), batch...)
	for i := range want {
		for j := i + 1; j < len(want); j++ {
			if bytes.Equal(want[i].Key, want[j].Key) {
				want[i] = want[j]
				want = append(want[:j], want[j+1:]...)
				j--
			}
		}
	}
	assertPersistedTrie(t, parallelContext, want)
	deltas := parallelTrie.TakeDeltas()
	for _, delta := range deltas {
		require.Equal(t, start[string(delta.Key)], delta.Prev)
	}
	for _, delta := range slices.Backward(deltas) {
		require.NoError(t, parallelContext.PutBranch(delta.Key, delta.Prev, delta.Data))
	}
	require.Equal(t, start, parallelContext.records)
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

func TestTrieParallelChurnParity(t *testing.T) {
	keys, prefixes := churnKeys()
	for _, workers := range []int{1, 2, 8} {
		t.Run(fmt.Sprintf("workers-%d", workers), func(t *testing.T) {
			serialContext := newTrieTestContext()
			parallelContext := newTrieTestContext()
			state := make(map[string]Op)
			rng := rand.New(rand.NewSource(0x51d734 + int64(workers)))
			for batch := range 64 {
				ops := churnBatch(rng, keys, prefixes, state)
				serialRoot, err := NewTrie(serialContext).Process(ops)
				require.NoError(t, err, "batch=%d", batch)
				parallelRoot, err := NewTrie(parallelContext).ProcessParallel(ops, workers)
				require.NoError(t, err, "batch=%d", batch)
				require.Equal(t, serialRoot, parallelRoot, "batch=%d", batch)
				require.Equal(t, serialContext.records, parallelContext.records, "batch=%d", batch)
				updateChurnState(state, ops)
				entries := churnEntries(state)
				require.Equal(t, eip8297.StateRootWithHash(entriesFromOps(entries), eip8297.SelectedHash()), parallelRoot, "batch=%d", batch)
				assertPersistedTrie(t, parallelContext, entries)
			}
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

func TestTrieParallelPhaseBDoesNotReadBucketRows(t *testing.T) {
	address := bytes.Repeat([]byte{0x46}, 20)
	account := eip8297.TreeKeyAccount(address, eip8297.BasicDataLeafKey)
	first := eip8297.TreeKeyStorage(address, storageSlot(64))
	second := eip8297.TreeKeyStorage(address, storageSlot(65))
	initial := []Op{
		{Key: account, Value: testTrieValue(1)},
		{Key: first, Value: testTrieValue(2)},
		{Key: second, Value: testTrieValue(3)},
	}
	sort.Slice(initial, func(i, j int) bool { return bytes.Compare(initial[i].Key, initial[j].Key) < 0 })
	ctx := newTrieTestContext()
	requireProcess(t, ctx, initial)
	ctx.reads = nil
	_, err := NewTrie(ctx).ProcessParallel([]Op{{Key: account, Value: testTrieValue(4)}}, 1)
	require.NoError(t, err)
	for _, key := range ctx.reads {
		path, err := eip8297.DecodeBitPath(key)
		if err == nil {
			require.LessOrEqual(t, path.BitLen, int16(264), "%x", key)
		}
	}
}

func TestTrieParallelUpperReadsDoNotGrowWithTree(t *testing.T) {
	target := eip8297.TreeKeyAccount(bytes.Repeat([]byte{0x01}, 20), eip8297.BasicDataLeafKey)
	build := func(extra int) *trieTestContext {
		ctx := newTrieTestContext()
		entries := []Op{{Key: target, Value: testTrieValue(1)}}
		for i := range extra {
			address := make([]byte, 20)
			address[18] = byte(i >> 8)
			address[19] = byte(i)
			entries = append(entries, Op{Key: eip8297.TreeKeyAccount(address, eip8297.BasicDataLeafKey), Value: testTrieValue(byte(i + 2))})
		}
		sort.Slice(entries, func(i, j int) bool { return bytes.Compare(entries[i].Key, entries[j].Key) < 0 })
		requireProcess(t, ctx, entries)
		ctx.reads = nil
		_, err := NewTrie(ctx).ProcessParallel([]Op{{Key: target, Value: testTrieValue(9)}}, 1)
		require.NoError(t, err)
		return ctx
	}
	small := build(1)
	large := build(5000)
	require.Equal(t, storedPathDepth(small, target), len(small.reads))
	require.Equal(t, storedPathDepth(large, target), len(large.reads))
}

func storedPathDepth(ctx *trieTestContext, target []byte) int {
	targetPath, err := keyPath(target)
	if err != nil {
		return 0
	}
	data := ctx.records[string(GlobalRootKey())]
	if len(data) == 0 {
		return 1
	}
	record, err := DecodeRecord(GlobalRootKey(), data)
	if err != nil {
		return 0
	}
	switch record.Form {
	case LeafRoot:
		return 1
	case ExtRoot:
		path := record.SelfExt
		if firstDifference(&path, &targetPath) < path.BitLen {
			return 1
		}
		rowPath := path.Slice(0, (path.BitLen/4)*4)
		return 1 + storedRowPathDepth(ctx, rowPath, &targetPath)
	case RowRoot:
		return 1 + storedRowPathDepth(ctx, eip8297.Bitpath{}, &targetPath)
	default:
		return 0
	}
}

func storedRowPathDepth(ctx *trieTestContext, rowPath eip8297.Bitpath, target *eip8297.Bitpath) int {
	key, err := rowKeyForPath(&rowPath)
	if err != nil {
		return 0
	}
	data := ctx.records[string(key)]
	if len(data) == 0 {
		return 0
	}
	record, err := DecodeRecord(key, data)
	if err != nil || record.Form != RowRoot {
		return 0
	}
	row := rowFromRecord(rowPath, key, data, &record)
	slot := slotAt(target, rowPath.BitLen)
	cell := row.cell(slot)
	if cell.Kind != BranchCell {
		return 1
	}
	full := branchPath(row, slot, cell)
	if firstDifference(&full, target) < full.BitLen {
		return 1
	}
	split := branchSplit(row, slot, cell)
	childPath, err := rowChildPath(row, slot, cell.Prefix, split)
	if err != nil {
		return 1
	}
	return 1 + storedRowPathDepth(ctx, childPath, target)
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

func TestTrieParallelWhaleSubtasksRunConcurrently(t *testing.T) {
	address := bytes.Repeat([]byte{0x77}, 20)
	ops := make([]Op, 0, 16)
	stem := eip8297.TreeKeyStorage(address, storageSlot(64))[:33]
	for slot := range 8 {
		key := storageKeyWithSuffix(stem, 0x20, byte(slot))
		ops = append(ops, Op{Key: key, Value: testTrieValue(byte(slot))})
	}
	for slot := range 8 {
		key := storageKeyWithSuffix(stem, 0x40, byte(slot))
		ops = append(ops, Op{Key: key, Value: testTrieValue(byte(slot + 8))})
	}
	sort.Slice(ops, func(i, j int) bool { return bytes.Compare(ops[i].Key, ops[j].Key) < 0 })
	ctx := newTrieTestContext()
	trie := NewTrie(ctx)
	trie.SetTrieContextFactory(func(context.Context) (commitment.PatriciaContext, func()) { return ctx, func() {} })
	started := make(chan struct{})
	var count atomic.Int32
	trie.SetPhaseHook(func(task phaseTask, op *Op) error {
		if task.kind != phaseBucketSubtask || op != nil {
			return nil
		}
		if count.Add(1) == 2 {
			close(started)
		}
		select {
		case <-started:
			return nil
		case <-time.After(time.Second):
			return fmt.Errorf("whale subtasks did not start concurrently")
		}
	})
	parallelRoot, err := trie.ProcessParallelWithThreshold(ops, 2, 8)
	require.NoError(t, err)
	require.Equal(t, int32(2), count.Load())
	serialContext := newTrieTestContext()
	serialRoot, err := NewTrie(serialContext).Process(ops)
	require.NoError(t, err)
	require.Equal(t, serialRoot, parallelRoot)
	require.Equal(t, serialContext.records, ctx.records)
	offContext := newTrieTestContext()
	offRoot, err := NewTrie(offContext).ProcessParallelWithThreshold(ops, 2, 0)
	require.NoError(t, err)
	require.Equal(t, parallelRoot, offRoot)
	require.Equal(t, ctx.records, offContext.records)
}

func TestTrieParallelWhaleDeltasAllKeysHaveRoundStartPrev(t *testing.T) {
	address := bytes.Repeat([]byte{0x78}, 20)
	initial := make([]Op, 0, 16)
	batch := make([]Op, 0, 16)
	for slot := byte(64); slot < 80; slot++ {
		key := eip8297.TreeKeyStorage(address, storageSlot(slot))
		initial = append(initial, Op{Key: key, Value: testTrieValue(slot)})
		batch = append(batch, Op{Key: key, Value: testTrieValue(slot + 1)})
	}
	ctx := newTrieTestContext()
	requireProcess(t, ctx, initial)
	start := cloneRecords(ctx.records)
	trie := NewTrie(ctx)
	_, err := trie.ProcessParallelWithThreshold(batch, 2, 8)
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
