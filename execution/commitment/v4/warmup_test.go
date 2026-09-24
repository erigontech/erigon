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
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/commitment"
)

type warmupTraceContext struct {
	commitment.PatriciaContext
	mu   sync.Mutex
	keys [][]byte
}

func (c *warmupTraceContext) Branch(key []byte) ([]byte, kv.Step, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.keys = append(c.keys, bytes.Clone(key))
	return c.PatriciaContext.Branch(key)
}

func (c *warmupTraceContext) branchKeys() [][]byte {
	c.mu.Lock()
	defer c.mu.Unlock()
	keys := make([][]byte, len(c.keys))
	for i, key := range c.keys {
		keys[i] = bytes.Clone(key)
	}
	return keys
}

type warmupSafeContext struct {
	ctx commitment.PatriciaContext
	mu  sync.Mutex
}

func (c *warmupSafeContext) Branch(key []byte) ([]byte, kv.Step, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	data, step, err := c.ctx.Branch(key)
	return bytes.Clone(data), step, err
}

func (c *warmupSafeContext) PutBranch(key, data, prev []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.ctx.PutBranch(key, data, prev)
}

func (c *warmupSafeContext) Account(key []byte) (*commitment.Update, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	update, err := c.ctx.Account(key)
	if update == nil {
		return nil, err
	}
	return update.Copy(), err
}

func (c *warmupSafeContext) Storage(key []byte) (*commitment.Update, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	update, err := c.ctx.Storage(key)
	if update == nil {
		return nil, err
	}
	return update.Copy(), err
}

func TestTrieProcessStartsWarmuperWhenEnabled(t *testing.T) {
	trie, updates := NewTrie(t.TempDir(), commitment.TrieConfig{})
	processCtx := newMockContext()
	warmupCtx := newMockContext()
	trie.ResetContext(processCtx)

	address := make([]byte, 20)
	address[0] = 1
	update := fullAccountUpdate(1, 2, common.Hash{})
	updates.TouchPlainKeyDirect(string(address), &update)

	var factoryCalls atomic.Int32
	_, err := trie.Process(context.Background(), updates, "", nil, commitment.WarmupConfig{
		Enabled: true,
		CtxFactory: func(context.Context) (commitment.PatriciaContext, func()) {
			factoryCalls.Add(1)
			return warmupCtx, nil
		},
		NumWorkers: 1,
		MaxDepth:   commitment.WarmupMaxDepth,
	})
	require.NoError(t, err)
	require.Equal(t, int32(1), factoryCalls.Load())
}

func TestPartitionUpdatesDrivesWarmuper(t *testing.T) {
	for _, n := range []int{1, 2 * hashParallelMin} {
		updates := commitment.NewUpdates(commitment.ModeCollect, t.TempDir(), commitment.KeyToHexNibbleHash)
		for i := range n {
			update := fullAccountUpdate(1, 2, common.Hash{})
			updates.TouchPlainKeyDirect(string(benchAddr(i)), &update)
		}

		warmupCtx := newMockContext()
		w := commitment.NewWarmuper(context.Background(), commitment.WarmupConfig{
			CtxFactory: func(context.Context) (commitment.PatriciaContext, func()) { return warmupCtx, nil },
			NumWorkers: 1,
			MaxDepth:   commitment.WarmupMaxDepth,
			Key:        warmupKeyV4,
			Step:       warmupStepV4,
		})
		w.Start()
		_, _, _, err := partitionUpdates(context.Background(), updates, 4, w)
		require.NoError(t, err)
		require.Eventually(t, func() bool { return w.Stats().KeysProcessed == uint64(n) }, 10*time.Second, time.Millisecond)
		w.CloseAndWait()
		require.NotEmpty(t, warmupCtx.branchCalls)
	}
}

func TestWarmupV4KeyAndStepAccountDescent(t *testing.T) {
	hashedKey := make([]byte, 128)
	hashedKey[0] = 2
	hashedKey[1] = 3
	rootRecord := recordFixture(0, 0, 1<<2, 0, 0, nil, nil, nil)
	childRecord := recordFixture(0, 1, 1<<3, 1<<3, 0, nil, nil, nil)
	addrHash := hashAddressPath(hashedKey[:64])
	records := map[string][]byte{
		string(AccountNodeKey(nil, nil)):           rootRecord,
		string(AccountNodeKey([]byte{2}, nil)):     childRecord,
		string(StorageNodeKey(addrHash, nil, nil)): nil,
	}
	wantKeys := [][]byte{AccountNodeKey(nil, nil), AccountNodeKey([]byte{2}, nil), StorageNodeKey(addrHash, nil, nil)}
	var scratch [66]byte
	var gotKeys [][]byte
	for depth := 0; ; {
		key := warmupKeyV4(hashedKey, depth, scratch[:])
		gotKeys = append(gotKeys, bytes.Clone(key))
		nextDepth, stop := warmupStepV4(records[string(key)], hashedKey, depth)
		if stop {
			break
		}
		depth = nextDepth
	}
	require.Equal(t, wantKeys, gotKeys)
}

func TestWarmupV4KeyPlaneCrossing(t *testing.T) {
	accountPath := make([]byte, 64)
	storagePath := make([]byte, 64)
	for i := range accountPath {
		accountPath[i] = byte(i & 0x0f)
		storagePath[i] = byte((i + 7) & 0x0f)
	}
	hashedKey := append(bytes.Clone(accountPath), storagePath...)
	addrHash := hashAddressPath(accountPath)
	var scratch [66]byte

	for depth := range 64 {
		got := warmupKeyV4(hashedKey, depth, scratch[:])
		require.Equal(t, AccountNodeKey(accountPath[:depth], nil), got)
	}
	got := warmupKeyV4(hashedKey, 64, scratch[:])
	require.Equal(t, StorageNodeKey(addrHash, nil, nil), got)
	got = warmupKeyV4(hashedKey, 65, scratch[:])
	require.Equal(t, StorageNodeKey(addrHash, storagePath[:1], nil), got)

	accountOnly := accountPath[:64]
	got = warmupKeyV4(accountOnly, 64, scratch[:])
	require.Equal(t, AccountNodeKey(accountOnly, nil), got)

	got = warmupKeyV4(hashedKey, 64, scratch[:])
	require.Equal(t, tagStorageNode, got[0])
}

func TestWarmupV4StepSelfExtensionAndTruncated(t *testing.T) {
	record := recordFixture(hdrHasSelfExt, 0, 1<<4, 0, 0, extFixture([]byte{1, 2}), nil, nil)
	hashedKey := make([]byte, 64)
	copy(hashedKey, []byte{1, 2, 4})
	nextDepth, stop := warmupStepV4(record, hashedKey, 0)
	require.False(t, stop)
	require.Equal(t, 2, nextDepth)

	hashedKey[1] = 5
	nextDepth, stop = warmupStepV4(record, hashedKey, 0)
	require.True(t, stop)
	require.Zero(t, nextDepth)

	require.NotPanics(t, func() {
		nextDepth, stop = warmupStepV4([]byte{hdrHasSelfExt}, hashedKey, 0)
	})
	require.True(t, stop)
	require.Zero(t, nextDepth)
}

func TestWarmupV4StepUsesPlaneDepthAndExtensionLength(t *testing.T) {
	record := recordFixture(hdrHasChildExt, 0, 1<<4, 0, 1<<4, nil, map[int][]byte{4: extFixture([]byte{1, 2})}, nil)
	hashedKey := make([]byte, 128)
	hashedKey[64] = 4
	nextDepth, stop := warmupStepV4(record, hashedKey, 64)
	require.False(t, stop)
	require.Equal(t, 67, nextDepth)

	leafRecord := recordFixture(0, 0, 1<<4, 1<<4, 0, nil, nil,
		map[int]leafFixture{4: {suffix: bytes.Repeat([]byte{0x0c}, 31), value: []byte{9}}})
	nextDepth, stop = warmupStepV4(leafRecord, hashedKey, 64)
	require.True(t, stop, "a storage-plane leaf must end the descent, not restart it")
	require.NotEqual(t, 64, nextDepth, "restarting at 64 from the storage plane rewinds and re-reads the same record")

	storageKey := make([]byte, 128)
	storageKey[0] = 4
	nextDepth, stop = warmupStepV4(leafRecord, storageKey, 0)
	require.False(t, stop, "an account-plane leaf on a 128-nibble key must cross to the storage plane")
	require.Equal(t, 64, nextDepth)
}

func TestWarmupV4KeyShapes(t *testing.T) {
	hashedKey := bytes.Repeat([]byte{0x0b}, 128)
	var scratch [66]byte
	for _, depth := range []int{1, 2, 63, 64, 65, 128} {
		got := warmupKeyV4(hashedKey, depth, scratch[:])
		var want []byte
		if depth < 64 {
			want = AccountNodeKey(hashedKey[:depth], nil)
		} else {
			want = StorageNodeKey(hashAddressPath(hashedKey[:64]), hashedKey[64:depth], nil)
		}
		require.Equal(t, want, got)
		if depth&1 != 0 {
			require.Zero(t, got[len(got)-2]&0x0f)
		}
	}

}

func TestWarmupV4ReadsAccountPlaneRecords(t *testing.T) {
	ctx := newMockContext()
	hashedKey := make([]byte, 64)
	hashedKey[0] = 2
	ctx.branches[string(AccountNodeKey(nil, nil))] = recordFixture(0, 0, 1<<2, 0, 0, nil, nil, nil)
	ctx.branches[string(AccountNodeKey([]byte{2}, nil))] = recordFixture(0, 0, 0, 0, 0, nil, nil, nil)
	w := commitment.NewWarmuper(context.Background(), commitment.WarmupConfig{
		CtxFactory: func(context.Context) (commitment.PatriciaContext, func()) { return ctx, nil },
		NumWorkers: 1,
		MaxDepth:   commitment.WarmupMaxDepth,
		Key:        warmupKeyV4,
		Step:       warmupStepV4,
	})
	w.Start()
	w.WarmKey(hashedKey, 0, 0)
	require.NoError(t, w.WaitBufferFree(0))
	w.CloseAndWait()
	require.Equal(t, [][]byte{AccountNodeKey(nil, nil), AccountNodeKey([]byte{2}, nil)}, ctx.branchCalls)
}

func TestWarmupV4StepStopsOnEmptyOrMissingRecord(t *testing.T) {
	_, stop := warmupStepV4([]byte{0}, make([]byte, 64), 0)
	require.True(t, stop)
	_, stop = warmupStepV4(nil, make([]byte, 64), 0)
	require.True(t, stop)
}

func TestWarmupV4ReadsStoragePlaneRecord(t *testing.T) {
	hashedKey := make([]byte, 128)
	hashedKey[0] = 2
	hashedKey[1] = 3
	hashedKey[64] = 4
	hashedKey[65] = 5
	addrHash := hashAddressPath(hashedKey[:64])
	ctx := newMockContext()
	ctx.branches[string(AccountNodeKey(nil, nil))] = recordFixture(0, 0, 1<<2, 0, 0, nil, nil, nil)
	ctx.branches[string(AccountNodeKey([]byte{2}, nil))] = recordFixture(0, 1, 1<<3, 1<<3, 0, nil, nil, nil)
	ctx.branches[string(StorageNodeKey(addrHash, nil, nil))] = recordFixture(0, 0, 1<<4, 0, 0, nil, nil, nil)
	ctx.branches[string(StorageNodeKey(addrHash, []byte{4}, nil))] = recordFixture(0, 1, 0, 1<<5, 0, nil, nil, nil)
	w := commitment.NewWarmuper(context.Background(), commitment.WarmupConfig{
		CtxFactory: func(context.Context) (commitment.PatriciaContext, func()) { return ctx, nil },
		NumWorkers: 1,
		MaxDepth:   commitment.WarmupMaxDepth,
		Key:        warmupKeyV4,
		Step:       warmupStepV4,
	})
	w.Start()
	w.WarmKey(hashedKey, 0, 0)
	require.NoError(t, w.WaitBufferFree(0))
	w.CloseAndWait()

	var storageKeys [][]byte
	for _, key := range ctx.branchCalls {
		if len(key) != 0 && key[0] == tagStorageNode {
			storageKeys = append(storageKeys, key)
		}
	}
	require.NotEmpty(t, storageKeys)
}

func TestWarmupV4StorageDescentReachesBeyondRoot(t *testing.T) {
	trie, initial := NewTrie(t.TempDir(), commitment.TrieConfig{})
	trieCtx := newMockContext()
	safeCtx := &warmupSafeContext{ctx: trieCtx}
	trie.ResetContext(safeCtx)

	address := bytes.Repeat([]byte{0x11}, 20)
	storageKeys := make([][]byte, 0, 2)
	seen := make(map[byte]struct{})
	var firstNibble byte
	for i := byte(0); len(storageKeys) < 2; i++ {
		slot := make([]byte, 32)
		slot[31] = i
		plainKey := append(append([]byte(nil), address...), slot...)
		hashedKey := commitment.KeyToHexNibbleHash(plainKey)
		if len(storageKeys) == 0 {
			firstNibble = hashedKey[64]
		} else if hashedKey[64] != firstNibble {
			continue
		}
		if _, ok := seen[hashedKey[65]]; ok {
			continue
		}
		seen[hashedKey[65]] = struct{}{}
		storageKeys = append(storageKeys, plainKey)
	}
	account := fullAccountUpdate(1, 2, common.Hash{})
	initial.TouchPlainKeyDirect(string(address), &account)
	for _, storageKey := range storageKeys {
		initial.TouchPlainKeyDirect(string(storageKey), phaseAStorageUpdate([]byte{1}))
	}
	_, err := trie.Process(context.Background(), initial, "", nil, commitment.WarmupConfig{})
	require.NoError(t, err)

	traceCtx := &warmupTraceContext{PatriciaContext: safeCtx}
	next := commitment.NewUpdates(commitment.ModeCollect, t.TempDir(), commitment.KeyToHexNibbleHash)
	next.TouchPlainKeyDirect(string(address), &account)
	for _, storageKey := range storageKeys {
		next.TouchPlainKeyDirect(string(storageKey), phaseAStorageUpdate([]byte{2}))
	}
	w := commitment.NewWarmuper(context.Background(), commitment.WarmupConfig{
		CtxFactory: func(context.Context) (commitment.PatriciaContext, func()) { return traceCtx, nil },
		NumWorkers: 4,
		MaxDepth:   commitment.WarmupMaxDepth,
		Key:        warmupKeyV4,
		Step:       warmupStepV4,
	})
	w.Start()
	_, _, _, err = partitionUpdates(context.Background(), next, 4, w)
	require.NoError(t, err)
	require.Eventually(t, func() bool { return w.Stats().KeysProcessed == uint64(1+len(storageKeys)) }, 10*time.Second, time.Millisecond)
	w.CloseAndWait()

	maxStorageDepth := 0
	storageRootReads := 0
	traceKeys := traceCtx.branchKeys()
	for _, key := range traceKeys {
		if len(key) == 0 || key[0] != tagStorageNode {
			continue
		}
		if key[len(key)-1] == 0 {
			storageRootReads++
		}
		if depth := 64 + int(key[len(key)-1]); depth > maxStorageDepth {
			maxStorageDepth = depth
		}
	}
	require.Greater(t, maxStorageDepth, 64)
	require.GreaterOrEqual(t, storageRootReads, 1)
}
