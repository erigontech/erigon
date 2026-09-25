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

package v3

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
	"github.com/erigontech/erigon/internal/commitmenttest/runner"
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

func newV3Warmuper(ctx commitment.PatriciaContext, workers int) *commitment.Warmuper {
	w := commitment.NewWarmuper(context.Background(), commitment.WarmupConfig{
		CtxFactory: func(context.Context) (commitment.PatriciaContext, func()) { return ctx, nil },
		NumWorkers: workers,
		MaxDepth:   commitment.WarmupMaxDepth,
		Key:        warmupKeyV3,
		Step:       warmupStepV3,
	})
	w.Start()
	return w
}

func warmPartition(t *testing.T, w *commitment.Warmuper, updates *commitment.Updates, keys int) {
	t.Helper()
	defer w.CloseAndWait()
	defer updates.Close()
	_, _, _, err := partitionUpdates(context.Background(), updates, 4, w)
	require.NoError(t, err)
	require.Eventually(t, func() bool { return w.Stats().KeysProcessed == uint64(keys) }, 10*time.Second, time.Millisecond)
	w.CloseAndWait()
}

func warmKey(t *testing.T, records map[string][]byte, hashedKey []byte) [][]byte {
	t.Helper()
	ctx := newMockContext()
	ctx.branches = records
	w := newV3Warmuper(ctx, 1)
	defer w.CloseAndWait()
	w.WarmKey(hashedKey, 0, 0)
	require.NoError(t, w.WaitBufferFree(0))
	w.CloseAndWait()
	return ctx.branchCalls
}

func TestWarmup(t *testing.T) {
	var scratch [66]byte
	accountRecords := func() map[string][]byte {
		return map[string][]byte{
			string(AccountNodeKey(nil, nil)):       recordFixture(0, 0, 1<<2, 0, 0, nil, nil, nil),
			string(AccountNodeKey([]byte{2}, nil)): recordFixture(0, 1, 1<<3, 1<<3, 0, nil, nil, nil),
		}
	}

	t.Run("process_starts_warmuper", func(t *testing.T) {
		trie, updates := NewTrie(t.TempDir(), commitment.TrieConfig{})
		defer trie.Release()
		defer updates.Close()
		trie.ResetContext(newMockContext())
		address := make([]byte, 20)
		address[0] = 1
		update := fullAccountUpdate(1, 2, common.Hash{})
		updates.TouchPlainKeyDirect(string(address), &update)
		var factoryCalls atomic.Int32
		warmupCtx := newMockContext()
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
	})

	t.Run("partition_drives_warmuper", func(t *testing.T) {
		for _, n := range []int{1, 2 * hashParallelMin} {
			updates := commitment.NewUpdates(commitment.ModeCollect, t.TempDir(), commitment.KeyToHexNibbleHash)
			for i := range n {
				update := fullAccountUpdate(1, 2, common.Hash{})
				updates.TouchPlainKeyDirect(string(benchAddr(i)), &update)
			}
			warmupCtx := newMockContext()
			warmPartition(t, newV3Warmuper(warmupCtx, 1), updates, n)
			require.NotEmpty(t, warmupCtx.branchCalls)
		}
	})

	t.Run("key_and_step_account_descent", func(t *testing.T) {
		hashedKey := make([]byte, 128)
		hashedKey[0], hashedKey[1] = 2, 3
		addrHash := hashAddressPath(hashedKey[:64])
		records := accountRecords()
		records[string(StorageNodeKey(addrHash, nil, nil))] = nil
		var gotKeys [][]byte
		for depth := 0; ; {
			key := warmupKeyV3(hashedKey, depth, scratch[:])
			gotKeys = append(gotKeys, bytes.Clone(key))
			nextDepth, stop := warmupStepV3(records[string(key)], hashedKey, depth)
			if stop {
				break
			}
			depth = nextDepth
		}
		require.Equal(t, [][]byte{AccountNodeKey(nil, nil), AccountNodeKey([]byte{2}, nil), StorageNodeKey(addrHash, nil, nil)}, gotKeys)
	})

	t.Run("key_plane_crossing", func(t *testing.T) {
		accountPath, storagePath := make([]byte, 64), make([]byte, 64)
		for i := range accountPath {
			accountPath[i] = byte(i & 0x0f)
			storagePath[i] = byte((i + 7) & 0x0f)
		}
		hashedKey := append(bytes.Clone(accountPath), storagePath...)
		addrHash := hashAddressPath(accountPath)
		for depth := range 64 {
			require.Equal(t, AccountNodeKey(accountPath[:depth], nil), warmupKeyV3(hashedKey, depth, scratch[:]))
		}
		require.Equal(t, StorageNodeKey(addrHash, nil, nil), warmupKeyV3(hashedKey, 64, scratch[:]))
		require.Equal(t, StorageNodeKey(addrHash, storagePath[:1], nil), warmupKeyV3(hashedKey, 65, scratch[:]))
		accountOnly := accountPath[:64]
		require.Equal(t, AccountNodeKey(accountOnly, nil), warmupKeyV3(accountOnly, 64, scratch[:]))
		require.Equal(t, tagStorageNode, warmupKeyV3(hashedKey, 64, scratch[:])[0])
	})

	t.Run("key_shapes", func(t *testing.T) {
		hashedKey := bytes.Repeat([]byte{0x0b}, 128)
		for _, depth := range []int{1, 2, 63, 64, 65, 128} {
			got := warmupKeyV3(hashedKey, depth, scratch[:])
			want := AccountNodeKey(hashedKey[:min(depth, 64)], nil)
			if depth >= 64 {
				want = StorageNodeKey(hashAddressPath(hashedKey[:64]), hashedKey[64:depth], nil)
			}
			require.Equal(t, want, got)
			if depth&1 != 0 {
				require.Zero(t, got[len(got)-2]&0x0f)
			}
		}
	})

	t.Run("step_self_extension_and_truncated", func(t *testing.T) {
		record := recordFixture(hdrHasSelfExt, 0, 1<<4, 0, 0, extFixture([]byte{1, 2}), nil, nil)
		hashedKey := make([]byte, 64)
		copy(hashedKey, []byte{1, 2, 4})
		nextDepth, stop := warmupStepV3(record, hashedKey, 0)
		require.False(t, stop)
		require.Equal(t, 2, nextDepth)

		hashedKey[1] = 5
		nextDepth, stop = warmupStepV3(record, hashedKey, 0)
		require.True(t, stop)
		require.Zero(t, nextDepth)

		require.NotPanics(t, func() {
			nextDepth, stop = warmupStepV3([]byte{hdrHasSelfExt}, hashedKey, 0)
		})
		require.True(t, stop)
		require.Zero(t, nextDepth)
	})

	t.Run("step_plane_depth_and_extension_length", func(t *testing.T) {
		record := recordFixture(hdrHasChildExt, 0, 1<<4, 0, 1<<4, nil, map[int][]byte{4: extFixture([]byte{1, 2})}, nil)
		hashedKey := make([]byte, 128)
		hashedKey[64] = 4
		nextDepth, stop := warmupStepV3(record, hashedKey, 64)
		require.False(t, stop)
		require.Equal(t, 67, nextDepth)

		leafRecord := recordFixture(0, 0, 1<<4, 1<<4, 0, nil, nil,
			map[int]leafFixture{4: {suffix: bytes.Repeat([]byte{0x0c}, 31), value: []byte{9}}})
		nextDepth, stop = warmupStepV3(leafRecord, hashedKey, 64)
		require.True(t, stop, "a storage-plane leaf must end the descent, not restart it")
		require.NotEqual(t, 64, nextDepth, "restarting at 64 from the storage plane rewinds and re-reads the same record")

		storageKey := make([]byte, 128)
		storageKey[0] = 4
		nextDepth, stop = warmupStepV3(leafRecord, storageKey, 0)
		require.False(t, stop, "an account-plane leaf on a 128-nibble key must cross to the storage plane")
		require.Equal(t, 64, nextDepth)
	})

	t.Run("step_stops_on_empty_or_missing_record", func(t *testing.T) {
		_, stop := warmupStepV3([]byte{0}, make([]byte, 64), 0)
		require.True(t, stop)
		_, stop = warmupStepV3(nil, make([]byte, 64), 0)
		require.True(t, stop)
	})

	t.Run("reads_account_plane_records", func(t *testing.T) {
		hashedKey := make([]byte, 64)
		hashedKey[0] = 2
		records := map[string][]byte{
			string(AccountNodeKey(nil, nil)):       recordFixture(0, 0, 1<<2, 0, 0, nil, nil, nil),
			string(AccountNodeKey([]byte{2}, nil)): recordFixture(0, 0, 0, 0, 0, nil, nil, nil),
		}
		require.Equal(t, [][]byte{AccountNodeKey(nil, nil), AccountNodeKey([]byte{2}, nil)}, warmKey(t, records, hashedKey))
	})

	t.Run("reads_storage_plane_record", func(t *testing.T) {
		hashedKey := make([]byte, 128)
		hashedKey[0], hashedKey[1], hashedKey[64], hashedKey[65] = 2, 3, 4, 5
		addrHash := hashAddressPath(hashedKey[:64])
		records := accountRecords()
		records[string(StorageNodeKey(addrHash, nil, nil))] = recordFixture(0, 0, 1<<4, 0, 0, nil, nil, nil)
		records[string(StorageNodeKey(addrHash, []byte{4}, nil))] = recordFixture(0, 1, 0, 1<<5, 0, nil, nil, nil)
		var storageKeys [][]byte
		for _, key := range warmKey(t, records, hashedKey) {
			if len(key) != 0 && key[0] == tagStorageNode {
				storageKeys = append(storageKeys, key)
			}
		}
		require.NotEmpty(t, storageKeys)
	})

	t.Run("storage_descent_reaches_beyond_root", func(t *testing.T) {
		trie, initial := NewTrie(t.TempDir(), commitment.TrieConfig{})
		defer trie.Release()
		defer initial.Close()
		safeCtx := runner.NewMemory(runner.ContextSpec{})
		trie.ResetContext(safeCtx)
		address := bytes.Repeat([]byte{0x11}, 20)
		storageKeys := make([][]byte, 0, 2)
		seen := make(map[byte]struct{})
		var firstNibble byte
		for i := byte(0); len(storageKeys) < 2; i++ {
			slot := make([]byte, 32)
			slot[31] = i
			plainKey := slotKey(address, slot)
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
		warmPartition(t, newV3Warmuper(traceCtx, 4), next, 1+len(storageKeys))

		maxStorageDepth, storageRootReads := 0, 0
		traceCtx.mu.Lock()
		defer traceCtx.mu.Unlock()
		for _, key := range traceCtx.keys {
			if len(key) == 0 || key[0] != tagStorageNode {
				continue
			}
			if key[len(key)-1] == 0 {
				storageRootReads++
			}
			maxStorageDepth = max(maxStorageDepth, 64+int(key[len(key)-1]))
		}
		require.Greater(t, maxStorageDepth, 64)
		require.GreaterOrEqual(t, storageRootReads, 1)
	})
}
