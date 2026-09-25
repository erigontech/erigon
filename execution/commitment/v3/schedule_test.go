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
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/commitment"
)

type barrierContext struct {
	commitment.PatriciaContext
	arrived bool
	want    int32
	seen    *atomic.Int32
	release chan struct{}
}

func (c *barrierContext) Branch(key []byte) ([]byte, kv.Step, error) {
	if !c.arrived {
		c.arrived = true
		if c.seen.Add(1) == c.want {
			close(c.release)
		}
		select {
		case <-c.release:
		case <-time.After(15 * time.Second):
			return nil, 0, fmt.Errorf("only %d of %d storage workers ever claimed a task", c.seen.Load(), c.want)
		}
	}
	return c.PatriciaContext.Branch(key)
}

func TestScheduling(t *testing.T) {
	storage := benchEntries("storage", 256)
	t.Run("deferred_builds_worker_contexts", func(t *testing.T) {
		_, _, calls := runV3(t, newShardedContext(), v3Config{deferred: true}, storage)
		require.Greater(t, calls, int32(1), "deferred rounds must still build per-worker contexts for the storage phase")
	})

	t.Run("worker_bound_during_trie_process", func(t *testing.T) {
		first := commitment.KeyToHexNibbleHash(parityAddress(0))[0]
		entries := make([]parityUpdate, 0, 16)
		for i := 0; i < 256 && len(entries) < 16; i++ {
			address := parityAddress(i)
			if commitment.KeyToHexNibbleHash(address)[0] != first {
				continue
			}
			entries = append(entries, parityUpdate{key: address, update: accountParityUpdate(i)}, parityUpdate{key: slotKey(address, paritySlot(i)), update: storageParityUpdate(i)})
		}
		require.Len(t, entries, 16)
		_, _, calls := runV3(t, newShardedContext(), v3Config{workers: 2}, entries)
		require.Equal(t, int32(2), calls)
	})

	t.Run("storage_phase_spreads_tasks_across_workers", func(t *testing.T) {
		const workers = 4
		var seen atomic.Int32
		release := make(chan struct{})
		runV3(t, newShardedContext(), v3Config{workers: workers, wrap: func(ctx commitment.PatriciaContext) commitment.PatriciaContext {
			return &barrierContext{PatriciaContext: ctx, want: workers, seen: &seen, release: release}
		}}, benchEntries("storage", 2*workers))
	})

	t.Run("storage_phase_worker_contexts", func(t *testing.T) {
		calls := func(tasks []storageTask, workers, fanOutMin int) int32 {
			var calls atomic.Int32
			factory := func(context.Context) (commitment.PatriciaContext, func()) {
				calls.Add(1)
				return newMockContext(), nil
			}
			require.NoError(t, runStoragePhase(context.Background(), newMockContext(), factory, tasks, make([][32]byte, len(tasks)), make([]deltaParts, len(tasks)), workers, fanOutMin))
			return calls.Load()
		}
		wipes := make([]storageTask, 64)
		for i := range wipes {
			wipes[i].wipe = true
			wipes[i].addrHash[0] = byte(i)
		}
		wide := func() []storageTask {
			task := storageTask{addrHash: [32]byte{1}}
			for i := range 64 {
				path := make([]byte, 64)
				path[0], path[1] = byte(i%16), byte(i/16)
				task.entries = append(task.entries, storageEntry{path: path, value: []byte{byte(i + 1)}, op: storagePut})
			}
			return []storageTask{task}
		}
		require.Equal(t, int32(4), calls(wipes, 4, 0))
		require.Equal(t, int32(1), calls(wide(), 2, 1<<20))
		require.Greater(t, calls(wide(), 2, 16), int32(1))
	})

	t.Run("field_update_preserves_storage_root", func(t *testing.T) {
		address := parityAddress(3)
		ctx := newShardedContext()
		storageRoot := func() []byte {
			root, err := unfold(ctx, nil, planeAccount, nil)
			require.NoError(t, err)
			value, ok, _ := accountLeafAt(root, commitment.KeyToHexNibbleHash(address))
			require.True(t, ok)
			_, _, _, storageRoot, err := decodeAccountLeaf(value)
			require.NoError(t, err)
			return storageRoot
		}
		runV3(t, ctx, v3Config{workers: 1}, []parityUpdate{{key: address, update: accountParityUpdate(3)}, {key: slotKey(address, paritySlot(3)), update: storageParityUpdate(3)}})
		before := storageRoot()
		runV3(t, ctx, v3Config{workers: 1}, []parityUpdate{{key: address, update: &commitment.Update{Flags: commitment.BalanceUpdate, Balance: *uint256.NewInt(99)}}})
		require.Equal(t, before, storageRoot())
	})

	whale := benchAddr(7)
	whaleSeed := []parityUpdate{{key: whale, update: accountParityUpdate(7)}}
	whaleNext := []parityUpdate{{key: whale, update: accountParityUpdate(8)}}
	for i := range 5 * defaultStorageFanOutMin {
		slot := slotKey(whale, benchSlot(i))
		switch {
		case i >= 4*defaultStorageFanOutMin:
			whaleNext = append(whaleNext, parityUpdate{key: slot, update: storageParityUpdate(i)})
		case i%3 == 0:
			whaleNext = append(whaleNext, parityUpdate{key: slot, update: &commitment.Update{Flags: commitment.DeleteUpdate}})
		case i%3 == 1:
			whaleNext = append(whaleNext, parityUpdate{key: slot, update: storageParityUpdate(i + 1)})
		}
		if i < 4*defaultStorageFanOutMin {
			whaleSeed = append(whaleSeed, parityUpdate{key: slot, update: storageParityUpdate(i)})
		}
	}

	contracts := make([]parityUpdate, 0, 64)
	for i := range 32 {
		contracts = append(contracts, parityUpdate{key: parityAddress(i), update: accountParityUpdate(i)}, parityUpdate{key: slotKey(parityAddress(i), paritySlot(i)), update: storageParityUpdate(i)})
	}

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
	groupSeed := make([]parityUpdate, 0, 2*len(dense))
	for _, i := range append(append([]int(nil), dense...), sparse...) {
		groupSeed = append(groupSeed, parityUpdate{key: parityAddress(i), update: accountParityUpdate(i)})
		if i%3 == 0 {
			groupSeed = append(groupSeed, parityUpdate{key: slotKey(parityAddress(i), paritySlot(i)), update: storageParityUpdate(i)})
		}
	}
	deleted := &commitment.Update{Flags: commitment.DeleteUpdate}
	groupNext := []parityUpdate{{key: parityAddress(sparse[0]), update: deleted}, {key: parityAddress(dense[3]), update: deleted}}
	for k, i := range dense[10:400] {
		groupNext = append(groupNext, parityUpdate{key: parityAddress(i), update: accountParityUpdate(i + 1)})
		if k%4 == 0 {
			groupNext = append(groupNext, parityUpdate{key: slotKey(parityAddress(i), paritySlot(i+7)), update: storageParityUpdate(i + 2)})
		}
	}
	for i := 100000; i < 100200; i++ {
		if commitment.KeyToHexNibbleHash(parityAddress(i))[0] != 0 {
			groupNext = append(groupNext, parityUpdate{key: parityAddress(i), update: accountParityUpdate(i)})
		}
	}

	for _, tc := range []struct {
		name   string
		base   []parityUpdate
		rounds [][]parityUpdate
		a, b   v3Config
	}{
		{"deferred_matches_inline", nil, [][]parityUpdate{storage}, v3Config{}, v3Config{deferred: true}},
		{"account_fold_parallel_matches_serial", nil, [][]parityUpdate{benchEntries("storage", 512)}, v3Config{workers: 1}, v3Config{workers: 8}},
		{"storage_fan_out_matches_serial", nil, [][]parityUpdate{whaleSeed, whaleNext}, v3Config{workers: 1}, v3Config{workers: 8}},
		{"many_contracts_match_serial", nil, [][]parityUpdate{contracts}, v3Config{workers: 1}, v3Config{workers: 4}},
		{"pipelined_account_groups_match_serial", groupSeed, [][]parityUpdate{groupNext}, v3Config{workers: 1}, v3Config{workers: 4}},
	} {
		t.Run(tc.name, func(t *testing.T) { requireSameRuns(t, tc.base, tc.a, tc.b, tc.rounds...) })
	}
}
