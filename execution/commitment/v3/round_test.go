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
	"fmt"
	"maps"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	keccak "github.com/erigontech/fastkeccak"
	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/empty"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/diagnostics/metrics"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/internal/commitmenttest"
	"github.com/erigontech/erigon/internal/commitmenttest/runner"
)

func TestPartition(t *testing.T) {
	path := func(n byte) []byte { return commitmenttest.Path(nil, n) }
	nonce := &commitment.Update{Flags: commitment.NonceUpdate, Nonce: 7}
	deleted := &commitment.Update{Flags: commitment.DeleteUpdate}
	for _, tc := range []struct {
		name     string
		stream   []phaseAInput
		accounts []accountEntry
		slots    int
		wipe     bool
	}{
		{"E34/PartitionStoragePrefixAndAccounts", []phaseAInput{
			{hashedKey: append(path(1), path(3)...), plainKey: bytes.Repeat([]byte{0xa}, 52), update: phaseAStorageUpdate([]byte{1})},
			{hashedKey: append(path(1), path(4)...), plainKey: bytes.Repeat([]byte{0xa}, 52), update: phaseAStorageUpdate([]byte{2})},
			{hashedKey: path(1), plainKey: bytes.Repeat([]byte{0xa}, 20), update: nonce},
			{hashedKey: path(2), plainKey: bytes.Repeat([]byte{0xb}, 20)},
		}, []accountEntry{{hashedKey: path(1), update: &commitment.Update{Flags: commitment.NonceUpdate, Nonce: 7}, storageDirty: true}, {hashedKey: path(2)}}, 2, false},
		{"E34/PhaseAStorageOnlyUpdateKeepsAccountEntrySeparate", []phaseAInput{{hashedKey: append(path(9), path(2)...), update: phaseAStorageUpdate([]byte{0xaa})}}, []accountEntry{{hashedKey: path(9), storageDirty: true}}, 1, false},
		{"E35/PartitionAccountDeleteCreatesWipeJob", []phaseAInput{{hashedKey: path(3), plainKey: bytes.Repeat([]byte{0x11}, 20), update: deleted}}, []accountEntry{{hashedKey: path(3), update: &commitment.Update{Flags: commitment.DeleteUpdate}}}, 0, true},
		{"E35/PartitionDeleteWithStorageWriteStillWipes", []phaseAInput{{hashedKey: path(4), update: deleted}, {hashedKey: append(path(4), path(5)...), update: phaseAStorageUpdate([]byte{1})}}, []accountEntry{{hashedKey: path(4), update: &commitment.Update{Flags: commitment.DeleteUpdate}, storageDirty: true}}, 0, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			storage, accounts := partition(tc.stream)
			require.Equal(t, tc.accounts, accounts)
			for i, want := range tc.accounts {
				if want.update != nil {
					require.Equal(t, want.update.Flags == commitment.DeleteUpdate, accounts[i].update.Deleted())
				}
			}
			require.Len(t, storage, 1)
			require.Len(t, storage[0].entries, tc.slots)
			require.Equal(t, tc.wipe, storage[0].wipe)
			require.Equal(t, packPath(tc.accounts[0].hashedKey, nil), storage[0].addrHash[:])
		})
	}
	for _, tc := range []struct {
		name string
		wipe bool
	}{
		{"E35/DeleteThenWriteClearsWipe", false},
		{"E35/SelfDestructOnlyWriteSetCreatesStorageWipe", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			updates := testUpdates(t, commitment.ModeUpdate, nil)
			if tc.wipe {
				ws := &state.WriteSet{}
				ws.SetSelfDestruct(accounts.InternAddress(common.Address{0xd4}), &state.VersionedWrite[bool]{Val: true})
				ws.TouchUpdates(updates)
			} else {
				addr := string(bytes.Repeat([]byte{0xe5}, 20))
				for _, op := range []commitmenttest.Op{{Delete: true}, {Account: &commitmenttest.AccountValue{Fields: commitmenttest.BalanceField}}} {
					updates.TouchPlainKeyDirect(addr, runner.Update(op))
				}
			}
			p := &partitioner{}
			require.NoError(t, updates.HashSort(context.Background(), nil, func(hashedKey, _ []byte, update *commitment.Update) error {
				return p.add(bytes.Clone(hashedKey), update)
			}))
			storage, accounts := p.done()
			require.Len(t, accounts, 1)
			require.Equal(t, tc.wipe, accounts[0].update.Deleted())
			if !tc.wipe {
				require.Empty(t, storage)
				return
			}
			require.Len(t, storage, 1)
			require.True(t, storage[0].wipe)
			ctx := newMockContext()
			storageRound(t, ctx, storage[0].addrHash, []commitmenttest.Op{{Key: commitmenttest.Path([]byte{1}, 2), Storage: []byte{1}}})
			_, err := runStorageTask(ctx, storage[0])
			require.NoError(t, err)
			require.Contains(t, ctx.branches, string(StorageNodeKey(storage[0].addrHash, nil, nil)))
			require.Empty(t, ctx.branches[string(StorageNodeKey(storage[0].addrHash, nil, nil))])
		})
	}
	t.Run("E34/E63/PartitionFeedMatchesSerialPartition", func(t *testing.T) {
		items := make([]feedEntry, 0, hashParallelMin*3)
		for i := range hashParallelMin {
			addr := commitmenttest.Key(commitmenttest.KeySpec{Kind: "bench-address", Size: 20}, i)
			op := commitmenttest.Op{Key: addr, Account: &commitmenttest.AccountValue{Fields: commitmenttest.BalanceField}, Delete: i%7 == 0}
			items = append(items, feedEntry{plainKey: string(op.Key), update: runner.Update(op)})
			for j := range i % 4 {
				key := append(bytes.Clone(addr), commitmenttest.Key(commitmenttest.KeySpec{Kind: "bench-slot", Size: 32}, i*8+j)...)
				items = append(items, feedEntry{plainKey: string(key), update: runner.Update(commitmenttest.Op{Storage: []byte{}})})
			}
		}
		hashFeed(items, 1)
		sorted := slices.Clone(items)
		slices.SortFunc(sorted, compareFeed)
		serial := &partitioner{}
		for _, e := range sorted {
			require.NoError(t, serial.add(e.hashedKey, e.update))
		}
		wantStorage, wantAccounts := serial.done()
		gotStorage, gotAccounts, seen, err := partitionFeed(slices.Clone(items), 8)
		require.NoError(t, err)
		require.Equal(t, len(items), seen)
		require.Len(t, gotAccounts, len(wantAccounts))
		for i, want := range wantAccounts {
			require.Equal(t, want.hashedKey, gotAccounts[i].hashedKey)
			require.Equal(t, want.storageDirty, gotAccounts[i].storageDirty)
			require.Same(t, want.update, gotAccounts[i].update)
		}
		byAddr := func(tasks []storageTask) map[[32]byte]storageTask {
			out := make(map[[32]byte]storageTask, len(tasks))
			for _, task := range tasks {
				out[task.addrHash] = task
			}
			return out
		}
		want, got := byAddr(wantStorage), byAddr(gotStorage)
		require.Len(t, gotStorage, len(wantStorage))
		require.Len(t, got, len(want))
		for addr, w := range want {
			g := got[addr]
			require.Equal(t, w.wipe, g.wipe)
			require.Len(t, g.entries, len(w.entries))
			for i := range w.entries {
				require.Equal(t, w.entries[i].path, g.entries[i].path)
			}
		}
	})
}

func TestStorageTransitions(t *testing.T) {
	path := func(first, fill byte) []byte { return commitmenttest.Path([]byte{first}, fill) }
	put := func(p []byte, value byte) commitmenttest.Op { return commitmenttest.Op{Key: p, Storage: []byte{value}} }
	del := commitmenttest.DeleteAt
	for _, tc := range []struct {
		name     string
		addr     byte
		rounds   [][]commitmenttest.Op
		forms    []string
		relation string
		value    []byte
	}{
		{"E36/leaf-branch-leaf-empty", 0x42, [][]commitmenttest.Op{{put(path(1, 2), 1)}, {put(path(1, 3), 2)}, {del(path(1, 3))}, {del(path(1, 2))}}, []string{"leaf", "branch", "leaf", "empty"}, "", nil},
		{"E38/absent-delete", 0, [][]commitmenttest.Op{{del(path(1, 2))}}, []string{"missing"}, "", nil},
		{"E38/colliding-delete", 0x42, [][]commitmenttest.Op{{put(path(0, 5), 1), put(path(1, 2), 2)}, {del(path(1, 3))}}, nil, "equal", nil},
		{"E36/E56/batch-independent", 0x91, [][]commitmenttest.Op{{put(path(4, 5), 1)}, {put(path(6, 7), 2)}}, nil, "bulk", nil},
		{"E35/delete-rewrite", 0x37, [][]commitmenttest.Op{{put(path(8, 9), 0x11)}, {del(path(8, 9)), put(path(8, 9), 0x22)}}, nil, "different", []byte{0x22}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := runner.NewMemory(runner.ContextSpec{Borrowed: true, ForbidStateReads: true})
			addr := [32]byte{tc.addr}
			state := make(commitmenttest.State)
			var first, root [32]byte
			for i, ops := range tc.rounds {
				root = storageRound(t, ctx, addr, ops)
				state.Apply(ops)
				requireStorageState(t, ctx, addr, state)
				if i == 0 {
					first = root
				}
				if len(tc.forms) == 0 {
					continue
				}
				data, exists := ctx.Records()[string(StorageNodeKey(addr, nil, nil))]
				switch tc.forms[i] {
				case "missing":
					require.False(t, exists)
					require.Empty(t, data)
				case "empty":
					require.True(t, exists)
					require.Empty(t, data)
				default:
					require.NotEqual(t, empty.RootHash, root)
					require.NoError(t, Validate(data, 0))
					require.Equal(t, tc.forms[i] == "leaf", Record{data: data}.isLeafRoot())
				}
			}
			switch tc.relation {
			case "equal":
				require.Equal(t, first, root)
			case "different":
				require.NotEqual(t, first, root)
			case "bulk":
				bulk := runner.NewMemory(runner.ContextSpec{})
				require.Equal(t, storageRound(t, bulk, addr, state.Ops()), root)
				key := StorageNodeKey(addr, nil, nil)
				require.Equal(t, bulk.Records()[string(key)], ctx.Records()[string(key)])
			}
			if tc.value != nil {
				require.Equal(t, tc.value, ctx.Records()[string(StorageNodeKey(addr, nil, nil))][34:])
			}
		})
	}
	for _, tc := range []struct {
		name     string
		addr     byte
		paths    [][]byte
		drop     []int
		reinsert bool
	}{
		{"E36/delete/sole sibling leaves", 0x5a, [][]byte{slotPath(0), slotPath(1)}, []int{1}, false},
		{"E36/delete/collapse nested branch", 0x5a, [][]byte{slotPath(1, 2, 3), slotPath(1, 2, 4), slotPath(9)}, []int{1}, false},
		{"E36/delete/drop middle of three", 0x5a, [][]byte{slotPath(1), slotPath(2), slotPath(3)}, []int{1}, false},
		{"E36/delete/drop one of deep pair", 0x5a, [][]byte{slotPath(5, 5, 1), slotPath(5, 5, 2), slotPath(5, 6)}, []int{0}, false},
		{"E36/delete/drop deep pair, keep far leaf", 0x5a, [][]byte{slotPath(5, 5, 1), slotPath(5, 5, 2), slotPath(5, 6)}, []int{0, 1}, false},
		{"E36/delete/drop deep pair, two far leaves", 0x5a, [][]byte{slotPath(5, 5, 1), slotPath(5, 5, 2), slotPath(5, 6), slotPath(8)}, []int{0, 1}, false},
		{"E36/delete/drop deep pair, no far leaf", 0x5a, [][]byte{slotPath(5, 5, 1), slotPath(5, 5, 2)}, []int{0, 1}, false},
		{"E36/delete/drop all but one", 0x5a, [][]byte{slotPath(7), slotPath(8), slotPath(9)}, []int{0, 2}, false},
		{"E35/E36/reinsert", 0x7c, [][]byte{slotPath(2), slotPath(3, 1), slotPath(3, 2)}, []int{1}, true},
		{"E36/E59/exact-paths/2", 0x42, [][]byte{slotPath(5, 5, 1), slotPath(5, 6)}, nil, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			addr := [32]byte{tc.addr}
			ctx := runner.NewMemory(runner.ContextSpec{Borrowed: true, ForbidStateReads: true})
			ops := storageOps(tc.paths)
			before := storageRound(t, ctx, addr, ops)
			records := liveStorageRecords(ctx.Records())
			state := make(commitmenttest.State)
			state.Apply(ops)
			requireStorageState(t, ctx, addr, state)
			if tc.drop == nil {
				return
			}
			deletes := make([]commitmenttest.Op, len(tc.drop))
			for i, at := range tc.drop {
				deletes[i] = del(tc.paths[at])
			}
			root := storageRound(t, ctx, addr, deletes)
			state.Apply(deletes)
			requireStorageState(t, ctx, addr, state)
			if tc.reinsert {
				root = storageRound(t, ctx, addr, []commitmenttest.Op{ops[tc.drop[0]]})
				require.Equal(t, before, root)
				require.Equal(t, records, liveStorageRecords(ctx.Records()))
			} else {
				fresh := runner.NewMemory(runner.ContextSpec{})
				require.Equal(t, storageRound(t, fresh, addr, state.Ops()), root)
				requireLiveRecords(t, fresh.Records(), ctx.Records(), tc.name)
			}
		})
	}
}

func TestStorageWipe(t *testing.T) {
	for _, tc := range []struct {
		name       string
		addr       byte
		paths      [][]byte
		minRecords int
		borrowed   bool
	}{
		{"E37/E39/masks", 0xa1, [][]byte{commitmenttest.Path([]byte{2, 3}, 4), commitmenttest.Path([]byte{2, 5}, 6)}, 2, false},
		{"E37/missing", 0xb2, nil, 0, false},
		{"E33/E37/aliasing", 0xd4, [][]byte{
			commitmenttest.Path([]byte{1, 1}, 4), commitmenttest.Path([]byte{1, 2}, 4),
			commitmenttest.Path([]byte{1, 3}, 4), commitmenttest.Path([]byte{1, 4}, 4),
			commitmenttest.Path([]byte{5, 7, 1}, 6), commitmenttest.Path([]byte{5, 7, 2}, 6),
		}, 3, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			addr := [32]byte{tc.addr}
			ctx := newMockContext()
			if tc.paths != nil {
				ops := make([]commitmenttest.Op, len(tc.paths))
				for i, path := range tc.paths {
					size := 1
					if tc.borrowed {
						size = 32
					}
					ops[i] = commitmenttest.Op{Key: path, Storage: bytes.Repeat([]byte{byte(i + 1)}, size)}
				}
				storageRound(t, ctx, addr, ops)
			}
			before := slices.Collect(maps.Keys(ctx.branches))
			require.GreaterOrEqual(t, len(before), tc.minRecords)
			ctx.branchCalls, ctx.accountCalls, ctx.storageCalls = nil, 0, 0
			if tc.borrowed {
				ctx.branchBuf = make([]byte, 0, 4096)
			}
			_, err := runStorageTask(ctx, storageTask{addrHash: addr, wipe: true})
			require.NoError(t, err)
			for _, key := range before {
				require.Contains(t, ctx.branches, key)
				require.Empty(t, ctx.branches[key], "record %x survived the wipe", key)
			}
			require.Zero(t, ctx.accountCalls)
			require.Zero(t, ctx.storageCalls)
			if tc.paths == nil {
				require.Empty(t, ctx.branches)
			} else {
				require.Contains(t, ctx.branches, string(StorageNodeKey(addr, nil, nil)))
				require.NotEmpty(t, ctx.branchCalls)
			}
		})
	}
	t.Run("E25/E37/malformed-child", func(t *testing.T) {
		var address [32]byte
		address[0] = 0xc3
		root := fork(nil)
		root.setStoredChild(2, bytes.Repeat([]byte{1}, 32), nil)
		root.setStoredChild(3, bytes.Repeat([]byte{2}, 32), nil)
		ctx := newMockContext()
		ctx.branches[string(StorageNodeKey(address, nil, nil))] = encodeRecord(root, 0, nil)
		ctx.branches[string(StorageNodeKey(address, []byte{2}, nil))] = []byte{recordFormat}

		_, err := runStorageTask(ctx, storageTask{addrHash: address, wipe: true})
		require.ErrorIs(t, err, ErrRecordTruncated)
	})
}

func TestDeletedAccountWipesStorageWrittenInTheSameBatch(t *testing.T) {
	addr := parityAddress(7)
	slotA, slotB := commitmenttest.SlotKey(addr, paritySlot(8)), commitmenttest.SlotKey(addr, paritySlot(9))
	differential(t, commitmenttest.Case{
		ID: "delete-with-slot-write",
		Rounds: [][]commitmenttest.Op{
			{commitmenttest.AccountAt(addr, commitmenttest.AccountSpec{Kind: "parity", Number: 7}), commitmenttest.StorageAt(slotA, 8), commitmenttest.StorageAt(slotB, 9)},
			{commitmenttest.StorageAt(slotA, 80), commitmenttest.DeleteAt(addr)},
			{commitmenttest.AccountAt(addr, commitmenttest.AccountSpec{Kind: "parity", Number: 70})},
		},
		Assertions: commitmenttest.Assertions{TolerateHPHDrift: true},
	}, []runner.RunSpec{{Name: "v3", Mode: commitment.ModeCollect, Workers: 1}})
}

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
		entries := make([]commitmenttest.Op, 0, 16)
		for i := 0; i < 256 && len(entries) < 16; i++ {
			address := parityAddress(i)
			if commitment.KeyToHexNibbleHash(address)[0] != first {
				continue
			}
			entries = append(entries, parityAccount(address, i), commitmenttest.StorageAt(commitmenttest.SlotKey(address, paritySlot(i)), i))
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
		runV3(t, ctx, v3Config{workers: 1}, []commitmenttest.Op{parityAccount(address, 3), commitmenttest.StorageAt(commitmenttest.SlotKey(address, paritySlot(3)), 3)})
		before := storageRoot()
		runV3(t, ctx, v3Config{workers: 1}, []commitmenttest.Op{{Key: address, Account: &commitmenttest.AccountValue{Fields: commitmenttest.BalanceField, Balance: *uint256.NewInt(99)}}})
		require.Equal(t, before, storageRoot())
	})

	whale := benchAddr(7)
	whaleSeed := []commitmenttest.Op{parityAccount(whale, 7)}
	whaleNext := []commitmenttest.Op{parityAccount(whale, 8)}
	for i := range 5 * defaultStorageFanOutMin {
		slot := commitmenttest.SlotKey(whale, benchSlot(i))
		switch {
		case i >= 4*defaultStorageFanOutMin:
			whaleNext = append(whaleNext, commitmenttest.StorageAt(slot, i))
		case i%3 == 0:
			whaleNext = append(whaleNext, commitmenttest.DeleteAt(slot))
		case i%3 == 1:
			whaleNext = append(whaleNext, commitmenttest.StorageAt(slot, i+1))
		}
		if i < 4*defaultStorageFanOutMin {
			whaleSeed = append(whaleSeed, commitmenttest.StorageAt(slot, i))
		}
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
	groupSeed := make([]commitmenttest.Op, 0, 2*len(dense))
	for _, i := range append(append([]int(nil), dense...), sparse...) {
		groupSeed = append(groupSeed, parityAccount(parityAddress(i), i))
		if i%3 == 0 {
			groupSeed = append(groupSeed, commitmenttest.StorageAt(commitmenttest.SlotKey(parityAddress(i), paritySlot(i)), i))
		}
	}
	groupNext := []commitmenttest.Op{commitmenttest.DeleteAt(parityAddress(sparse[0])), commitmenttest.DeleteAt(parityAddress(dense[3]))}
	for k, i := range dense[10:400] {
		groupNext = append(groupNext, parityAccount(parityAddress(i), i+1))
		if k%4 == 0 {
			groupNext = append(groupNext, commitmenttest.StorageAt(commitmenttest.SlotKey(parityAddress(i), paritySlot(i+7)), i+2))
		}
	}
	for i := 100000; i < 100200; i++ {
		if commitment.KeyToHexNibbleHash(parityAddress(i))[0] != 0 {
			groupNext = append(groupNext, parityAccount(parityAddress(i), i))
		}
	}

	for _, tc := range []struct {
		name   string
		base   []commitmenttest.Op
		rounds [][]commitmenttest.Op
		a, b   v3Config
	}{
		{"deferred_matches_inline", nil, [][]commitmenttest.Op{storage}, v3Config{}, v3Config{deferred: true}},
		{"account_fold_parallel_matches_serial", nil, [][]commitmenttest.Op{benchEntries("storage", 512)}, v3Config{workers: 1}, v3Config{workers: 8}},
		{"storage_fan_out_matches_serial", nil, [][]commitmenttest.Op{whaleSeed, whaleNext}, v3Config{workers: 1}, v3Config{workers: 8}},
		{"pipelined_account_groups_match_serial", groupSeed, [][]commitmenttest.Op{groupNext}, v3Config{workers: 1}, v3Config{workers: 4}},
	} {
		t.Run(tc.name, func(t *testing.T) { requireSameRuns(t, tc.base, tc.a, tc.b, tc.rounds...) })
	}
}

type leafRefContext struct {
	*mockContext
	corrupt bool
}

func (c *leafRefContext) LeafRefs(key, data []byte) *commitment.LeafRefs {
	refs := ComputeLeafRefs(key, data)
	if refs != nil && c.corrupt {
		for i := range refs.Refs {
			refs.Refs[i][0] ^= 0xff
		}
	}
	return refs
}

func TestPrefetch(t *testing.T) {
	serial := v3Config{workers: 1}
	seeded := func(t *testing.T, contracts int) (*mockContext, *Trie) {
		seed := make([]commitmenttest.Op, 0, contracts*3)
		for i := range contracts {
			seed = append(seed, parityAccount(benchAddr(i), i))
			for j := range 2 {
				seed = append(seed, commitmenttest.StorageAt(commitmenttest.SlotKey(benchAddr(i), benchSlot(i*2+j)), i+j))
			}
		}
		ctx := newMockContext()
		tr := &Trie{}
		t.Cleanup(tr.Release)
		runV3(t, ctx, v3Config{workers: 1, trie: tr}, seed)
		return ctx, tr
	}

	t.Run("path_covers_round_reads", func(t *testing.T) {
		const contracts = 3000
		ctx, tr := seeded(t, contracts)
		cases := []struct{ account, slot int }{
			{7, 14},
			{1234, 2468},
			{2999, 5998},
			{contracts + 5, 0},
			{11, contracts*2 + 11},
			{contracts + 17, contracts*2 + 17},
		}
		for i := range contracts {
			first, second := keccak.Sum256(benchSlot(i*2)), keccak.Sum256(benchSlot(i*2+1))
			if first[0]>>4 == second[0]>>4 {
				cases = append(cases, struct{ account, slot int }{i, i * 2})
				break
			}
		}
		require.Len(t, cases, 7)
		for _, c := range cases {
			addr, slot := benchAddr(c.account), benchSlot(c.slot)
			addrHash, slotHash := keccak.Sum256(addr), keccak.Sum256(slot)
			prefetched := map[string]bool{}
			read := func(key []byte) []byte {
				prefetched[string(key)] = true
				return ctx.branches[string(key)]
			}
			PrefetchPath(read, addrHash[:], nil, 0)
			PrefetchPath(read, addrHash[:], slotHash[:], 64)

			ctx.branchCalls = nil
			runV3(t, ctx, v3Config{workers: 1, trie: tr}, []commitmenttest.Op{
				parityAccount(addr, c.account+1),
				commitmenttest.StorageAt(commitmenttest.SlotKey(addr, slot), c.slot+5),
			})
			require.NotEmpty(t, ctx.branchCalls)
			for _, key := range ctx.branchCalls {
				require.Truef(t, prefetched[string(key)], "account %d slot %d: round read %x was not prefetched", c.account, c.slot, key)
			}
		}
	})

	t.Run("leaf_refs_keep_roots_and_records", func(t *testing.T) {
		const contracts = 2000
		base, _ := seeded(t, contracts)
		next := make([]commitmenttest.Op, 0, 600)
		for i := 0; i < contracts; i += 10 {
			next = append(next,
				parityAccount(benchAddr(i), i+3),
				commitmenttest.StorageAt(commitmenttest.SlotKey(benchAddr(i), benchSlot(i*2)), i+7),
				parityAccount(benchAddr(contracts+i), i+11))
		}
		run := func(refs, corrupt bool) ([]byte, map[string][]byte) {
			clone := newMockContext()
			maps.Copy(clone.branches, base.branches)
			var ctx commitment.PatriciaContext = clone
			if refs {
				ctx = &leafRefContext{mockContext: clone, corrupt: corrupt}
			}
			roots, _, _ := runV3(t, ctx, serial, next)
			return roots[0], clone.branches
		}
		plainRoot, plainBranches := run(false, false)
		refRoot, refBranches := run(true, false)
		require.Equal(t, plainRoot, refRoot)
		require.Equal(t, plainBranches, refBranches)

		corruptRoot, _ := run(true, true)
		require.NotEqual(t, plainRoot, corruptRoot)
	})
}

func TestWarmup(t *testing.T) {
	var scratch [66]byte
	accountRecords := func() map[string][]byte {
		return map[string][]byte{
			string(AccountNodeKey(nil, nil)):       commitmenttest.Record(&commitmenttest.RecordSpec{ChildMask: 1 << 2}),
			string(AccountNodeKey([]byte{2}, nil)): commitmenttest.Record(&commitmenttest.RecordSpec{ChildMask: 1 << 3, LeafMask: 1 << 3}),
		}
	}

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
		record := commitmenttest.Record(&commitmenttest.RecordSpec{Flags: hdrHasSelfExt, ChildMask: 1 << 4, SelfExtension: extFixture([]byte{1, 2})})
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
		record := commitmenttest.Record(&commitmenttest.RecordSpec{Flags: hdrHasChildExt, ChildMask: 1 << 4, ExtensionMask: 1 << 4, Extensions: [16][]byte{4: extFixture([]byte{1, 2})}})
		hashedKey := make([]byte, 128)
		hashedKey[64] = 4
		nextDepth, stop := warmupStepV3(record, hashedKey, 64)
		require.False(t, stop)
		require.Equal(t, 67, nextDepth)

		leafRecord := commitmenttest.Record(&commitmenttest.RecordSpec{ChildMask: 1 << 4, LeafMask: 1 << 4, Leaves: [16]commitmenttest.Leaf{4: {Suffix: bytes.Repeat([]byte{0x0c}, 31), Value: []byte{9}}}})
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
}

func TestMetrics(t *testing.T) {
	names := []string{"commitment_keys_total", "commitment_branch_read_bytes_total", "commitment_branch_write_bytes_total", "commitment_branch_writes_total"}
	read := func() []uint64 {
		values := make([]uint64, len(names))
		for i, name := range names {
			values[i] = metrics.GetOrCreateCounter(name).GetValueUint64()
		}
		return values
	}

	t.Run("process", func(t *testing.T) {
		c := newShardedContext()
		cfg := v3Config{trie: &Trie{}}
		defer cfg.trie.Release()
		runV3(t, c, cfg, benchEntries("storage", 64))
		grown := benchEntries("storage", 128)
		before := read()
		runV3(t, c, cfg, grown)
		after := read()
		require.EqualValues(t, len(grown), after[0]-before[0], "commitment_keys_total counts each key of the round once")
		for i := 1; i < len(names); i++ {
			require.Positive(t, after[i]-before[i], "%s moved", names[i])
		}
	})

	t.Run("deferred_bills_branch_writes", func(t *testing.T) {
		before := read()
		runV3(t, newShardedContext(), v3Config{deferred: true}, benchEntries("storage", 64))
		after := read()
		for i := 2; i < len(names); i++ {
			require.Positive(t, after[i]-before[i], "deferred rounds still bill %s", names[i])
		}
	})
}

type slotFactory struct {
	ctx   commitment.PatriciaContext
	slots chan struct{}
}

func (f *slotFactory) factory(ctx context.Context) (commitment.PatriciaContext, func()) {
	if kv.IsNonBlockingAcquire(ctx) {
		select {
		case f.slots <- struct{}{}:
		default:
			return nil, nil
		}
	} else {
		select {
		case f.slots <- struct{}{}:
		case <-ctx.Done():
			return nil, nil
		}
	}
	return f.ctx, func() { <-f.slots }
}

func requireFinishes(t *testing.T, label string, run func() error) {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- run() }()
	select {
	case err := <-done:
		require.NoError(t, err, label)
	case <-time.After(10 * time.Second):
		t.Fatalf("%s: hung with a two-slot context factory", label)
	}
}

func TestBoundedContextFactoryNeverDeadlocks(t *testing.T) {
	entries := benchEntries("storage", 40)
	updates := updatesOf(entries)
	for i := range 20 {
		c := newShardedContext()
		f := &slotFactory{ctx: c, slots: make(chan struct{}, 2)}
		tr := &Trie{scheduleWorkers: 2}
		tr.ResetContext(c)
		tr.SetTrieContextFactory(f.factory)
		u := benchUpdatesIn(t.TempDir(), commitment.ModeCollect, entries, updates)
		requireFinishes(t, fmt.Sprintf("process run %d", i), func() error {
			_, err := tr.Process(context.Background(), u, "", nil, commitment.WarmupConfig{})
			return err
		})
		tr.Release()
	}

	paths := make([][]byte, 64)
	for i := range paths {
		paths[i] = commitmenttest.Path([]byte{byte(i % 16), byte(i / 16)}, 5)
	}
	tasks := []storageTask{storageTaskFor([32]byte{1}, storageOps(paths)), storageTaskFor([32]byte{2}, storageOps(paths))}
	for i := range 20 {
		c := newShardedContext()
		f := &slotFactory{ctx: c, slots: make(chan struct{}, 2)}
		roots, parts := make([][32]byte, len(tasks)), make([]deltaParts, len(tasks))
		requireFinishes(t, fmt.Sprintf("storage phase run %d", i), func() error {
			return runStoragePhase(context.Background(), c, f.factory, tasks, roots, parts, 2, 16)
		})
	}
}
