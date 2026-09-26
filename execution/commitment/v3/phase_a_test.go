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
	"maps"
	"slices"
	"testing"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/empty"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/internal/commitmenttest"
	"github.com/erigontech/erigon/internal/commitmenttest/runner"
	"github.com/stretchr/testify/require"
)

func TestPartition(t *testing.T) {
	path := func(n byte) []byte { return bytes.Repeat([]byte{n}, 64) }
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
		{"E35/PartitionLaterStorageWriteSuppressesWipe", []phaseAInput{{hashedKey: path(4), update: deleted}, {hashedKey: append(path(4), path(5)...), update: phaseAStorageUpdate([]byte{1})}}, []accountEntry{{hashedKey: path(4), update: &commitment.Update{Flags: commitment.DeleteUpdate}, storageDirty: true}}, 1, false},
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
			storageRound(t, ctx, storage[0].addrHash, []commitmenttest.Op{{Key: append([]byte{1}, bytes.Repeat([]byte{2}, 63)...), Storage: []byte{1}}})
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
		gotStorage, gotAccounts, seen, err := partitionFeed(slices.Clone(items), 8, nil)
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
	path := func(first, fill byte) []byte { return append([]byte{first}, bytes.Repeat([]byte{fill}, 63)...) }
	put := func(p []byte, value byte) commitmenttest.Op { return commitmenttest.Op{Key: p, Storage: []byte{value}} }
	del := func(p []byte) commitmenttest.Op { return commitmenttest.Op{Key: p, Delete: true} }
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
		{"E36/E59/exact-paths/0", 0x40, [][]byte{slotPath(5, 5, 1), slotPath(5, 5, 2), slotPath(5, 6)}, nil, false},
		{"E36/E59/exact-paths/1", 0x41, [][]byte{slotPath(1, 2, 3), slotPath(1, 2, 4), slotPath(9)}, nil, false},
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
				got := liveStorageRecords(ctx.Records())
				for key, value := range liveStorageRecords(fresh.Records()) {
					require.Equal(t, value, got[key], "record %x must match the fresh trie's", key)
				}
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
		{"E37/E39/masks", 0xa1, [][]byte{append([]byte{2, 3}, bytes.Repeat([]byte{4}, 62)...), append([]byte{2, 5}, bytes.Repeat([]byte{6}, 62)...)}, 2, false},
		{"E37/missing", 0xb2, nil, 0, false},
		{"E33/E37/aliasing", 0xd4, [][]byte{
			append([]byte{1, 1}, bytes.Repeat([]byte{4}, 62)...), append([]byte{1, 2}, bytes.Repeat([]byte{4}, 62)...),
			append([]byte{1, 3}, bytes.Repeat([]byte{4}, 62)...), append([]byte{1, 4}, bytes.Repeat([]byte{4}, 62)...),
			append([]byte{5, 7, 1}, bytes.Repeat([]byte{6}, 61)...), append([]byte{5, 7, 2}, bytes.Repeat([]byte{6}, 61)...),
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
