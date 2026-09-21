// Copyright 2026 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful, but WITHOUT ANY
// WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
// FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License for
// more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package v4

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/empty"
	"github.com/erigontech/erigon/execution/commitment"
)

func phaseAStorageUpdate(value []byte) *commitment.Update {
	update := &commitment.Update{Flags: commitment.StorageUpdate, StorageLen: int8(len(value))}
	copy(update.Storage[:], value)
	return update
}

func phaseAInputFor(path []byte, update *commitment.Update) phaseAInput {
	hashed := append([]byte(nil), path...)
	return phaseAInput{hashedKey: hashed, update: update}
}

func TestPartitionStoragePrefixAndAccounts(t *testing.T) {
	accountA := bytes.Repeat([]byte{1}, 64)
	accountB := bytes.Repeat([]byte{2}, 64)
	stream := []phaseAInput{
		{hashedKey: append(append([]byte(nil), accountA...), bytes.Repeat([]byte{3}, 64)...), plainKey: bytes.Repeat([]byte{0xa}, 52), update: phaseAStorageUpdate([]byte{1})},
		{hashedKey: append(append([]byte(nil), accountA...), bytes.Repeat([]byte{4}, 64)...), plainKey: bytes.Repeat([]byte{0xa}, 52), update: phaseAStorageUpdate([]byte{2})},
		{hashedKey: accountA, plainKey: bytes.Repeat([]byte{0xa}, 20), update: &commitment.Update{Flags: commitment.NonceUpdate, Nonce: 7}},
		{hashedKey: accountB, plainKey: bytes.Repeat([]byte{0xb}, 20)},
	}

	storage, accounts := partition(stream)
	require.Len(t, storage, 1)
	require.Len(t, storage[0].entries, 2)
	require.Len(t, accounts, 2)
	require.Equal(t, accountA, accounts[0].hashedKey)
	require.Equal(t, uint64(7), accounts[0].update.Nonce)
	require.Equal(t, packPath(accountA, nil), storage[0].addrHash[:])
	require.Equal(t, accountB, accounts[1].hashedKey)
	require.Nil(t, accounts[1].update)
	require.True(t, accounts[0].storageDirty)
}

func TestPhaseAStorageTransitions(t *testing.T) {
	ctx := newMockContext()
	var address [32]byte
	address[0] = 0x42
	pathA := append([]byte{1}, bytes.Repeat([]byte{2}, 63)...)
	pathB := append([]byte{1}, bytes.Repeat([]byte{3}, 63)...)

	root, err := runStorageTask(ctx, storageTask{addrHash: address, entries: []storageEntry{{path: pathA, update: phaseAStorageUpdate([]byte{1})}}})
	require.NoError(t, err)
	require.NotEqual(t, empty.RootHash, root)
	rootData := ctx.branches[string(StorageRootKey(address))]
	require.NoError(t, Validate(rootData, 0))
	require.True(t, NewRecord(rootData, 0).isLeafRoot())

	_, err = runStorageTask(ctx, storageTask{addrHash: address, entries: []storageEntry{{path: pathB, update: phaseAStorageUpdate([]byte{2})}}})
	require.NoError(t, err)
	rootData = ctx.branches[string(StorageRootKey(address))]
	require.NoError(t, Validate(rootData, 0))
	require.False(t, NewRecord(rootData, 0).isLeafRoot())

	_, err = runStorageTask(ctx, storageTask{addrHash: address, entries: []storageEntry{{path: pathB, update: &commitment.Update{Flags: commitment.DeleteUpdate}}}})
	require.NoError(t, err)
	rootData = ctx.branches[string(StorageRootKey(address))]
	require.NoError(t, Validate(rootData, 0))
	require.True(t, NewRecord(rootData, 0).isLeafRoot())

	_, err = runStorageTask(ctx, storageTask{addrHash: address, entries: []storageEntry{{path: pathA, update: &commitment.Update{Flags: commitment.DeleteUpdate}}}})
	require.NoError(t, err)
	require.Empty(t, ctx.branches[string(StorageRootKey(address))])
}

func TestPhaseAStorageOnlyUpdateKeepsAccountEntrySeparate(t *testing.T) {
	account := bytes.Repeat([]byte{0x9}, 64)
	path := append(append([]byte(nil), account...), bytes.Repeat([]byte{0x2}, 64)...)
	storage, accounts := partition([]phaseAInput{{hashedKey: path, update: phaseAStorageUpdate([]byte{0xaa})}})
	require.Len(t, storage, 1)
	require.Len(t, accounts, 1)
	require.Nil(t, accounts[0].update)
	require.True(t, accounts[0].storageDirty)
}

func TestPhaseAD10StorageRootIsIndependentOfTheCurrentBatch(t *testing.T) {
	var address [32]byte
	address[0] = 0x91
	pathA := append([]byte{4}, bytes.Repeat([]byte{5}, 63)...)
	pathB := append([]byte{6}, bytes.Repeat([]byte{7}, 63)...)
	ctx := newMockContext()
	_, err := runStorageTask(ctx, storageTask{addrHash: address, entries: []storageEntry{{path: pathA, update: phaseAStorageUpdate([]byte{1})}}})
	require.NoError(t, err)
	secondRoot, err := runStorageTask(ctx, storageTask{addrHash: address, entries: []storageEntry{{path: pathB, update: phaseAStorageUpdate([]byte{2})}}})
	require.NoError(t, err)

	bulk := newMockContext()
	bulkRoot, err := runStorageTask(bulk, storageTask{addrHash: address, entries: []storageEntry{{path: pathA, update: phaseAStorageUpdate([]byte{1})}, {path: pathB, update: phaseAStorageUpdate([]byte{2})}}})
	require.NoError(t, err)
	require.Equal(t, bulkRoot, secondRoot)
	require.Equal(t, bulk.branches[string(StorageRootKey(address))], ctx.branches[string(StorageRootKey(address))])
}

func TestPhaseAStorageWipeThenReinsert(t *testing.T) {
	var address [32]byte
	address[0] = 0x37
	path := append([]byte{8}, bytes.Repeat([]byte{9}, 63)...)
	ctx := newMockContext()
	initial, err := runStorageTask(ctx, storageTask{addrHash: address, entries: []storageEntry{{path: path, update: phaseAStorageUpdate([]byte{0x11})}}})
	require.NoError(t, err)
	final, err := runStorageTask(ctx, storageTask{addrHash: address, entries: []storageEntry{
		{path: path, update: &commitment.Update{Flags: commitment.DeleteUpdate}},
		{path: path, update: phaseAStorageUpdate([]byte{0x22})},
	}})
	require.NoError(t, err)
	require.NotEqual(t, initial, final)
	require.Equal(t, []byte{0x22}, func() []byte {
		_, value := NewRecord(ctx.branches[string(StorageRootKey(address))], 0).LeafRootBody()
		return value
	}())
}

func TestPhaseAStorageTwoToOneTombstonesOrphanedChild(t *testing.T) {
	var address [32]byte
	address[0] = 0x61
	child := fork([]byte{2})
	child.plane = planeStorage
	pathA := append([]byte{2, 3}, bytes.Repeat([]byte{4}, 62)...)
	pathB := append([]byte{2, 5}, bytes.Repeat([]byte{6}, 62)...)
	child.setLeaf(3, packPath(pathA[2:], nil), []byte{1})
	child.setLeaf(5, packPath(pathB[2:], nil), []byte{2})
	childHash, err := fold(child, 1)
	require.NoError(t, err)
	rootPath := append([]byte{9}, bytes.Repeat([]byte{8}, 63)...)
	root := fork(nil)
	root.plane = planeStorage
	root.setStoredChild(2, childHash[:], nil)
	root.setLeaf(9, packPath(rootPath[1:], nil), []byte{3})
	ctx := newMockContext()
	ctx.branches[string(StorageRootKey(address))] = encodeRecord(root, 0, nil)
	ctx.branches[string(StorageNodeKey(address, []byte{2}, nil))] = encodeRecord(child, 1, nil)

	_, err = runStorageTask(ctx, storageTask{addrHash: address, entries: []storageEntry{
		{path: pathB, update: &commitment.Update{Flags: commitment.DeleteUpdate}},
		{path: rootPath, update: &commitment.Update{Flags: commitment.DeleteUpdate}},
	}})
	require.NoError(t, err)
	require.Empty(t, ctx.branches[string(StorageNodeKey(address, []byte{2}, nil))])
	require.True(t, NewRecord(ctx.branches[string(StorageRootKey(address))], 0).isLeafRoot())
}
