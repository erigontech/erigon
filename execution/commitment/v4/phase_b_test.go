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
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser
// General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package v4

import (
	"bytes"
	"sort"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/empty"
	"github.com/erigontech/erigon/execution/commitment"
)

func TestRunAccountTrieStorageOnlyUpdate(t *testing.T) {
	address := bytes.Repeat([]byte{0x11}, 20)
	accountPath := commitment.KeyToHexNibbleHash(address)
	ctx := newMockContext()
	initial := fullAccountUpdate(7, 21, common.HexToHash("0x1234"))
	_, err := runAccountTrie(ctx, []accountEntry{{hashedKey: accountPath, plainKey: address, update: &initial}}, nil)
	require.NoError(t, err)

	slot := bytes.Repeat([]byte{0x22}, 32)
	storagePath := commitment.KeyToNibblizedHash(slot)
	addrHash := hashAddressPath(accountPath)
	storageRoot, err := runStorageTask(ctx, storageTask{addrHash: addrHash, entries: []storageEntry{{path: storagePath, update: phaseAStorageUpdate([]byte{0xaa})}}})
	require.NoError(t, err)

	ctx.accountCalls = 0
	ctx.storageCalls = 0
	got, err := runAccountTrie(ctx, []accountEntry{{hashedKey: accountPath, plainKey: address, storageDirty: true}}, map[[32]byte][32]byte{addrHash: storageRoot})
	require.NoError(t, err)
	require.Zero(t, ctx.accountCalls)
	require.Zero(t, ctx.storageCalls)

	final := initial
	want := hexPatriciaRoot(t, planeAccount, [][]byte{address, append(append([]byte(nil), address...), slot...)}, make([][]byte, 2), []commitment.Update{final, storageUpdate(0xaa)})
	require.Equal(t, want, got[:])
	_, _, _, gotStorageRoot, err := decodeAccountLeaf(accountLeafFromContext(t, ctx, accountPath))
	require.NoError(t, err)
	require.Equal(t, storageRoot[:], gotStorageRoot)
}

func TestRunAccountTrieFieldOnlyUpdate(t *testing.T) {
	address := bytes.Repeat([]byte{0x33}, 20)
	path := commitment.KeyToHexNibbleHash(address)
	ctx := newMockContext()
	initial := fullAccountUpdate(2, 3, common.HexToHash("0x1111"))
	_, err := runAccountTrie(ctx, []accountEntry{{hashedKey: path, plainKey: address, update: &initial}}, nil)
	require.NoError(t, err)

	partial := &commitment.Update{Flags: commitment.BalanceUpdate, Balance: *uint256.NewInt(99)}
	ctx.accountCalls = 0
	ctx.storageCalls = 0
	got, err := runAccountTrie(ctx, []accountEntry{{hashedKey: path, plainKey: address, update: partial}}, nil)
	require.NoError(t, err)
	require.Zero(t, ctx.accountCalls)
	require.Zero(t, ctx.storageCalls)

	final := fullAccountUpdate(initial.Nonce, 99, initial.CodeHash)
	want := hexPatriciaRoot(t, planeAccount, [][]byte{address}, make([][]byte, 1), []commitment.Update{final})
	require.Equal(t, want, got[:])
}

func TestRunAccountTrieCombinedFieldAndStorageUpdate(t *testing.T) {
	address := bytes.Repeat([]byte{0x44}, 20)
	accountPath := commitment.KeyToHexNibbleHash(address)
	slot := bytes.Repeat([]byte{0x55}, 32)
	slotPath := commitment.KeyToNibblizedHash(slot)
	addrHash := hashAddressPath(accountPath)
	ctx := newMockContext()
	storageRoot, err := runStorageTask(ctx, storageTask{addrHash: addrHash, entries: []storageEntry{{path: slotPath, update: phaseAStorageUpdate([]byte{0xbb})}}})
	require.NoError(t, err)

	update := fullAccountUpdate(8, 13, common.HexToHash("0x2222"))
	got, err := runAccountTrie(ctx, []accountEntry{{hashedKey: accountPath, plainKey: address, update: &update, storageDirty: true}}, map[[32]byte][32]byte{addrHash: storageRoot})
	require.NoError(t, err)
	require.Zero(t, ctx.accountCalls)
	require.Zero(t, ctx.storageCalls)

	want := hexPatriciaRoot(t, planeAccount, [][]byte{address, append(append([]byte(nil), address...), slot...)}, make([][]byte, 2), []commitment.Update{update, storageUpdate(0xbb)})
	require.Equal(t, want, got[:])
}

func TestRunAccountTrieNewAccountWithStorage(t *testing.T) {
	address := bytes.Repeat([]byte{0x66}, 20)
	accountPath := commitment.KeyToHexNibbleHash(address)
	slot := bytes.Repeat([]byte{0x77}, 32)
	slotPath := commitment.KeyToNibblizedHash(slot)
	addrHash := hashAddressPath(accountPath)
	ctx := newMockContext()
	storageRoot, err := runStorageTask(ctx, storageTask{addrHash: addrHash, entries: []storageEntry{{path: slotPath, update: phaseAStorageUpdate([]byte{0xcc})}}})
	require.NoError(t, err)

	got, err := runAccountTrie(ctx, []accountEntry{{hashedKey: accountPath, plainKey: address, storageDirty: true}}, map[[32]byte][32]byte{addrHash: storageRoot})
	require.NoError(t, err)
	want := hexPatriciaRoot(t, planeAccount, [][]byte{address, append(append([]byte(nil), address...), slot...)}, make([][]byte, 2), []commitment.Update{{Flags: commitment.BalanceUpdate | commitment.NonceUpdate | commitment.CodeUpdate, CodeHash: empty.CodeHash}, storageUpdate(0xcc)})
	require.Equal(t, want, got[:])
}

func TestRunAccountTrieMatchesHexPatriciaFor1000Accounts(t *testing.T) {
	ctx := newMockContext()
	entries := make([]accountEntry, 0, 1000)
	keys := make([][]byte, 0, 1000)
	updates := make([]commitment.Update, 0, 1000)
	for i := range 1000 {
		address := make([]byte, 20)
		address[0] = byte(i >> 8)
		address[1] = byte(i)
		address[19] = 1
		path := commitment.KeyToHexNibbleHash(address)
		update := fullAccountUpdate(uint64(i+1), uint64(i*3+1), common.HexToHash("0x1234"))
		entries = append(entries, accountEntry{hashedKey: path, plainKey: address, update: &update})
		keys = append(keys, address)
		updates = append(updates, update)
	}
	sort.Slice(entries, func(i, j int) bool { return bytes.Compare(entries[i].hashedKey, entries[j].hashedKey) < 0 })
	got, err := runAccountTrie(ctx, entries, nil)
	require.NoError(t, err)
	want := hexPatriciaRoot(t, planeAccount, keys, make([][]byte, len(keys)), updates)
	require.Equal(t, want, got[:])
}

func fullAccountUpdate(nonce, balance uint64, codeHash common.Hash) commitment.Update {
	return commitment.Update{
		Flags:    commitment.BalanceUpdate | commitment.NonceUpdate | commitment.CodeUpdate,
		Nonce:    nonce,
		Balance:  *uint256.NewInt(balance),
		CodeHash: codeHash,
	}
}

func storageUpdate(value byte) commitment.Update {
	update := commitment.Update{Flags: commitment.StorageUpdate, StorageLen: 1}
	update.Storage[0] = value
	return update
}

func accountLeafFromContext(t *testing.T, ctx *mockContext, path []byte) []byte {
	t.Helper()
	rootData := ctx.branches[string(AccountRootKey())]
	n, err := unfold(ctx, nil, planeAccount, nil, &unfoldScratch{})
	require.NoError(t, err)
	value, ok := accountLeafAt(n, path)
	require.True(t, ok)
	require.NotEmpty(t, rootData)
	return value
}
