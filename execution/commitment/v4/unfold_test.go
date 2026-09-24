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
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/commitment"
)

func TestUnfoldReadsOneExactRecordWithoutStateReads(t *testing.T) {
	ctx := newMockContext()
	path := []byte{1, 2, 3}
	hash := bytes.Repeat([]byte{0xab}, 32)
	n := fork(path)
	n.setStoredChild(2, hash, []byte{4, 5, 6})
	n.setLeaf(7, packPath(bytes.Repeat([]byte{9}, 60), nil), []byte{0x42})
	ctx.branches[string(AccountNodeKey(path, nil))] = encodeRecord(n, len(path), nil)

	got, err := unfold(ctx, path, planeAccount, nil)
	require.NoError(t, err)
	require.Equal(t, [][]byte{AccountNodeKey(path, nil)}, ctx.branchCalls)
	require.Equal(t, 0, ctx.accountCalls)
	require.Equal(t, 0, ctx.storageCalls)
	require.Equal(t, path, got.path)
	require.Equal(t, hash, got.childHashAt(2))
	require.Equal(t, []byte{4, 5, 6}, got.childExtAt(2))
	require.Equal(t, []byte{0x42}, got.leafValueAt(7))
}

func TestUnfoldStorageUsesAddressQualifiedKey(t *testing.T) {
	ctx := newMockContext()
	path := []byte{4, 5}
	var addrHash [32]byte
	addrHash[0] = 0x9a
	hash := bytes.Repeat([]byte{0xcd}, 32)
	n := fork(path)
	n.setStoredChild(6, hash, nil)
	ctx.branches[string(StorageNodeKey(addrHash, path, nil))] = encodeRecord(n, len(path), nil)

	got, err := unfold(ctx, path, planeStorage, addrHash[:])
	require.NoError(t, err)
	require.Equal(t, [][]byte{StorageNodeKey(addrHash, path, nil)}, ctx.branchCalls)
	require.Equal(t, path, got.path)
	require.Equal(t, hash, got.childHashAt(6))
}

func TestUnfoldRootForms(t *testing.T) {
	t.Run("leaf root", func(t *testing.T) {
		ctx := newMockContext()
		fullPath := bytes.Repeat([]byte{3}, 64)
		n := fork(nil)
		n.setLeaf(int(fullPath[0]), packPath(fullPath[1:], nil), []byte{0x01, 0x02})
		ctx.branches[string(AccountRootKey())] = encodeRecord(n, 0, nil)

		got, err := unfold(ctx, nil, planeAccount, nil)
		require.NoError(t, err)
		require.Equal(t, []byte{0x01, 0x02}, got.leafValueAt(3))
		require.Equal(t, packPath(fullPath[1:], nil), got.leafSuffixAt(3))
	})

	t.Run("extension root", func(t *testing.T) {
		ctx := newMockContext()
		ext := []byte{1, 2, 3}
		hash := bytes.Repeat([]byte{0x55}, 32)
		n := fork(ext)
		n.setStoredChild(4, hash, nil)
		ctx.branches[string(AccountRootKey())] = encodeRecord(n, 0, nil)

		got, err := unfold(ctx, nil, planeAccount, nil)
		require.NoError(t, err)
		require.Equal(t, ext, got.path)
		require.Equal(t, hash, got.childHashAt(4))
	})
}

func TestUnfoldMissingAndTombstone(t *testing.T) {
	ctx := newMockContext()
	got, err := unfold(ctx, nil, planeAccount, nil)
	require.NoError(t, err)
	require.Nil(t, got)

	ctx.branches[string(AccountRootKey())] = []byte{}
	got, err = unfold(ctx, nil, planeAccount, nil)
	require.NoError(t, err)
	require.NotNil(t, got)
	require.Empty(t, got.childMask)
}

func TestUnfoldRejectsMalformedRecord(t *testing.T) {
	ctx := newMockContext()
	ctx.branches[string(AccountRootKey())] = []byte{recordFormat}
	got, err := unfold(ctx, nil, planeAccount, nil)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrRecordTruncated)
	require.Nil(t, got)
}

func TestUnfoldRejectsInvalidPlaneAndAddress(t *testing.T) {
	ctx := newMockContext()
	_, err := unfold(ctx, nil, 0xff, nil)
	require.Error(t, err)
	_, err = unfold(ctx, nil, planeStorage, nil)
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrUnfoldAddress))
}

type ownedBranchContext struct{ *mockContext }

func (c ownedBranchContext) BranchOwned(key []byte) ([]byte, kv.Step, error) {
	return c.branches[string(key)], 0, nil
}

func TestUnfoldKeepsOwnedBranchBytes(t *testing.T) {
	m := newMockContext()
	address := [32]byte{7}
	_, err := runStorageTask(m, storageTask{addrHash: address, entries: []storageEntry{
		entryOf(append([]byte{1}, bytes.Repeat([]byte{2}, 63)...), phaseAStorageUpdate([]byte{1})),
		entryOf(append([]byte{3}, bytes.Repeat([]byte{4}, 63)...), phaseAStorageUpdate([]byte{2})),
	}})
	require.NoError(t, err)
	stored := m.branches[string(StorageRootKey(address))]
	require.NotEmpty(t, stored)

	for _, ctx := range []commitment.PatriciaContext{ownedBranchContext{m}, newMeteredContext(ownedBranchContext{m})} {
		n, err := unfold(ctx, nil, planeStorage, address[:])
		require.NoError(t, err)
		require.Same(t, &stored[0], &n.raw[0])
	}

	n, err := unfold(m, nil, planeStorage, address[:])
	require.NoError(t, err)
	require.NotSame(t, &m.branchBuf[0], &n.raw[0])
}
