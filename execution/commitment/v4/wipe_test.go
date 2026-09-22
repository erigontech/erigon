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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/types/accounts"
)

func TestPartitionAccountDeleteCreatesWipeJob(t *testing.T) {
	accountPath := bytes.Repeat([]byte{3}, 64)
	storage, accounts := partition([]phaseAInput{{
		hashedKey: accountPath,
		plainKey:  bytes.Repeat([]byte{0x11}, 20),
		update:    &commitment.Update{Flags: commitment.DeleteUpdate},
	}})

	require.Len(t, accounts, 1)
	require.True(t, accounts[0].update.Deleted())
	require.Len(t, storage, 1)
	require.True(t, storage[0].wipe)
	require.Empty(t, storage[0].entries)
	require.Equal(t, hashAddressPath(accountPath), storage[0].addrHash)
}

func TestPartitionLaterStorageWriteSuppressesWipe(t *testing.T) {
	accountPath := bytes.Repeat([]byte{4}, 64)
	storagePath := append(append([]byte(nil), accountPath...), bytes.Repeat([]byte{5}, 64)...)
	storage, accounts := partition([]phaseAInput{
		{hashedKey: accountPath, update: &commitment.Update{Flags: commitment.DeleteUpdate}},
		{hashedKey: storagePath, update: phaseAStorageUpdate([]byte{1})},
	})

	require.Len(t, accounts, 1)
	require.True(t, accounts[0].update.Deleted())
	require.Len(t, storage, 1)
	require.False(t, storage[0].wipe)
	require.Len(t, storage[0].entries, 1)
}

func TestDeleteThenWriteClearsWipe(t *testing.T) {
	addr := string(bytes.Repeat([]byte{0xe5}, 20))
	updates := commitment.NewUpdates(commitment.ModeUpdate, t.TempDir(), commitment.KeyToHexNibbleHash)
	updates.TouchPlainKeyDirect(addr, &commitment.Update{Flags: commitment.DeleteUpdate})
	updates.TouchPlainKeyDirect(addr, &commitment.Update{Flags: commitment.BalanceUpdate})

	var stream []phaseAInput
	err := updates.HashSort(context.Background(), nil, func(hashedKey, plainKey []byte, update *commitment.Update) error {
		stream = append(stream, phaseAInput{hashedKey: bytes.Clone(hashedKey), plainKey: bytes.Clone(plainKey), update: update})
		return nil
	})
	require.NoError(t, err)
	storage, accounts := partition(stream)
	require.Len(t, accounts, 1)
	require.False(t, accounts[0].update.Deleted())
	require.Empty(t, storage)
}

func TestSelfDestructOnlyWriteSetCreatesStorageWipe(t *testing.T) {
	addr := accounts.InternAddress(common.Address{0xd4})
	ws := &state.WriteSet{}
	ws.SetSelfDestruct(addr, &state.VersionedWrite[bool]{Val: true})
	updates := commitment.NewUpdates(commitment.ModeUpdate, t.TempDir(), commitment.KeyToHexNibbleHash)
	ws.TouchUpdates(updates)

	var stream []phaseAInput
	err := updates.HashSort(context.Background(), nil, func(hashedKey, plainKey []byte, update *commitment.Update) error {
		stream = append(stream, phaseAInput{hashedKey: bytes.Clone(hashedKey), plainKey: bytes.Clone(plainKey), update: update})
		return nil
	})
	require.NoError(t, err)
	storage, accounts := partition(stream)
	require.Len(t, accounts, 1)
	require.True(t, accounts[0].update.Deleted())
	require.Len(t, storage, 1)
	require.True(t, storage[0].wipe)

	ctx := newMockContext()
	path := append([]byte{1}, bytes.Repeat([]byte{2}, 63)...)
	_, err = runStorageTask(ctx, storageTask{addrHash: storage[0].addrHash, entries: []storageEntry{{path: path, update: phaseAStorageUpdate([]byte{1})}}})
	require.NoError(t, err)
	_, err = runStorageTask(ctx, storage[0])
	require.NoError(t, err)
	require.Empty(t, ctx.branches[string(StorageRootKey(storage[0].addrHash))])
}

func TestWipeStorageRecordsFromMasks(t *testing.T) {
	var address [32]byte
	address[0] = 0xa1
	pathA := append([]byte{2, 3}, bytes.Repeat([]byte{4}, 62)...)
	pathB := append([]byte{2, 5}, bytes.Repeat([]byte{6}, 62)...)
	ctx := newMockContext()
	_, err := runStorageTask(ctx, storageTask{addrHash: address, entries: []storageEntry{
		{path: pathA, update: phaseAStorageUpdate([]byte{1})},
		{path: pathB, update: phaseAStorageUpdate([]byte{2})},
	}})
	require.NoError(t, err)

	before := make(map[string][]byte, len(ctx.branches))
	for key, data := range ctx.branches {
		before[key] = bytes.Clone(data)
	}
	require.GreaterOrEqual(t, len(before), 2)
	ctx.branchCalls = nil
	ctx.accountCalls = 0
	ctx.storageCalls = 0
	_, err = runStorageTask(ctx, storageTask{addrHash: address, wipe: true})
	require.NoError(t, err)
	for key := range before {
		require.Contains(t, ctx.branches, key)
		require.Empty(t, ctx.branches[key])
	}
	require.Contains(t, ctx.branches, string(StorageRootKey(address)))
	require.Zero(t, ctx.accountCalls)
	require.Zero(t, ctx.storageCalls)
	require.NotEmpty(t, ctx.branchCalls)
}

func TestWipeMissingStorageEmitsNoDelta(t *testing.T) {
	var address [32]byte
	address[0] = 0xb2
	ctx := newMockContext()
	_, err := runStorageTask(ctx, storageTask{addrHash: address, wipe: true})
	require.NoError(t, err)
	require.Empty(t, ctx.branches)
}

func TestWipeRejectsMalformedChildRecord(t *testing.T) {
	var address [32]byte
	address[0] = 0xc3
	root := fork(nil)
	root.setStoredChild(2, bytes.Repeat([]byte{1}, 32), nil)
	root.setStoredChild(3, bytes.Repeat([]byte{2}, 32), nil)
	ctx := newMockContext()
	ctx.branches[string(StorageRootKey(address))] = encodeRecord(root, 0, nil)
	ctx.branches[string(StorageNodeKey(address, []byte{2}, nil))] = []byte{recordFormat}

	_, err := runStorageTask(ctx, storageTask{addrHash: address, wipe: true})
	require.ErrorIs(t, err, ErrRecordTruncated)
}

func TestWipeEnumeratesAllBranchChildrenUnderAliasingContext(t *testing.T) {
	var address [32]byte
	address[0] = 0xd4
	paths := [][]byte{
		append([]byte{1, 1}, bytes.Repeat([]byte{4}, 62)...),
		append([]byte{1, 2}, bytes.Repeat([]byte{4}, 62)...),
		append([]byte{1, 3}, bytes.Repeat([]byte{4}, 62)...),
		append([]byte{1, 4}, bytes.Repeat([]byte{4}, 62)...),
		append([]byte{5, 7, 1}, bytes.Repeat([]byte{6}, 61)...),
		append([]byte{5, 7, 2}, bytes.Repeat([]byte{6}, 61)...),
	}
	entries := make([]storageEntry, len(paths))
	for i, p := range paths {
		entries[i] = storageEntry{path: p, update: phaseAStorageUpdate(bytes.Repeat([]byte{byte(i + 1)}, 32))}
	}
	ctx := newMockContext()
	_, err := runStorageTask(ctx, storageTask{addrHash: address, entries: entries})
	require.NoError(t, err)

	before := make(map[string]struct{}, len(ctx.branches))
	for key, data := range ctx.branches {
		if len(data) != 0 {
			before[key] = struct{}{}
		}
	}
	require.GreaterOrEqual(t, len(before), 3)

	_, err = runStorageTask(&grownBufContext{inner: ctx, buf: make([]byte, 0, 4096)}, storageTask{addrHash: address, wipe: true})
	require.NoError(t, err)
	for key := range before {
		require.Empty(t, ctx.branches[key], "record %x survived the wipe", key)
	}
}

type grownBufContext struct {
	inner *mockContext
	buf   []byte
}

func (g *grownBufContext) Branch(key []byte) ([]byte, kv.Step, error) {
	data, step, err := g.inner.Branch(key)
	if err != nil || data == nil {
		return nil, step, err
	}
	g.buf = append(g.buf[:0], data...)
	return g.buf, step, nil
}

func (g *grownBufContext) PutBranch(key, data, prev []byte) error {
	return g.inner.PutBranch(key, data, prev)
}
func (g *grownBufContext) Account(k []byte) (*commitment.Update, error) { return g.inner.Account(k) }
func (g *grownBufContext) Storage(k []byte) (*commitment.Update, error) { return g.inner.Storage(k) }

var _ commitment.PatriciaContext = (*grownBufContext)(nil)
