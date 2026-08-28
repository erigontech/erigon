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

package commitment

import (
	"bytes"
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/commitment/nibbles"
)

func storageLeafBranch(t *testing.T, nibble byte, account, slot []byte) BranchData {
	t.Helper()

	var data cellEncodeData
	data.storageAddrLen = length.Addr + length.Hash
	copy(data.storageAddr[:length.Addr], account)
	copy(data.storageAddr[length.Addr:], slot)
	data.stateHashLen = length.Hash
	for i := range data.stateHash {
		data.stateHash[i] = byte(i + 1)
	}

	record := EncodeLeafChild(&data)
	var records [16][]byte
	records[nibble] = record
	read, err := SynthesizeBranchRow(1<<nibble, true, records, 1<<nibble, nil)
	require.NoError(t, err)
	return read.Data[2:]
}

func setAccountAncestor(hph *HexPatriciaHashed, depth int16, path []byte, account []byte) {
	hph.activeRows = 1
	hph.currentKeyLen = depth
	copy(hph.currentKey[:depth], path[:depth])
	hph.depths[0] = depth
	ancestor := &hph.grid[0][path[depth-1]]
	ancestor.accountAddrLen = length.Addr
	copy(ancestor.accountAddr[:], account)
}

func TestStorageLeafAddressHoistAtVaryingDepths(t *testing.T) {
	t.Parallel()

	account := bytes.Repeat([]byte{0x2a}, length.Addr)
	slot := bytes.Repeat([]byte{0x5b}, length.Hash)
	for _, tc := range []struct {
		name          string
		ancestorDepth int16
		targetDepth   int16
	}{
		{name: "account boundary", ancestorDepth: 64, targetDepth: 65},
		{name: "above boundary", ancestorDepth: 32, targetDepth: 70},
		{name: "deep divergence without depth 64 row", ancestorDepth: 31, targetDepth: 111},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := bytes.Repeat([]byte{0x7}, int(tc.ancestorDepth))
			hph := newHexPatriciaHashed()
			hph.cfg.EdgeRecords = true
			setAccountAncestor(hph, tc.ancestorDepth, path, account)

			err := hph.decodeBranchIntoRow(1, tc.targetDepth, storageLeafBranch(t, 4, account, slot), false)
			require.NoError(t, err)
			got := &hph.grid[1][4]
			require.Equal(t, int16(length.Addr+length.Hash), got.storageAddrLen)
			require.Equal(t, append(bytes.Clone(account), slot...), got.storageAddr[:got.storageAddrLen])
		})
	}
}

func TestStorageLeafAddressHoistUsesRootAccount(t *testing.T) {
	t.Parallel()

	account := bytes.Repeat([]byte{0x41}, length.Addr)
	slot := bytes.Repeat([]byte{0x82}, length.Hash)
	hph := newHexPatriciaHashed()
	hph.cfg.EdgeRecords = true
	hph.root.accountAddrLen = length.Addr
	copy(hph.root.accountAddr[:], account)

	require.NoError(t, hph.decodeBranchIntoRow(0, 65, storageLeafBranch(t, 9, account, slot), false))
	got := &hph.grid[0][9]
	require.Equal(t, append(bytes.Clone(account), slot...), got.storageAddr[:got.storageAddrLen])
}

func TestAccountLeafDoesNotUseStorageAddressHoist(t *testing.T) {
	t.Parallel()

	data := recordTestData("account", nil)
	record := EncodeLeafChild(&data)
	var records [16][]byte
	records[3] = record
	read, err := SynthesizeBranchRow(1<<3, true, records, 1<<3, nil)
	require.NoError(t, err)

	hph := newHexPatriciaHashed()
	hph.cfg.EdgeRecords = true
	require.NoError(t, hph.decodeBranchIntoRow(0, 1, read.Data[2:], false))
	got := &hph.grid[0][3]
	require.Equal(t, int16(length.Addr), got.accountAddrLen)
	require.Zero(t, got.storageAddrLen)
	require.Equal(t, data.accountAddr[:], got.accountAddr[:])
}

func TestStorageLeafWithoutAccountContextFailsExplicitly(t *testing.T) {
	t.Parallel()

	hph := newHexPatriciaHashed()
	hph.cfg.EdgeRecords = true
	err := hph.decodeBranchIntoRow(0, 65, storageLeafBranch(t, 2, bytes.Repeat([]byte{1}, length.Addr), bytes.Repeat([]byte{2}, length.Hash)), false)
	require.Error(t, err, "a 32-byte storage slot must not be accepted as a complete plain key")
	require.ErrorContains(t, err, "enclosing account")
}

type hoistStateContext struct {
	account []byte
	storage []byte
	calls   [][]byte
}

func (*hoistStateContext) Branch([]byte) ([]byte, kv.Step, error) { return nil, 0, nil }

func (*hoistStateContext) PutBranch([]byte, []byte, []byte) error { return nil }

func (c *hoistStateContext) Account(key []byte) (*Update, error) {
	c.calls = append(c.calls, append([]byte("account:"), key...))
	return &Update{Flags: BalanceUpdate}, nil
}

func (c *hoistStateContext) Storage(key []byte) (*Update, error) {
	c.calls = append(c.calls, append([]byte("storage:"), key...))
	var update Update
	update.Flags = StorageUpdate
	update.StorageLen = 1
	update.Storage[0] = 1
	return &update, nil
}

func TestStorageAddressHoistSurvivesStateRoundTrip(t *testing.T) {
	t.Parallel()

	account := bytes.Repeat([]byte{0x31}, length.Addr)
	slot := bytes.Repeat([]byte{0x72}, length.Hash)
	storageKey := append(bytes.Clone(account), slot...)
	ctx := &hoistStateContext{account: account, storage: storageKey}
	cfg := DefaultTrieConfig()
	cfg.EdgeRecords = true

	hph := NewHexPatriciaHashed(length.Addr, ctx, cfg)
	defer hph.Release()
	hph.rootPresent = true
	hph.rootChecked = true
	hph.root.accountAddrLen = length.Addr
	copy(hph.root.accountAddr[:], account)
	hph.root.storageAddrLen = length.Addr + length.Hash
	copy(hph.root.storageAddr[:], storageKey)

	blob, err := hph.EncodeCurrentState(nil)
	require.NoError(t, err)

	restoredContext := &hoistStateContext{account: account, storage: storageKey}
	restored := NewHexPatriciaHashed(length.Addr, restoredContext, cfg)
	defer restored.Release()
	require.NoError(t, restored.SetState(blob))
	require.Len(t, restoredContext.calls, 2)
	require.Equal(t, storageKey, restored.root.storageAddr[:restored.root.storageAddrLen])
	require.Equal(t, append([]byte("account:"), account...), restoredContext.calls[0])
	require.Equal(t, append([]byte("storage:"), storageKey...), restoredContext.calls[1])
	require.Equal(t, hph.root.accountAddr[:], restored.root.accountAddr[:])
}

type edgeRecordContext struct {
	*MockState
}

func (c *edgeRecordContext) BranchWithMask(pref []byte, mask uint16, maskKnown bool) ([]byte, kv.Step, [16]uint16, uint16, error) {
	nodeKey := nibbles.EncodeKeyV3(nibbles.CompactToHex(pref))
	wanted := mask
	if !maskKnown {
		wanted = ^uint16(0)
	}
	var records [16][]byte
	var present uint16
	for bitset := wanted; bitset != 0; bitset &= bitset - 1 {
		bit := bitset & -bitset
		nibble := bitsTrailingZeros16(bit)
		if record, ok := c.cm[string(nibbles.ChildKeyV3(nodeKey, byte(nibble)))]; ok {
			records[nibble] = bytes.Clone(record)
			present |= bit
		}
	}
	read, err := SynthesizeBranchRow(mask, maskKnown, records, present, nil)
	return read.Data, 0, read.ChildMasks, read.ChildMasksKnown, err
}

func TestStorageLeafAddressHoistAcrossRestoredTrie(t *testing.T) {
	t.Parallel()

	keys1, updates1 := fixtureBaseAccounts().
		Storage("8e5476fc5990638a4fb0b5fd3f61bb4b5c5f395e", "0000000000000000000000000000000000000000000000000000000000000001", "0303").
		Storage("ba7a3b7b095d3370c022ca655c790f0c0ead66f5", "0000000000000000000000000000000000000000000000000000000000000002", "0404").
		Build()
	keys2, updates2 := NewUpdateBuilder().
		Storage("8e5476fc5990638a4fb0b5fd3f61bb4b5c5f395e", "24f3a02dc65eda502dbf75919e795458413d3c45b38bb35b51235432707900ed", "0707").
		Storage("ba7a3b7b095d3370c022ca655c790f0c0ead66f5", "0fa41642c48ecf8f2059c275353ce4fee173b3a8ce5480f040c4d2901603d14e", "0808").
		Build()

	cfg := DefaultTrieConfig()
	cfg.EdgeRecords = true
	ms := NewMockState(t)
	ctx := &edgeRecordContext{MockState: ms}
	hph := NewHexPatriciaHashed(length.Addr, ctx, cfg)
	defer hph.Release()
	require.NoError(t, ms.applyPlainUpdates(keys1, updates1))
	upds1 := WrapKeyUpdates(t, ModeDirect, KeyToHexNibbleHash, keys1, updates1)
	_, err := hph.Process(context.Background(), upds1, "", nil, WarmupConfig{})
	upds1.Close()
	require.NoError(t, err)
	state, err := hph.EncodeCurrentState(nil)
	require.NoError(t, err)

	restored := NewHexPatriciaHashed(length.Addr, ctx, cfg)
	defer restored.Release()
	require.NoError(t, restored.SetState(state))
	require.NoError(t, ms.applyPlainUpdates(keys2, updates2))
	upds2 := WrapKeyUpdates(t, ModeDirect, KeyToHexNibbleHash, keys2, updates2)
	defer upds2.Close()
	_, err = restored.Process(context.Background(), upds2, "", nil, WarmupConfig{})
	require.NoError(t, err)

	oracleState := NewMockState(t)
	require.NoError(t, oracleState.applyPlainUpdates(keys1, updates1))
	require.NoError(t, oracleState.applyPlainUpdates(keys2, updates2))
	oracle := NewHexPatriciaHashed(length.Addr, oracleState, DefaultTrieConfig())
	defer oracle.Release()
	combinedKeys := append(append([][]byte{}, keys1...), keys2...)
	combinedUpdates := append(append([]Update{}, updates1...), updates2...)
	combined := WrapKeyUpdates(t, ModeDirect, KeyToHexNibbleHash, combinedKeys, combinedUpdates)
	defer combined.Close()
	want, err := oracle.Process(context.Background(), combined, "", nil, WarmupConfig{})
	require.NoError(t, err)
	got, err := restored.RootHash()
	require.NoError(t, err)
	require.Equal(t, want, got)
}

func TestStorageLeafRecordContainsSlotOnly(t *testing.T) {
	t.Parallel()

	data := recordTestData("storage", nil)
	record := EncodeLeafChild(&data)
	require.Len(t, record, 1+length.Hash+length.Hash)
	require.Equal(t, data.storageAddr[length.Addr:], record[1+length.Hash:])
}
