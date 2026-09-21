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
	"sort"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/empty"
	"github.com/erigontech/erigon/execution/commitment"
)

type incrementalOp struct {
	key    []byte
	update *commitment.Update
	read   bool
}

func TestIncrementalReloadAndBulkParity(t *testing.T) {
	batches, final := incrementalBatches()
	ctxV4 := newParityContext()
	ctxHPH := newParityContext()
	v4 := &Trie{}
	v4.ResetContext(ctxV4)
	hph := commitment.NewHexPatriciaHashed(20, ctxHPH, commitment.DefaultTrieConfig())
	t.Cleanup(func() {
		v4.Release()
		hph.Release()
	})

	for batchIndex, batch := range batches {
		setIncrementalState(ctxHPH, batch)
		rootV4, err := v4.Process(context.Background(), incrementalUpdates(t, batch), "", nil, commitment.WarmupConfig{})
		require.NoErrorf(t, err, "v4 batch %d", batchIndex+1)
		rootHPH, err := hph.Process(context.Background(), incrementalUpdates(t, batch), "", nil, commitment.WarmupConfig{})
		require.NoError(t, err)
		require.Equal(t, rootHPH, rootV4, "batch %d", batchIndex+1)
		require.Zero(t, ctxV4.accountCalls)
		require.Zero(t, ctxV4.storageCalls)

		state, err := v4.EncodeState(uint64(batchIndex+1), uint64(batchIndex+1), nil)
		require.NoError(t, err)
		reloaded := &Trie{}
		reloaded.ResetContext(ctxV4)
		blockNum, txNum, err := reloaded.RestoreState(state)
		require.NoError(t, err)
		require.Equal(t, uint64(batchIndex+1), blockNum)
		require.Equal(t, uint64(batchIndex+1), txNum)
		reloadedRoot, err := reloaded.Process(context.Background(), commitment.NewUpdates(commitment.ModeUpdate, t.TempDir(), commitment.KeyToHexNibbleHash), "", nil, commitment.WarmupConfig{})
		require.NoError(t, err)
		require.Equal(t, rootV4, reloadedRoot, "reloaded batch %d", batchIndex+1)
		require.Zero(t, ctxV4.accountCalls)
		require.Zero(t, ctxV4.storageCalls)
		reloaded.Release()
	}

	incrementalRecords := liveBranches(ctxV4)
	bulkContext := newParityContext()
	bulk := &Trie{}
	bulk.ResetContext(bulkContext)
	_, err := bulk.Process(context.Background(), incrementalUpdates(t, final), "", nil, commitment.WarmupConfig{})
	require.NoError(t, err)
	require.Equal(t, incrementalRecords, liveBranches(bulkContext))
	require.Equal(t, mustRoot(t, v4), mustRoot(t, bulk))
	bulk.Release()
}

func TestIncrementalUnwindAndReexecute(t *testing.T) {
	_, final := incrementalBatches()
	ctxV4 := newParityContext()
	ctxHPH := newParityContext()
	v4 := &Trie{}
	v4.ResetContext(ctxV4)
	hph := commitment.NewHexPatriciaHashed(20, ctxHPH, commitment.DefaultTrieConfig())
	t.Cleanup(func() {
		v4.Release()
		hph.Release()
	})

	setIncrementalState(ctxHPH, final)
	rootV4, err := v4.Process(context.Background(), incrementalUpdates(t, final), "", nil, commitment.WarmupConfig{})
	require.NoError(t, err)
	initialRoot := bytes.Clone(rootV4)
	rootHPH, err := hph.Process(context.Background(), incrementalUpdates(t, final), "", nil, commitment.WarmupConfig{})
	require.NoError(t, err)
	require.Equal(t, rootHPH, rootV4)

	deletes := make([]incrementalOp, 0, len(final))
	for _, op := range final {
		if op.read {
			continue
		}
		deletes = append(deletes, incrementalOp{key: op.key, update: incrementalDeleteUpdate()})
	}
	setIncrementalState(ctxHPH, deletes)
	rootV4, err = v4.Process(context.Background(), incrementalUpdates(t, deletes), "", nil, commitment.WarmupConfig{})
	require.NoError(t, err)
	rootHPH, err = hph.Process(context.Background(), incrementalUpdates(t, deletes), "", nil, commitment.WarmupConfig{})
	require.NoError(t, err)
	require.Equal(t, rootHPH, rootV4)
	require.Equal(t, empty.RootHash, common.BytesToHash(rootV4))

	setIncrementalState(ctxHPH, final)
	rootV4, err = v4.Process(context.Background(), incrementalUpdates(t, final), "", nil, commitment.WarmupConfig{})
	require.NoError(t, err)
	rootHPH, err = hph.Process(context.Background(), incrementalUpdates(t, final), "", nil, commitment.WarmupConfig{})
	require.NoError(t, err)
	require.Equal(t, rootHPH, rootV4)
	require.Equal(t, initialRoot, rootV4)
}

func incrementalBatches() ([][]incrementalOp, []incrementalOp) {
	addressA := bytes.Repeat([]byte{0x11}, 20)
	addressB := bytes.Repeat([]byte{0x22}, 20)
	slotA1 := append(append([]byte(nil), addressA...), bytes.Repeat([]byte{0x31}, 32)...)
	slotA2 := append(append([]byte(nil), addressA...), bytes.Repeat([]byte{0x32}, 32)...)
	slotA3 := append(append([]byte(nil), addressA...), bytes.Repeat([]byte{0x33}, 32)...)
	slotB1 := append(append([]byte(nil), addressB...), bytes.Repeat([]byte{0x41}, 32)...)

	batch1 := []incrementalOp{
		{key: addressA, update: incrementalAccountUpdate(1, 10)},
		{key: addressB, update: incrementalAccountUpdate(2, 20)},
		{key: slotA1, update: incrementalStorageUpdate(1)},
		{key: slotB1, update: incrementalStorageUpdate(2)},
	}
	batch2 := []incrementalOp{
		{key: addressA, update: incrementalAccountUpdate(3, 30)},
		{key: addressA, update: incrementalAccountUpdate(4, 40)},
		{key: slotA2, update: incrementalStorageUpdate(3)},
		{key: slotA3, update: incrementalDeleteUpdate()},
		{key: slotA3, update: incrementalStorageUpdate(4)},
	}
	batch3 := []incrementalOp{
		{key: addressA, read: true},
		{key: slotA1, read: true},
		{key: addressB, update: incrementalDeleteUpdate()},
	}
	final := []incrementalOp{
		{key: addressA, update: incrementalAccountUpdate(4, 40)},
		{key: slotA1, update: incrementalStorageUpdate(1)},
		{key: slotA2, update: incrementalStorageUpdate(3)},
		{key: slotA3, update: incrementalStorageUpdate(4)},
	}
	return [][]incrementalOp{batch1, batch2, batch3}, final
}

func incrementalUpdates(t *testing.T, ops []incrementalOp) *commitment.Updates {
	t.Helper()
	updates := commitment.NewUpdates(commitment.ModeUpdate, t.TempDir(), commitment.KeyToHexNibbleHash)
	for _, op := range ops {
		if op.read {
			updates.TouchPlainKey(string(op.key), nil, func(*commitment.KeyUpdate, []byte) {})
			continue
		}
		updates.TouchPlainKeyDirect(string(op.key), op.update)
	}
	return updates
}

func setIncrementalState(ctx *parityContext, ops []incrementalOp) {
	for _, op := range ops {
		if op.read {
			continue
		}
		if op.update.Deleted() {
			if len(op.key) == 20 {
				delete(ctx.accounts, string(op.key))
			} else {
				delete(ctx.storage, string(op.key))
			}
			continue
		}
		if op.update.Flags&commitment.StorageUpdate != 0 {
			ctx.storage[string(op.key)] = op.update.Copy()
		} else if len(op.key) == 20 {
			ctx.accounts[string(op.key)] = op.update.Copy()
		}
	}
}

func liveBranches(ctx *parityContext) map[string][]byte {
	keys := make([]string, 0, len(ctx.branches))
	for key, value := range ctx.branches {
		if len(value) != 0 {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	result := make(map[string][]byte, len(keys))
	for _, key := range keys {
		result[key] = bytes.Clone(ctx.branches[key])
	}
	return result
}

func mustRoot(t *testing.T, trie *Trie) []byte {
	t.Helper()
	root, err := trie.RootHash()
	require.NoError(t, err)
	return root
}

func incrementalAccountUpdate(nonce, balance uint64) *commitment.Update {
	value := uint256.NewInt(balance)
	return &commitment.Update{
		Flags:    commitment.BalanceUpdate | commitment.NonceUpdate | commitment.CodeUpdate,
		Balance:  *value,
		Nonce:    nonce,
		CodeHash: common.HexToHash("0x1234"),
	}
}

func incrementalStorageUpdate(value byte) *commitment.Update {
	update := &commitment.Update{Flags: commitment.StorageUpdate, StorageLen: 1}
	update.Storage[0] = value
	return update
}

func incrementalDeleteUpdate() *commitment.Update {
	return &commitment.Update{Flags: commitment.DeleteUpdate}
}
