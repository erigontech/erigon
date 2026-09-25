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
	"sort"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/empty"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/internal/commitmenttest"
)

type incrementalOp struct {
	key    []byte
	update *commitment.Update
	read   bool
}

func TestIncrementalReloadAndBulkParity(t *testing.T) {
	batches, final := incrementalBatches()
	ctxV3 := newParityContext()
	ctxHPH := newParityContext()
	v3 := &Trie{}
	v3.ResetContext(ctxV3)
	hph := commitment.NewHexPatriciaHashed(20, ctxHPH, commitment.DefaultTrieConfig())
	t.Cleanup(func() {
		v3.Release()
		hph.Release()
	})

	for batchIndex, batch := range batches {
		setIncrementalState(ctxHPH, batch)
		rootV3, err := v3.Process(context.Background(), incrementalUpdates(t, commitment.ModeCollect, batch), "", nil, commitment.WarmupConfig{})
		require.NoErrorf(t, err, "v3 batch %d", batchIndex+1)
		rootHPH, err := hph.Process(context.Background(), incrementalUpdates(t, commitment.ModeUpdate, batch), "", nil, commitment.WarmupConfig{})
		require.NoError(t, err)
		require.Equal(t, rootHPH, rootV3, "batch %d", batchIndex+1)
		require.Zero(t, ctxV3.accountCalls)
		require.Zero(t, ctxV3.storageCalls)

		state, err := v3.EncodeState(uint64(batchIndex+1), uint64(batchIndex+1), nil)
		require.NoError(t, err)
		reloaded := &Trie{}
		reloaded.ResetContext(ctxV3)
		blockNum, txNum, err := reloaded.RestoreState(state)
		require.NoError(t, err)
		require.Equal(t, uint64(batchIndex+1), blockNum)
		require.Equal(t, uint64(batchIndex+1), txNum)
		reloadedRoot, err := reloaded.Process(context.Background(), commitment.NewUpdates(commitment.ModeCollect, t.TempDir(), commitment.KeyToHexNibbleHash), "", nil, commitment.WarmupConfig{})
		require.NoError(t, err)
		require.Equal(t, rootV3, reloadedRoot, "reloaded batch %d", batchIndex+1)
		require.Zero(t, ctxV3.accountCalls)
		require.Zero(t, ctxV3.storageCalls)
		reloaded.Release()
	}

	incrementalRecords := liveBranches(ctxV3)
	bulkContext := newParityContext()
	bulk := &Trie{}
	bulk.ResetContext(bulkContext)
	_, err := bulk.Process(context.Background(), incrementalUpdates(t, commitment.ModeCollect, final), "", nil, commitment.WarmupConfig{})
	require.NoError(t, err)
	require.Equal(t, incrementalRecords, liveBranches(bulkContext))
	require.Equal(t, mustRoot(t, v3), mustRoot(t, bulk))
	bulk.Release()
}

func TestIncrementalUnwindAndReexecute(t *testing.T) {
	_, final := incrementalBatches()
	ctxV3 := newParityContext()
	ctxHPH := newParityContext()
	v3 := &Trie{}
	v3.ResetContext(ctxV3)
	hph := commitment.NewHexPatriciaHashed(20, ctxHPH, commitment.DefaultTrieConfig())
	t.Cleanup(func() {
		v3.Release()
		hph.Release()
	})

	setIncrementalState(ctxHPH, final)
	rootV3, err := v3.Process(context.Background(), incrementalUpdates(t, commitment.ModeCollect, final), "", nil, commitment.WarmupConfig{})
	require.NoError(t, err)
	initialRoot := bytes.Clone(rootV3)
	rootHPH, err := hph.Process(context.Background(), incrementalUpdates(t, commitment.ModeUpdate, final), "", nil, commitment.WarmupConfig{})
	require.NoError(t, err)
	require.Equal(t, rootHPH, rootV3)

	deletes := make([]incrementalOp, 0, len(final))
	for _, op := range final {
		if op.read {
			continue
		}
		deletes = append(deletes, incrementalOp{key: op.key, update: incrementalDeleteUpdate()})
	}
	setIncrementalState(ctxHPH, deletes)
	rootV3, err = v3.Process(context.Background(), incrementalUpdates(t, commitment.ModeCollect, deletes), "", nil, commitment.WarmupConfig{})
	require.NoError(t, err)
	rootHPH, err = hph.Process(context.Background(), incrementalUpdates(t, commitment.ModeUpdate, deletes), "", nil, commitment.WarmupConfig{})
	require.NoError(t, err)
	require.Equal(t, rootHPH, rootV3)
	require.Equal(t, empty.RootHash, common.BytesToHash(rootV3))

	setIncrementalState(ctxHPH, final)
	rootV3, err = v3.Process(context.Background(), incrementalUpdates(t, commitment.ModeCollect, final), "", nil, commitment.WarmupConfig{})
	require.NoError(t, err)
	rootHPH, err = hph.Process(context.Background(), incrementalUpdates(t, commitment.ModeUpdate, final), "", nil, commitment.WarmupConfig{})
	require.NoError(t, err)
	require.Equal(t, rootHPH, rootV3)
	require.Equal(t, initialRoot, rootV3)
}

func incrementalBatches() ([][]incrementalOp, []incrementalOp) {
	c, err := commitmenttest.Generate(commitmenttest.MathRand(0), commitmenttest.SequenceSpec{Kind: "incremental"})
	if err != nil {
		panic(err)
	}
	batches := make([][]incrementalOp, len(c.Rounds))
	state := make(commitmenttest.State)
	for i, round := range c.Rounds {
		batches[i] = incrementalEntries(round)
		state.Apply(round)
	}
	return batches, incrementalEntries(state.Ops())
}

func incrementalUpdates(t *testing.T, mode commitment.Mode, ops []incrementalOp) *commitment.Updates {
	t.Helper()
	updates := commitment.NewUpdates(mode, t.TempDir(), commitment.KeyToHexNibbleHash)
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
	return testAccountUpdate(commitmenttest.Account(commitmenttest.AccountSpec{Nonce: nonce, Balance: balance, CodeHash: common.HexToHash("0x1234")}))
}

func incrementalStorageUpdate(value byte) *commitment.Update {
	return storageUpdate(commitmenttest.Storage(commitmenttest.StorageSpec{Value: []byte{value}}))
}

func incrementalDeleteUpdate() *commitment.Update {
	return &commitment.Update{Flags: commitment.DeleteUpdate}
}
