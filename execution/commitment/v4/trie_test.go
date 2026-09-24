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
	"github.com/erigontech/erigon/execution/commitment"
)

func TestInitializeTrieAndUpdatesV4(t *testing.T) {
	cfg := commitment.DefaultTrieConfig()
	cfg.Variant = commitment.VariantCommitmentV4
	trie, updates := commitment.InitializeTrieAndUpdates(commitment.ModeCollect, t.TempDir(), cfg)

	require.IsType(t, &Trie{}, trie)
	require.Equal(t, commitment.ModeCollect, updates.Mode())
	require.Equal(t, commitment.VariantCommitmentV4, trie.Variant())
}

func TestTrieProcessDoesNotReadState(t *testing.T) {
	trie, updates := NewTrie(t.TempDir(), commitment.TrieConfig{})
	ctx := newMockContext()
	trie.ResetContext(ctx)

	address := bytes.Repeat([]byte{0x11}, 20)
	slot := bytes.Repeat([]byte{0x22}, 32)
	account := fullAccountUpdate(3, 5, common.HexToHash("0x1234"))
	updates.TouchPlainKeyDirect(string(address), &account)
	storage := phaseAStorageUpdate([]byte{0xaa})
	updates.TouchPlainKeyDirect(string(append(append([]byte(nil), address...), slot...)), storage)

	_, err := trie.Process(context.Background(), updates, "", nil, commitment.WarmupConfig{})
	require.NoError(t, err)
	require.Zero(t, ctx.accountCalls)
	require.Zero(t, ctx.storageCalls)
	require.NotEqual(t, common.Hash{}, func() common.Hash {
		root, rootErr := trie.RootHash()
		require.NoError(t, rootErr)
		return common.BytesToHash(root)
	}())
}

func TestTrieDeferredUpdatesWaitForApply(t *testing.T) {
	trie := &Trie{}
	ctx := newMockContext()
	trie.ResetContext(ctx)
	trie.SetDeferCommitmentUpdates(true)

	updates := commitment.NewUpdates(commitment.ModeCollect, t.TempDir(), commitment.KeyToHexNibbleHash)
	address := bytes.Repeat([]byte{0x11}, 20)
	account := fullAccountUpdate(3, 5, common.HexToHash("0x1234"))
	updates.TouchPlainKeyDirect(string(address), &account)

	_, err := trie.Process(context.Background(), updates, "", nil, commitment.WarmupConfig{})
	require.NoError(t, err)
	require.Zero(t, ctx.putCalls)

	pending := &commitment.PendingCommitmentUpdate{Deltas: trie.TakeDeferredDeltas()}
	require.NotNil(t, pending.Deltas)
	require.NoError(t, pending.Apply(ctx.PutBranch))
	require.NotZero(t, ctx.putCalls)
}

func TestTrieProcessRejectsWrongUpdateMode(t *testing.T) {
	trie, _ := NewTrie(t.TempDir(), commitment.TrieConfig{})
	trie.ResetContext(newMockContext())
	updates := commitment.NewUpdates(commitment.ModeDirect, t.TempDir(), commitment.KeyToHexNibbleHash)

	_, err := trie.Process(context.Background(), updates, "", nil, commitment.WarmupConfig{})
	require.EqualError(t, err, "commitment v4: Process requires ModeCollect updates")
}
