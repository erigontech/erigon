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
	"testing"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/stretchr/testify/require"
)

func TestTrieAPI(t *testing.T) {
	for _, tc := range []struct {
		name string
		run  func(*testing.T)
	}{
		{"E45/InitializeTrieAndUpdatesV3", func(t *testing.T) {
			cfg := commitment.DefaultTrieConfig()
			cfg.Variant = commitment.VariantCommitmentV3
			trie, updates := commitment.InitializeTrieAndUpdates(commitment.ModeCollect, t.TempDir(), cfg)
			t.Cleanup(trie.Release)
			t.Cleanup(updates.Close)

			require.IsType(t, &Trie{}, trie)
			require.Equal(t, commitment.ModeCollect, updates.Mode())
			require.Equal(t, commitment.VariantCommitmentV3, trie.Variant())
		}},
		{"E39/TrieProcessDoesNotReadState", func(t *testing.T) {
			trie, updates := NewTrie(t.TempDir(), commitment.TrieConfig{})
			t.Cleanup(trie.Release)
			t.Cleanup(updates.Close)
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
			root, err := trie.RootHash()
			require.NoError(t, err)
			require.NotEqual(t, common.Hash{}, common.BytesToHash(root))
		}},
		{"E46/TrieDeferredUpdatesWaitForApply", func(t *testing.T) {
			trie := &Trie{}
			t.Cleanup(trie.Release)
			ctx := newMockContext()
			trie.ResetContext(ctx)
			trie.SetDeferCommitmentUpdates(true)

			updates := commitment.NewUpdates(commitment.ModeCollect, t.TempDir(), commitment.KeyToHexNibbleHash)
			t.Cleanup(updates.Close)
			address := bytes.Repeat([]byte{0x11}, 20)
			account := fullAccountUpdate(3, 5, common.HexToHash("0x1234"))
			updates.TouchPlainKeyDirect(string(address), &account)

			_, err := trie.Process(context.Background(), updates, "", nil, commitment.WarmupConfig{})
			require.NoError(t, err)
			require.Zero(t, ctx.putCalls)

			deltas := trie.TakeDeferredDeltas()
			require.NotNil(t, deltas)
			require.NoError(t, applyDeltas(deltas, ctx.PutBranch))
			require.NotZero(t, ctx.putCalls)
		}},
		{"E45/TrieProcessRejectsWrongUpdateMode", func(t *testing.T) {
			trie, unused := NewTrie(t.TempDir(), commitment.TrieConfig{})
			t.Cleanup(trie.Release)
			t.Cleanup(unused.Close)
			trie.ResetContext(newMockContext())
			updates := commitment.NewUpdates(commitment.ModeDirect, t.TempDir(), commitment.KeyToHexNibbleHash)
			t.Cleanup(updates.Close)

			_, err := trie.Process(context.Background(), updates, "", nil, commitment.WarmupConfig{})
			require.EqualError(t, err, "commitment v3: Process requires ModeCollect updates")
		}},
	} {
		t.Run(tc.name, tc.run)
	}
}
