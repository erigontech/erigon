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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/empty"
	"github.com/erigontech/erigon/common/length"
)

// The same trie carried across blocks, as a running node holds it. A sole-account root folds via
// propagate and writes no root branch record, so its state travels only in the carried trie or
// the state blob — a fresh trie per batch is not a lifecycle this shape has.
func carriedRoot(t *testing.T, k1 [][]byte, u1 []Update, k2 [][]byte, u2 []Update) []byte {
	t.Helper()
	ms := NewMockState(t)
	tr := NewHexPatriciaHashed(length.Addr, ms, DefaultTrieConfig())
	processBatch(t, ms, tr, k1, u1)
	return processBatch(t, ms, tr, k2, u2)
}

// A trie whose only leaf is one account reaches it through a root extension down to depth 64.
// Bumping the account and deleting a subset of its slots must keep the untouched survivors under
// both production lifecycles, matching a fresh trie built from the final state.
func TestSoleAccount_StorageCollapseIncremental(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name      string
		survivors int
	}{
		{"leaf_survivor", 1},
		{"branch_survivor", 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			a := addrHex(findAddressForNibble(3, 4242))
			surv := storageLocsForNibble(0x2, tc.survivors, 1)
			gone := append(storageLocsForNibble(0x8, 6, 1000), storageLocsForNibble(0xd, 6, 1000000)...)

			ub1 := NewUpdateBuilder().Balance(a, 1)
			ubf := NewUpdateBuilder().Balance(a, 2)
			for _, loc := range surv {
				ub1.Storage(a, loc, loc)
				ubf.Storage(a, loc, loc)
			}
			ub2 := NewUpdateBuilder().Balance(a, 2)
			for _, loc := range gone {
				ub1.Storage(a, loc, loc)
				ub2.DeleteStorage(a, loc)
			}
			k1, u1 := ub1.Build()
			k2, u2 := ub2.Build()
			kf, uf := ubf.Build()

			want, _ := engineRoot(t, modeSeq, 0, kf, uf)
			restored, _ := incrementalRoot(t, modeSeq, 0, k1, u1, k2, u2)
			require.Equal(t, want, carriedRoot(t, k1, u1, k2, u2), "carried trie lost the untouched surviving slots")
			require.Equal(t, want, restored, "state-restored trie lost the untouched surviving slots")
		})
	}
}

// Delete-driven endgames of the same shape: wiping all storage must leave a bare account, and
// deleting the account with its slots must collapse the trie to the empty root.
func TestSoleAccount_DeleteIncremental(t *testing.T) {
	t.Parallel()
	a := addrHex(findAddressForNibble(3, 4243))
	all := append(append(storageLocsForNibble(0x2, 2, 2), storageLocsForNibble(0x8, 6, 2000)...), storageLocsForNibble(0xd, 6, 2000000)...)
	ub1 := NewUpdateBuilder().Balance(a, 1)
	for _, loc := range all {
		ub1.Storage(a, loc, loc)
	}
	k1, u1 := ub1.Build()

	t.Run("delete_storage_keeps_account", func(t *testing.T) {
		t.Parallel()
		ub2 := NewUpdateBuilder().Balance(a, 2)
		for _, loc := range all {
			ub2.DeleteStorage(a, loc)
		}
		k2, u2 := ub2.Build()

		kf, uf := NewUpdateBuilder().Balance(a, 2).Build()
		want, _ := engineRoot(t, modeSeq, 0, kf, uf)
		restored, _ := incrementalRoot(t, modeSeq, 0, k1, u1, k2, u2)
		require.Equal(t, want, carriedRoot(t, k1, u1, k2, u2), "carried trie: deleting all storage must leave the bare account")
		require.Equal(t, want, restored, "state-restored trie: deleting all storage must leave the bare account")
	})

	t.Run("delete_account_empties_trie", func(t *testing.T) {
		t.Parallel()
		ub2 := NewUpdateBuilder().Delete(a)
		for _, loc := range all {
			ub2.DeleteStorage(a, loc)
		}
		k2, u2 := ub2.Build()

		restored, _ := incrementalRoot(t, modeSeq, 0, k1, u1, k2, u2)
		require.Equal(t, empty.RootHash[:], carriedRoot(t, k1, u1, k2, u2), "carried trie: deleting the sole account must empty the trie")
		require.Equal(t, empty.RootHash[:], restored, "state-restored trie: deleting the sole account must empty the trie")
	})
}
