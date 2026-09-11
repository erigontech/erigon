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
func lifecycleRoots(t *testing.T, batches ...*UpdateBuilder) (carried, restored []byte) {
	t.Helper()
	ms, msr := NewMockState(t), NewMockState(t)
	tr := NewHexPatriciaHashed(length.Addr, ms, DefaultTrieConfig())
	var blob []byte
	for _, ub := range batches {
		k, u := ub.Build()
		carried = processBatch(t, ms, tr, k, u)
		restored, blob = processModeBatchState(t, msr, modeSeq, 0, k, u, blob)
	}
	return carried, restored
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
			kf, uf := ubf.Build()

			want, _ := engineRoot(t, modeSeq, 0, kf, uf)
			carried, restored := lifecycleRoots(t, ub1, ub2)
			require.Equal(t, want, carried, "carried trie lost the untouched surviving slots")
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

	t.Run("delete_storage_keeps_account", func(t *testing.T) {
		t.Parallel()
		ub2 := NewUpdateBuilder().Balance(a, 2)
		for _, loc := range all {
			ub2.DeleteStorage(a, loc)
		}

		kf, uf := NewUpdateBuilder().Balance(a, 2).Build()
		want, _ := engineRoot(t, modeSeq, 0, kf, uf)
		carried, restored := lifecycleRoots(t, ub1, ub2)
		require.Equal(t, want, carried, "carried trie: deleting all storage must leave the bare account")
		require.Equal(t, want, restored, "state-restored trie: deleting all storage must leave the bare account")
	})

	t.Run("delete_account_empties_trie", func(t *testing.T) {
		t.Parallel()
		ub2 := NewUpdateBuilder().Delete(a)
		for _, loc := range all {
			ub2.DeleteStorage(a, loc)
		}

		carried, restored := lifecycleRoots(t, ub1, ub2)
		require.Equal(t, empty.RootHash[:], carried, "carried trie: deleting the sole account must empty the trie")
		require.Equal(t, empty.RootHash[:], restored, "state-restored trie: deleting the sole account must empty the trie")
	})
}

// The same shape re-expanding: a sole account whose storage collapses to one slot and then grows
// back must re-insert the survivor under its own first storage nibble. The root cell's derived
// navigation path has to hash the slot alone, not the whole account-plus-slot plain key.
func TestSoleAccount_CollapseThenReexpand(t *testing.T) {
	t.Parallel()
	a := addrHex(findAddressForNibble(3, 7777))
	surv := storageLocsForNibble(0x2, 1, 11)
	gone := storageLocsForNibble(0x8, 1, 3000)
	fresh := storageLocsForNibble(0x5, 1, 5000)

	ub1 := NewUpdateBuilder().Balance(a, 1)
	ubf := NewUpdateBuilder().Balance(a, 3)
	for _, loc := range surv {
		ub1.Storage(a, loc, loc)
		ubf.Storage(a, loc, loc)
	}
	ub2 := NewUpdateBuilder().Balance(a, 2)
	for _, loc := range gone {
		ub1.Storage(a, loc, loc)
		ub2.DeleteStorage(a, loc)
	}
	ub3 := NewUpdateBuilder().Balance(a, 3)
	for _, loc := range fresh {
		ub3.Storage(a, loc, loc)
		ubf.Storage(a, loc, loc)
	}
	kf, uf := ubf.Build()

	want, _ := engineRoot(t, modeSeq, 0, kf, uf)
	carried, restored := lifecycleRoots(t, ub1, ub2, ub3)
	require.Equal(t, want, carried, "carried trie re-expanded the survivor under the wrong nibble")
	require.Equal(t, want, restored, "state-restored trie re-expanded the survivor under the wrong nibble")
}

func TestSoleAccount_StorageBranchUnderExtension(t *testing.T) {
	t.Parallel()
	a := addrHex(findAddressForNibble(3, 4244))
	under := storageLocsForNibble(0x6, 2, 17)
	fresh := storageLocsForNibble(0x0, 1, 4711)

	ub1 := NewUpdateBuilder().Balance(a, 1)
	ubf := NewUpdateBuilder().Balance(a, 2)
	for _, loc := range under {
		ub1.Storage(a, loc, loc)
		ubf.Storage(a, loc, loc)
	}
	ub2 := NewUpdateBuilder().Balance(a, 2)
	for _, loc := range fresh {
		ub2.Storage(a, loc, loc)
		ubf.Storage(a, loc, loc)
	}
	kf, uf := ubf.Build()

	want, _ := engineRoot(t, modeSeq, 0, kf, uf)
	carried, restored := lifecycleRoots(t, ub1, ub2)
	require.Equal(t, want, carried, "carried trie lost the storage extension below the sole account")
	require.Equal(t, want, restored, "state-restored trie lost the storage extension below the sole account")
}
