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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/length"
)

// Zero-vs-absent. EIP-8297 makes them the same state: a leaf whose value is 32
// zero bytes is not stored, and reads back as the zero it stood for. So the
// domain's shared encoding of the two needs no presence bit, and both a delete
// and a zero write remove the leaf.

// TestPBinStorageZeroWriteRemovesLeaf covers the update-stream side: the zeroed
// slot is touched, so its leaf is in the grid when the absent read lands.
func TestPBinStorageZeroWriteRemovesLeaf(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name       string
		kept, gone uint64
	}{
		{name: "storage zone", kept: 257, gone: 256},
		{name: "account header zone", kept: 6, gone: 5},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			addr := pbinOracleAddr(41)
			stored := new(pbinTestCorpus).
				storage(addr, pbinOracleSlot(tc.gone), 0x01).
				storage(addr, pbinOracleSlot(tc.kept), 0x02)

			pph, ms := pbinTestEngine(t)
			require.NoError(t, ms.applyPlainUpdates(stored.plainKeys, stored.updates))
			before := pbinTestProcess(t, pph, stored.plainKeys, stored.updates)

			zeroed := new(pbinTestCorpus).storage(addr, pbinOracleSlot(tc.gone))
			require.NoError(t, ms.applyPlainUpdates(zeroed.plainKeys, []Update{{Flags: DeleteUpdate}}))

			pph.Reset()
			root := pbinTestProcess(t, pph, zeroed.plainKeys, zeroed.updates)

			survivorOnly := new(pbinTestCorpus).storage(addr, pbinOracleSlot(tc.kept), 0x02)
			require.Equal(t, survivorOnly.oracleRoot(t), root,
				"a zeroed slot leaves the tree it would have had without the slot")
			require.NotEqual(t, before, root)
		})
	}
}

// TestPBinStorageZeroOnUntouchedSiblingKeepsLeaf pins the fold path, where the
// rule does not yet hold: a slot zeroed without being in the update set is
// rehydrated from its branch record and committed as 32 zero bytes, which under
// the current spec is a state the tree cannot hold. Removal lives on the update
// path only. The domain always carries a zeroed slot in the same block's update
// set, so this is out of reach through ordinary execution.
func TestPBinStorageZeroOnUntouchedSiblingKeepsLeaf(t *testing.T) {
	t.Parallel()

	addr := pbinOracleAddr(42)
	stored := new(pbinTestCorpus).
		storage(addr, pbinOracleSlot(256), 0x01).
		storage(addr, pbinOracleSlot(257), 0x02)

	pph, ms := pbinTestEngine(t)
	require.NoError(t, ms.applyPlainUpdates(stored.plainKeys, stored.updates))
	pbinTestProcess(t, pph, stored.plainKeys, stored.updates)

	gone := new(pbinTestCorpus).storage(addr, pbinOracleSlot(256))
	require.NoError(t, ms.applyPlainUpdates(gone.plainKeys, []Update{{Flags: DeleteUpdate}}))

	touched := new(pbinTestCorpus).storage(addr, pbinOracleSlot(257), 0x0B)
	require.NoError(t, ms.applyPlainUpdates(touched.plainKeys, touched.updates))

	pph.Reset()
	root := pbinTestProcess(t, pph, touched.plainKeys, touched.updates)

	// The zero leaf is not a state entries() can express, since it filters zeros.
	survivor := new(pbinTestCorpus).storage(addr, pbinOracleSlot(257), 0x0B)
	withZeroLeaf := append(survivor.entries(t), pbinOracleEntry{
		key:   pbinTreeKeyStorage(addr, pbinOracleSlot(256)),
		value: make([]byte, pbinValueLength),
	})
	want := pbinOracleRoot(withZeroLeaf)
	require.Equal(t, want[:], root)

	require.NotEqual(t, survivor.oracleRoot(t), root, "the leaf survives as a zero")
}

func TestPBinLoadCellStateAbsentRead(t *testing.T) {
	t.Parallel()

	t.Run("storage", func(t *testing.T) {
		t.Parallel()

		pph, _ := pbinTestEngine(t)
		c := pbinTestEmptyCell()
		c.kind = pbinNodeLeaf
		c.storageAddrLen = length.Addr + length.Hash
		copy(c.storageAddr[:], append(bytes.Clone(pbinOracleAddr(43)), pbinOracleSlot(1000)...))

		require.NoError(t, pph.loadCellState(&c))
		require.True(t, c.loaded.storage())
		require.False(t, c.Update.Deleted())
		value := pbinEncodeStorageValue(c.Update.Storage[:c.Update.StorageLen])
		require.Equal(t, make([]byte, length.Hash), value[:])
	})

	t.Run("account", func(t *testing.T) {
		t.Parallel()

		pph, _ := pbinTestEngine(t)
		c := pbinTestEmptyCell()
		c.kind = pbinNodeLeaf
		c.accountAddrLen = length.Addr
		copy(c.accountAddr[:], pbinOracleAddr(44))

		require.ErrorIs(t, pph.loadCellState(&c), errPBinDeleteUnsupported)
	})
}

// TestPBinAccountRemovalDropsBothSubtrees: an account owns its header stem and
// its storage prefix, and removing it removes those two subtrees whole — header
// storage slots and header code chunks included, and storage the fold was handed
// no list of. A bystander account must survive untouched.
func TestPBinAccountRemovalDropsBothSubtrees(t *testing.T) {
	t.Parallel()

	addr, bystander := pbinOracleAddr(45), pbinOracleAddr(48)
	stored := new(pbinTestCorpus).
		accountWithCodeBytes(addr, 3, 7, bytes.Repeat([]byte{0x01}, 31*4)).
		storage(addr, pbinOracleSlot(5), 0x01).   // header window
		storage(addr, pbinOracleSlot(256), 0x02). // storage zone
		storage(addr, pbinOracleSlot(1<<20), 0x03).
		account(bystander, 1, 2, common.Hash{0x48})

	pph, ms := pbinTestEngine(t)
	stored.applyTo(t, ms)
	pbinTestProcess(t, pph, stored.plainKeys, stored.updates)

	removal := new(pbinTestCorpus).account(addr, 0, 0, common.Hash{})
	require.NoError(t, ms.applyPlainUpdates(removal.plainKeys, []Update{{Flags: DeleteUpdate}}))

	pph.Reset()
	root := pbinTestProcess(t, pph, removal.plainKeys, removal.updates)

	survivor := new(pbinTestCorpus).account(bystander, 1, 2, common.Hash{0x48})
	require.Equal(t, survivor.oracleRoot(t), root,
		"nothing of the removed account may survive, and nothing of the other may go")
}

// TestPBinFoldDeleteRunsOnProcess: removing the last leaf of a subtree collapses
// it, and the collapse is observable as the zero-length record foldDelete writes
// at a bit-path key. storeRoot makes the sole other zero-length write, and only
// at the root key.
func TestPBinFoldDeleteRunsOnProcess(t *testing.T) {
	t.Parallel()

	addr := pbinOracleAddr(46)
	slots := []uint64{0, 1, 63, 256, 257, 258}
	stored := new(pbinTestCorpus)
	for i, slot := range slots {
		stored.storage(addr, pbinOracleSlot(slot), byte(i+1))
	}
	stored.account(pbinOracleAddr(47), 1, 2, common.Hash{0x47})

	pph, ctx, ms := pbinTestStrictEngine(t)
	require.NoError(t, ms.applyPlainUpdates(stored.plainKeys, stored.updates))
	pbinTestProcess(t, pph, stored.plainKeys, stored.updates)

	zeroed, want := new(pbinTestCorpus), new(pbinTestCorpus)
	for _, slot := range slots {
		zeroed.storage(addr, pbinOracleSlot(slot))
	}
	// An absent key with no leaf of its own contributes nothing — the case a zero
	// write over a live leaf must not be confused with.
	zeroed.storage(addr, pbinOracleSlot(1<<20))
	want.account(pbinOracleAddr(47), 1, 2, common.Hash{0x47})

	for i := range zeroed.plainKeys {
		require.NoError(t, ms.applyPlainUpdates(zeroed.plainKeys[i:i+1], []Update{{Flags: DeleteUpdate}}))
	}
	ctx.puts = nil
	pph.Reset()
	root := pbinTestProcess(t, pph, zeroed.plainKeys, zeroed.updates)
	require.Equal(t, want.oracleRoot(t), root)

	var collapsed int
	for _, put := range ctx.puts {
		if len(put.data) == 0 {
			collapsed++
		}
	}
	require.NotZero(t, collapsed, "every stored leaf was zeroed, so subtrees must collapse")
}
