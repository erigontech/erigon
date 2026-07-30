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

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/length"
)

// Zero-vs-absent. The domain has one encoding for both — an absent read — while
// EIP-8297 has no removal and commits a zero value as a present leaf (the
// reference's zero_value_present vector). The engine holds the presence bit the
// domain lacks: a zeroed slot under a live leaf keeps the leaf and commits 32
// zero bytes, an absent key with no leaf contributes nothing, and an absent
// account over a live leaf is a removal the EIP does not describe (Q1) and stays
// refused.
//
// The expected roots come from the oracle, which the reference's own root
// vectors — zero_value_present among them — pin in pbin_specroots_test.go.

// TestPBinStorageDeleteKeepsLeafAsPresentZero covers the update-stream side: the
// zeroed slot is touched, so its leaf is in the grid when the absent read lands.
func TestPBinStorageDeleteKeepsLeafAsPresentZero(t *testing.T) {
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

			want := new(pbinTestCorpus).
				storage(addr, pbinOracleSlot(tc.gone)).
				storage(addr, pbinOracleSlot(tc.kept), 0x02)
			require.Equal(t, want.oracleRoot(t), root)
			require.NotEqual(t, before, root)

			survivorOnly := new(pbinTestCorpus).storage(addr, pbinOracleSlot(tc.kept), 0x02)
			require.NotEqual(t, survivorOnly.oracleRoot(t), root,
				"a zeroed slot keeps its leaf: dropping it is a different tree")
		})
	}
}

// TestPBinStorageDeleteOnUntouchedSiblingIsPresentZero is the same rule reached
// through the fold: the zeroed slot is never touched, so its leaf is rehydrated
// from the branch record and hashed with whatever the state read returns.
func TestPBinStorageDeleteOnUntouchedSiblingIsPresentZero(t *testing.T) {
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

	want := new(pbinTestCorpus).
		storage(addr, pbinOracleSlot(256)).
		storage(addr, pbinOracleSlot(257), 0x0B)
	require.Equal(t, want.oracleRoot(t), root)
}

// TestPBinLoadCellStateAbsentRead pins the two arms apart at the site they share:
// an absent storage read fills the leaf with 32 zero bytes, an absent account
// read refuses.
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

// TestPBinAccountRemovalStillRefused holds Q1 open: turning an absent account
// into a zero-valued BASIC_DATA leaf is consistent with eip:345-347 but is not
// verified against the reference, and it would silently change the root.
func TestPBinAccountRemovalStillRefused(t *testing.T) {
	t.Parallel()

	addr := pbinOracleAddr(45)
	stored := new(pbinTestCorpus).account(addr, 3, 7, common.Hash{0x45})

	pph, ms := pbinTestEngine(t)
	require.NoError(t, ms.applyPlainUpdates(stored.plainKeys, stored.updates))
	pbinTestProcess(t, pph, stored.plainKeys, stored.updates)

	require.NoError(t, ms.applyPlainUpdates(stored.plainKeys, []Update{{Flags: DeleteUpdate}}))
	pph.Reset()
	upd := WrapKeyUpdates(t, ModeDirect, pbinKeyHasher(), stored.plainKeys, stored.updates)
	_, err := pph.Process(context.Background(), upd, "", nil, WarmupConfig{})
	require.ErrorIs(t, err, errPBinDeleteUnsupported)
}

// TestPBinFoldDeleteUnreachableFromProcess guards H12: foldDelete collapses
// nodes the reference leaves in place, and nothing on the Process path may
// reach it. Its only observable is the zero-length record it writes at a
// bit-path key — storeRoot is the sole other zero-length write, and only at the
// root key — so a run that zeroes every leaf it stored must produce none.
func TestPBinFoldDeleteUnreachableFromProcess(t *testing.T) {
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
		want.storage(addr, pbinOracleSlot(slot))
	}
	// An absent key with no leaf of its own is the case a zero write must not be
	// confused with: it contributes nothing and leaves no empty row behind.
	zeroed.storage(addr, pbinOracleSlot(1<<20))
	want.account(pbinOracleAddr(47), 1, 2, common.Hash{0x47})

	for i := range zeroed.plainKeys {
		require.NoError(t, ms.applyPlainUpdates(zeroed.plainKeys[i:i+1], []Update{{Flags: DeleteUpdate}}))
	}
	ctx.puts = nil
	pph.Reset()
	root := pbinTestProcess(t, pph, zeroed.plainKeys, zeroed.updates)
	require.Equal(t, want.oracleRoot(t), root)

	require.NotEmpty(t, ctx.puts)
	for _, put := range ctx.puts {
		require.NotEmpty(t, put.data, "zero-length record at %x: foldDelete ran", put.prefix)
	}
}
