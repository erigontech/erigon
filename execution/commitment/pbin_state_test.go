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

	"github.com/erigontech/erigon/common/length"
)

// TestPBinRestartRoundTripDeepPath guards H6: two same-group storage slots share
// the first 520 bits of their tree keys, so the tree's one branch sits deeper
// than any depth a single byte can hold. The engine must encode its state after
// a full fold, restore it in a fresh engine, and keep folding correctly past
// the restart.
func TestPBinRestartRoundTripDeepPath(t *testing.T) {
	t.Parallel()

	addr := pbinOracleAddr(51)
	stored := new(pbinTestCorpus).
		storage(addr, pbinOracleSlot(256), 0x01).
		storage(addr, pbinOracleSlot(257), 0x02)

	pph, ms := pbinTestEngine(t)
	require.NoError(t, ms.applyPlainUpdates(stored.plainKeys, stored.updates))
	rootBefore := pbinTestProcess(t, pph, stored.plainKeys, stored.updates)

	require.Equal(t, pbinNodeBranch, pph.grid.root.kind)
	require.Greater(t, int(pph.grid.root.prefix.bitLen), 256, "the corpus must put the branch past byte-depth range")

	blob, err := pph.EncodeCurrentState(nil)
	require.NoError(t, err)

	restored := NewPBinPatriciaHashed(ms)
	require.NoError(t, restored.SetState(blob))
	rootAfter, err := restored.RootHash()
	require.NoError(t, err)
	require.Equal(t, rootBefore, rootAfter, "restored engine must reproduce the pre-restart root")

	more := new(pbinTestCorpus).storage(addr, pbinOracleSlot(258), 0x03)
	require.NoError(t, ms.applyPlainUpdates(more.plainKeys, more.updates))
	rootContinued := pbinTestProcess(t, restored, more.plainKeys, more.updates)

	full := new(pbinTestCorpus).
		storage(addr, pbinOracleSlot(256), 0x01).
		storage(addr, pbinOracleSlot(257), 0x02).
		storage(addr, pbinOracleSlot(258), 0x03)
	require.Equal(t, full.oracleRoot(t), rootContinued, "the restored engine must keep folding correctly")
}

// TestPBinStateBlobRoundTripsFlags checks the three root flags survive the blob:
// they are the only engine state beside the root cell, so losing one changes how
// the next run treats the stored tree.
func TestPBinStateBlobRoundTripsFlags(t *testing.T) {
	t.Parallel()

	ms, storedRoot := pbinTestStoredTree(t)
	pph := NewPBinPatriciaHashed(ms)
	require.NoError(t, pph.loadRoot())

	blob, err := pph.EncodeCurrentState(nil)
	require.NoError(t, err)

	restored := NewPBinPatriciaHashed(ms)
	require.NoError(t, restored.SetState(blob))
	require.Equal(t, pph.rootChecked, restored.rootChecked)
	require.Equal(t, pph.rootTouched, restored.rootTouched)
	require.Equal(t, pph.rootPresent, restored.rootPresent)

	root, err := restored.RootHash()
	require.NoError(t, err)
	require.Equal(t, storedRoot, root)
}

// TestPBinSetStateEmptyResetsToStored pins the hex convention: no state blob
// resets the engine, and the tree is then found again through the stored root
// record rather than being lost.
func TestPBinSetStateEmptyResetsToStored(t *testing.T) {
	t.Parallel()

	ms, storedRoot := pbinTestStoredTree(t)
	pph := NewPBinPatriciaHashed(ms)
	require.NoError(t, pph.SetState(nil))
	require.False(t, pph.rootChecked)

	root, err := pph.RootHash()
	require.NoError(t, err)
	require.Equal(t, storedRoot, root)
}

// TestPBinSetStateRejectsForeignBlob: the blob is read back by whatever engine
// the datadir opens with, so a pbin engine handed a hex blob (or a damaged pbin
// one) must refuse it instead of decoding garbage into the root cell.
func TestPBinSetStateRejectsForeignBlob(t *testing.T) {
	t.Parallel()

	hexBlob, err := NewHexPatriciaHashed(length.Addr, nil, DefaultTrieConfig()).EncodeCurrentState(nil)
	require.NoError(t, err)

	pph, ms := pbinTestEngine(t)
	validBlob, err := pph.EncodeCurrentState(nil)
	require.NoError(t, err)

	for name, blob := range map[string][]byte{
		"hex state blob": hexBlob,
		"truncated":      validBlob[:len(validBlob)-1],
		"trailing bytes": append(append([]byte{}, validBlob...), 0x00),
		"marker only":    {validBlob[0]},
		"unknown flags":  {validBlob[0], 0xF8, 0, 0},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			fresh := NewPBinPatriciaHashed(ms)
			require.Error(t, fresh.SetState(blob), "blob %x must be refused", blob)
		})
	}
}

// TestPBinStateRefusesOpenRows pins the precondition the root-cell blob rests
// on: with a row still open, part of the tree lives in the grid arrays and a
// root-cell snapshot would silently drop it.
func TestPBinStateRefusesOpenRows(t *testing.T) {
	t.Parallel()

	pph, _ := pbinTestEngine(t)
	pph.grid.activeRows = 1

	_, err := pph.EncodeCurrentState(nil)
	require.Error(t, err)
	require.Error(t, pph.SetState(nil))
}
