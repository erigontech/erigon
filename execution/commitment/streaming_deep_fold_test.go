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
	"context"
	"math/bits"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/empty"
	"github.com/erigontech/erigon/common/length"
)

// computeCellHashAt hashes a folded subtree cell exactly as a parent branch would
// when it stitches the cell into its row at the given depth.
func computeCellHashAt(t *testing.T, ms *MockState, c cell, depth int16) []byte {
	t.Helper()
	w := NewHexPatriciaHashed(length.Addr, ms, DefaultTrieConfig())
	defer w.Release()
	h, err := w.computeCellHash(&c, depth, make([]byte, 0, 256))
	require.NoError(t, err)
	return append([]byte(nil), h...)
}

// foldWhaleStorageChildren folds each present storage nibble subtree through the
// production deep-fold helper and returns the depth-65 child cells.
func foldWhaleStorageChildren(t *testing.T, ms *MockState, accNib int, groups [16][]storKV) ([16]cell, uint16) {
	t.Helper()
	var children [16]cell
	var present uint16
	for x := range 16 {
		if len(groups[x]) == 0 {
			continue
		}
		present |= uint16(1) << x
		g := make([]touchedKey, len(groups[x]))
		for i := range groups[x] {
			g[i] = touchedKey{hk: groups[x][i].hk, pk: groups[x][i].pk, upd: &groups[x][i].upd}
		}
		w := NewHexPatriciaHashed(length.Addr, ms, DefaultTrieConfig())
		c, err := foldStorageChildCell(w, accNib, g)
		require.NoError(t, err)
		w.Release()
		children[x] = c
	}
	return children, present
}

// TestAggregateStorageRoot_ResetsDestinationCell is the stale-grid regression: a
// pooled worker whose destination grid cell still carries account fields from a
// prior use must not leak them into the folded subtree cell. foldBranch at
// depth >= 64 does not clear upCell.accountAddrLen, so without resetting the
// destination cell the folded storageRoot cell is hashed by a parent as an
// account leaf and diverges.
func TestAggregateStorageRoot_ResetsDestinationCell(t *testing.T) {
	_, accHash, accNib, _, pk, upds, groups := whaleByNibble(3000)
	ms := NewMockState(t)
	require.NoError(t, ms.applyPlainUpdates(pk, upds))
	children, present := foldWhaleStorageChildren(t, ms, accNib, groups)
	require.Greater(t, bits.OnesCount16(present), 1, "whale must span several storage nibbles")

	cfg := DefaultTrieConfig()

	wc := NewHexPatriciaHashed(length.Addr, ms, cfg)
	wc.grid[0][accNib].reset()
	cleanCh := children
	clean, err := aggregateStorageRoot(wc, accHash, &cleanCh, present)
	require.NoError(t, err)
	wc.Release()

	wd := NewHexPatriciaHashed(length.Addr, ms, cfg)
	stale := &wd.grid[0][accNib]
	stale.accountAddrLen = 20
	stale.stateHashLen = length.Hash
	for i := range stale.stateHash {
		stale.stateHash[i] = 0xAB
	}
	for i := range stale.accountAddr {
		stale.accountAddr[i] = 0xCD
	}
	dirtyCh := children
	dirty, err := aggregateStorageRoot(wd, accHash, &dirtyCh, present)
	require.NoError(t, err)
	wd.Release()

	require.Equal(t,
		computeCellHashAt(t, ms, clean, 64),
		computeCellHashAt(t, ms, dirty, 64),
		"folded storageRoot cell inherited stale pooled-grid state")
}

// TestDeepFold_StorageRootParity drives the production deep-fold child helper
// (foldStorageChildCell at depth 64) and checks the resulting account root equals
// the sequential ModeDirect oracle.
func TestDeepFold_StorageRootParity(t *testing.T) {
	addr, accHash, accNib, accUpd, pk, upds, groups := whaleByNibble(20_000)
	ms := NewMockState(t)
	require.NoError(t, ms.applyPlainUpdates(pk, upds))

	seq := NewHexPatriciaHashed(length.Addr, ms, DefaultTrieConfig())
	seqUpd := WrapKeyUpdates(t, ModeDirect, KeyToHexNibbleHash, pk, upds)
	seqRoot, err := seq.Process(context.Background(), seqUpd, "", nil, WarmupConfig{})
	require.NoError(t, err)
	seqUpd.Close()

	children, present := foldWhaleStorageChildren(t, ms, accNib, groups)

	asm := NewHexPatriciaHashed(length.Addr, ms, DefaultTrieConfig())
	defer asm.Release()
	asm.grid[0][accNib].reset()
	copy(asm.currentKey[:], accHash[:64])
	asm.currentKeyLen = 64
	asm.depths[0] = 64
	asm.depths[1] = 65
	asm.activeRows = 2
	var ac cell
	ac.accountAddrLen = int16(len(addr))
	copy(ac.accountAddr[:], addr)
	ac.CodeHash = empty.CodeHash
	ac.setFromUpdate(&accUpd)
	asm.grid[0][accNib] = ac
	asm.touchMap[0] = uint16(1) << accNib
	asm.afterMap[0] = uint16(1) << accNib
	for x := range 16 {
		if present&(uint16(1)<<x) != 0 {
			asm.grid[1][x] = children[x]
		}
	}
	asm.touchMap[1] = present
	asm.afterMap[1] = present
	for asm.activeRows > 0 {
		require.NoError(t, asm.fold())
	}
	conRoot, err := asm.RootHash()
	require.NoError(t, err)
	require.Equal(t, seqRoot, conRoot, "deep-fold account root != sequential")
}

// TestAggregateSubtreeRoot_DepthGeneralization checks the generalized mount folds
// the same child set to the same branch hash at a mid-account depth, the
// account/storage boundary, and a storage-interior (mid-extension) depth — a
// branch node's hash is independent of the prefix path above it, and the children
// here are hash-only so their hashing is depth-independent.
func TestAggregateSubtreeRoot_DepthGeneralization(t *testing.T) {
	ms := NewMockState(t)
	rnd := rand.New(rand.NewSource(99))
	var children [16]cell
	var present uint16
	for _, x := range []int{1, 4, 9, 13} {
		var c cell
		c.hashLen = length.Hash
		rnd.Read(c.hash[:])
		children[x] = c
		present |= uint16(1) << x
	}

	const lastNib = 7
	mkPrefix := func(d int) []byte {
		p := make([]byte, d)
		r := rand.New(rand.NewSource(int64(d) * 31))
		for i := range p {
			p[i] = byte(r.Intn(16))
		}
		p[d-1] = lastNib
		return p
	}

	var hashes [][]byte
	for _, d := range []int{60, 64, 70} {
		w := NewHexPatriciaHashed(length.Addr, ms, DefaultTrieConfig())
		ch := children
		c, err := aggregateSubtreeRoot(w, mkPrefix(d), &ch, present)
		require.NoError(t, err)
		require.Equal(t, int16(length.Hash), c.hashLen, "depth %d: branch cell must carry a hash", d)
		require.EqualValues(t, 0, c.extLen, "depth %d: direct children carry no extension", d)
		hashes = append(hashes, computeCellHashAt(t, ms, c, int16(d)))
		w.Release()
	}
	require.Equal(t, hashes[0], hashes[1], "mid-account vs boundary branch hash diverged")
	require.Equal(t, hashes[1], hashes[2], "boundary vs storage-interior branch hash diverged")
}
