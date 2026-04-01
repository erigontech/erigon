// Copyright 2024 The Erigon Authors
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

	"github.com/erigontech/erigon/db/kv"
)

// trieReaderTestCtx is a mock PatriciaContext backed by a map of compact prefix -> branch data.
type trieReaderTestCtx struct {
	branches map[string][]byte
}

func newTrieReaderTestCtx() *trieReaderTestCtx {
	return &trieReaderTestCtx{branches: make(map[string][]byte)}
}

func (tc *trieReaderTestCtx) Branch(prefix []byte) ([]byte, kv.Step, error) {
	return tc.branches[string(prefix)], 0, nil
}

func (tc *trieReaderTestCtx) PutBranch(prefix, data, prevData []byte) error { return nil }
func (tc *trieReaderTestCtx) Account(plainKey []byte) (*Update, error)      { return nil, nil }
func (tc *trieReaderTestCtx) Storage(plainKey []byte) (*Update, error)      { return nil, nil }
func (tc *trieReaderTestCtx) TxNum() uint64                                 { return 0 }

// putBranch stores branch data for the given nibble prefix using the BranchEncoder.
func (tc *trieReaderTestCtx) putBranch(nibblePrefix []byte, cells [16]*cell) {
	var afterMap uint16
	for i := 0; i < 16; i++ {
		if cells[i] != nil {
			afterMap |= uint16(1) << i
		}
	}
	be := NewBranchEncoder(1024)
	data, _, err := be.EncodeBranch(afterMap, afterMap, afterMap, func(nibble int, skip bool) (*cell, error) {
		if cells[nibble] != nil {
			return cells[nibble], nil
		}
		return &cell{}, nil
	})
	if err != nil {
		panic(err)
	}
	key := HexNibblesToCompactBytes(nibblePrefix)
	tc.branches[string(key)] = bytes.Clone(data)
}

// makeAccountCell creates a cell representing an account leaf with the given plain address.
func makeAccountCell(addr []byte, hash []byte) *cell {
	c := &cell{}
	c.accountAddrLen = int16(len(addr))
	copy(c.accountAddr[:], addr)
	if len(hash) > 0 {
		c.hashLen = int16(len(hash))
		copy(c.hash[:], hash)
	}
	return c
}

// makeStorageCell creates a cell representing a storage leaf.
func makeStorageCell(addr []byte, hash []byte) *cell {
	c := &cell{}
	c.storageAddrLen = int16(len(addr))
	copy(c.storageAddr[:], addr)
	if len(hash) > 0 {
		c.hashLen = int16(len(hash))
		copy(c.hash[:], hash)
	}
	return c
}

// makeBranchCell creates a cell that represents a branch hash (subtree reference).
func makeBranchCell(hash []byte) *cell {
	c := &cell{}
	c.hashLen = int16(len(hash))
	copy(c.hash[:], hash)
	return c
}

// makeExtensionCell creates a cell with an extension (hashed key nibbles) and a hash.
// ext is the nibble sequence for the extension path.
func makeExtensionCell(ext []byte, hash []byte) *cell {
	c := &cell{}
	c.extLen = int16(len(ext))
	copy(c.extension[:], ext)
	if len(hash) > 0 {
		c.hashLen = int16(len(hash))
		copy(c.hash[:], hash)
	}
	return c
}

func dummyHash() []byte {
	h := make([]byte, 32)
	for i := range h {
		h[i] = byte(i + 1)
	}
	return h
}

func TestTrieReader_AccountLookupHit(t *testing.T) {
	t.Parallel()

	// Build a simple single-level trie:
	// Root branch has nibble 0xa pointing to an account leaf.
	ctx := newTrieReaderTestCtx()
	addr := bytes.Repeat([]byte{0xAB}, 20)

	var rootCells [16]*cell
	rootCells[0xa] = makeAccountCell(addr, dummyHash())
	ctx.putBranch(nil, rootCells) // root = empty nibble prefix

	// hashedKey: nibble 0xa followed by 63 zero nibbles (64 total)
	hashedKey := make([]byte, 64)
	hashedKey[0] = 0xa

	tr := NewTrieReader(ctx, 64)
	c, found, err := tr.Lookup(hashedKey)
	require.NoError(t, err)
	require.True(t, found, "expected account to be found")
	require.Equal(t, int16(20), c.accountAddrLen)
	require.Equal(t, addr, c.accountAddr[:c.accountAddrLen])
}

func TestTrieReader_Miss(t *testing.T) {
	t.Parallel()

	// Root branch has nibble 0x5 but we query nibble 0x3.
	ctx := newTrieReaderTestCtx()
	var rootCells [16]*cell
	rootCells[0x5] = makeAccountCell(bytes.Repeat([]byte{0xCC}, 20), dummyHash())
	ctx.putBranch(nil, rootCells)

	hashedKey := make([]byte, 64)
	hashedKey[0] = 0x3

	tr := NewTrieReader(ctx, 64)
	_, found, err := tr.Lookup(hashedKey)
	require.NoError(t, err)
	require.False(t, found, "expected key not found")
}

func TestTrieReader_MissEmptyTrie(t *testing.T) {
	t.Parallel()

	// No branch data at all.
	ctx := newTrieReaderTestCtx()
	hashedKey := make([]byte, 64)
	hashedKey[0] = 0x1

	tr := NewTrieReader(ctx, 64)
	_, found, err := tr.Lookup(hashedKey)
	require.NoError(t, err)
	require.False(t, found, "expected not found in empty trie")
}

func TestTrieReader_ExtensionTraversal(t *testing.T) {
	t.Parallel()

	// Build a trie with an extension node:
	// Root: nibble 0x3 → extension [0x7, 0x2] → Branch at depth 3
	// Branch at prefix [3,7,2]: nibble 0x1 → account leaf
	ctx := newTrieReaderTestCtx()

	// Root level: nibble 3 points to an extension+hash cell
	var rootCells [16]*cell
	rootCells[0x3] = makeExtensionCell([]byte{0x7, 0x2}, dummyHash())
	ctx.putBranch(nil, rootCells)

	// Branch at [3, 7, 2]: nibble 1 points to an account leaf
	var deepCells [16]*cell
	addr := bytes.Repeat([]byte{0xDD}, 20)
	deepCells[0x1] = makeAccountCell(addr, dummyHash())
	ctx.putBranch([]byte{0x3, 0x7, 0x2}, deepCells)

	// hashedKey: [3, 7, 2, 1, 0, 0, ...] (64 nibbles)
	hashedKey := make([]byte, 64)
	hashedKey[0] = 0x3
	hashedKey[1] = 0x7
	hashedKey[2] = 0x2
	hashedKey[3] = 0x1

	tr := NewTrieReader(ctx, 64)
	c, found, err := tr.Lookup(hashedKey)
	require.NoError(t, err)
	require.True(t, found, "expected account found after extension traversal")
	require.Equal(t, int16(20), c.accountAddrLen)
	require.Equal(t, addr, c.accountAddr[:c.accountAddrLen])
}

func TestTrieReader_ExtensionMismatch(t *testing.T) {
	t.Parallel()

	// Extension expects [7, 2] but key has [7, 9].
	ctx := newTrieReaderTestCtx()
	var rootCells [16]*cell
	rootCells[0x3] = makeExtensionCell([]byte{0x7, 0x2}, dummyHash())
	ctx.putBranch(nil, rootCells)

	hashedKey := make([]byte, 64)
	hashedKey[0] = 0x3
	hashedKey[1] = 0x7
	hashedKey[2] = 0x9 // mismatch

	tr := NewTrieReader(ctx, 64)
	_, found, err := tr.Lookup(hashedKey)
	require.NoError(t, err)
	require.False(t, found, "expected miss on extension mismatch")
}

func TestTrieReader_MultiLevelDescent(t *testing.T) {
	t.Parallel()

	// Build a trie with 12 levels of branch-hash nodes, then an account leaf.
	// Path: nibbles [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0xa, 0xb] then account at nibble 0xc.
	ctx := newTrieReaderTestCtx()
	depth := 12

	nibbles := []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0xa, 0xb}

	// Create branch-hash cells at each level.
	for d := 0; d < depth; d++ {
		var cells [16]*cell
		cells[nibbles[d]] = makeBranchCell(dummyHash())
		ctx.putBranch(nibbles[:d], cells)
	}

	// At the final level, put an account leaf at nibble 0xc.
	var leafCells [16]*cell
	addr := bytes.Repeat([]byte{0xEE}, 20)
	leafCells[0xc] = makeAccountCell(addr, dummyHash())
	ctx.putBranch(nibbles[:depth], leafCells)

	// hashedKey: [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, a, b, c, 0, 0, ...] (64 nibbles)
	hashedKey := make([]byte, 64)
	copy(hashedKey, nibbles)
	hashedKey[depth] = 0xc

	tr := NewTrieReader(ctx, 64)
	c, found, err := tr.Lookup(hashedKey)
	require.NoError(t, err)
	require.True(t, found, "expected account found after multi-level descent (depth > 9)")
	require.Equal(t, int16(20), c.accountAddrLen)
	require.Equal(t, addr, c.accountAddr[:c.accountAddrLen])
}

func TestTrieReader_StorageLookup(t *testing.T) {
	t.Parallel()

	// Single-level trie with a storage leaf.
	ctx := newTrieReaderTestCtx()
	storAddr := bytes.Repeat([]byte{0xFF}, 52) // 20 addr + 32 hash

	var rootCells [16]*cell
	rootCells[0x7] = makeStorageCell(storAddr, dummyHash())
	ctx.putBranch(nil, rootCells)

	hashedKey := make([]byte, 64)
	hashedKey[0] = 0x7

	tr := NewTrieReader(ctx, 64)
	c, found, err := tr.Lookup(hashedKey)
	require.NoError(t, err)
	require.True(t, found, "expected storage leaf found")
	require.Equal(t, int16(52), c.storageAddrLen)
}

func TestTrieReader_MultipleChildrenInBranch(t *testing.T) {
	t.Parallel()

	// Root has cells at nibbles 0x2, 0x5, 0xb — verify we parse the correct one.
	ctx := newTrieReaderTestCtx()
	addr2 := bytes.Repeat([]byte{0x22}, 20)
	addr5 := bytes.Repeat([]byte{0x55}, 20)
	addrB := bytes.Repeat([]byte{0xBB}, 20)

	var rootCells [16]*cell
	rootCells[0x2] = makeAccountCell(addr2, dummyHash())
	rootCells[0x5] = makeAccountCell(addr5, dummyHash())
	rootCells[0xb] = makeAccountCell(addrB, dummyHash())
	ctx.putBranch(nil, rootCells)

	tr := NewTrieReader(ctx, 64)

	for _, tc := range []struct {
		nibble byte
		addr   []byte
	}{
		{0x2, addr2},
		{0x5, addr5},
		{0xb, addrB},
	} {
		hashedKey := make([]byte, 64)
		hashedKey[0] = tc.nibble

		c, found, err := tr.Lookup(hashedKey)
		require.NoError(t, err)
		require.True(t, found, "expected hit for nibble %x", tc.nibble)
		require.Equal(t, tc.addr, c.accountAddr[:c.accountAddrLen])
	}
}
