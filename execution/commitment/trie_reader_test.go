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
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/commitment/nibbles"
)

// errTrieReaderTestCtx is a mock PatriciaContext that returns an error for a specific prefix.
type errTrieReaderTestCtx struct {
	trieReaderTestCtx
	errPrefix string
}

func (tc *errTrieReaderTestCtx) Branch(prefix []byte) ([]byte, kv.Step, error) {
	if string(prefix) == tc.errPrefix {
		return nil, 0, fmt.Errorf("disk I/O error")
	}
	return tc.trieReaderTestCtx.Branch(prefix)
}

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
	var encData [16]cellEncodeData
	for i := 0; i < 16; i++ {
		if cells[i] != nil {
			afterMap |= uint16(1) << i
			encData[i] = cellEncodeDataFromCell(cells[i])
		}
	}
	be := NewBranchEncoder(1024)
	data, err := be.EncodeBranch(afterMap, afterMap, afterMap, &encData)
	if err != nil {
		panic(err)
	}
	key := nibbles.HexToCompact(nibblePrefix)
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

	// Build a single-level trie: root branch has the account leaf at the
	// nibble determined by the actual keccak hash of the address.
	ctx := newTrieReaderTestCtx()
	addr := bytes.Repeat([]byte{0xAB}, 20)
	hashedKey := KeyToHexNibbleHash(addr)

	var rootCells [16]*cell
	rootCells[hashedKey[0]] = makeAccountCell(addr, dummyHash())
	ctx.putBranch(nil, rootCells)

	tr := NewTrieReader(ctx, length.Addr)
	c, found, err := tr.Lookup(hashedKey)
	require.NoError(t, err)
	require.True(t, found, "expected account to be found")
	require.Equal(t, int16(20), c.accountAddrLen)
	require.Equal(t, addr, c.accountAddr[:c.accountAddrLen])
}

func TestTrieReader_Miss(t *testing.T) {
	t.Parallel()

	// Root branch has one nibble but we query a different nibble.
	ctx := newTrieReaderTestCtx()
	addr := bytes.Repeat([]byte{0xCC}, 20)
	hashedAddr := KeyToHexNibbleHash(addr)
	var rootCells [16]*cell
	rootCells[hashedAddr[0]] = makeAccountCell(addr, dummyHash())
	ctx.putBranch(nil, rootCells)

	// Query a key whose first nibble differs.
	missKey := make([]byte, 64)
	missKey[0] = (hashedAddr[0] + 1) % 16 // guaranteed different nibble
	tr := NewTrieReader(ctx, length.Addr)
	_, found, err := tr.Lookup(missKey)
	require.NoError(t, err)
	require.False(t, found, "expected key not found")
}

func TestTrieReader_MissEmptyTrie(t *testing.T) {
	t.Parallel()

	// No branch data at all.
	ctx := newTrieReaderTestCtx()
	hashedKey := make([]byte, 64)
	hashedKey[0] = 0x1

	tr := NewTrieReader(ctx, length.Addr)
	_, found, err := tr.Lookup(hashedKey)
	require.NoError(t, err)
	require.False(t, found, "expected not found in empty trie")
}

func TestTrieReader_ExtensionTraversal(t *testing.T) {
	t.Parallel()

	// Build a trie with a pure extension+hash node (no account) at the root,
	// leading to a deeper branch with a hash-consistent account leaf.
	ctx := newTrieReaderTestCtx()
	addr := bytes.Repeat([]byte{0xDD}, 20)
	hashedKey := KeyToHexNibbleHash(addr)

	// Root: nibble hashedKey[0] → extension [hashedKey[1], hashedKey[2]] → branch at depth 3
	var rootCells [16]*cell
	rootCells[hashedKey[0]] = makeExtensionCell(hashedKey[1:3], dummyHash())
	ctx.putBranch(nil, rootCells)

	// Branch at prefix [hk[0], hk[1], hk[2]]: nibble hk[3] → account leaf
	var deepCells [16]*cell
	deepCells[hashedKey[3]] = makeAccountCell(addr, dummyHash())
	ctx.putBranch(hashedKey[:3], deepCells)

	tr := NewTrieReader(ctx, length.Addr)
	c, found, err := tr.Lookup(hashedKey)
	require.NoError(t, err)
	require.True(t, found, "expected account found after extension traversal")
	require.Equal(t, int16(20), c.accountAddrLen)
	require.Equal(t, addr, c.accountAddr[:c.accountAddrLen])
}

func TestTrieReader_ExtensionMismatch(t *testing.T) {
	t.Parallel()

	// Extension expects [7, 2] but key has a different second nibble.
	ctx := newTrieReaderTestCtx()
	var rootCells [16]*cell
	rootCells[0x3] = makeExtensionCell([]byte{0x7, 0x2}, dummyHash())
	ctx.putBranch(nil, rootCells)

	hashedKey := make([]byte, 64)
	hashedKey[0] = 0x3
	hashedKey[1] = 0x7
	hashedKey[2] = 0x9 // mismatch with extension [7, 2]

	tr := NewTrieReader(ctx, length.Addr)
	_, found, err := tr.Lookup(hashedKey)
	require.NoError(t, err)
	require.False(t, found, "expected miss on extension mismatch")
}

func TestTrieReader_MultiLevelDescent(t *testing.T) {
	t.Parallel()

	// Build a trie with multiple levels of branch-hash nodes, then an account leaf.
	// Use a real address so the hashed key is consistent.
	ctx := newTrieReaderTestCtx()
	addr := bytes.Repeat([]byte{0xEE}, 20)
	hashedKey := KeyToHexNibbleHash(addr)
	depth := 12

	// Create branch-hash cells following the actual hashed key path.
	for d := 0; d < depth; d++ {
		var cells [16]*cell
		cells[hashedKey[d]] = makeBranchCell(dummyHash())
		ctx.putBranch(hashedKey[:d], cells)
	}

	// At the final level, put an account leaf.
	var leafCells [16]*cell
	leafCells[hashedKey[depth]] = makeAccountCell(addr, dummyHash())
	ctx.putBranch(hashedKey[:depth], leafCells)

	tr := NewTrieReader(ctx, length.Addr)
	c, found, err := tr.Lookup(hashedKey)
	require.NoError(t, err)
	require.True(t, found, "expected account found after multi-level descent (depth > 9)")
	require.Equal(t, int16(20), c.accountAddrLen)
	require.Equal(t, addr, c.accountAddr[:c.accountAddrLen])
}

func TestTrieReader_StorageLookup(t *testing.T) {
	t.Parallel()

	// Storage lookups are best tested via HPH round-trip (see TestTrieReader_RoundTripWithHPH).
	// This mock test verifies the basic mechanics using a cell at depth >= 64
	// where only the storage slot hash matters.
	ctx := newTrieReaderTestCtx()
	storAddr := bytes.Repeat([]byte{0xFF}, 52) // 20 addr + 32 slot
	hashedKey := KeyToHexNibbleHash(storAddr)  // 128 nibbles
	require.Equal(t, 128, len(hashedKey), "storage hashed key must be 128 nibbles")

	// Place a branch at depth 64 (the account/storage boundary).
	var storageCells [16]*cell
	storageCells[hashedKey[64]] = makeStorageCell(storAddr, dummyHash())
	ctx.putBranch(hashedKey[:64], storageCells)

	// To reach depth 64, we need branch-hash cells along the account path.
	// For simplicity, create a single branch at root pointing to depth 64
	// via chain of hash cells at key depths.
	for d := 0; d < 64; d++ {
		var cells [16]*cell
		cells[hashedKey[d]] = makeBranchCell(dummyHash())
		ctx.putBranch(hashedKey[:d], cells)
	}

	tr := NewTrieReader(ctx, length.Addr)
	c, found, err := tr.Lookup(hashedKey)
	require.NoError(t, err)
	require.True(t, found, "expected storage leaf found")
	require.Equal(t, int16(52), c.storageAddrLen)
}

func TestTrieReader_MultipleChildrenInBranch(t *testing.T) {
	t.Parallel()

	// Root has multiple account cells — verify we parse the correct one.
	// Pick addresses that hash to different first nibbles.
	ctx := newTrieReaderTestCtx()
	addrs := [][]byte{
		bytes.Repeat([]byte{0x22}, 20),
		bytes.Repeat([]byte{0x55}, 20),
		bytes.Repeat([]byte{0xBB}, 20),
	}

	var rootCells [16]*cell
	hashedKeys := make([][]byte, len(addrs))
	for i, addr := range addrs {
		hk := KeyToHexNibbleHash(addr)
		hashedKeys[i] = hk
		rootCells[hk[0]] = makeAccountCell(addr, dummyHash())
	}

	ctx.putBranch(nil, rootCells)
	tr := NewTrieReader(ctx, length.Addr)

	for i, addr := range addrs {
		c, found, err := tr.Lookup(hashedKeys[i])
		require.NoError(t, err)
		require.True(t, found, "expected hit for addr %x", addr)
		require.Equal(t, addr, c.accountAddr[:c.accountAddrLen])
	}
}

func TestTrieReader_BranchError(t *testing.T) {
	t.Parallel()

	// Root branch has a hash cell, so Lookup descends.
	// The second Branch() call (at depth 1) returns an error.
	inner := newTrieReaderTestCtx()
	var rootCells [16]*cell
	rootCells[0x5] = makeBranchCell(dummyHash())
	inner.putBranch(nil, rootCells)

	hashedKey := make([]byte, 64)
	hashedKey[0] = 0x5
	secondPrefix := nibbles.HexToCompact(hashedKey[:1])

	ctx := &errTrieReaderTestCtx{
		trieReaderTestCtx: *inner,
		errPrefix:         string(secondPrefix),
	}

	tr := NewTrieReader(ctx, length.Addr)
	_, _, err := tr.Lookup(hashedKey)
	require.Error(t, err)
	require.Contains(t, err.Error(), "disk I/O error")
	require.Contains(t, err.Error(), "Branch at depth 1")
}

func TestTrieReader_EmptyKey(t *testing.T) {
	t.Parallel()

	ctx := newTrieReaderTestCtx()
	addr := bytes.Repeat([]byte{0xAA}, 20)
	hk := KeyToHexNibbleHash(addr)
	var rootCells [16]*cell
	rootCells[hk[0]] = makeAccountCell(addr, dummyHash())
	ctx.putBranch(nil, rootCells)

	tr := NewTrieReader(ctx, length.Addr)

	// Empty key should return not-found without error.
	_, found, err := tr.Lookup([]byte{})
	require.NoError(t, err)
	require.False(t, found)

	// Nil key should behave the same.
	_, found, err = tr.Lookup(nil)
	require.NoError(t, err)
	require.False(t, found)
}

// TestTrieReader_RoundTripWithHPH verifies that TrieReader can look up keys
// from branch data produced by a real HexPatriciaHashed Process cycle.
func TestTrieReader_RoundTripWithHPH(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	ms := NewMockState(t)

	// Use real 20-byte account addresses (accountKeyLen = length.Addr).
	hph := NewHexPatriciaHashed(int16(length.Addr), ms)
	hph.SetTrace(false)

	// Build updates: several accounts with balance/nonce, plus storage.
	plainKeys, updates := NewUpdateBuilder().
		Balance("f000000000000000000000000000000000000001", 100).
		Nonce("f000000000000000000000000000000000000001", 1).
		Balance("f000000000000000000000000000000000000002", 200).
		Balance("f000000000000000000000000000000000000003", 300).
		Balance("a000000000000000000000000000000000000004", 400).
		Balance("b000000000000000000000000000000000000005", 500).
		Balance("c000000000000000000000000000000000000006", 600).
		Balance("d000000000000000000000000000000000000007", 700).
		Storage("f000000000000000000000000000000000000001",
			"0000000000000000000000000000000000000000000000000000000000000001", "01").
		Storage("f000000000000000000000000000000000000001",
			"0000000000000000000000000000000000000000000000000000000000000002", "02").
		Storage("a000000000000000000000000000000000000004",
			"0000000000000000000000000000000000000000000000000000000000000003", "ff").
		Build()

	upds := WrapKeyUpdates(t, ModeDirect, KeyToHexNibbleHash, plainKeys, updates)
	defer upds.Close()

	err := ms.applyPlainUpdates(plainKeys, updates)
	require.NoError(t, err)

	rootHash, err := hph.Process(ctx, upds, "", nil, WarmupConfig{})
	require.NoError(t, err)
	require.NotEmpty(t, rootHash)

	t.Logf("rootHash: %x, branches stored: %d", rootHash, len(ms.cm))
	require.True(t, len(ms.cm) > 0, "expected at least one branch stored")

	// Now use TrieReader with the same MockState to look up each key.
	reader := NewTrieReader(ms, length.Addr)

	// Track which keys are accounts vs storage so we check the right field.
	for i, pk := range plainKeys {
		if updates[i].Flags&DeleteUpdate != 0 {
			continue
		}

		hashedKey := KeyToHexNibbleHash(pk)
		c, found, err := reader.Lookup(hashedKey)
		require.NoError(t, err, "Lookup failed for plainKey %x (hashed %x)", pk, hashedKey)

		isStorage := len(pk) > length.Addr
		if isStorage {
			require.True(t, found, "storage key %x not found (hashed %x)", pk, hashedKey)
			require.True(t, c.storageAddrLen > 0,
				"storage key %x: found but storageAddrLen=0", pk)
			require.Equal(t, pk, c.storageAddr[:c.storageAddrLen],
				"storage key %x: plain key mismatch", pk)
		} else {
			require.True(t, found, "account key %x not found (hashed %x)", pk, hashedKey)
			require.True(t, c.accountAddrLen > 0,
				"account key %x: found but accountAddrLen=0", pk)
			require.Equal(t, pk, c.accountAddr[:c.accountAddrLen],
				"account key %x: plain key mismatch", pk)
		}
	}

	// Verify miss: an unwritten key should not be found.
	missKey := make([]byte, length.Addr)
	missKey[0] = 0xEE
	missKey[1] = 0xEE
	hashedMiss := KeyToHexNibbleHash(missKey)
	_, found, err := reader.Lookup(hashedMiss)
	require.NoError(t, err)
	require.False(t, found, "unwritten key should not be found")
}

// TestTrieReader_RoundTripWithHPH_ManyAccounts uses a larger account set to
// exercise deeper trie structures and extension nodes.
func TestTrieReader_RoundTripWithHPH_ManyAccounts(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	ms := NewMockState(t)

	hph := NewHexPatriciaHashed(int16(length.Addr), ms)
	hph.SetTrace(false)

	// Generate 100 accounts with distinct addresses.
	ub := NewUpdateBuilder()
	for i := 0; i < 100; i++ {
		addr := fmt.Sprintf("%040x", i+1) // 20-byte hex addresses
		ub.Balance(addr, uint64(1000+i))
		ub.Nonce(addr, uint64(i))
	}

	plainKeys, updates := ub.Build()
	upds := WrapKeyUpdates(t, ModeDirect, KeyToHexNibbleHash, plainKeys, updates)
	defer upds.Close()

	err := ms.applyPlainUpdates(plainKeys, updates)
	require.NoError(t, err)

	rootHash, err := hph.Process(ctx, upds, "", nil, WarmupConfig{})
	require.NoError(t, err)
	require.NotEmpty(t, rootHash)

	t.Logf("rootHash: %x, branches: %d, accounts: %d", rootHash, len(ms.cm), len(plainKeys))

	reader := NewTrieReader(ms, length.Addr)

	foundCount := 0
	for i, pk := range plainKeys {
		if updates[i].Flags&DeleteUpdate != 0 {
			continue
		}
		hashedKey := KeyToHexNibbleHash(pk)
		c, found, err := reader.Lookup(hashedKey)
		require.NoError(t, err, "Lookup error for key %x", pk)
		if found && c.accountAddrLen > 0 {
			require.Equal(t, pk, c.accountAddr[:c.accountAddrLen])
			foundCount++
		}
	}
	t.Logf("Found %d/%d accounts via TrieReader", foundCount, len(plainKeys))
	require.Equal(t, len(plainKeys), foundCount, "expected all accounts to be found")
}
