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
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/commitment/nibbles"
)

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

func (tc *trieReaderTestCtx) putBranch(nibblePrefix []byte, cells [16]*cell) {
	var afterMap uint16
	var encData [16]cellEncodeData
	for i := range 16 {
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

func makeBranchCell(hash []byte) *cell {
	c := &cell{}
	c.hashLen = int16(len(hash))
	copy(c.hash[:], hash)
	return c
}

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

	ctx := newTrieReaderTestCtx()
	addr := bytes.Repeat([]byte{0xCC}, 20)
	hashedAddr := KeyToHexNibbleHash(addr)
	var rootCells [16]*cell
	rootCells[hashedAddr[0]] = makeAccountCell(addr, dummyHash())
	ctx.putBranch(nil, rootCells)

	missKey := make([]byte, 64)
	missKey[0] = (hashedAddr[0] + 1) % 16
	tr := NewTrieReader(ctx, length.Addr)
	_, found, err := tr.Lookup(missKey)
	require.NoError(t, err)
	require.False(t, found, "expected key not found")
}

func TestTrieReader_MissEmptyTrie(t *testing.T) {
	t.Parallel()

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

	ctx := newTrieReaderTestCtx()
	addr := bytes.Repeat([]byte{0xDD}, 20)
	hashedKey := KeyToHexNibbleHash(addr)

	var rootCells [16]*cell
	rootCells[hashedKey[0]] = makeExtensionCell(hashedKey[1:3], dummyHash())
	ctx.putBranch(nil, rootCells)

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

	ctx := newTrieReaderTestCtx()
	var rootCells [16]*cell
	rootCells[0x3] = makeExtensionCell([]byte{0x7, 0x2}, dummyHash())
	ctx.putBranch(nil, rootCells)

	hashedKey := make([]byte, 64)
	hashedKey[0] = 0x3
	hashedKey[1] = 0x7
	hashedKey[2] = 0x9

	tr := NewTrieReader(ctx, length.Addr)
	_, found, err := tr.Lookup(hashedKey)
	require.NoError(t, err)
	require.False(t, found, "expected miss on extension mismatch")
}

func TestTrieReader_MultiLevelDescent(t *testing.T) {
	t.Parallel()

	ctx := newTrieReaderTestCtx()
	addr := bytes.Repeat([]byte{0xEE}, 20)
	hashedKey := KeyToHexNibbleHash(addr)
	depth := 12

	for d := range depth {
		var cells [16]*cell
		cells[hashedKey[d]] = makeBranchCell(dummyHash())
		ctx.putBranch(hashedKey[:d], cells)
	}

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

	ctx := newTrieReaderTestCtx()
	storAddr := bytes.Repeat([]byte{0xFF}, 52) // 20 addr + 32 slot
	hashedKey := KeyToHexNibbleHash(storAddr)  // 128 nibbles
	require.Equal(t, 128, len(hashedKey), "storage hashed key must be 128 nibbles")

	var storageCells [16]*cell
	storageCells[hashedKey[64]] = makeStorageCell(storAddr, dummyHash())
	ctx.putBranch(hashedKey[:64], storageCells)

	for d := range 64 {
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

	_, found, err := tr.Lookup([]byte{})
	require.NoError(t, err)
	require.False(t, found)

	_, found, err = tr.Lookup(nil)
	require.NoError(t, err)
	require.False(t, found)
}

func TestTrieReader_RoundTripWithHPH(t *testing.T) {
	t.Parallel()

	ms := NewMockState(t)

	hph := NewHexPatriciaHashed(int16(length.Addr), ms, DefaultTrieConfig())
	hph.SetTraceWriter(nil)

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

	rootHash := processBatch(t, ms, hph, plainKeys, updates)
	require.NotEmpty(t, rootHash)

	t.Logf("rootHash: %x, branches stored: %d", rootHash, len(ms.cm))
	require.True(t, len(ms.cm) > 0, "expected at least one branch stored")

	reader := NewTrieReader(ms, length.Addr)

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

	missKey := make([]byte, length.Addr)
	missKey[0] = 0xEE
	missKey[1] = 0xEE
	hashedMiss := KeyToHexNibbleHash(missKey)
	_, found, err := reader.Lookup(hashedMiss)
	require.NoError(t, err)
	require.False(t, found, "unwritten key should not be found")
}

func TestTrieReader_RoundTripWithHPH_ManyAccounts(t *testing.T) {
	t.Parallel()

	ms := NewMockState(t)

	hph := NewHexPatriciaHashed(int16(length.Addr), ms, DefaultTrieConfig())
	hph.SetTraceWriter(nil)

	ub := NewUpdateBuilder()
	for i := range 100 {
		addr := fmt.Sprintf("%040x", i+1) // 20-byte hex addresses
		ub.Balance(addr, uint64(1000+i))
		ub.Nonce(addr, uint64(i))
	}

	plainKeys, updates := ub.Build()
	rootHash := processBatch(t, ms, hph, plainKeys, updates)
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
