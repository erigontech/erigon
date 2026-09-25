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

package pbt

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/commitment/eip8297"
)

type trieTestContext struct {
	records map[string][]byte
	reads   [][]byte
	writes  []trieTestWrite
}

type trieTestWrite struct {
	key  []byte
	data []byte
	prev []byte
}

func newTrieTestContext() *trieTestContext {
	return &trieTestContext{records: make(map[string][]byte)}
}

func (c *trieTestContext) Branch(key []byte) ([]byte, kv.Step, error) {
	c.reads = append(c.reads, bytes.Clone(key))
	return bytes.Clone(c.records[string(key)]), 0, nil
}

func (c *trieTestContext) PutBranch(key, data, prev []byte) error {
	old := c.records[string(key)]
	if !bytes.Equal(old, prev) {
		return fmt.Errorf("previous record mismatch for %x", key)
	}
	c.writes = append(c.writes, trieTestWrite{key: bytes.Clone(key), data: bytes.Clone(data), prev: bytes.Clone(prev)})
	if len(data) == 0 {
		delete(c.records, string(key))
	} else {
		c.records[string(key)] = bytes.Clone(data)
	}
	return nil
}

func (c *trieTestContext) Account([]byte) (*commitment.Update, error) {
	return nil, fmt.Errorf("unexpected account read")
}

func (c *trieTestContext) Storage([]byte) (*commitment.Update, error) {
	return nil, fmt.Errorf("unexpected storage read")
}

var _ commitment.PatriciaContext = (*trieTestContext)(nil)

func TestTrieSerialInsertsPersistedBatches(t *testing.T) {
	ctx := newTrieTestContext()
	first := []byte{eip8297.CodeZone, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1}
	second := bytes.Clone(first)
	second[len(second)-1] = 2
	third := bytes.Clone(first)
	third[len(third)-1] = 3
	ops := []Op{{Key: first, Value: testTrieValue(1)}}
	trie := NewTrie(ctx)
	root, err := trie.Process(ops)
	require.NoError(t, err)
	require.Equal(t, eip8297.StateRoot([]eip8297.Entry{{Key: first, Value: testTrieValueBytes(1)}}), root)
	require.NoError(t, trie.Verify())

	ctx.reads = nil
	trie = NewTrie(ctx)
	root, err = trie.Process([]Op{{Key: second, Value: testTrieValue(2)}})
	require.NoError(t, err)
	require.Equal(t, eip8297.StateRoot([]eip8297.Entry{
		{Key: first, Value: testTrieValueBytes(1)},
		{Key: second, Value: testTrieValueBytes(2)},
	}), root)
	require.NoError(t, trie.Verify())

	ctx.reads = nil
	trie = NewTrie(ctx)
	root, err = trie.Process([]Op{{Key: third, Value: testTrieValue(3)}})
	require.NoError(t, err)
	require.Equal(t, eip8297.StateRoot([]eip8297.Entry{
		{Key: first, Value: testTrieValueBytes(1)},
		{Key: second, Value: testTrieValueBytes(2)},
		{Key: third, Value: testTrieValueBytes(3)},
	}), root)
	require.NoError(t, trie.Verify())
}

func TestTrieRootForms(t *testing.T) {
	ctx := newTrieTestContext()
	trie := NewTrie(ctx)
	account := accountKey(0, eip8297.BasicDataLeafKey)
	storage := eip8297.TreeKeyStorage(bytes.Repeat([]byte{1}, 20), storageSlotKey())
	root, err := trie.Process([]Op{{Key: account, Value: testTrieValue(1)}})
	require.NoError(t, err)
	require.Equal(t, eip8297.StateRoot([]eip8297.Entry{{Key: account, Value: testTrieValueBytes(1)}}), root)
	root, err = trie.Process([]Op{{Key: storage, Value: testTrieValue(2)}})
	require.NoError(t, err)
	require.Equal(t, eip8297.StateRoot([]eip8297.Entry{
		{Key: account, Value: testTrieValueBytes(1)},
		{Key: storage, Value: testTrieValueBytes(2)},
	}), root)
	require.NoError(t, trie.Verify())
}

func TestTrieFoldDoesNotReadState(t *testing.T) {
	ctx := newTrieTestContext()
	trie := NewTrie(ctx)
	key := accountKey(0, eip8297.BasicDataLeafKey)
	_, err := trie.Process([]Op{{Key: key, Value: testTrieValue(1)}})
	require.NoError(t, err)
	ctx.reads = nil
	_, err = trie.RootHash()
	require.NoError(t, err)
	require.Empty(t, ctx.reads)
}

func TestTrieBranchInsertJoinAndNewRow(t *testing.T) {
	a := trieCodeKey(0, 0, 1)
	b := trieCodeKey(0, 2, 2)
	join := trieCodeKey(0, 8, 3)
	storage := eip8297.TreeKeyStorage(bytes.Repeat([]byte{1}, 20), storageSlotKey())
	storageValue := eip8297.EncodeStorageValue([]byte{9})
	seed := func() *trieTestContext {
		ctx := newTrieTestContext()
		trie := NewTrie(ctx)
		_, err := trie.Process([]Op{{Key: a, Value: testTrieValue(1)}, {Key: b, Value: testTrieValue(2)}})
		require.NoError(t, err)
		trie = NewTrie(ctx)
		_, err = trie.Process([]Op{{Key: storage, Value: storageValue}})
		require.NoError(t, err)
		return ctx
	}
	ctx := seed()
	ctx.reads = nil
	trie := NewTrie(ctx)
	root, err := trie.Process([]Op{{Key: join, Value: testTrieValue(3)}})
	require.NoError(t, err)
	require.Equal(t, eip8297.StateRoot([]eip8297.Entry{
		{Key: a, Value: testTrieValueBytes(1)},
		{Key: b, Value: testTrieValueBytes(2)},
		{Key: storage, Value: storageValue[:]},
		{Key: join, Value: testTrieValueBytes(3)},
	}), root)
	childPath, err := keyPath(a)
	require.NoError(t, err)
	childPath.Truncate(20)
	childKey, err := EncodeRowKey(&childPath)
	require.NoError(t, err)
	require.Equal(t, [][]byte{GlobalRootKey(), childKey}, ctx.reads)
	require.NoError(t, trie.Verify())

	newRow := trieCodeKey(0x10, 0, 4)
	ctx = seed()
	ctx.reads = nil
	trie = NewTrie(ctx)
	root, err = trie.Process([]Op{{Key: newRow, Value: testTrieValue(4)}})
	require.NoError(t, err)
	require.Equal(t, eip8297.StateRoot([]eip8297.Entry{
		{Key: a, Value: testTrieValueBytes(1)},
		{Key: b, Value: testTrieValueBytes(2)},
		{Key: storage, Value: storageValue[:]},
		{Key: newRow, Value: testTrieValueBytes(4)},
	}), root)
	require.Equal(t, [][]byte{GlobalRootKey()}, ctx.reads)
	require.NoError(t, trie.Verify())
}

func TestTrieRootFormsPersisted(t *testing.T) {
	ctx := newTrieTestContext()
	trie := NewTrie(ctx)
	root, err := trie.Process(nil)
	require.NoError(t, err)
	require.Equal(t, eip8297.EmptyTreeHash, root)
	require.Empty(t, ctx.records)

	keyA := trieCodeKey(0, 0, 1)
	keyB := trieCodeKey(0, 2, 2)
	trie = NewTrie(ctx)
	_, err = trie.Process([]Op{{Key: keyA, Value: testTrieValue(1)}})
	require.NoError(t, err)
	record, err := DecodeRecord(GlobalRootKey(), ctx.records[string(GlobalRootKey())])
	require.NoError(t, err)
	require.Equal(t, LeafRoot, record.Form)

	trie = NewTrie(ctx)
	_, err = trie.Process([]Op{{Key: keyB, Value: testTrieValue(2)}})
	require.NoError(t, err)
	record, err = DecodeRecord(GlobalRootKey(), ctx.records[string(GlobalRootKey())])
	require.NoError(t, err)
	require.Equal(t, ExtRoot, record.Form)

	rowKey := eip8297.TreeKeyStorage(bytes.Repeat([]byte{1}, 20), storageSlotKey())
	trie = NewTrie(ctx)
	_, err = trie.Process([]Op{{Key: rowKey, Value: eip8297.EncodeStorageValue([]byte{3})}})
	require.NoError(t, err)
	record, err = DecodeRecord(GlobalRootKey(), ctx.records[string(GlobalRootKey())])
	require.NoError(t, err)
	require.Equal(t, RowRoot, record.Form)
}

func TestTrieIncrementalRecordParity(t *testing.T) {
	storage := eip8297.TreeKeyStorage(bytes.Repeat([]byte{2}, 20), storageSlotKey())
	ops := []Op{
		{Key: trieCodeKey(0, 0, 1), Value: testTrieValue(1)},
		{Key: trieCodeKey(0, 2, 2), Value: testTrieValue(2)},
		{Key: trieCodeKey(0, 8, 3), Value: testTrieValue(3)},
		{Key: trieCodeKey(0x10, 0, 4), Value: testTrieValue(4)},
		{Key: storage, Value: eip8297.EncodeStorageValue([]byte{5})},
	}
	incrementalContext := newTrieTestContext()
	for i := range ops {
		trie := NewTrie(incrementalContext)
		incrementalRoot, err := trie.Process(ops[i : i+1])
		require.NoError(t, err)
		require.NoError(t, trie.Verify())

		freshContext := newTrieTestContext()
		freshRoot, err := NewTrie(freshContext).Process(ops[:i+1])
		require.NoError(t, err)
		require.Equal(t, freshRoot, incrementalRoot)
		require.Equal(t, freshContext.records, incrementalContext.records)
	}
}

func testTrieValue(seed byte) [eip8297.ValueLength]byte {
	var value [eip8297.ValueLength]byte
	value[len(value)-1] = seed
	return value
}

func testTrieValueBytes(seed byte) []byte {
	value := testTrieValue(seed)
	return value[:]
}

func trieCodeKey(byteOne, byteTwo, seed byte) []byte {
	key := make([]byte, eip8297.CodeKeyLength)
	key[0] = eip8297.CodeZone
	key[1] = byteOne
	key[2] = byteTwo
	key[len(key)-1] = seed
	return key
}
