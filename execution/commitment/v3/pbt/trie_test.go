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
	"sort"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/commitment/eip8297"
)

type trieTestContext struct {
	mu             sync.Mutex
	records        map[string][]byte
	reads          [][]byte
	writes         []trieTestWrite
	rejectNilPrev  bool
	keepTombstones bool
	readHook       func([]byte)
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
	c.mu.Lock()
	c.reads = append(c.reads, bytes.Clone(key))
	data := bytes.Clone(c.records[string(key)])
	hook := c.readHook
	c.mu.Unlock()
	if hook != nil {
		hook(bytes.Clone(key))
	}
	return data, 0, nil
}

func (c *trieTestContext) PutBranch(key, data, prev []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	old := c.records[string(key)]
	if c.rejectNilPrev && len(old) != 0 && len(prev) == 0 {
		return fmt.Errorf("nil previous record for %x", key)
	}
	if !bytes.Equal(old, prev) {
		return fmt.Errorf("previous record mismatch for %x", key)
	}
	c.writes = append(c.writes, trieTestWrite{key: bytes.Clone(key), data: bytes.Clone(data), prev: bytes.Clone(prev)})
	if len(data) == 0 && !c.keepTombstones {
		delete(c.records, string(key))
	} else {
		c.records[string(key)] = bytes.Clone(data)
	}
	return nil
}

func (c *trieTestContext) Records() map[string][]byte {
	return c.records
}

func (c *trieTestContext) Account([]byte) (*commitment.Update, error) {
	return nil, fmt.Errorf("unexpected account read")
}

func (c *trieTestContext) Storage([]byte) (*commitment.Update, error) {
	return nil, fmt.Errorf("unexpected storage read")
}

var _ commitment.PatriciaContext = (*trieTestContext)(nil)

func assertPersistedTrie(t *testing.T, ctx *trieTestContext, entries []Op) {
	t.Helper()
	ordered := append([]Op(nil), entries...)
	sort.Slice(ordered, func(i, j int) bool { return bytes.Compare(ordered[i].Key, ordered[j].Key) < 0 })
	wantEntries := make([]eip8297.Entry, len(ordered))
	for i, entry := range ordered {
		wantEntries[i] = eip8297.Entry{Key: entry.Key, Value: entry.Value[:]}
	}
	wantRoot := eip8297.StateRootWithHash(wantEntries, eip8297.SelectedHash())

	reopened := NewTrie(ctx)
	gotRoot := eip8297.EmptyTreeHash
	var err error
	require.NotPanics(t, func() {
		gotRoot, err = reopened.Process(nil)
	})
	require.NoError(t, err)
	require.Equal(t, wantRoot, gotRoot)
	require.NoError(t, reopened.Verify())

	freshContext := newTrieTestContext()
	freshRoot, err := NewTrie(freshContext).Process(ordered)
	require.NoError(t, err)
	require.Equal(t, wantRoot, freshRoot)
	require.Equal(t, freshContext.records, ctx.records)
}

func requireProcess(t *testing.T, ctx *trieTestContext, ops []Op) {
	t.Helper()
	_, err := NewTrie(ctx).Process(ops)
	require.NoError(t, err)
}

func TestTriePersistsEveryChangedRow(t *testing.T) {
	keyA := trieCodeKey(0, 0, 1)
	keyB := trieCodeKey(2, 0, 2)
	keyC := trieCodeKey(1, 0, 3)
	keyD := trieCodeKey(0, 1, 4)
	entries := []Op{{Key: keyA, Value: testTrieValue(1)}, {Key: keyB, Value: testTrieValue(2)}}
	ctx := newTrieTestContext()

	_, err := NewTrie(ctx).Process(entries)
	require.NoError(t, err)
	assertPersistedTrie(t, ctx, entries)

	entries = append(entries, Op{Key: keyC, Value: testTrieValue(3)})
	_, err = NewTrie(ctx).Process(entries[2:])
	require.NoError(t, err)
	assertPersistedTrie(t, ctx, entries)

	entries[0].Value = testTrieValue(5)
	_, err = NewTrie(ctx).Process([]Op{entries[0]})
	require.NoError(t, err)
	assertPersistedTrie(t, ctx, entries)

	entries = append(entries, Op{Key: keyD, Value: testTrieValue(4)})
	_, err = NewTrie(ctx).Process(entries[3:])
	require.NoError(t, err)
	assertPersistedTrie(t, ctx, entries)
}

func TestTrieOwnsOperationKeys(t *testing.T) {
	keyA := trieCodeKey(0, 0, 1)
	keyACopy := bytes.Clone(keyA)
	keyB := trieCodeKey(0x80, 0, 2)
	ctx := newTrieTestContext()
	trie := NewTrie(ctx)
	_, err := trie.Process([]Op{{Key: keyA, Value: testTrieValue(1)}})
	require.NoError(t, err)
	keyA[0] ^= 0xff
	_, err = trie.Process([]Op{{Key: keyB, Value: testTrieValue(2)}})
	require.NoError(t, err)

	wantContext := newTrieTestContext()
	want, err := NewTrie(wantContext).Process([]Op{{Key: keyACopy, Value: testTrieValue(1)}, {Key: keyB, Value: testTrieValue(2)}})
	require.NoError(t, err)
	got, err := NewTrie(ctx).Process(nil)
	require.NoError(t, err)
	require.Equal(t, want, got)
	require.Equal(t, wantContext.records, ctx.records)
}

func TestTrieResetsPendingRoundAtProcessEntry(t *testing.T) {
	ctx := newTrieTestContext()
	trie := NewTrie(ctx)
	keyA := trieCodeKey(0, 0, 1)
	keyB := trieCodeKey(0, 2, 2)
	keyC := trieCodeKey(0, 8, 3)
	keyD := trieCodeKey(0x10, 0, 4)

	_, err := trie.ProcessParallel([]Op{{Key: keyA, Value: testTrieValue(1)}, {Key: keyB, Value: testTrieValue(2)}, {Key: keyC, Value: testTrieValue(3)}}, 2)
	require.NoError(t, err)
	var got common.Hash
	require.NotPanics(t, func() {
		got, err = trie.Process([]Op{{Key: keyB, Value: testTrieValue(4)}, {Key: keyD, Value: testTrieValue(5)}})
	})
	require.NoError(t, err)
	wantContext := newTrieTestContext()
	want, err := NewTrie(wantContext).Process([]Op{
		{Key: keyA, Value: testTrieValue(1)},
		{Key: keyB, Value: testTrieValue(4)},
		{Key: keyC, Value: testTrieValue(3)},
		{Key: keyD, Value: testTrieValue(5)},
	})
	require.NoError(t, err)
	require.Equal(t, want, got)
	require.Equal(t, wantContext.records, ctx.records)
}

func TestTrieBatchReadsOnlyPreviousRightEdge(t *testing.T) {
	keyA := trieCodeKey(0, 0, 1)
	keyB := trieCodeKey(0x10, 0, 2)
	keyC := trieCodeKey(0x20, 0, 3)
	keyD := trieCodeKey(0x80, 0, 4)
	first := []Op{{Key: keyA, Value: testTrieValue(1)}, {Key: keyB, Value: testTrieValue(2)}, {Key: keyD, Value: testTrieValue(4)}}
	ctx := newTrieTestContext()
	requireProcess(t, ctx, first)

	ctx.mu.Lock()
	previous := make(map[string][]byte, len(ctx.records))
	for key, value := range ctx.records {
		previous[key] = bytes.Clone(value)
	}
	ctx.reads = nil
	ctx.mu.Unlock()
	requireProcess(t, ctx, []Op{{Key: keyC, Value: testTrieValue(3)}})

	keyPath, err := keyPath(keyC)
	require.NoError(t, err)
	rightEdge := make(map[string]struct{})
	for key := range previous {
		path, err := eip8297.DecodeBitPath([]byte(key))
		if bytes.Equal([]byte(key), GlobalRootKey()) || err == nil && keyPath.HasPrefix(&path) {
			rightEdge[key] = struct{}{}
		}
	}
	ctx.mu.Lock()
	reads := append([][]byte(nil), ctx.reads...)
	ctx.mu.Unlock()
	for _, read := range reads {
		if _, existed := previous[string(read)]; existed {
			_, allowed := rightEdge[string(read)]
			require.True(t, allowed, "read completed row %x outside right edge", read)
		}
	}
	require.Len(t, reads, len(rightEdge))
}

func TestTriePersistsPrefixSplitRows(t *testing.T) {
	tests := []struct {
		name    string
		initial []Op
		insert  Op
	}{
		{
			name:    "bit 12",
			initial: []Op{{Key: trieCodeKey(0, 0, 1), Value: testTrieValue(1)}, {Key: trieCodeKey(0x10, 0, 2), Value: testTrieValue(2)}},
			insert:  Op{Key: trieCodeKey(0x08, 0, 3), Value: testTrieValue(3)},
		},
		{
			name:    "bit 16",
			initial: []Op{{Key: trieCodeKey(0, 0, 1), Value: testTrieValue(1)}, {Key: trieCodeKey(0x10, 0, 2), Value: testTrieValue(2)}},
			insert:  Op{Key: trieCodeKey(0, 0x80, 3), Value: testTrieValue(3)},
		},
		{
			name:    "bit 8 then bit 271",
			initial: []Op{{Key: trieCodeKey(0, 0, 0), Value: testTrieValue(1)}, {Key: trieCodeKey(0x80, 0, 2), Value: testTrieValue(2)}},
			insert:  Op{Key: trieCodeKey(0, 0, 1), Value: testTrieValue(3)},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := newTrieTestContext()
			_, err := NewTrie(ctx).Process(tt.initial)
			require.NoError(t, err)
			assertPersistedTrie(t, ctx, tt.initial)

			entries := append(append([]Op(nil), tt.initial...), tt.insert)
			_, err = NewTrie(ctx).Process([]Op{tt.insert})
			require.NoError(t, err)
			assertPersistedTrie(t, ctx, entries)
		})
	}

	address := bytes.Repeat([]byte{0x72}, 20)
	account := accountKey(0, eip8297.BasicDataLeafKey)
	first := eip8297.TreeKeyStorage(address, storageSlot(64))
	second := eip8297.TreeKeyStorage(address, storageSlot(65))
	initial := []Op{{Key: account, Value: testTrieValue(1)}, {Key: first, Value: testTrieValue(2)}}
	ctx := newTrieTestContext()
	_, err := NewTrie(ctx).Process(initial)
	require.NoError(t, err)
	assertPersistedTrie(t, ctx, initial)
	entries := append(append([]Op(nil), initial...), Op{Key: second, Value: testTrieValue(3)})
	_, err = NewTrie(ctx).Process([]Op{entries[2]})
	require.NoError(t, err)
	assertPersistedTrie(t, ctx, entries)
}

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
	entries := []Op{{Key: first, Value: testTrieValue(1)}}
	assertPersistedTrie(t, ctx, entries)

	ctx.reads = nil
	trie = NewTrie(ctx)
	root, err = trie.Process([]Op{{Key: second, Value: testTrieValue(2)}})
	require.NoError(t, err)
	require.Equal(t, eip8297.StateRoot([]eip8297.Entry{
		{Key: first, Value: testTrieValueBytes(1)},
		{Key: second, Value: testTrieValueBytes(2)},
	}), root)
	require.NoError(t, trie.Verify())
	entries = append(entries, Op{Key: second, Value: testTrieValue(2)})
	assertPersistedTrie(t, ctx, entries)

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
	entries = append(entries, Op{Key: third, Value: testTrieValue(3)})
	assertPersistedTrie(t, ctx, entries)
}

func TestTrieRootForms(t *testing.T) {
	ctx := newTrieTestContext()
	trie := NewTrie(ctx)
	account := accountKey(0, eip8297.BasicDataLeafKey)
	storage := eip8297.TreeKeyStorage(bytes.Repeat([]byte{1}, 20), storageSlotKey())
	root, err := trie.Process([]Op{{Key: account, Value: testTrieValue(1)}})
	require.NoError(t, err)
	require.Equal(t, eip8297.StateRoot([]eip8297.Entry{{Key: account, Value: testTrieValueBytes(1)}}), root)
	entries := []Op{{Key: account, Value: testTrieValue(1)}}
	assertPersistedTrie(t, ctx, entries)
	root, err = trie.Process([]Op{{Key: storage, Value: testTrieValue(2)}})
	require.NoError(t, err)
	require.Equal(t, eip8297.StateRoot([]eip8297.Entry{
		{Key: account, Value: testTrieValueBytes(1)},
		{Key: storage, Value: testTrieValueBytes(2)},
	}), root)
	require.NoError(t, trie.Verify())
	entries = append(entries, Op{Key: storage, Value: testTrieValue(2)})
	assertPersistedTrie(t, ctx, entries)
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
		entries := []Op{{Key: a, Value: testTrieValue(1)}, {Key: b, Value: testTrieValue(2)}}
		_, err := trie.Process(entries)
		require.NoError(t, err)
		assertPersistedTrie(t, ctx, entries)
		trie = NewTrie(ctx)
		_, err = trie.Process([]Op{{Key: storage, Value: storageValue}})
		require.NoError(t, err)
		entries = append(entries, Op{Key: storage, Value: storageValue})
		assertPersistedTrie(t, ctx, entries)
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
	assertPersistedTrie(t, ctx, []Op{{Key: a, Value: testTrieValue(1)}, {Key: b, Value: testTrieValue(2)}, {Key: storage, Value: storageValue}, {Key: join, Value: testTrieValue(3)}})

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
	assertPersistedTrie(t, ctx, []Op{{Key: a, Value: testTrieValue(1)}, {Key: b, Value: testTrieValue(2)}, {Key: storage, Value: storageValue}, {Key: newRow, Value: testTrieValue(4)}})
}

func TestTrieRootFormsPersisted(t *testing.T) {
	ctx := newTrieTestContext()
	trie := NewTrie(ctx)
	root := eip8297.EmptyTreeHash
	var err error
	require.NotPanics(t, func() {
		root, err = trie.Process(nil)
	})
	require.NoError(t, err)
	require.Equal(t, eip8297.EmptyTreeHash, root)
	require.Empty(t, ctx.records)
	assertPersistedTrie(t, ctx, nil)

	keyA := trieCodeKey(0, 0, 1)
	keyB := trieCodeKey(0, 2, 2)
	trie = NewTrie(ctx)
	_, err = trie.Process([]Op{{Key: keyA, Value: testTrieValue(1)}})
	require.NoError(t, err)
	record, err := DecodeRecord(GlobalRootKey(), ctx.records[string(GlobalRootKey())])
	require.NoError(t, err)
	require.Equal(t, LeafRoot, record.Form)
	assertPersistedTrie(t, ctx, []Op{{Key: keyA, Value: testTrieValue(1)}})

	trie = NewTrie(ctx)
	_, err = trie.Process([]Op{{Key: keyB, Value: testTrieValue(2)}})
	require.NoError(t, err)
	record, err = DecodeRecord(GlobalRootKey(), ctx.records[string(GlobalRootKey())])
	require.NoError(t, err)
	require.Equal(t, ExtRoot, record.Form)
	assertPersistedTrie(t, ctx, []Op{{Key: keyA, Value: testTrieValue(1)}, {Key: keyB, Value: testTrieValue(2)}})

	rowKey := eip8297.TreeKeyStorage(bytes.Repeat([]byte{1}, 20), storageSlotKey())
	trie = NewTrie(ctx)
	_, err = trie.Process([]Op{{Key: rowKey, Value: eip8297.EncodeStorageValue([]byte{3})}})
	require.NoError(t, err)
	record, err = DecodeRecord(GlobalRootKey(), ctx.records[string(GlobalRootKey())])
	require.NoError(t, err)
	require.Equal(t, RowRoot, record.Form)
	assertPersistedTrie(t, ctx, []Op{{Key: keyA, Value: testTrieValue(1)}, {Key: keyB, Value: testTrieValue(2)}, {Key: rowKey, Value: eip8297.EncodeStorageValue([]byte{3})}})
}

func TestTrieProcessEmptyLoadsPersistedRoot(t *testing.T) {
	ctx := newTrieTestContext()
	key := trieCodeKey(0, 0, 1)
	_, err := NewTrie(ctx).Process([]Op{{Key: key, Value: testTrieValue(1)}})
	require.NoError(t, err)

	root := eip8297.EmptyTreeHash
	require.NotPanics(t, func() {
		root, err = NewTrie(ctx).Process(nil)
	})
	require.NoError(t, err)
	require.Equal(t, eip8297.StateRoot([]eip8297.Entry{{Key: key, Value: testTrieValueBytes(1)}}), root)
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
		assertPersistedTrie(t, incrementalContext, ops[:i+1])

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
