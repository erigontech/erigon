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
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/commitment/eip8297"
)

type leafRefTestContext struct {
	*trieTestContext
	refData []byte
	refs    *commitment.LeafRefs
}

func (c *leafRefTestContext) LeafRefs(key, data []byte) *commitment.LeafRefs {
	if !bytes.Equal(c.refData, data) {
		return nil
	}
	if c.refs != nil {
		return c.refs
	}
	return ComputeLeafRefs(key, data)
}

func TestPBinLeafRefsRejectChangedRecordBytes(t *testing.T) {
	first := trieCodeKey(0, 0, 1)
	second := trieCodeKey(0, 2, 2)
	base := newTrieTestContext()
	_, err := NewTrie(base).Process([]Op{{Key: first, Value: testTrieValue(1)}, {Key: second, Value: testTrieValue(2)}})
	require.NoError(t, err)
	plain := newTrieTestContext()
	plain.records = cloneTrieRecords(base.records)
	refs := &leafRefTestContext{trieTestContext: &trieTestContext{records: cloneTrieRecords(base.records)}}
	refs.refData = bytes.Clone(base.records[string(GlobalRootKey())])
	refs.refs = ComputeLeafRefs(GlobalRootKey(), refs.refData)
	changedRecord, err := DecodeRecord(GlobalRootKey(), plain.records[string(GlobalRootKey())])
	require.NoError(t, err)
	changedRecord.Cells[2].Value[0]++
	changed, err := EncodeRecord(GlobalRootKey(), &changedRecord)
	require.NoError(t, err)
	plain.records[string(GlobalRootKey())] = changed
	refs.records[string(GlobalRootKey())] = bytes.Clone(changed)
	_, err = NewTrie(plain).Process([]Op{{Key: first, Value: testTrieValue(3)}})
	require.NoError(t, err)
	_, err = NewTrie(refs).Process([]Op{{Key: first, Value: testTrieValue(3)}})
	require.NoError(t, err)
	require.Equal(t, plain.records, refs.records)
}

func TestPBinComputeLeafRefsHashesEveryRowCell(t *testing.T) {
	key := GlobalRootKey()
	address := make([]byte, 20)
	accountKey := eip8297.TreeKeyAccount(address, eip8297.BasicDataLeafKey)
	slot := make([]byte, 32)
	slot[31] = eip8297.HeaderStorageSlots
	storageKey := eip8297.TreeKeyStorage(address, slot)
	record := Record{Form: RowRoot}
	record.Cells[0] = leafCell(accountKey, testTrieValue(1)).Cell
	record.Cells[15] = leafCell(storageKey, testTrieValue(2)).Cell
	encoded, err := EncodeRecord(key, &record)
	require.NoError(t, err)
	refs := ComputeLeafRefs(key, encoded)
	require.NotNil(t, refs)
	require.Equal(t, uint16(0x8001), refs.Mask)
	require.Equal(t, leafHash(&record.Cells[0]), common.Hash(refs.Refs[0]))
	require.Equal(t, leafHash(&record.Cells[15]), common.Hash(refs.Refs[1]))
}

func TestPBinLeafRefsReduceFoldHashing(t *testing.T) {
	first := eip8297.TreeKeyCodeChunk(common.Hash{}, 0)
	address := bytes.Repeat([]byte{0x01}, 20)
	slot := make([]byte, 32)
	slot[31] = eip8297.HeaderStorageSlots
	second := eip8297.TreeKeyStorage(address, slot)
	base := newTrieTestContext()
	_, err := NewTrie(base).Process([]Op{{Key: first, Value: testTrieValue(1)}, {Key: second, Value: testTrieValue(2)}})
	require.NoError(t, err)
	changedRecord, err := DecodeRecord(GlobalRootKey(), base.records[string(GlobalRootKey())])
	require.NoError(t, err)
	changedRecord.Cells[2].Value[0]++
	changed, err := EncodeRecord(GlobalRootKey(), &changedRecord)
	require.NoError(t, err)
	plain := newTrieTestContext()
	plain.records = cloneTrieRecords(base.records)
	plain.records[string(GlobalRootKey())] = bytes.Clone(changed)
	withRefs := &leafRefTestContext{trieTestContext: &trieTestContext{records: cloneTrieRecords(plain.records)}, refData: bytes.Clone(changed)}
	withRefs.refs = ComputeLeafRefs(GlobalRootKey(), changed)
	count := func(ctx commitment.PatriciaContext) int64 {
		var calls atomic.Int64
		previous := hashHook
		hashHook = func([]byte) { calls.Add(1) }
		defer func() { hashHook = previous }()
		_, processErr := NewTrie(ctx).Process([]Op{{Key: first, Value: testTrieValue(3)}})
		require.NoError(t, processErr)
		return calls.Load()
	}
	plainCalls := count(plain)
	refCalls := count(withRefs)
	require.Less(t, refCalls, plainCalls)
}

func cloneTrieRecords(records map[string][]byte) map[string][]byte {
	clone := make(map[string][]byte, len(records))
	for key, value := range records {
		clone[key] = bytes.Clone(value)
	}
	return clone
}
