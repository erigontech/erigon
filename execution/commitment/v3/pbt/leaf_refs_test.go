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
	"unsafe"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/commitment/eip8297"
)

type leafRefTestContext struct {
	*trieTestContext
	refKey  []byte
	refData []byte
	refs    *commitment.LeafRefs
}

func (c *leafRefTestContext) LeafRefs(key, data []byte) *commitment.LeafRefs {
	if !bytes.Equal(c.refKey, key) || !bytes.Equal(c.refData, data) {
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
	third := trieCodeKey(0x80, 0, 3)
	base := newTrieTestContext()
	_, err := NewTrie(base).Process([]Op{{Key: first, Value: testTrieValue(1)}, {Key: second, Value: testTrieValue(2)}, {Key: third, Value: testTrieValue(3)}})
	require.NoError(t, err)
	plain := newTrieTestContext()
	plain.records = cloneTrieRecords(base.records)
	firstPath, err := keyPath(first)
	require.NoError(t, err)
	var refKey []byte
	var changedRecord Record
	changedSlot := -1
	for key, raw := range base.records {
		path, pathErr := eip8297.DecodeBitPath([]byte(key))
		if pathErr != nil || path.BitLen == 0 {
			continue
		}
		record, recordErr := DecodeRecord([]byte(key), raw)
		if recordErr != nil || record.Form != RowRoot || !pathHasPrefix(&firstPath, &path) || firstPath.BitLen < path.BitLen+4 {
			continue
		}
		firstSlot := slotAt(&firstPath, path.BitLen)
		for slot := range record.Cells {
			if slot != firstSlot && record.Cells[slot].Kind != EmptyCell {
				refKey = bytes.Clone([]byte(key))
				changedRecord = record
				changedSlot = slot
				break
			}
		}
		if changedSlot != -1 {
			break
		}
	}
	require.NotEqual(t, -1, changedSlot)
	if changedRecord.Cells[changedSlot].Kind == LeafCell {
		changedRecord.Cells[changedSlot].Value[0]++
	} else {
		changedRecord.Cells[changedSlot].Left[0]++
	}
	changed, err := EncodeRecord(refKey, &changedRecord)
	require.NoError(t, err)
	plain.records[string(refKey)] = changed
	refs := &leafRefTestContext{trieTestContext: &trieTestContext{records: cloneTrieRecords(base.records)}, refKey: refKey, refData: bytes.Clone(base.records[string(refKey)])}
	refs.refs = ComputeLeafRefs(refKey, refs.refData)
	refs.records[string(refKey)] = bytes.Clone(changed)
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
	record.Cells[0] = *leafCell(accountKey, testTrieValue(1)).Cell
	record.Cells[15] = *leafCell(storageKey, testTrieValue(2)).Cell
	encoded, err := EncodeRecord(key, &record)
	require.NoError(t, err)
	refs := ComputeLeafRefs(key, encoded)
	require.NotNil(t, refs)
	require.Equal(t, uint16(0x8001), refs.Mask)
	require.Equal(t, leafHash(&record.Cells[0]), common.Hash(refs.Refs[0]))
	require.Equal(t, leafHash(&record.Cells[15]), common.Hash(refs.Refs[1]))
}

func TestPBinRowNodeLayout(t *testing.T) {
	t.Logf("rowNode=%d rowCell=%d cell=%d", unsafe.Sizeof(rowNode{}), unsafe.Sizeof(rowCell{}), unsafe.Sizeof(Cell{}))
	require.Less(t, unsafe.Sizeof(rowCell{}), unsafe.Sizeof(Cell{}))
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
	withRefs := &leafRefTestContext{trieTestContext: &trieTestContext{records: cloneTrieRecords(plain.records)}, refKey: GlobalRootKey(), refData: bytes.Clone(changed)}
	withRefs.refs = ComputeLeafRefs(GlobalRootKey(), changed)
	count := func(ctx commitment.PatriciaContext) (common.Hash, int64) {
		var calls atomic.Int64
		previous := hashHook
		hashHook = func([]byte) { calls.Add(1) }
		defer func() { hashHook = previous }()
		root, processErr := NewTrie(ctx).Process([]Op{{Key: first, Value: testTrieValue(3)}})
		require.NoError(t, processErr)
		return root, calls.Load()
	}
	plainRoot, plainCalls := count(plain)
	refRoot, refCalls := count(withRefs)
	require.Equal(t, plainRoot, refRoot)
	require.Equal(t, plain.records, withRefs.records)
	require.Less(t, refCalls, plainCalls)
}

func cloneTrieRecords(records map[string][]byte) map[string][]byte {
	clone := make(map[string][]byte, len(records))
	for key, value := range records {
		clone[key] = bytes.Clone(value)
	}
	return clone
}
