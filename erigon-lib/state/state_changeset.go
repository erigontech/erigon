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

package state

import (
	"bytes"
	"encoding/binary"
	"math"
	"sort"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/kv/dbutils"
)

type StateChangeSet struct {
	Diffs [kv.DomainLen]StateDiffDomain // there are 4 domains of state changes
}

func (s *StateChangeSet) Copy() *StateChangeSet {
	res := *s
	for i := range s.Diffs {
		res.Diffs[i] = *s.Diffs[i].Copy()
	}
	return &res
}

type DomainEntryDiff struct {
	Key           []byte
	Value         []byte
	PrevStepBytes []byte
}

// StateDiffDomain represents a domain of state changes.
type StateDiffDomain struct {
	// We can probably flatten these into single slices for GC/cache optimization
	keys          map[string][]byte
	prevValues    map[string][]byte
	prevValsSlice []DomainEntryDiff
}

func (d *StateDiffDomain) Copy() *StateDiffDomain {
	res := &StateDiffDomain{}
	res.keys = make(map[string][]byte)
	res.prevValues = make(map[string][]byte)
	for k, v := range d.keys {
		res.keys[k] = v
	}
	for k, v := range d.prevValues {
		res.prevValues[k] = v
	}
	return res
}

// RecordDelta records a state change.
func (d *StateDiffDomain) DomainUpdate(key1, key2, prevValue, stepBytes []byte, prevStep uint64) {
	if d.keys == nil {
		d.keys = make(map[string][]byte)
	}
	if d.prevValues == nil {
		d.prevValues = make(map[string][]byte)
	}
	prevStepBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(prevStepBytes, ^prevStep)

	key := append(common.Copy(key1), key2...)

	if _, ok := d.keys[string(key)]; !ok {
		d.keys[string(key)] = prevStepBytes
	}

	prevValue = common.Copy(prevValue)

	valsKey := string(append(common.Copy(key), stepBytes...))
	if _, ok := d.prevValues[valsKey]; !ok {
		if bytes.Equal(stepBytes, prevStepBytes) {
			d.prevValues[valsKey] = prevValue
		} else {
			d.prevValues[valsKey] = []byte{} // We need to delete the current step but restore the previous one
		}
		d.prevValsSlice = nil
	}
}

func (d *StateDiffDomain) GetDiffSet() (keysToValue []DomainEntryDiff) {
	if len(d.prevValsSlice) != 0 {
		return d.prevValsSlice
	}
	d.prevValsSlice = make([]DomainEntryDiff, 0, len(d.prevValues))
	for k, v := range d.prevValues {
		d.prevValsSlice = append(d.prevValsSlice, DomainEntryDiff{
			Key:           []byte(k),
			Value:         v,
			PrevStepBytes: d.keys[k[:len(k)-8]],
		})
	}
	sort.Slice(d.prevValsSlice, func(i, j int) bool {
		return string(d.prevValsSlice[i].Key) < string(d.prevValsSlice[j].Key)
	})

	return d.prevValsSlice
}

func SerializeDiffSet(diffSet []DomainEntryDiff, out []byte) []byte {
	ret := out
	// Write a small dictionary for prevStepBytes
	dict := make(map[string]byte)
	id := byte(0x00)
	for _, diff := range diffSet {
		if _, ok := dict[string(diff.PrevStepBytes)]; ok {
			continue
		}
		dict[string(diff.PrevStepBytes)] = id
		id++
	}
	// Write the dictionary
	ret = append(ret, byte(len(dict)))
	for k, v := range dict {
		ret = append(ret, []byte(k)...) // k is always 8 bytes
		ret = append(ret, v)            // v is always 1 byte
	}
	// Write the diffSet
	tmp := make([]byte, 4)
	binary.BigEndian.PutUint32(tmp, uint32(len(diffSet)))
	ret = append(ret, tmp...)
	for _, diff := range diffSet {
		// write uint32(len(key)) + key + uint32(len(value)) + value + prevStepBytes
		binary.BigEndian.PutUint32(tmp, uint32(len(diff.Key)))
		ret = append(ret, tmp...)
		ret = append(ret, diff.Key...)
		binary.BigEndian.PutUint32(tmp, uint32(len(diff.Value)))
		ret = append(ret, tmp...)
		ret = append(ret, diff.Value...)
		ret = append(ret, dict[string(diff.PrevStepBytes)])
	}
	return ret
}

func SerializeDiffSetBufLen(diffSet []DomainEntryDiff) int {
	// Write a small dictionary for prevStepBytes
	dict := make(map[string]byte)
	id := byte(0x00)
	for _, diff := range diffSet {
		if _, ok := dict[string(diff.PrevStepBytes)]; ok {
			continue
		}
		dict[string(diff.PrevStepBytes)] = id
		id++
	}
	// Write the dictionary
	ret := 1 + 9*len(dict)
	// Write the diffSet
	ret += 4
	for _, diff := range diffSet {
		ret += 4 + len(diff.Key) + 4 + len(diff.Value) + 1
	}
	return ret
}

func DeserializeDiffSet(in []byte) []DomainEntryDiff {
	if len(in) == 0 {
		return nil
	}
	dictLen := int(in[0])
	in = in[1:]
	dict := make(map[byte][]byte)
	for i := 0; i < dictLen; i++ {
		key := in[:8]
		value := in[8]
		dict[value] = key
		in = in[9:]
	}
	diffSetLen := binary.BigEndian.Uint32(in)
	in = in[4:]
	diffSet := make([]DomainEntryDiff, diffSetLen)
	for i := 0; i < int(diffSetLen); i++ {
		keyLen := binary.BigEndian.Uint32(in)
		in = in[4:]
		key := in[:keyLen]
		in = in[keyLen:]
		valueLen := binary.BigEndian.Uint32(in)
		in = in[4:]
		value := in[:valueLen]
		in = in[valueLen:]
		prevStepBytes := dict[in[0]]
		in = in[1:]
		diffSet[i] = DomainEntryDiff{
			Key:           key,
			Value:         value,
			PrevStepBytes: prevStepBytes,
		}
	}
	return diffSet
}

func MergeDiffSets(newer, older []DomainEntryDiff) []DomainEntryDiff {
	// Algorithm
	// Iterate over the newer diffSet
	// If the key in older[i] < key in newer[i] then add older[i] to the result
	// If the key in older[i] == key in newer[i] then add older[i] to the result
	// Else add newer[i] to the result

	// We assume that both diffSets are sorted by key
	var result []DomainEntryDiff
	i, j := 0, 0
	for i < len(newer) && j < len(older) {
		cmp := bytes.Compare(older[j].Key, newer[i].Key)
		if cmp < 0 {
			result = append(result, older[j])
			j++
		} else if cmp == 0 {
			result = append(result, older[j])
			i++
			j++
		} else {
			result = append(result, newer[i])
			i++
		}
	}
	for i < len(newer) {
		result = append(result, newer[i])
		i++
	}
	for j < len(older) {
		result = append(result, older[j])
		j++
	}
	return result
}

func (d *StateChangeSet) SerializeKeys(out []byte) []byte {
	// Do  diff_length + diffSet
	ret := out
	tmp := make([]byte, 4)
	for i := range d.Diffs {

		diffSet := d.Diffs[i].GetDiffSet()
		binary.BigEndian.PutUint32(tmp, uint32(SerializeDiffSetBufLen(diffSet)))
		ret = append(ret, tmp...)
		ret = SerializeDiffSet(diffSet, ret)
	}
	return ret
}

func DeserializeKeys(in []byte) [kv.DomainLen][]DomainEntryDiff {
	var ret [kv.DomainLen][]DomainEntryDiff
	for i := range ret {
		diffSetLen := binary.BigEndian.Uint32(in)
		in = in[4:]
		ret[i] = DeserializeDiffSet(in[:diffSetLen])
		in = in[diffSetLen:]
	}
	return ret
}

const diffChunkKeyLen = 48
const diffChunkLen = 4*1024 - 32

func WriteDiffSet(tx kv.RwTx, blockNumber uint64, blockHash common.Hash, diffSet *StateChangeSet) error {
	// Write the diffSet to the database
	keys := diffSet.SerializeKeys(nil)
	chunkCount := (len(keys) + diffChunkLen - 1) / diffChunkLen
	// Data Format
	// dbutils.BlockBodyKey(blockNumber, blockHash) -> chunkCount
	// dbutils.BlockBodyKey(blockNumber, blockHash) + index -> chunk
	// Write the chunk count
	if err := tx.Put(kv.ChangeSets3, dbutils.BlockBodyKey(blockNumber, blockHash), dbutils.EncodeBlockNumber(uint64(chunkCount))); err != nil {
		return err
	}

	key := make([]byte, diffChunkKeyLen)
	for i := 0; i < chunkCount; i++ {
		start := i * diffChunkLen
		end := (i + 1) * diffChunkLen
		if end > len(keys) {
			end = len(keys)
		}
		binary.BigEndian.PutUint64(key, blockNumber)
		copy(key[8:], blockHash[:])
		binary.BigEndian.PutUint64(key[40:], uint64(i))

		if err := tx.Put(kv.ChangeSets3, key, keys[start:end]); err != nil {
			return err
		}
	}
	return nil
}

func ReadDiffSet(tx kv.Tx, blockNumber uint64, blockHash common.Hash) ([kv.DomainLen][]DomainEntryDiff, bool, error) {
	// Read the diffSet from the database
	chunkCountBytes, err := tx.GetOne(kv.ChangeSets3, dbutils.BlockBodyKey(blockNumber, blockHash))
	if err != nil {
		return [kv.DomainLen][]DomainEntryDiff{}, false, err
	}
	if len(chunkCountBytes) == 0 {
		return [kv.DomainLen][]DomainEntryDiff{}, false, nil
	}
	chunkCount, err := dbutils.DecodeBlockNumber(chunkCountBytes)
	if err != nil {
		return [kv.DomainLen][]DomainEntryDiff{}, false, err
	}

	key := make([]byte, 48)
	val := make([]byte, 0, diffChunkLen*chunkCount)
	for i := uint64(0); i < chunkCount; i++ {
		binary.BigEndian.PutUint64(key, blockNumber)
		copy(key[8:], blockHash[:])
		binary.BigEndian.PutUint64(key[40:], i)
		chunk, err := tx.GetOne(kv.ChangeSets3, key)
		if err != nil {
			return [kv.DomainLen][]DomainEntryDiff{}, false, err
		}
		if len(chunk) == 0 {
			return [kv.DomainLen][]DomainEntryDiff{}, false, nil
		}
		val = append(val, chunk...)
	}

	return DeserializeKeys(val), true, nil
}

func ReadLowestUnwindableBlock(tx kv.Tx) (uint64, error) {
	changesetsCursor, err := tx.Cursor(kv.ChangeSets3)
	if err != nil {
		return 0, err
	}
	defer changesetsCursor.Close()

	/* Rationale */
	/*
		We need to find the first block number in the changesets table that has a valid block number, however we need to avoid gaps.
		In the table there are 2 kinds of keys:
			1. BlockBodyKey(blockNumber, blockHash) -> chunkCount
			2. BlockBodyKey(blockNumber, blockHash) + index -> chunk
		Since key 1 is always lexigographically smaller than key 2, then if we have key 1, we also must have all key 2s for that block number without gaps in chunks.
		Therefore, we can iterate over the keys and find the first key that conform to key 1 (aka len(key) == 40).
	*/
	var first []byte
	for first, _, err = changesetsCursor.First(); first != nil; first, _, err = changesetsCursor.Next() {
		if err != nil {
			return 0, err
		}
		if len(first) == 40 {
			break
		}
	}
	if len(first) < 8 {
		return math.MaxUint64, nil
	}

	blockNumber, err := dbutils.DecodeBlockNumber(first[:8])
	if err != nil {
		return 0, err
	}
	return blockNumber, nil

}
