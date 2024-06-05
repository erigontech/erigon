package state

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"sort"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/dbutils"
)

const MaxFastChangesets = 64

type StateDiffKind uint8

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
	fmt.Println(dictLen)
	in = in[1:]
	dict := make(map[byte][]byte)
	for i := 0; i < dictLen; i++ {
		key := in[:8]
		value := in[8]
		dict[value] = key
		in = in[9:]
	}
	fmt.Println(dict)
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

func WriteDiffSet(tx kv.RwTx, blockNumber uint64, blockHash common.Hash, diffSet *StateChangeSet) error {
	// Write the diffSet to the database
	keys := diffSet.SerializeKeys(nil)
	return tx.Put(kv.ChangeSets3, dbutils.BlockBodyKey(blockNumber, blockHash), keys)
}

func ReadDiffSet(tx kv.Tx, blockNumber uint64, blockHash common.Hash) ([kv.DomainLen][]DomainEntryDiff, bool, error) {
	// Read the diffSet from the database
	keys, err := tx.GetOne(kv.ChangeSets3, dbutils.BlockBodyKey(blockNumber, blockHash))
	if err != nil {
		return [kv.DomainLen][]DomainEntryDiff{}, false, err
	}
	if len(keys) == 0 {
		return [kv.DomainLen][]DomainEntryDiff{}, false, nil
	}
	return DeserializeKeys(keys), true, nil
}
