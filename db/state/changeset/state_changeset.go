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

package changeset

import (
	"encoding/binary"
	"fmt"
	"strings"
	"sync"
	"time"
	"unsafe"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/types/accounts"
)

type StateChangeSet struct {
	Diffs [kv.DomainLen]kv.DomainDiff // there are 4 domains of state changes
}

func (s *StateChangeSet) Copy() *StateChangeSet {
	res := *s
	for i := range s.Diffs {
		res.Diffs[i] = *s.Diffs[i].Copy()
	}
	return &res
}
func SerializeDiffSet(diffSet []kv.DomainEntryDiff, out []byte) []byte {
	ret := out
	// Write a small dictionary for prevStepBytes
	dict := make(map[string]byte)
	id := byte(0x00)
	for _, diff := range diffSet {
		prevStepS := toStringZeroCopy(diff.PrevStepBytes)
		if _, ok := dict[prevStepS]; ok {
			continue
		}
		dict[prevStepS] = id
		id++
	}
	// Write the dictionary
	ret = append(ret, byte(len(dict)))
	for k, v := range dict {
		ret = append(ret, []byte(k)...) // k is always 8 bytes
		ret = append(ret, v)            // v is always 1 byte
	}
	// Write the diffSet
	var tmp [4]byte
	binary.BigEndian.PutUint32(tmp[:], uint32(len(diffSet)))
	ret = append(ret, tmp[:]...)
	for _, diff := range diffSet {
		// write uint32(len(key)) + key + uint32(len(value)) + value + prevStepBytes
		binary.BigEndian.PutUint32(tmp[:], uint32(len(diff.Key)))
		ret = append(ret, tmp[:]...)
		ret = append(ret, diff.Key...)
		binary.BigEndian.PutUint32(tmp[:], uint32(len(diff.Value)))
		ret = append(ret, tmp[:]...)
		ret = append(ret, diff.Value...)
		ret = append(ret, dict[toStringZeroCopy(diff.PrevStepBytes)])
	}
	return ret
}

func serializeDiffSetBufLen(diffSet []kv.DomainEntryDiff) int {
	// Write a small dictionary for prevStepBytes
	dict := make(map[string]byte)
	id := byte(0x00)
	for _, diff := range diffSet {
		prevStepS := toStringZeroCopy(diff.PrevStepBytes)
		if _, ok := dict[prevStepS]; ok {
			continue
		}
		dict[prevStepS] = id
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

func DeserializeDiffSet(in []byte) []kv.DomainEntryDiff {
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
	diffSet := make([]kv.DomainEntryDiff, diffSetLen)
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
		diffSet[i] = kv.DomainEntryDiff{
			Key:           toStringZeroCopy(key),
			Value:         value,
			PrevStepBytes: prevStepBytes,
		}
	}
	return diffSet
}

func MergeDiffSets(newer, older []kv.DomainEntryDiff) []kv.DomainEntryDiff {
	// Algorithm
	// Iterate over the newer diffSet
	// If the key in older[i] < key in newer[i] then add older[i] to the result
	// If the key in older[i] == key in newer[i] then add older[i] to the result
	// Else add newer[i] to the result

	// We assume that both diffSets are sorted by key
	var result []kv.DomainEntryDiff
	i, j := 0, 0
	for i < len(newer) && j < len(older) {
		cmp := strings.Compare(older[j].Key, newer[i].Key)
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

func (d *StateChangeSet) serializeKeys(out []byte, blockNumber uint64) []byte {
	// Do  diff_length + diffSet
	ret := out
	tmp := make([]byte, 4)
	for i := range d.Diffs {
		diffSet := d.Diffs[i].GetDiffSet()
		binary.BigEndian.PutUint32(tmp, uint32(serializeDiffSetBufLen(diffSet)))
		ret = append(ret, tmp...)

		if dbg.TraceUnwinds {
			if i == int(kv.AccountsDomain) && dbg.TraceDomain(uint16(kv.AccountsDomain)) {
				for _, entry := range diffSet {
					address := entry.Key[:len(entry.Key)-8]
					keyStep := ^binary.BigEndian.Uint64([]byte(entry.Key[len(entry.Key)-8:]))
					prevStep := ^binary.BigEndian.Uint64(entry.PrevStepBytes)
					if len(entry.Value) > 0 {
						var account accounts.Account
						if err := accounts.DeserialiseV3(&account, entry.Value); err == nil {
							fmt.Printf("diffset (Block:%d): acc %x: {Balance: %d, Nonce: %d, Inc: %d, CodeHash: %x}, step: %d\n", blockNumber, address, &account.Balance, account.Nonce, account.Incarnation, account.CodeHash, keyStep)
						}
					} else {
						if keyStep != prevStep {
							if prevStep == 0 {
								fmt.Printf("diffset (Block:%d): acc %x: [empty], step: %d\n", blockNumber, address, keyStep)
							} else {
								fmt.Printf("diffset (Block:%d): acc: %x, in prev step: {key: %d, prev: %d}\n", blockNumber, address, keyStep, prevStep)
							}
						} else {
							fmt.Printf("diffset (Block:%d): del acc: %x, step: %d\n", blockNumber, address, keyStep)
						}
					}
				}
			}
			if i == int(kv.StorageDomain) && dbg.TraceDomain(uint16(kv.StorageDomain)) {
				for _, entry := range diffSet {
					var address common.Address
					var location common.Hash
					copy(address[:], entry.Key[:length.Addr])
					copy(location[:], entry.Key[length.Addr:len(entry.Key)-8])
					keyStep := ^binary.BigEndian.Uint64([]byte(entry.Key[len(entry.Key)-8:]))
					prevStep := ^binary.BigEndian.Uint64(entry.PrevStepBytes)
					if len(entry.Value) > 0 {
						fmt.Printf("diffset (Block:%d): storage [%x %x] => [%x]\n", blockNumber, address, location, entry.Value)
					} else {
						if keyStep != prevStep {
							if prevStep == 0 {
								fmt.Printf("diffset (Block:%d): storage [%x %x] => [empty], step: %d\n", blockNumber, address, location, keyStep)
							} else {
								fmt.Printf("diffset (Block:%d): storage [%x %x], in prev step: {key: %d, prev: %d}\n", blockNumber, address, location, keyStep, prevStep)
							}
						} else {
							fmt.Printf("diffset (Block:%d): storage [%x %x] => [empty], step: %d\n", blockNumber, address, location, keyStep)
						}
					}
				}
			}
			if i == int(kv.CommitmentDomain) && dbg.TraceDomain(uint16(kv.CommitmentDomain)) {
				for _, entry := range diffSet {
					if entry.Value == nil {
						fmt.Printf("diffset (Block:%d): commitment [%x] => [empty]\n", blockNumber, entry.Key[:len(entry.Key)-8])
					} else {
						if entry.Key[:len(entry.Key)-8] == "state" {
							fmt.Printf("diffset (Block:%d): commitment [%s] => [%x]\n", blockNumber, entry.Key[:len(entry.Key)-8], entry.Value)
						} else {
							fmt.Printf("diffset (Block:%d): commitment [%x] => [%x]\n", blockNumber, entry.Key[:len(entry.Key)-8], entry.Value)
						}
					}
				}
			}
		}
		ret = SerializeDiffSet(diffSet, ret)
	}
	return ret
}

func deserializeKeys(in []byte) [kv.DomainLen][]kv.DomainEntryDiff {
	var ret [kv.DomainLen][]kv.DomainEntryDiff
	for i := range ret {
		diffSetLen := binary.BigEndian.Uint32(in)
		in = in[4:]
		ret[i] = DeserializeDiffSet(in[:diffSetLen])
		in = in[diffSetLen:]
	}
	return ret
}

func SerializeStateChangeSet(diffSet *StateChangeSet, blockNumber uint64) []byte {
	if diffSet == nil {
		return nil
	}
	return diffSet.serializeKeys(nil, blockNumber)
}

func DeserializeStateChangeSet(in []byte) [kv.DomainLen][]kv.DomainEntryDiff {
	return deserializeKeys(in)
}
func toStringZeroCopy(v []byte) string {
	if len(v) == 0 {
		return ""
	}
	return unsafe.String(&v[0], len(v))
}

type DomainIOMetrics struct {
	CacheReadCount    int64
	CacheReadDuration time.Duration
	CacheGetCount     int64
	CachePutCount     int64
	CacheGetSize      int
	CacheGetKeySize   int
	CacheGetValueSize int
	CachePutSize      int
	CachePutKeySize   int
	CachePutValueSize int
	DbReadCount       int64
	DbReadDuration    time.Duration
	FileReadCount     int64
	FileReadDuration  time.Duration
}

type DomainMetrics struct {
	sync.RWMutex
	DomainIOMetrics
	Domains map[kv.Domain]*DomainIOMetrics
}

func (dm *DomainMetrics) UpdateCacheReads(domain kv.Domain, start time.Time) {
	dm.Lock()
	defer dm.Unlock()
	dm.CacheReadCount++
	readDuration := time.Since(start)
	dm.CacheReadDuration += readDuration
	if d, ok := dm.Domains[domain]; ok {
		d.CacheReadCount++
		d.CacheReadDuration += readDuration
	} else {
		dm.Domains[domain] = &DomainIOMetrics{
			CacheReadCount:    1,
			CacheReadDuration: readDuration,
		}
	}
}

func (dm *DomainMetrics) UpdateDbReads(domain kv.Domain, start time.Time) {
	dm.Lock()
	defer dm.Unlock()
	dm.DbReadCount++
	readDuration := time.Since(start)
	dm.DbReadDuration += readDuration
	if d, ok := dm.Domains[domain]; ok {
		d.DbReadCount++
		d.DbReadDuration += readDuration
	} else {
		dm.Domains[domain] = &DomainIOMetrics{
			DbReadCount:    1,
			DbReadDuration: readDuration,
		}
	}
}

func (dm *DomainMetrics) UpdateFileReads(domain kv.Domain, start time.Time) {
	dm.Lock()
	defer dm.Unlock()
	dm.FileReadCount++
	readDuration := time.Since(start)
	dm.FileReadDuration += readDuration
	if d, ok := dm.Domains[domain]; ok {
		d.FileReadCount++
		d.FileReadDuration += readDuration
	} else {
		dm.Domains[domain] = &DomainIOMetrics{
			FileReadCount:    1,
			FileReadDuration: readDuration,
		}
	}
}
