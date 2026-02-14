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
	"math"
	"strings"
	"sync"
	"time"
	"unsafe"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbutils"
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
	if len(diffSet) == 0 {
		return append(out, 0, 0, 0, 0, 0) // dict len (1) + diffSet len (4)
	}

	// Build dictionary using fixed array instead of map.
	// PrevStepBytes is always 8 bytes, so we use uint64 for fast comparison.
	// There are typically very few unique values (< 10), so linear search beats map.
	var dictKeys [256]uint64
	dictLen := 0
	totalKeyLen := 0
	totalValueLen := 0

	// First pass: build dictionary and count sizes
	for i := range diffSet {
		prevStep := binary.BigEndian.Uint64(diffSet[i].PrevStepBytes)

		// Linear search for existing entry
		found := false
		for j := 0; j < dictLen; j++ {
			if dictKeys[j] == prevStep {
				found = true
				break
			}
		}
		if !found {
			dictKeys[dictLen] = prevStep
			dictLen++
		}
		totalKeyLen += len(diffSet[i].Key)
		totalValueLen += len(diffSet[i].Value)
	}

	// Pre-calculate total size and ensure capacity
	// dict: 1 + 9*dictLen
	// diffSet header: 4
	// per entry: 4 + keyLen + 4 + valueLen + 1
	totalSize := len(out) + 1 + 9*dictLen + 4 + len(diffSet)*(4+4+1) + totalKeyLen + totalValueLen
	if cap(out) < totalSize {
		ret := make([]byte, len(out), totalSize)
		copy(ret, out)
		out = ret
	}
	ret := out

	// Write dictionary length
	ret = append(ret, byte(dictLen))

	// Write dictionary entries
	for j := 0; j < dictLen; j++ {
		ret = binary.BigEndian.AppendUint64(ret, dictKeys[j])
		ret = append(ret, byte(j))
	}

	// Write diffSet length
	ret = binary.BigEndian.AppendUint32(ret, uint32(len(diffSet)))

	// Second pass: write entries with dict lookup (dict is small, so this is fast)
	for i := range diffSet {
		prevStep := binary.BigEndian.Uint64(diffSet[i].PrevStepBytes)

		// Find index in dict
		var idx byte
		for j := 0; j < dictLen; j++ {
			if dictKeys[j] == prevStep {
				idx = byte(j)
				break
			}
		}

		// write uint32(len(key)) + key + uint32(len(value)) + value + prevStepBytes
		ret = binary.BigEndian.AppendUint32(ret, uint32(len(diffSet[i].Key)))
		ret = append(ret, diffSet[i].Key...)
		ret = binary.BigEndian.AppendUint32(ret, uint32(len(diffSet[i].Value)))
		ret = append(ret, diffSet[i].Value...)
		ret = append(ret, idx)
	}
	return ret
}

func serializeDiffSetBufLen(diffSet []kv.DomainEntryDiff) int {
	// Count unique prevStepBytes using slice instead of map
	var dictKeys [256]uint64
	dictLen := 0
	totalEntrySize := 0

	for i := range diffSet {
		prevStep := binary.BigEndian.Uint64(diffSet[i].PrevStepBytes)

		// Linear search for existing entry
		found := false
		for j := 0; j < dictLen; j++ {
			if dictKeys[j] == prevStep {
				found = true
				break
			}
		}
		if !found {
			dictKeys[dictLen] = prevStep
			dictLen++
		}
		totalEntrySize += 4 + len(diffSet[i].Key) + 4 + len(diffSet[i].Value) + 1
	}
	// dict: 1 + 9*dictLen, diffSet header: 4, entries: totalEntrySize
	return 1 + 9*dictLen + 4 + totalEntrySize
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

const DiffChunkKeyLen = 48
const DiffChunkLen = 4*1024 - 32

type threadSafeBuf struct {
	b []byte
	sync.Mutex
}

var writeDiffsetBuf = &threadSafeBuf{}

func WriteDiffSet(tx kv.RwTx, blockNumber uint64, blockHash common.Hash, diffSet *StateChangeSet) error {
	writeDiffsetBuf.Lock()
	defer writeDiffsetBuf.Unlock()

	writeDiffsetBuf.b = diffSet.serializeKeys(writeDiffsetBuf.b[:0], blockNumber)
	keys := writeDiffsetBuf.b

	c, err := tx.RwCursor(kv.ChangeSets3)
	if err != nil {
		return err
	}
	defer c.Close()

	chunkCount := (len(keys) + DiffChunkLen - 1) / DiffChunkLen
	// Data Format
	// dbutils.BlockBodyKey(blockNumber, blockHash) -> chunkCount
	// dbutils.BlockBodyKey(blockNumber, blockHash) + index -> chunk
	// Write the chunk count
	if err := tx.Put(kv.ChangeSets3, dbutils.BlockBodyKey(blockNumber, blockHash), dbutils.EncodeBlockNumber(uint64(chunkCount))); err != nil {
		return err
	}

	key := make([]byte, DiffChunkKeyLen)
	binary.BigEndian.PutUint64(key, blockNumber)
	copy(key[8:], blockHash[:])

	for i := 0; i < chunkCount; i++ {
		start := i * DiffChunkLen
		end := min((i+1)*DiffChunkLen, len(keys))
		binary.BigEndian.PutUint64(key[40:], uint64(i))

		if err := c.Put(key, keys[start:end]); err != nil {
			return err
		}
	}

	if dbg.TraceUnwinds {
		var diffStats strings.Builder
		if diffSet != nil {
			first := true
			for d, diff := range &diffSet.Diffs {
				if first {
					diffStats.WriteString(" ")
					first = false
				} else {
					diffStats.WriteString(", ")
				}
				diffStats.WriteString(fmt.Sprintf("%s: %d", kv.Domain(d), diff.Len()))
			}
		}
		fmt.Printf("[dbg] diffset (Block:%d) %x:%s chunkCount: %d, %s\n", blockNumber, blockHash, diffStats.String(), chunkCount, dbg.Stack())
	}
	return nil
}

func ReadDiffSet(tx kv.Tx, blockNumber uint64, blockHash common.Hash) ([kv.DomainLen][]kv.DomainEntryDiff, bool, error) {
	// Read the diffSet from the database
	chunkCountBytes, err := tx.GetOne(kv.ChangeSets3, dbutils.BlockBodyKey(blockNumber, blockHash))
	if err != nil {
		return [kv.DomainLen][]kv.DomainEntryDiff{}, false, err
	}
	if len(chunkCountBytes) == 0 {
		return [kv.DomainLen][]kv.DomainEntryDiff{}, false, nil
	}
	chunkCount, err := dbutils.DecodeBlockNumber(chunkCountBytes)
	if err != nil {
		return [kv.DomainLen][]kv.DomainEntryDiff{}, false, err
	}

	key := make([]byte, 48)
	val := make([]byte, 0, DiffChunkLen*chunkCount)
	for i := uint64(0); i < chunkCount; i++ {
		binary.BigEndian.PutUint64(key, blockNumber)
		copy(key[8:], blockHash[:])
		binary.BigEndian.PutUint64(key[40:], i)
		chunk, err := tx.GetOne(kv.ChangeSets3, key)
		if err != nil {
			return [kv.DomainLen][]kv.DomainEntryDiff{}, false, err
		}
		if len(chunk) == 0 {
			return [kv.DomainLen][]kv.DomainEntryDiff{}, false, nil
		}
		val = append(val, chunk...)
	}

	return deserializeKeys(val), true, nil
}
func ReadLowestUnwindableBlock(tx kv.Tx) (uint64, error) {
	//TODO: move this function somewhere from `commitment`/`state` pkg
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
func toStringZeroCopy(v []byte) string {
	if len(v) == 0 {
		return ""
	}
	return unsafe.String(&v[0], len(v))
}

func toBytesZeroCopy(s string) []byte { return unsafe.Slice(unsafe.StringData(s), len(s)) }

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
