package state

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"math"
	"sort"
	"sync"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/dbutils"
)

var bufPool = sync.Pool{
	New: func() interface{} {
		return &bytes.Buffer{}
	},
}

type StateChangeSetAccumulator struct {
	Diffs                 [kv.DomainLen]DomainChangesAccumulator // there are 4 domains of state changes
	InvertedIndiciesDiffs [kv.StandaloneIdxLen]InvertedIndexChangesAccumulator
}

// StateDiffDomain represents a domain of state changes.
type DomainChangesAccumulator struct {
	// We can probably flatten these into single slices for GC/cache optimization
	keys       map[string][]byte
	prevValues map[string][]byte
	keyToTxNum map[string][]byte
}

type InvertedIndexChangesAccumulator struct {
	keyToTxNum map[string][]byte
	diffset    []InvertedIndexEntryDiff
}

type DomainEntryDiff struct {
	Key           []byte
	Value         []byte
	PrevStepBytes []byte
	TxNum         []byte
}

type InvertedIndexEntryDiff struct {
	Key   []byte
	TxNum []byte
}

type StateChangeset struct {
	DomainDiffs [kv.DomainLen][]DomainEntryDiff
	IdxDiffs    [kv.StandaloneIdxLen][]InvertedIndexEntryDiff
}

// RecordDelta records a state change.
func (d *DomainChangesAccumulator) DomainUpdate(key1, key2, prevValue, stepBytes []byte, prevStep uint64, txBytes []byte) {
	if d.keys == nil {
		d.keys = make(map[string][]byte)
	}
	if d.prevValues == nil {
		d.prevValues = make(map[string][]byte)
	}
	if d.keyToTxNum == nil {
		d.keyToTxNum = make(map[string][]byte)
	}
	prevStepBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(prevStepBytes, ^prevStep)

	key := append(common.Copy(key1), key2...)
	keyStr := string(key)
	if _, ok := d.keys[keyStr]; !ok {
		d.keys[keyStr] = prevStepBytes
	}

	prevValue = common.Copy(prevValue)
	txBytes = common.Copy(txBytes)

	valsKey := string(append(common.Copy(key), stepBytes...))
	if _, ok := d.prevValues[valsKey]; !ok {
		if bytes.Equal(stepBytes, prevStepBytes) {
			d.prevValues[valsKey] = prevValue
		} else {
			d.prevValues[valsKey] = []byte{} // We need to delete the current step but restore the previous one
		}
	}
	if _, ok := d.keyToTxNum[keyStr]; !ok {
		d.keyToTxNum[keyStr] = txBytes
	}
}

func (d *InvertedIndexChangesAccumulator) InvertedIndexUpdate(key, txBytes []byte) {
	if d.keyToTxNum == nil {
		d.keyToTxNum = make(map[string][]byte)
	}
	keyStr := string(key)
	if _, ok := d.keyToTxNum[keyStr]; !ok {
		d.keyToTxNum[keyStr] = txBytes
	}
}

func mergeSortedLists[T any](newer, older []T, cmpFunc func(i, j int) int) []T {
	// Algorithm
	// Iterate over the newer diffSet
	// If the key in older[i] < key in newer[i] then add older[i] to the result
	// If the key in older[i] == key in newer[i] then add older[i] to the result
	// Else add newer[i] to the result

	// We assume that both diffSets are sorted by key
	var result []T
	i, j := 0, 0
	for i < len(newer) && j < len(older) {
		cmp := cmpFunc(i, j)
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

func (d *StateChangeSetAccumulator) Changeset() StateChangeset {
	var ret StateChangeset
	for idx := range d.Diffs {
		for k, v := range d.Diffs[idx].prevValues {
			ret.DomainDiffs[idx] = append(ret.DomainDiffs[idx], DomainEntryDiff{
				Key:           []byte(k),
				Value:         v,
				PrevStepBytes: d.Diffs[idx].keys[k[:len(k)-8]],
				TxNum:         d.Diffs[idx].keyToTxNum[k[:len(k)-8]],
			})
		}
		sort.Slice(ret.DomainDiffs[idx], func(i, j int) bool {
			return bytes.Compare(ret.DomainDiffs[idx][i].Key, ret.DomainDiffs[idx][j].Key) < 0
		})
	}
	for idx := range d.InvertedIndiciesDiffs {
		for k, v := range d.InvertedIndiciesDiffs[idx].keyToTxNum {
			ret.IdxDiffs[idx] = append(ret.IdxDiffs[idx], InvertedIndexEntryDiff{
				Key:   []byte(k),
				TxNum: v,
			})
		}
		sort.Slice(ret.IdxDiffs[idx], func(i, j int) bool {
			return bytes.Compare(ret.IdxDiffs[idx][i].Key, ret.IdxDiffs[idx][j].Key) < 0
		})
	}
	return ret
}

func (ch *StateChangeset) MergeWithOlder(older *StateChangeset) {
	for idx := range ch.DomainDiffs {
		ch.DomainDiffs[idx] = mergeSortedLists(ch.DomainDiffs[idx], older.DomainDiffs[idx], func(i, j int) int {
			return bytes.Compare(older.DomainDiffs[idx][j].Key, ch.DomainDiffs[idx][i].Key)
		})

	}
	for idx := range ch.IdxDiffs {
		ch.IdxDiffs[idx] = mergeSortedLists(ch.IdxDiffs[idx], older.IdxDiffs[idx], func(i, j int) int {
			return bytes.Compare(older.IdxDiffs[idx][j].Key, ch.IdxDiffs[idx][i].Key)
		})
	}
}

const diffChunkLen = 8*1024 - 32

func WriteDiffSet(tx kv.RwTx, blockNumber uint64, blockHash common.Hash, diffSet StateChangeset) error {
	// Write the diffSet to the database
	buffer := bufPool.Get().(*bytes.Buffer)
	defer bufPool.Put(buffer)
	buffer.Reset()

	if !sort.SliceIsSorted(diffSet.DomainDiffs[0], func(i, j int) bool {
		return bytes.Compare(diffSet.DomainDiffs[0][i].Key, diffSet.DomainDiffs[0][j].Key) < 0
	}) {
		fmt.Println("AXY4")
	}

	gobEncoder := gob.NewEncoder(buffer)
	if err := gobEncoder.Encode(diffSet); err != nil {
		return err
	}
	data := buffer.Bytes()

	chunkCount := (len(data) + diffChunkLen - 1) / diffChunkLen
	// Data Format
	// dbutils.BlockBodyKey(blockNumber, blockHash) -> chunkCount
	// dbutils.BlockBodyKey(blockNumber, blockHash) + index -> chunk
	// Write the chunk count
	if err := tx.Put(kv.ChangeSets3, dbutils.BlockBodyKey(blockNumber, blockHash), dbutils.EncodeBlockNumber(uint64(chunkCount))); err != nil {
		return err
	}

	key := make([]byte, 48)
	for i := 0; i < chunkCount; i++ {
		start := i * diffChunkLen
		end := (i + 1) * diffChunkLen
		if end > len(data) {
			end = len(data)
		}
		binary.BigEndian.PutUint64(key, blockNumber)
		copy(key[8:], blockHash[:])
		binary.BigEndian.PutUint64(key[40:], uint64(i))

		if err := tx.Put(kv.ChangeSets3, key, data[start:end]); err != nil {
			return err
		}
	}
	return nil
}

func ReadDiffSet(tx kv.Tx, blockNumber uint64, blockHash common.Hash) (StateChangeset, bool, error) {
	// Read the diffSet from the database
	chunkCountBytes, err := tx.GetOne(kv.ChangeSets3, dbutils.BlockBodyKey(blockNumber, blockHash))
	if err != nil {
		return StateChangeset{}, false, err
	}
	if len(chunkCountBytes) == 0 {
		return StateChangeset{}, false, nil
	}
	chunkCount, err := dbutils.DecodeBlockNumber(chunkCountBytes)
	if err != nil {
		return StateChangeset{}, false, err
	}

	key := make([]byte, 48)
	val := make([]byte, 0, diffChunkLen*chunkCount)
	for i := uint64(0); i < chunkCount; i++ {
		binary.BigEndian.PutUint64(key, blockNumber)
		copy(key[8:], blockHash[:])
		binary.BigEndian.PutUint64(key[40:], i)
		chunk, err := tx.GetOne(kv.ChangeSets3, key)
		if err != nil {
			return StateChangeset{}, false, err
		}
		if len(chunk) == 0 {
			return StateChangeset{}, false, nil
		}
		val = append(val, chunk...)
	}

	buffer := bytes.NewBuffer(val)
	gobDecoder := gob.NewDecoder(buffer)
	var diffSet StateChangeset
	if err := gobDecoder.Decode(&diffSet); err != nil {
		return StateChangeset{}, false, err
	}

	return diffSet, true, nil
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
