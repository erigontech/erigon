package state

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"sort"

	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv"
)

const MaxFastChangesets = 64

type StateDiffKind uint8

type StateChangeSet struct {
	BeginTxIndex uint64                        // Txn index to unwind to
	Diffs        [kv.DomainLen]StateDiffDomain // there are 4 domains of state changes
}

type KVPair struct {
	Key   []byte
	Value []byte
}

// StateDiffDomain represents a domain of state changes.
type StateDiffDomain struct {
	// We can probably flatten these into single slices for GC/cache optimization
	keys          map[string][]byte
	prevValues    map[string][]byte
	keysSlice     []KVPair
	prevValsSlice []KVPair
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

	key := append(key1, key2...)
	prevValue = common.Copy(prevValue)

	d.keys[string(key)] = prevStepBytes
	fmt.Println(stepBytes, prevStepBytes)
	if bytes.Equal(stepBytes, prevStepBytes) {
		d.prevValues[string(append(key, stepBytes...))] = prevValue
	} else {
		d.prevValues[string(append(key, stepBytes...))] = nil
	}
	d.keysSlice = nil
	d.prevValsSlice = nil
}

func (d *StateDiffDomain) GetKeys() (keysToSteps []KVPair, keysToValue []KVPair) {
	if d.keysSlice != nil && d.prevValsSlice != nil {
		return d.keysSlice, d.prevValsSlice
	}
	d.keysSlice = make([]KVPair, 0, len(d.keys))
	d.prevValsSlice = make([]KVPair, 0, len(d.prevValues))

	for k, v := range d.keys {
		d.keysSlice = append(d.keysSlice, KVPair{Key: []byte(k), Value: v})
	}
	for k, v := range d.prevValues {
		d.prevValsSlice = append(d.prevValsSlice, KVPair{Key: []byte(k), Value: v})
	}
	sort.Slice(d.keysSlice, func(i, j int) bool {
		return string(d.keysSlice[i].Key) < string(d.keysSlice[j].Key)
	})
	sort.Slice(d.prevValsSlice, func(i, j int) bool {
		return string(d.prevValsSlice[i].Key) < string(d.prevValsSlice[j].Key)
	})

	return d.keysSlice, d.prevValsSlice
}

type ChangesetStorage struct {
	st *lru.Cache[common.Hash, *StateChangeSet]
}

func NewChangesetStorage() *ChangesetStorage {
	st, _ := lru.New[common.Hash, *StateChangeSet](MaxFastChangesets)
	return &ChangesetStorage{st: st}
}

func (s *ChangesetStorage) Get(hash common.Hash) (*StateChangeSet, bool) {
	return s.st.Get(hash)
}

func (s *ChangesetStorage) Put(hash common.Hash, cs *StateChangeSet) {
	if cs == nil {
		return
	}
	fmt.Println("ChangesetStorage.Put", hash, cs.BeginTxIndex,
		"diffs[0].keys", len(cs.Diffs[0].keys),
		"diffs[1].keys", len(cs.Diffs[1].keys),
		"diffs[2].keys", len(cs.Diffs[2].keys),
		"diffs[3].keys", len(cs.Diffs[3].keys),
		"diffs[0].prevValues", len(cs.Diffs[0].prevValues),
		"diffs[1].prevValues", len(cs.Diffs[1].prevValues),
		"diffs[2].prevValues", len(cs.Diffs[2].prevValues),
		"diffs[3].prevValues", len(cs.Diffs[3].prevValues))
	s.st.Add(hash, cs)
}

var GlobalChangesetStorage = NewChangesetStorage()
