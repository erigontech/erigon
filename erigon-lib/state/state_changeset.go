package state

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"sort"
	"time"

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

func (s *StateChangeSet) Compress() {
	start := time.Now()
	for i := range s.Diffs {
		s.Diffs[i].Compress()
	}
	fmt.Println("DEBUG: StateChangeSet.Compress() took", time.Since(start))
}

type KVPair struct {
	Key   []byte
	Value []byte
}

// StateDiffDomain represents a domain of state changes.
type StateDiffDomain struct {
	// We can probably flatten these into single slices for GC/cache optimization
	keys       []KVPair
	prevValues []KVPair
	sorted     bool
	compressed bool
}

// RecordDelta records a state change.
func (d *StateDiffDomain) DomainUpdate(key1, key2, prevValue []byte, prevStep uint64) {
	prevStepBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(prevStepBytes, ^prevStep)
	key := append(key1, key2...)
	prevValue = common.Copy(prevValue)

	d.keys = append(d.keys, KVPair{Key: key, Value: prevStepBytes})                                   // Key -> Step
	d.prevValues = append(d.prevValues, KVPair{Key: append(key, prevStepBytes...), Value: prevValue}) // Key + Step -> Value
	d.sorted = false
	d.compressed = false
}

func (d *StateDiffDomain) sort() {
	if d.sorted {
		return
	}
	sort.Slice(d.keys, func(i, j int) bool {
		return bytes.Compare(d.keys[i].Key, d.keys[j].Key) < 0
	})
	sort.Slice(d.prevValues, func(i, j int) bool {
		return bytes.Compare(d.prevValues[i].Key, d.prevValues[j].Key) < 0
	})
	d.sorted = true
}

func (d *StateDiffDomain) Compress() {
	if d.compressed {
		return
	}
	// map reduce value in backward order
	mapPrevValues := make(map[string][]byte)
	for i := len(d.prevValues) - 1; i >= 0; i-- {
		kv := d.prevValues[i]
		mapPrevValues[string(kv.Key)] = kv.Value
	}

	mapPrevKeys := make(map[string][]byte)
	for i := len(d.keys) - 1; i >= 0; i-- {
		kv := d.keys[i]
		mapPrevKeys[string(kv.Key)] = kv.Value
	}
	d.prevValues = d.prevValues[:0]
	d.keys = d.keys[:0]

	for k, v := range mapPrevKeys {
		d.keys = append(d.keys, KVPair{Key: []byte(k), Value: v})
	}
	for k, v := range mapPrevValues {
		d.prevValues = append(d.prevValues, KVPair{Key: []byte(k), Value: v})
	}
	fmt.Println("DEBUG: StateDiffDomain.Compress() d.keys:", len(d.keys), len(d.prevValues))
	d.compressed = true
	d.sorted = false
}

func (d *StateDiffDomain) GetKeys() (keysToSteps []KVPair, keysToValue []KVPair) {
	d.Compress()
	d.sort()
	return d.keys, d.prevValues
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
	s.st.Add(hash, cs)
}

var GlobalChangesetStorage = NewChangesetStorage()
