package state

import (
	"bytes"
	"encoding/binary"
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

func (s *StateChangeSet) Copy() *StateChangeSet {
	res := *s
	for i := range s.Diffs {
		res.Diffs[i] = *s.Diffs[i].Copy()
	}
	return &res
}

func (s *StateChangeSet) Merge(older *StateChangeSet) {
	if older == nil {
		return
	}
	for i := range s.Diffs {
		s.Diffs[i].Merge(&older.Diffs[i])
	}

	s.BeginTxIndex = older.BeginTxIndex
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

func (d *StateDiffDomain) Merge(older *StateDiffDomain) {
	if older == nil {
		return
	}
	if d.keys == nil {
		d.keys = make(map[string][]byte)
	}
	if d.prevValues == nil {
		d.prevValues = make(map[string][]byte)
	}
	for k, v := range older.keys {
		d.keys[k] = v
	}
	for k, v := range older.prevValues {
		d.prevValues[k] = v
	}
	d.keysSlice = nil
	d.prevValsSlice = nil
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
		d.keysSlice = nil
	}

	prevValue = common.Copy(prevValue)

	valsKey := string(append(common.Copy(key), stepBytes...))
	if _, ok := d.prevValues[valsKey]; !ok {
		if bytes.Equal(stepBytes, prevStepBytes) {
			d.prevValues[valsKey] = prevValue
		} else {
			d.prevValues[valsKey] = []byte{}
		}
		d.prevValsSlice = nil
	}
}

func (d *StateDiffDomain) GetKeys() (keysToSteps []KVPair, keysToValue []KVPair) {
	if len(d.keysSlice) != 0 && len(d.prevValsSlice) != 0 {
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

	s.st.Add(hash, cs)
}

var GlobalChangesetStorage = NewChangesetStorage()
