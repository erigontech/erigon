package state

import (
	"bytes"
	"encoding/binary"
	"sort"

	"github.com/ledgerwatch/erigon-lib/common"
)

type StateDiffKind uint8

type StateChangeSet struct {
	BeginTxIndex uint64             // Txn index to unwind to
	Diffs        [4]StateDiffDomain // there are 4 domains of state changes
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

func (d *StateDiffDomain) GetKeys() (keysToSteps []KVPair, keysToValue []KVPair) {
	d.sort()
	return d.keys, d.prevValues
}
