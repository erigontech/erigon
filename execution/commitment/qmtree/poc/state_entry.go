package poc

import (
	"encoding/binary"
	"sort"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/db/kv"
)

// StateChange represents a single key-value mutation in a transaction.
type StateChange struct {
	Domain kv.Domain
	Key    []byte
	Value  []byte
}

// StateEntry implements qmtree.Entry for a single transaction's state changes.
// The leaf hash is SHA256 of the canonically-encoded sorted changes.
type StateEntry struct {
	txNum   uint64
	hash    common.Hash
	changes []StateChange
}

// NewStateEntry creates a StateEntry from a list of state changes.
// Changes are sorted and hashed deterministically.
func NewStateEntry(txNum uint64, changes []StateChange) *StateEntry {
	sortChanges(changes)
	h := hashChanges(changes)
	return &StateEntry{
		txNum:   txNum,
		hash:    h,
		changes: changes,
	}
}

func (e *StateEntry) Hash() common.Hash    { return e.hash }
func (e *StateEntry) SerialNumber() uint64 { return e.txNum }
func (e *StateEntry) Len() int64           { return 0 }
func (e *StateEntry) Changes() []StateChange { return e.changes }

// sortChanges sorts by (domain, key) lexicographically.
func sortChanges(changes []StateChange) {
	sort.Slice(changes, func(i, j int) bool {
		if changes[i].Domain != changes[j].Domain {
			return changes[i].Domain < changes[j].Domain
		}
		return string(changes[i].Key) < string(changes[j].Key)
	})
}

// hashChanges produces the canonical SHA256 hash of sorted state changes.
//
// Encoding per change:
//
//	[1 byte: domain_id] [2 bytes: key_length BE] [key] [4 bytes: value_length BE] [value]
//
// Empty change list produces SHA256 of the empty byte slice.
func hashChanges(changes []StateChange) common.Hash {
	h := crypto.NewKeccakState()
	defer crypto.ReturnToPool(h)
	var buf [7]byte // max header per change: 1 + 2 + 4
	for _, c := range changes {
		buf[0] = byte(c.Domain)
		binary.BigEndian.PutUint16(buf[1:3], uint16(len(c.Key)))
		h.Write(buf[:3])
		h.Write(c.Key)
		binary.BigEndian.PutUint32(buf[0:4], uint32(len(c.Value)))
		h.Write(buf[:4])
		h.Write(c.Value)
	}
	var out common.Hash
	h.Read(out[:])
	return out
}
