package tools

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

// LeafHashInputs holds the additional hash components for the four-layer
// leaf structure. All fields are optional — zero values are used when
// the corresponding proof layer is not available.
type LeafHashInputs struct {
	PreStateHash     common.Hash // keccak256 of sorted pre-state reads
	TransitionHash   common.Hash // proof-of-transition hash (embeds exec hash)
	PreviousLeafHash common.Hash // hash of the previous leaf (chain inclusion)
}

// StateEntry implements qmtree.Entry for a single transaction's state changes.
// The leaf hash is keccak256(preStateHash || stateChangeHash || transitionHash || previousLeafHash).
type StateEntry struct {
	txNum   uint64
	hash    common.Hash
	changes []StateChange
}

// NewStateEntry creates a StateEntry from a list of state changes and
// optional proof layer inputs. Changes are sorted and hashed to produce
// the stateChangeHash component. The final leaf hash combines all four
// proof layers:
//
//	leaf = keccak256(preStateHash || stateChangeHash || transitionHash || previousLeafHash)
func NewStateEntry(txNum uint64, changes []StateChange, opts ...LeafHashInputs) *StateEntry {
	sortChanges(changes)
	stateChangeHash := hashChanges(changes)

	var leaf common.Hash
	if len(opts) > 0 {
		o := opts[0]
		h := crypto.NewKeccakState()
		defer crypto.ReturnToPool(h)
		h.Write(o.PreStateHash[:])
		h.Write(stateChangeHash[:])
		h.Write(o.TransitionHash[:])
		h.Write(o.PreviousLeafHash[:])
		h.Read(leaf[:])
	} else {
		leaf = stateChangeHash
	}

	return &StateEntry{
		txNum:   txNum,
		hash:    leaf,
		changes: changes,
	}
}

func (e *StateEntry) Hash() common.Hash    { return e.hash }
func (e *StateEntry) TxNum() uint64 { return e.txNum }
func (e *StateEntry) Len() int64           { return 0 }
func (e *StateEntry) Changes() []StateChange { return e.changes }
// Components is not implemented for PoC StateEntry (hash was computed externally).
func (e *StateEntry) Components() (pre, stateChange, transition common.Hash) { return }

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
