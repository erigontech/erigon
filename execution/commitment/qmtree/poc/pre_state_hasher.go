// Copyright 2025 The Erigon Authors
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

package poc

import (
	"encoding/binary"
	"sync"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/db/kv"
)

// PreStateHasher collects state reads during execution and produces a
// deterministic hash of the pre-state. Reads are sorted by (domain, key)
// before hashing to ensure determinism regardless of read order.
//
// The encoding per read matches hashChanges in state_entry.go:
//
//	[1 byte: domain_id] [2 bytes: key_length BE] [key] [4 bytes: value_length BE] [value]
type PreStateHasher struct {
	reads []StateChange // reuse StateChange type: Domain + Key + Value
}

var preStateHasherPool = sync.Pool{
	New: func() any { return &PreStateHasher{} },
}

// GetPreStateHasher returns a PreStateHasher from the pool.
func GetPreStateHasher() *PreStateHasher {
	return preStateHasherPool.Get().(*PreStateHasher)
}

// PutPreStateHasher returns a PreStateHasher to the pool.
func PutPreStateHasher(h *PreStateHasher) {
	preStateHasherPool.Put(h)
}

// Reset clears accumulated reads for reuse.
func (h *PreStateHasher) Reset() {
	h.reads = h.reads[:0]
}

// AddRead records a state read. The key and value are copied.
func (h *PreStateHasher) AddRead(domain kv.Domain, key, value []byte) {
	h.reads = append(h.reads, StateChange{
		Domain: domain,
		Key:    common.Copy(key),
		Value:  common.Copy(value),
	})
}

// Finalize sorts the collected reads and returns their keccak256 hash.
// After Finalize, the hasher can be Reset and reused.
func (h *PreStateHasher) Finalize() common.Hash {
	sortChanges(h.reads) // sort by (domain, key) — same function as state_entry.go

	hasher := crypto.NewKeccakState()
	defer crypto.ReturnToPool(hasher)

	var buf [7]byte
	for _, r := range h.reads {
		buf[0] = byte(r.Domain)
		binary.BigEndian.PutUint16(buf[1:3], uint16(len(r.Key)))
		hasher.Write(buf[:3])
		hasher.Write(r.Key)
		binary.BigEndian.PutUint32(buf[0:4], uint32(len(r.Value)))
		hasher.Write(buf[:4])
		hasher.Write(r.Value)
	}

	var out common.Hash
	hasher.Read(out[:])
	return out
}
