// Copyright 2026 The Erigon Authors
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

package commitment

import (
	"sync"
)

// plainKeyArena hands out stable plainKey copies from fixed chunks; a full chunk
// is replaced, not grown, so earlier sub-slices keep their backing until reset.
type plainKeyArena struct {
	buf []byte
}

const plainKeyArenaChunk = 64 * 1024

func (a *plainKeyArena) intern(b []byte) []byte {
	if len(b) > plainKeyArenaChunk {
		return append([]byte(nil), b...)
	}
	if cap(a.buf)-len(a.buf) < len(b) {
		a.buf = make([]byte, 0, plainKeyArenaChunk)
	}
	off := len(a.buf)
	a.buf = append(a.buf, b...)
	return a.buf[off : off+len(b) : off+len(b)]
}

func (a *plainKeyArena) reset() { a.buf = nil }

// parallelUpdate owns the per-batch state that drives parallel commitment: the
// path-compressed prefix trie of touched keys and a mutex-guarded slice that
// collects deferred branch updates from all workers.
//
// All Insert calls must be serialized by the caller (same constraint as
// prefixTrie). deferredCombined is the one mutable shared slice during the
// parallel phase — appendDeferred guards it.
type parallelUpdate struct {
	trie *prefixTrie

	deferredMu       sync.Mutex
	deferredCombined []*DeferredBranchUpdate

	keyArena plainKeyArena
}

func newParallelUpdate() *parallelUpdate {
	return &parallelUpdate{
		trie: newPrefixTrie(),
	}
}

// Insert adds a hashed key (in nibble form), its plainKey, and an optional
// carried value (nil = fold re-reads from ctx) to the prefix trie.
func (pu *parallelUpdate) Insert(hashedKey, plainKey []byte, update *Update) {
	pu.trie.Insert(hashedKey, plainKey, update)
}

// internKey copies plainKey into the per-batch arena for stable trie retention.
func (pu *parallelUpdate) internKey(plainKey []byte) []byte {
	return pu.keyArena.intern(plainKey)
}

// Reset clears all per-batch state so the parallelUpdate can be reused.
// The underlying arena is recycled.
func (pu *parallelUpdate) Reset() {
	if pu.trie != nil {
		pu.trie.Reset()
	}
	pu.deferredMu.Lock()
	for _, upd := range pu.deferredCombined {
		putDeferredUpdate(upd)
	}
	pu.deferredCombined = pu.deferredCombined[:0]
	pu.deferredMu.Unlock()
	pu.keyArena.reset()
}

// Close releases references owned by the parallelUpdate. After Close the
// instance must not be reused.
func (pu *parallelUpdate) Close() {
	pu.trie = nil
	pu.deferredMu.Lock()
	for _, upd := range pu.deferredCombined {
		putDeferredUpdate(upd)
	}
	pu.deferredCombined = nil
	pu.deferredMu.Unlock()
	pu.keyArena.reset()
}

// appendDeferred merges a worker's deferred branch updates into the shared
// slice. Safe for concurrent callers.
func (pu *parallelUpdate) appendDeferred(updates []*DeferredBranchUpdate) {
	if len(updates) == 0 {
		return
	}
	pu.deferredMu.Lock()
	pu.deferredCombined = append(pu.deferredCombined, updates...)
	pu.deferredMu.Unlock()
}
