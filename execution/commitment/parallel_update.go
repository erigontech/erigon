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

func (a *plainKeyArena) reset() { a.buf = a.buf[:0] }

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

// Insert is not safe for concurrent calls; the caller must serialize them.
func (pu *parallelUpdate) Insert(hashedKey, plainKey []byte, update *Update) {
	pu.trie.Insert(hashedKey, plainKey, update)
}

func (pu *parallelUpdate) internKey(plainKey []byte) []byte {
	return pu.keyArena.intern(plainKey)
}

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

func (pu *parallelUpdate) appendDeferred(updates []*DeferredBranchUpdate) {
	if len(updates) == 0 {
		return
	}
	pu.deferredMu.Lock()
	pu.deferredCombined = append(pu.deferredCombined, updates...)
	pu.deferredMu.Unlock()
}
