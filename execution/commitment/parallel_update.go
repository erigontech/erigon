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

// Grows geometrically for the same reason the prefix arena does: a fresh buffer is
// built per block, so a block touching two keys must not pay the full chunk.
const plainKeyArenaChunkMin = 1024
const plainKeyArenaChunkMax = 64 * 1024

func (a *plainKeyArena) intern(b []byte) []byte {
	if len(b) > plainKeyArenaChunkMax {
		return append([]byte(nil), b...)
	}
	if cap(a.buf)-len(a.buf) < len(b) {
		next := max(cap(a.buf)*2, plainKeyArenaChunkMin)
		a.buf = make([]byte, 0, min(max(next, len(b)), plainKeyArenaChunkMax))
	}
	off := len(a.buf)
	a.buf = append(a.buf, b...)
	return a.buf[off : off+len(b) : off+len(b)]
}

func (a *plainKeyArena) reset() { a.buf = a.buf[:0] }

const touchChunkKeys = 8192

const touchChunkBuffers = 2

type touchEntry struct {
	hashedKey []byte
	plainKey  []byte
	update    *Update
}

type parallelUpdate struct {
	trie *prefixTrie

	pending   []touchEntry
	chunkKeys int
	buildCh   chan []touchEntry
	freeCh    chan []touchEntry
	inflight  sync.WaitGroup

	deferredMu       sync.Mutex
	deferredCombined []*DeferredBranchUpdate

	keyArena plainKeyArena
}

func newParallelUpdate() *parallelUpdate {
	return &parallelUpdate{
		trie:      newPrefixTrie(),
		chunkKeys: touchChunkKeys,
	}
}

// Collect is not safe for concurrent calls; the caller must serialize them.
func (pu *parallelUpdate) Collect(hashedKey, plainKey []byte, update *Update) {
	pu.pending = append(pu.pending, touchEntry{hashedKey: hashedKey, plainKey: plainKey, update: update})
	if len(pu.pending) >= pu.chunkKeys {
		pu.handOff()
	}
}

func (pu *parallelUpdate) startBuilder() {
	if pu.freeCh == nil {
		pu.freeCh = make(chan []touchEntry, touchChunkBuffers)
		for range touchChunkBuffers {
			pu.freeCh <- make([]touchEntry, 0, pu.chunkKeys)
		}
	}
	pu.buildCh = make(chan []touchEntry, touchChunkBuffers)
	go func(build <-chan []touchEntry, free chan<- []touchEntry) {
		for c := range build {
			pu.insertChunk(c)
			clear(c)
			free <- c[:0]
			pu.inflight.Done()
		}
	}(pu.buildCh, pu.freeCh)
}

func (pu *parallelUpdate) stopBuilder() {
	if pu.buildCh != nil {
		close(pu.buildCh)
		pu.buildCh = nil
	}
}

func (pu *parallelUpdate) handOff() {
	if pu.trie == nil {
		pu.pending = pu.pending[:0]
		return
	}
	if pu.buildCh == nil {
		pu.startBuilder()
	}
	pu.inflight.Add(1)
	pu.buildCh <- pu.pending
	pu.pending = <-pu.freeCh
}

func (pu *parallelUpdate) insertChunk(c []touchEntry) {
	for i := range c {
		pu.trie.Insert(c[i].hashedKey, c[i].plainKey, c[i].update)
	}
}

func (pu *parallelUpdate) Build() {
	if len(pu.pending) > 0 {
		if pu.buildCh != nil {
			pu.handOff()
		} else {
			pu.insertChunk(pu.pending)
			clear(pu.pending)
			pu.pending = pu.pending[:0]
		}
	}
	pu.inflight.Wait()
	pu.stopBuilder()
}

func (pu *parallelUpdate) internKey(plainKey []byte) []byte {
	return pu.keyArena.intern(plainKey)
}

func (pu *parallelUpdate) drainDeferred() {
	pu.deferredMu.Lock()
	for _, upd := range pu.deferredCombined {
		putDeferredUpdate(upd)
	}
	pu.deferredCombined = nil
	pu.deferredMu.Unlock()
}

func (pu *parallelUpdate) Reset() {
	pu.inflight.Wait()
	pu.stopBuilder()
	clear(pu.pending)
	pu.pending = pu.pending[:0]
	if pu.trie != nil {
		pu.trie.Reset()
	}
	pu.drainDeferred()
	pu.keyArena.reset()
}

func (pu *parallelUpdate) Close() {
	pu.inflight.Wait()
	pu.stopBuilder()
	pu.freeCh = nil
	clear(pu.pending)
	pu.pending = nil
	pu.trie = nil
	pu.drainDeferred()
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
