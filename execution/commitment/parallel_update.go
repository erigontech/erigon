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
	"slices"
	"sync"
)

type plainKeyArena struct {
	buf []byte
}

// Grows geometrically for the same reason the prefix arena does: a fresh buffer is
// built per block, so a block touching two keys must not pay the full chunk.
const (
	plainKeyArenaChunkMin = 1024
	plainKeyArenaChunkMax = 64 * 1024
)

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

const touchChunkKeys = 16384

type touchEntry struct {
	hashedKey []byte
	plainKey  []byte
	update    *Update
}

type parallelUpdate struct {
	trie *prefixTrie

	pending  []touchEntry
	spare    []touchEntry
	inflight sync.WaitGroup

	deferredMu       sync.Mutex
	deferredCombined []*DeferredBranchUpdate

	keyArena plainKeyArena
}

func newParallelUpdate() *parallelUpdate {
	return &parallelUpdate{trie: newPrefixTrie()}
}

// Collect is not safe for concurrent calls; the caller must serialize them.
func (pu *parallelUpdate) Collect(hashedKey, plainKey []byte, update *Update) {
	pu.pending = append(pu.pending, touchEntry{hashedKey: hashedKey, plainKey: plainKey, update: update})
	if len(pu.pending) >= touchChunkKeys {
		pu.handOff()
	}
}

func resetEntries(c []touchEntry) []touchEntry {
	clear(c)
	return c[:0]
}

func (pu *parallelUpdate) handOff() {
	pu.inflight.Wait()
	c := pu.pending
	pu.pending, pu.spare = slices.Grow(pu.spare[:0], touchChunkKeys), c
	pu.inflight.Go(func() {
		pu.insertChunk(c)
		clear(c)
	})
}

func (pu *parallelUpdate) insertChunk(c []touchEntry) {
	for i := range c {
		pu.trie.Insert(c[i].hashedKey, c[i].plainKey, c[i].update)
	}
}

func (pu *parallelUpdate) Build() {
	pu.inflight.Wait()
	pu.insertChunk(pu.pending)
	pu.pending = resetEntries(pu.pending)
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
	pu.pending = resetEntries(pu.pending)
	if pu.trie != nil {
		pu.trie.Reset()
	}
	pu.drainDeferred()
	pu.keyArena.reset()
}

func (pu *parallelUpdate) Close() {
	pu.inflight.Wait()
	pu.pending, pu.spare = nil, nil
	pu.trie = nil
	pu.drainDeferred()
	pu.keyArena.reset()
}

// foldTombstones keeps, for a prefix a self-destruct tombstoned and the same Process
// rebuilt, only the rebuilt branch: a forked child checks it in before the parent's
// tombstone, and merged it onto the old stored branch. Written raw, it carries the
// tombstone's prev, so the domain diff still restores the old branch on unwind.
func (pu *parallelUpdate) foldTombstones() {
	pu.deferredMu.Lock()
	defer pu.deferredMu.Unlock()
	var tombs map[string]*DeferredBranchUpdate
	for _, u := range pu.deferredCombined {
		if u.rawWrite {
			if tombs == nil {
				tombs = map[string]*DeferredBranchUpdate{}
			}
			tombs[string(u.prefix)] = u
		}
	}
	if tombs == nil {
		return
	}
	folded := map[*DeferredBranchUpdate]bool{}
	for _, u := range pu.deferredCombined {
		if t, ok := tombs[string(u.prefix)]; ok && !u.rawWrite {
			u.rawWrite = true
			u.prev = reuseBytes(u.prev, t.prev)
			folded[t] = true
		}
	}
	pu.deferredCombined = slices.DeleteFunc(pu.deferredCombined, func(u *DeferredBranchUpdate) bool {
		if folded[u] {
			putDeferredUpdate(u)
			return true
		}
		return false
	})
}

func (pu *parallelUpdate) appendDeferred(updates []*DeferredBranchUpdate) {
	if len(updates) == 0 {
		return
	}
	pu.deferredMu.Lock()
	pu.deferredCombined = append(pu.deferredCombined, updates...)
	pu.deferredMu.Unlock()
}
