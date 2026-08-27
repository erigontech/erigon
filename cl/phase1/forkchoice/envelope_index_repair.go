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

package forkchoice

import (
	"sync"

	"github.com/erigontech/erigon/common"
)

type envelopeIndexRepairEntry struct {
	generation  uint64
	durable     bool
	valuesKnown bool
	blockNumber uint64
	blockHash   common.Hash
}

type envelopeIndexRepairToken struct {
	root        common.Hash
	generation  uint64
	valuesKnown bool
	blockNumber uint64
	blockHash   common.Hash
}

type envelopeIndexRepairTracker struct {
	mu             sync.Mutex
	entries        map[common.Hash]envelopeIndexRepairEntry
	order          []common.Hash
	nextGeneration uint64
}

func (t *envelopeIndexRepairTracker) reserve(root common.Hash) (envelopeIndexRepairToken, bool) {
	return t.add(root, false)
}

func (t *envelopeIndexRepairTracker) claim(root common.Hash) (envelopeIndexRepairToken, bool) {
	return t.add(root, true)
}

func (t *envelopeIndexRepairTracker) add(root common.Hash, durable bool) (envelopeIndexRepairToken, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if entry, ok := t.entries[root]; ok {
		return tokenForEnvelopeIndexRepair(root, entry), true
	}
	if len(t.entries) >= queueCacheSize {
		return envelopeIndexRepairToken{}, false
	}
	if t.entries == nil {
		t.entries = make(map[common.Hash]envelopeIndexRepairEntry, queueCacheSize)
	}
	t.nextGeneration++
	if t.nextGeneration == 0 {
		t.nextGeneration++
	}
	t.entries[root] = envelopeIndexRepairEntry{generation: t.nextGeneration, durable: durable}
	t.order = append(t.order, root)
	return tokenForEnvelopeIndexRepair(root, t.entries[root]), true
}

func (t *envelopeIndexRepairTracker) persisted(token envelopeIndexRepairToken, blockNumber uint64, blockHash common.Hash) {
	t.mu.Lock()
	defer t.mu.Unlock()
	entry, ok := t.entries[token.root]
	if !ok || entry.generation != token.generation {
		return
	}
	entry.durable = true
	entry.valuesKnown = true
	entry.blockNumber = blockNumber
	entry.blockHash = blockHash
	t.entries[token.root] = entry
}

func (t *envelopeIndexRepairTracker) setValues(token envelopeIndexRepairToken, blockNumber uint64, blockHash common.Hash) envelopeIndexRepairToken {
	t.mu.Lock()
	defer t.mu.Unlock()
	entry, ok := t.entries[token.root]
	if !ok || entry.generation != token.generation {
		return token
	}
	entry.valuesKnown = true
	entry.blockNumber = blockNumber
	entry.blockHash = blockHash
	t.entries[token.root] = entry
	return tokenForEnvelopeIndexRepair(token.root, entry)
}

func (t *envelopeIndexRepairTracker) release(token envelopeIndexRepairToken) {
	t.mu.Lock()
	defer t.mu.Unlock()
	entry, ok := t.entries[token.root]
	if !ok || entry.generation != token.generation || entry.durable {
		return
	}
	t.remove(token.root)
}

func (t *envelopeIndexRepairTracker) complete(token envelopeIndexRepairToken) {
	t.mu.Lock()
	defer t.mu.Unlock()
	entry, ok := t.entries[token.root]
	if !ok || entry.generation != token.generation {
		return
	}
	t.remove(token.root)
}

func (t *envelopeIndexRepairTracker) repairs() []envelopeIndexRepairToken {
	t.mu.Lock()
	defer t.mu.Unlock()
	repairs := make([]envelopeIndexRepairToken, 0, len(t.entries))
	for _, root := range t.order {
		entry, ok := t.entries[root]
		if ok && entry.durable {
			repairs = append(repairs, tokenForEnvelopeIndexRepair(root, entry))
		}
	}
	return repairs
}

func tokenForEnvelopeIndexRepair(root common.Hash, entry envelopeIndexRepairEntry) envelopeIndexRepairToken {
	return envelopeIndexRepairToken{
		root:        root,
		generation:  entry.generation,
		valuesKnown: entry.valuesKnown,
		blockNumber: entry.blockNumber,
		blockHash:   entry.blockHash,
	}
}

func (t *envelopeIndexRepairTracker) retryFailed(token envelopeIndexRepairToken) {
	t.mu.Lock()
	defer t.mu.Unlock()
	entry, ok := t.entries[token.root]
	if !ok || entry.generation != token.generation || !entry.durable {
		return
	}
	t.moveToBack(token.root)
}

func (t *envelopeIndexRepairTracker) remove(root common.Hash) {
	delete(t.entries, root)
	for i, queuedRoot := range t.order {
		if queuedRoot == root {
			t.order = append(t.order[:i], t.order[i+1:]...)
			return
		}
	}
}

func (t *envelopeIndexRepairTracker) moveToBack(root common.Hash) {
	for i, queuedRoot := range t.order {
		if queuedRoot == root {
			copy(t.order[i:], t.order[i+1:])
			t.order[len(t.order)-1] = root
			return
		}
	}
}
