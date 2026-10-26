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
// Package deriver rebuilds the L2 chain from the batches L1 settled.
package deriver

import (
	"slices"
	"sync"

	"github.com/erigontech/erigon/common"
)

// Record is a settled batch: where L1 applied it and the L2 block its
// settlement ended at.
type Record struct {
	L1Block     uint64
	L1BlockHash common.Hash
	TxHash      common.Hash
	LastL2Block uint64
}

// Index holds the settlements derived so far, in L1 order. It is safe for
// concurrent use.
type Index struct {
	mu      sync.RWMutex
	records []Record
	// from is the lowest L2 block Covering attributes: a settlement before
	// the records may have settled the ones below.
	from uint64
}

// Last returns the latest settlement.
func (x *Index) Last() (Record, bool) {
	x.mu.RLock()
	defer x.mu.RUnlock()
	return x.last()
}

func (x *Index) last() (Record, bool) {
	if len(x.records) == 0 {
		return Record{}, false
	}
	return x.records[len(x.records)-1], true
}

// Cursor is the L2 block L1's latest settlement ended at, zero before any.
func (x *Index) Cursor() uint64 {
	r, _ := x.Last()
	return r.LastL2Block
}

// Contains reports whether the batch posted by txHash is indexed.
func (x *Index) Contains(txHash common.Hash) bool {
	x.mu.RLock()
	defer x.mu.RUnlock()
	return x.contains(txHash)
}

func (x *Index) contains(txHash common.Hash) bool {
	for _, r := range x.records {
		if r.TxHash == txHash {
			return true
		}
	}
	return false
}

// Seed indexes r as the first settlement, one that may follow others.
func (x *Index) Seed(r Record) {
	x.mu.Lock()
	defer x.mu.Unlock()
	x.records = append(x.records, r)
	x.from = max(x.from, r.LastL2Block+1)
}

// Append indexes r unless its batch is indexed already.
func (x *Index) Append(r Record) bool {
	x.mu.Lock()
	defer x.mu.Unlock()
	if x.contains(r.TxHash) {
		return false
	}
	x.records = append(x.records, r)
	return true
}

// RetreatOnReorg drops the settlements above the L1 block ancestor and
// returns how many it dropped.
func (x *Index) RetreatOnReorg(ancestor uint64) int {
	x.mu.Lock()
	defer x.mu.Unlock()
	kept := x.records[:0]
	for _, r := range x.records {
		if r.L1Block <= ancestor {
			kept = append(kept, r)
		}
	}
	dropped := len(x.records) - len(kept)
	x.records = kept
	return dropped
}

// Prune drops the settlements of L1 blocks before the one holding the latest
// settlement up to l1Finalized: the scanner never returns below finality, and
// a reorg there halts.
func (x *Index) Prune(l1Finalized uint64) {
	x.mu.Lock()
	defer x.mu.Unlock()
	for i, r := range slices.Backward(x.records) {
		if r.L1Block <= l1Finalized {
			first := slices.IndexFunc(x.records[:i+1], func(s Record) bool { return s.L1Block == r.L1Block })
			for _, dropped := range x.records[:first] {
				x.from = max(x.from, dropped.LastL2Block+1)
			}
			x.records = slices.Delete(x.records, 0, first)
			return
		}
	}
}

// SettledAt returns the L2 block the latest settlement in L1 blocks up to
// number ended at.
func (x *Index) SettledAt(number uint64) (uint64, bool) {
	x.mu.RLock()
	defer x.mu.RUnlock()
	for _, r := range slices.Backward(x.records) {
		if r.L1Block <= number {
			return r.LastL2Block, true
		}
	}
	return 0, false
}

// Covering returns the first settlement that ended at or above L2 block
// number, the one that settled it, from the lowest block it attributes.
func (x *Index) Covering(number uint64) (Record, bool) {
	x.mu.RLock()
	defer x.mu.RUnlock()
	if number < x.from {
		return Record{}, false
	}
	for _, r := range x.records {
		if r.LastL2Block >= number {
			return r, true
		}
	}
	return Record{}, false
}
