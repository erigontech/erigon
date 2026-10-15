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

// Index holds the settlements derived so far, in L1 order.
type Index struct {
	records []Record
}

// Len is the number of settlements.
func (x *Index) Len() int { return len(x.records) }

// Last returns the latest settlement.
func (x *Index) Last() (Record, bool) {
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
	for _, r := range x.records {
		if r.TxHash == txHash {
			return true
		}
	}
	return false
}

// Append indexes r unless its batch is indexed already.
func (x *Index) Append(r Record) bool {
	if x.Contains(r.TxHash) {
		return false
	}
	x.records = append(x.records, r)
	return true
}

// CountAtL1Block counts the settlements of L1 block number.
func (x *Index) CountAtL1Block(number uint64) int {
	n := 0
	for _, r := range x.records {
		if r.L1Block == number {
			n++
		}
	}
	return n
}

// RetreatOnReorg drops the settlements above the L1 block ancestor and
// returns how many it dropped.
func (x *Index) RetreatOnReorg(ancestor uint64) int {
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

// SettledAt returns the L2 block the latest settlement in L1 blocks up to
// number ended at.
func (x *Index) SettledAt(number uint64) (uint64, bool) {
	for _, r := range slices.Backward(x.records) {
		if r.L1Block <= number {
			return r.LastL2Block, true
		}
	}
	return 0, false
}
