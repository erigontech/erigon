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

package composer

import (
	"maps"
	"slices"
	"sync"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/eez/protocol"
	"github.com/erigontech/erigon/execution/types"
)

// Resolution is what is known of a posted batch.
type Resolution uint8

const (
	// Pending is a batch sent with no verdict yet.
	Pending Resolution = iota
	// Settled is a batch L1 settled, kept until L1 finalizes it.
	Settled
	// Failed is a batch that did not land, or landed without reaching its
	// terminal; derivation passing its height overrides it.
	Failed
	// SettledPartial is a batch L1 kept part of: its height stays canonical
	// and its transactions await disposal.
	SettledPartial
)

func (r Resolution) String() string {
	switch r {
	case Settled:
		return "settled"
	case Failed:
		return "failed"
	case SettledPartial:
		return "settled partially"
	}
	return "pending"
}

// Posted is a ledger entry handed out for recovery, disposal or audit.
type Posted[T any] struct {
	// Height is the batch's terminal block.
	Height uint64
	// Post is the hash of the L1 transaction posting the batch.
	Post common.Hash
	// Terminal is the header of the batch's terminal block.
	Terminal *types.Header
	// Batch is the posted batch without its DA and proofs, which judging its
	// receipts does not read.
	Batch *protocol.ProofSystemBatch
	// Txs are the held transactions the batch carries.
	Txs []T
	// SlotSkipped reports a failure not owed to Txs.
	SlotSkipped bool
	// Landed reports a posting whose receipt showed it settling.
	Landed bool
}

type entry[T any] struct {
	Posted[T]
	resolution Resolution
	// confirmed reports that derivation passed the height; it releases Txs
	// once, whichever verdict came first.
	confirmed bool
}

// Ledger tracks the posted batches of a rollup from their sending to L1
// finality, keyed by terminal height. Every verdict names its posting: a
// height rebuilt while an old observer runs must not take the old verdict.
type Ledger[T any] struct {
	mu      sync.Mutex
	entries map[uint64]*entry[T]
}

// NewLedger returns an empty ledger.
func NewLedger[T any]() *Ledger[T] { return &Ledger[T]{entries: map[uint64]*entry[T]{}} }

// heights returns the entry heights in from..to, ascending.
func (l *Ledger[T]) heights(from, to uint64) []uint64 {
	var hs []uint64
	for h := range maps.Keys(l.entries) {
		if h >= from && h <= to {
			hs = append(hs, h)
		}
	}
	slices.Sort(hs)
	return hs
}

// Begin records batch p as sent, replacing an entry at its height.
func (l *Ledger[T]) Begin(p Posted[T]) {
	l.mu.Lock()
	defer l.mu.Unlock()
	p.SlotSkipped, p.Landed = false, false
	l.entries[p.Height] = &entry[T]{Posted: p}
}

// Blocking returns the lowest height above cursor, in any state: only
// derivation passing a batch proves that the next one reads a fresh cursor.
func (l *Ledger[T]) Blocking(cursor uint64) (uint64, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if hs := l.heights(cursor+1, ^uint64(0)); len(hs) > 0 {
		return hs[0], true
	}
	return 0, false
}

// ResolveBelowCursor applies derivation's cursor, which outranks any
// verdict: a batch at or below it is Settled when L1 settled it, else
// SettledPartial, as the cursor confirms the height and not which
// transactions ran. It returns the transactions of the Settled batches it
// confirms first.
func (l *Ledger[T]) ResolveBelowCursor(cursor uint64) []T {
	l.mu.Lock()
	defer l.mu.Unlock()
	var released []T
	for _, h := range l.heights(0, cursor) {
		e := l.entries[h]
		if e.resolution == Pending || e.resolution == Failed {
			e.resolution = SettledPartial
		}
		if e.resolution == Settled && !e.confirmed {
			released = append(released, e.Txs...)
			e.confirmed = true
		}
	}
	return released
}

// find returns the entry of post at height, or nil.
func (l *Ledger[T]) find(height uint64, post common.Hash) *entry[T] {
	if e := l.entries[height]; e != nil && e.Post == post {
		return e
	}
	return nil
}

// MarkSettled records that L1 settled post's batch.
func (l *Ledger[T]) MarkSettled(height uint64, post common.Hash) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if e := l.find(height, post); e != nil {
		e.resolution, e.Landed = Settled, true
	}
}

// MarkSettledPartial records that L1 kept part of post's pending batch.
func (l *Ledger[T]) MarkSettledPartial(height uint64, post common.Hash) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if e := l.find(height, post); e != nil {
		e.Landed = true
		if e.resolution == Pending {
			e.resolution = SettledPartial
		}
	}
}

// MarkFailed records that post's pending batch failed, slotSkipped when not
// owing to its transactions.
func (l *Ledger[T]) MarkFailed(height uint64, post common.Hash, slotSkipped bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if e := l.find(height, post); e != nil && e.resolution == Pending {
		e.resolution, e.SlotSkipped = Failed, slotSkipped
	}
}

// PartialToDispose returns the lowest SettledPartial batch at or below
// cursor, left in place until ClaimPartial: derivation has rebuilt its
// block, whose nonces show which transactions ran.
func (l *Ledger[T]) PartialToDispose(cursor uint64) (Posted[T], bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, h := range l.heights(0, cursor) {
		if e := l.entries[h]; e.resolution == SettledPartial {
			p := e.Posted
			p.Txs = slices.Clone(p.Txs)
			p.SlotSkipped = false
			return p, true
		}
	}
	return Posted[T]{}, false
}

// ClaimPartial settles post's partial batch, keeping spent, the
// transactions that ran; false when a reorg or a rebuild took it first.
func (l *Ledger[T]) ClaimPartial(height uint64, post common.Hash, spent []T) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	e := l.find(height, post)
	if e == nil {
		return false
	}
	e.Txs, e.resolution, e.confirmed = spent, Settled, true
	return true
}

// TakeFailed removes and returns the lowest Failed batch above cursor.
func (l *Ledger[T]) TakeFailed(cursor uint64) (Posted[T], bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, h := range l.heights(cursor+1, ^uint64(0)) {
		if e := l.entries[h]; e.resolution == Failed {
			delete(l.entries, h)
			return e.Posted, true
		}
	}
	return Posted[T]{}, false
}

// SettledAbove returns the Settled batches above cursor, whose settlement
// derivation has yet to see.
func (l *Ledger[T]) SettledAbove(cursor uint64) []Posted[T] {
	l.mu.Lock()
	defer l.mu.Unlock()
	var out []Posted[T]
	for _, h := range l.heights(cursor+1, ^uint64(0)) {
		if e := l.entries[h]; e.resolution == Settled && !e.confirmed {
			out = append(out, e.Posted)
		}
	}
	return out
}

// Unsettle turns post's Settled batch, unconfirmed by derivation, to
// Failed: L1 dropped the block that settled it.
func (l *Ledger[T]) Unsettle(height uint64, post common.Hash) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if e := l.find(height, post); e != nil && e.resolution == Settled && !e.confirmed {
		e.resolution, e.Landed = Failed, false
		return true
	}
	return false
}

// Repend turns post's Settled or SettledPartial batch back to Pending: L1
// dropped the block that settled it, and the posting may land again.
func (l *Ledger[T]) Repend(height uint64, post common.Hash) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if e := l.find(height, post); e != nil && (e.resolution == Settled || e.resolution == SettledPartial) {
		e.resolution, e.confirmed, e.Landed = Pending, false, false
		return true
	}
	return false
}

// RolledOut returns the Settled and SettledPartial batches above cursor,
// which an L1 reorg moved back.
func (l *Ledger[T]) RolledOut(cursor uint64) []Posted[T] {
	l.mu.Lock()
	defer l.mu.Unlock()
	var out []Posted[T]
	for _, h := range l.heights(cursor+1, ^uint64(0)) {
		if e := l.entries[h]; e.resolution == Settled || e.resolution == SettledPartial {
			out = append(out, e.Posted)
		}
	}
	return out
}

// ReinsertFailed puts back a Failed batch whose recovery failed.
func (l *Ledger[T]) ReinsertFailed(p Posted[T]) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.entries[p.Height] = &entry[T]{Posted: p, resolution: Failed}
}

// TakeRolledOut removes the Settled and SettledPartial batches above
// cursor, which an L1 reorg moved back, and returns their transactions.
// Pending and Failed batches keep their own paths.
func (l *Ledger[T]) TakeRolledOut(cursor uint64) []T {
	l.mu.Lock()
	defer l.mu.Unlock()
	var txs []T
	for _, h := range l.heights(cursor+1, ^uint64(0)) {
		if e := l.entries[h]; e.resolution == Settled || e.resolution == SettledPartial {
			delete(l.entries, h)
			txs = append(txs, e.Txs...)
		}
	}
	return txs
}

// TakeFinalized removes and returns the Settled batches at or below
// finalized for an audit of the postings that landed.
func (l *Ledger[T]) TakeFinalized(finalized uint64) []Posted[T] {
	l.mu.Lock()
	defer l.mu.Unlock()
	var out []Posted[T]
	for _, h := range l.heights(0, finalized) {
		if e := l.entries[h]; e.resolution == Settled {
			delete(l.entries, h)
			out = append(out, e.Posted)
		}
	}
	return out
}
