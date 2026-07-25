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

// Package execobserver is the extension point for custom, execution-flow-native
// indexers. Erigon indexes logs and trace senders/callers inline during block
// execution (execution/state.applyLogsAndTraces4 for the live stage_exec path,
// and stage_custom_trace for the CustomTraceMapReduce backfill). Those sites
// emit a fixed set of built-in inverted indices with no seam for a third party
// to observe the same per-tx logs + senders and build its own projection.
//
// This package is that seam: register an ExecObserver and it is invoked after
// each transaction's indexes are applied — with that tx's logs and trace
// senders/callers — and once per block end. A consumer chooses whether to emit
// tx-level data (from OnTx) or whole-block data (from OnBlockEnd, e.g. a
// consolidated best-bid/offer sampled across pools). Observers write into their
// OWN store; the passed kv.TemporalTx is a read handle onto chain state as of
// this point for optional lookups.
//
// Registration is process-global and keyed implicitly by chain via ChainID on
// each event (a single process may run several chains — e.g. an L1 + L2 pair —
// so an observer filters to the chain(s) it cares about). Observer callbacks
// MUST NOT return errors into execution: an indexing fault must never halt the
// chain, so callbacks are fire-and-forget and are expected to handle/log their
// own errors.
package execobserver

import (
	"sync"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/types"
	"github.com/holiman/uint256"
)

// TxEvent carries one transaction's execution-index output to observers.
type TxEvent struct {
	ChainID    *uint256.Int
	BlockNum   uint64
	BlockTime  uint64 // header timestamp (0 if unavailable on this exec path)
	TxNum      uint64
	Logs       []*types.Log
	Senders    []common.Address // trace "froms" (callers, at the top and nested frames)
	Callers    []common.Address // trace "tos" (callees)
	IsBlockEnd bool             // the synthetic block-end task, not a user tx
	// Tx is a read handle onto chain state as of this execution point. Do not
	// retain past the callback.
	Tx kv.TemporalTx
}

// BlockEvent fires once per block, after its final transaction — the hook for
// whole-block derived data.
type BlockEvent struct {
	ChainID   *uint256.Int
	BlockNum  uint64
	BlockTime uint64
	TxNum     uint64
	Tx        kv.TemporalTx
}

// CommitEvent fires at the FORK-CHOICE-UPDATE / commit boundary, once the
// canonical head has advanced (or unwound) to Head. It is the point at which a
// custom indexer flushes its staged writes ATOMICALLY, so the index only ever
// reflects blocks the chain actually committed. IsUnwind marks a reorg (the
// index should roll back blocks above Head). Fired on the commit goroutine, not
// the execution goroutine — so an observer must buffer per-tx and flush here
// rather than carry an exec-side write tx across goroutines.
type CommitEvent struct {
	ChainID  uint64
	Head     uint64
	IsUnwind bool
}

// ExecObserver receives execution-index events. Implementations must be
// non-blocking and must not panic; errors are the observer's to handle.
type ExecObserver interface {
	OnTx(TxEvent)
	OnBlockEnd(BlockEvent)
	// OnCommit fires at the fork-choice-update boundary; flush staged writes here.
	OnCommit(CommitEvent)
}

var (
	mu        sync.RWMutex
	observers []ExecObserver
)

// Register adds an observer. Safe to call at any time; typically at node start
// before execution begins.
func Register(o ExecObserver) {
	if o == nil {
		return
	}
	mu.Lock()
	observers = append(observers, o)
	mu.Unlock()
}

// HasObservers reports whether any observer is registered. Execution call sites
// gate event construction on this so there is ZERO overhead (no allocation, no
// address-set conversion) in the default, no-observer erigon build.
func HasObservers() bool {
	mu.RLock()
	n := len(observers)
	mu.RUnlock()
	return n > 0
}

// NotifyTx delivers a TxEvent to every observer.
func NotifyTx(ev TxEvent) {
	mu.RLock()
	obs := observers
	mu.RUnlock()
	for _, o := range obs {
		o.OnTx(ev)
	}
}

// NotifyBlockEnd delivers a BlockEvent to every observer.
func NotifyBlockEnd(ev BlockEvent) {
	mu.RLock()
	obs := observers
	mu.RUnlock()
	for _, o := range obs {
		o.OnBlockEnd(ev)
	}
}

// NotifyCommit delivers a CommitEvent (fork-choice-update) to every observer.
func NotifyCommit(ev CommitEvent) {
	mu.RLock()
	obs := observers
	mu.RUnlock()
	for _, o := range obs {
		o.OnCommit(ev)
	}
}
