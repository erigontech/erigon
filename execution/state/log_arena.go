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

package state

import (
	"fmt"
	"slices"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/types"
)

// reset is the ownership boundary: what it hands out stays live until reclaimed — a transaction's worth or a block's, by caller.
const (
	// What one transaction's gas can buy, under the EIP-7825 cap.
	maxLogsPerTxn     = int(params.MaxTxnGasLimit / params.LogGas)
	maxLogBytesPerTxn = int(params.MaxTxnGasLimit / params.LogDataGas)

	// The pool never shrinks: the full ceiling would park 13MB per arena, a tenth covers a p99 block's ~1700 logs.
	maxPooledLogEntries = maxLogsPerTxn / 10
	maxPooledLogBytes   = maxLogBytesPerTxn / 2

	// One large log must not take the whole budget from small entries, and a much lower cap would lose large-log reuse.
	maxPooledLogDataCap = maxPooledLogBytes / 8

	// Bounded by memory (32KB of pointers), not by a transaction's budget: the run's size follows a block.
	maxRetainedLogSlots = 32 * 1024 / 8
)

// logArena recycles a block's log entries through a pool, so memory use follows the largest transaction, not the widest block.
type logArena struct {
	entries      types.Logs   // current run's entries, in allocation order
	pool         []*types.Log // entries taken back at reset, for any transaction to reuse
	poolBytes    int          // Data the pool holds
	indexInBlock uint
}

// alloc's entry keeps unwritten fields from the previous tx and must be copied before being retained past this call.
func (a *logArena) alloc(j *journal, addr common.Address, txIndex, numTopics, dataSize int) *types.Log {
	j.addLogChange(txIndex)
	logIdx := len(a.entries)
	entries := slices.Grow(a.entries, 1)[:logIdx+1]
	a.entries = entries

	// Always take: prior reset/revertLast already emptied this slot, so no live entry is ever double-issued.
	lp := a.take()
	entries[logIdx] = lp
	lp.Address = addr
	lp.Topics = slices.Grow(lp.Topics[:0], numTopics)[:numTopics]
	lp.Data = slices.Grow(lp.Data[:0], dataSize)[:dataSize]
	lp.Removed = false
	lp.TxHash, lp.BlockHash = common.Hash{}, common.Hash{}
	lp.TxIndex = hexutil.Uint(txIndex)
	lp.BlockNumber = 0 // non-consensus field, assigned by the caller
	// Block-wide: receipts.DeriveFields reads Logs[0].Index for FirstLogIndexWithinBlock.
	lp.Index = hexutil.Uint(a.indexInBlock)
	a.indexInBlock++
	return lp
}

// take returns a pooled entry, Topics and Data included for alloc to grow into.
func (a *logArena) take() *types.Log {
	n := len(a.pool) - 1
	if n < 0 {
		return &types.Log{}
	}
	lp := a.pool[n]
	a.pool = a.pool[:n]
	a.poolBytes -= cap(lp.Data)
	return lp
}

// put pools the entry for reuse, dropping Data over budget; past the entry cap it drops the entry too.
func (a *logArena) put(lp *types.Log) {
	if len(a.pool) >= maxPooledLogEntries {
		return
	}
	if cap(lp.Data) > maxPooledLogDataCap || a.poolBytes+cap(lp.Data) > maxPooledLogBytes {
		lp.Data = nil
	}
	a.poolBytes += cap(lp.Data)
	a.pool = append(a.pool, lp)
}

// reset returns written entries to the pool, walking only what was written, not the block's full width.
func (a *logArena) reset() {
	a.indexInBlock = 0
	entries := a.entries
	for i, lp := range entries {
		if lp == nil {
			continue
		}
		a.put(lp)
		entries[i] = nil
	}
	if cap(entries) > maxRetainedLogSlots {
		a.entries = nil // an outlier keeps its entries, not the array that held them
	} else {
		a.entries = entries[:0]
	}
}

func (a *logArena) revertLast(txIndex int) {
	entries := a.entries
	last := len(entries) - 1
	if last < 0 {
		panic(fmt.Sprintf("can't revert log of tx %d: none were emitted", txIndex))
	}
	if lp := entries[last]; lp != nil {
		a.put(lp)
		entries[last] = nil
	}
	a.entries = entries[:last]
	a.indexInBlock--
}

// forTx returns txIndex's entries, the run's tail: newer reads as empty (nothing emitted yet); older panics rather than returning a stale tail.
func (a *logArena) forTx(txIndex int) types.Logs {
	entries := a.entries
	i := len(entries)
	if i > 0 && txIndex < int(entries[i-1].TxIndex) {
		panic(fmt.Sprintf("logs of tx %d asked for, the run has reached tx %d",
			txIndex, int(entries[i-1].TxIndex)))
	}
	for i > 0 && int(entries[i-1].TxIndex) == txIndex {
		i--
	}
	return entries[i:]
}

func (a *logArena) release() { *a = logArena{} }
