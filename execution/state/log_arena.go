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
	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/types"
)

// reset is the ownership boundary: everything handed out since the last one is
// live, and reset takes it back. What the arena holds is therefore one caller's
// unit of work — a transaction for the executor and the trace workers, a block
// for the assembler and the tooling, which reset once it is built.
const (
	// What one transaction's gas can buy, under the EIP-7825 cap.
	maxLogsPerTxn     = int(params.MaxTxnGasLimit / params.LogGas)     // 44739
	maxLogBytesPerTxn = int(params.MaxTxnGasLimit / params.LogDataGas) // 2MB

	// Fractions of those, because the pool never shrinks: the whole ceiling would
	// park 13MB of entries per arena, while a tenth still holds the p99 block of
	// 1706 logs that a caller resetting per block pools in one go.
	maxPooledLogEntries = maxLogsPerTxn / 10
	maxPooledLogBytes   = maxLogBytesPerTxn / 2

	// One large log must not take the whole budget and starve the small entries
	// real traffic is made of, and a cap much lower would throw away the reuse
	// that makes a block of large logs cheap.
	maxPooledLogDataCap = maxPooledLogBytes / 8

	// Slots the arena keeps between resets. The run is a block for a caller that
	// resets per block, so this is bounded by the memory it costs rather than by
	// a transaction's budget: 32KB of pointers, holding the p99 block of 1706.
	maxRetainedLogSlots = 32 * common.Kibi / 8
)

// logArena owns a block's log entries and recycles them through a pool, so what
// it holds follows the largest transaction rather than the widest block.
type logArena struct {
	entries      types.Logs   // the block so far, in order — one transaction, for a caller that resets per transaction
	pool         []*types.Log // entries taken back at reset, for any transaction to reuse
	poolBytes    int          // Data the pool holds
	indexInBlock uint
}

// alloc journals the allocation for revertLast, then returns txIndex's next
// entry sized for numTopics/dataSize. The caller must write every topic and data
// byte; what it leaves unwritten is the previous transaction's. The arena owns
// the entry, so it must never be handed out without copying.
func (a *logArena) alloc(j *journal, addr common.Address, txIndex, numTopics, dataSize int) *types.Log {
	j.addLogChange(txIndex)
	logIdx := len(a.entries)
	entries := slices.Grow(a.entries, 1)[:logIdx+1]
	a.entries = entries

	// Always take: reset and revertLast empty a slot before shrinking past it, so
	// a slot that still held an entry would be one two transactions share.
	lp := a.take()
	entries[logIdx] = lp
	lp.Address = addr
	lp.Topics = slices.Grow(lp.Topics[:0], numTopics)[:numTopics]
	lp.Data = slices.Grow(lp.Data[:0], dataSize)[:dataSize]
	lp.Removed = false
	lp.TxHash, lp.BlockHash = common.Hash{}, common.Hash{}
	lp.TxIndex = hexutil.Uint(txIndex)
	lp.BlockNumber = 0 // non-consensus field, assigned by the caller
	// Block-wide, not per-tx: receipts.DeriveFields reads Logs[0].Index to
	// recover FirstLogIndexWithinBlock.
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

// put keeps an entry for the next transaction, dropping the Data it may not
// hold. The entry itself is small enough to keep either way.
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

// reset takes back what was written: the entries return to the pool and the
// block goes empty. It walks only what the caller wrote, not the block's width.
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

// revertLast drops the entry allocated last, returning it to the pool.
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

// forTx returns the entries txIndex emitted, owned by the arena. They are held
// in one run rather than grouped, and a transaction's own are the tail of it,
// so any other transaction reads as empty.
func (a *logArena) forTx(txIndex int) types.Logs {
	entries := a.entries
	i := len(entries)
	if dbg.AssertEnabled && i > 0 && txIndex < int(entries[i-1].TxIndex) {
		// Newer than the tail is a transaction that emitted nothing, which is
		// how most of them end. Older is a caller reading what it has left.
		panic(fmt.Sprintf("logs of tx %d asked for, the run has reached tx %d",
			txIndex, int(entries[i-1].TxIndex)))
	}
	for i > 0 && int(entries[i-1].TxIndex) == txIndex {
		i--
	}
	return entries[i:]
}

func (a *logArena) release() { *a = logArena{} }
