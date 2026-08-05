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
	"github.com/erigontech/erigon/execution/types"
)

// reset is the ownership boundary: everything handed out since the last one is
// live, and reset takes it back. So what the arena has to hold is one caller's
// unit of work — a transaction for the executor and the trace workers, a block
// for the assembler and the tooling, which reset only once it is built. The pool
// is sized for the transaction: EIP-7825 caps one at MaxTxnGasLimit, which is
// 44k entries or 2MB of log data, and a real one emits a hundred at most.
const (
	// Entries kept for reuse, ~304 bytes each.
	maxPooledLogEntries = 1024

	// Data kept with them.
	maxPooledLogBytes = 1024 * 1024

	// No entry may hold more than an eighth of that. A single large log would
	// otherwise take the whole budget and starve the small entries that real
	// traffic is made of, while a cap far below it would throw away the reuse
	// that makes a block of large logs cheap.
	maxPooledLogDataCap = maxPooledLogBytes / 8

	// Slots one transaction may leave behind: the pointers alone are retention.
	maxLogSlotsPerTx = 4096
)

// logArena owns a block's log entries and recycles them through a pool, so what
// it holds follows the largest transaction rather than the widest block.
type logArena struct {
	byTx               []types.Logs // by txIndex+1; index 0 is the pre-transaction system calls
	filledLo, filledHi int          // transactions written since the last reset, as [lo, hi); hi == 0 means none
	pool               []*types.Log // entries taken back at reset, for any transaction to reuse
	poolBytes          int          // Data the pool holds
	indexInBlock       uint
	gen                uint32   // reset counter
	writtenGen         []uint32 // gen each transaction last wrote in; kept only under dbg.AssertEnabled
}

// alloc journals the allocation for revertLast, then returns txIndex's next
// entry sized for numTopics/dataSize. The caller must write every topic and data
// byte; what it leaves unwritten is the previous transaction's. The arena owns
// the entry, so it must never be handed out without copying.
func (a *logArena) alloc(j *journal, addr common.Address, txIndex, numTopics, dataSize int) *types.Log {
	j.addLogChange(txIndex)
	ti := txIndex + 1
	byTx := a.byTx
	if len(byTx) <= ti {
		byTx = slices.Grow(byTx, ti+1-len(byTx))[:ti+1]
		a.byTx = byTx
	}
	entries := byTx[ti]
	logIdx := len(entries)
	entries = slices.Grow(entries, 1)[:logIdx+1]
	byTx[ti] = entries
	a.markFilled(ti)
	if dbg.AssertEnabled {
		a.stampWrite(ti)
	}

	lp := entries[logIdx]
	if lp == nil {
		lp = a.take()
		entries[logIdx] = lp
	}
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

// take returns a pooled entry, keeping its Topics and Data for alloc to grow
// into, or a fresh one when the pool is empty.
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

func (a *logArena) markFilled(ti int) {
	if a.filledHi == 0 {
		a.filledLo, a.filledHi = ti, ti+1
		return
	}
	a.filledLo = min(a.filledLo, ti)
	a.filledHi = max(a.filledHi, ti+1)
}

// reset takes back what the transactions wrote: the entries return to the pool
// and their slots go empty. It walks only what was written, so it costs the
// caller's own work and not the block's.
func (a *logArena) reset() {
	a.indexInBlock = 0
	a.gen++
	all := a.byTx[:cap(a.byTx)]
	for i := a.filledLo; i < a.filledHi; i++ {
		entries := all[i]
		if cap(entries) > maxLogSlotsPerTx {
			all[i] = nil // one outlier transaction must not leave its slots behind
			continue
		}
		for j, lp := range entries {
			if lp == nil {
				continue
			}
			a.put(lp)
			entries[j] = nil
		}
		all[i] = entries[:0]
	}
	a.filledLo, a.filledHi = 0, 0
}

// revertLast drops the entry txIndex allocated last, returning it to the pool:
// behind the length, only a transaction re-emitting that many logs could reach
// it again.
func (a *logArena) revertLast(txIndex int) {
	if txIndex+1 >= len(a.byTx) {
		panic(fmt.Sprintf("can't revert log index %v, max: %v", txIndex, len(a.byTx)-1))
	}
	txnLogs := a.byTx[txIndex+1]
	last := len(txnLogs) - 1
	if lp := txnLogs[last]; lp != nil {
		a.put(lp)
		txnLogs[last] = nil
	}
	a.byTx[txIndex+1] = txnLogs[:last] // revert 1 log
	if last == 0 {
		a.byTx = a.byTx[:len(a.byTx)-1] // revert txn
	}
	a.indexInBlock--
}

// forTx returns the entries of txIndex, owned by the arena.
func (a *logArena) forTx(txIndex int) types.Logs {
	ti := txIndex + 1
	if ti >= len(a.byTx) {
		return nil
	}
	if dbg.AssertEnabled {
		a.assertLive(ti)
	}
	return a.byTx[ti]
}

// stampWrite records the reset cycle a transaction wrote in. The offset keeps
// zero meaning never written.
func (a *logArena) stampWrite(ti int) {
	if len(a.writtenGen) <= ti {
		a.writtenGen = slices.Grow(a.writtenGen, ti+1-len(a.writtenGen))[:ti+1]
	}
	a.writtenGen[ti] = a.gen + 1
}

// assertLive catches a read of logs the arena already took back. The answer
// would be an empty set, which a receipt records as "emitted no logs".
func (a *logArena) assertLive(ti int) {
	if ti < len(a.writtenGen) && a.writtenGen[ti] != 0 && a.writtenGen[ti] != a.gen+1 {
		panic(fmt.Sprintf("logs of tx %d were reclaimed by Reset: written in cycle %d, now %d",
			ti-1, a.writtenGen[ti]-1, a.gen))
	}
}

// release drops the entries.
func (a *logArena) release() { *a = logArena{} }
