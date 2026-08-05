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
	"github.com/erigontech/erigon/execution/types"
)

// Entries outlive the block that emitted them, so what the arena keeps must be
// capped: a burst of logs at a fresh tx index every block would otherwise add a
// high-water mark per block, forever. Every IntraBlockState carries an arena,
// and the ones that reset across blocks — exec workers, and the trace workers
// an RPC request spawns — sit at that mark, so the budget multiplies by them.
//
// A block cannot exceed gasLimit/8 bytes of log data (LogDataGas) or
// gasLimit/375 entries (LogGas) — 5.6MB and 120k at a 45M limit. Both budgets
// sit below that, so an outlier block is trimmed rather than held.
const (
	maxReusableLogEntries = 4096
	maxReusableLogBytes   = 4 * 1024 * 1024
	maxReusableLogDataCap = 64 * 1024
)

// logArena owns a block's log entries and keeps them for the next block to
// reuse. The executor resets once per transaction, so the budget is tracked as
// entries are allocated, not rescanned.
type logArena struct {
	byTx               []types.Logs // by txIndex+1; index 0 is the pre-transaction system calls
	filledLo, filledHi int          // transactions written since the last reset, as [lo, hi); hi == 0 means none
	// Against the maxReusableLog budget. Overcounting is safe: it only pulls in
	// a trim, which recounts.
	reusableEntries, reusableBytes int
	oversized                      []logPos // buffers past maxReusableLogDataCap, newest last
	indexInBlock                   uint
}

// logPos addresses one entry. Entries never move, so it stays valid.
type logPos struct{ ti, idx int }

// alloc journals the allocation for revertLast, then returns txIndex's next
// entry sized for numTopics/dataSize. The caller must write every topic and data
// byte; what it leaves unwritten is the previous block's. The arena owns the
// entry, so it must never be handed out without copying.
func (a *logArena) alloc(j *journal, addr common.Address, txIndex, numTopics, dataSize int) *types.Log {
	j.addLogChange(txIndex)
	ti := txIndex + 1
	byTx := a.byTx
	if len(byTx) <= ti {
		byTx = slices.Grow(byTx, ti+1-len(byTx))[:ti+1]
		a.byTx = byTx
	}
	entries := byTx[ti]
	logIdx, entriesCap := len(entries), cap(entries)
	entries = slices.Grow(entries, 1)[:logIdx+1]
	byTx[ti] = entries
	a.reusableEntries += cap(entries) - entriesCap
	a.markFilled(ti)

	lp := entries[logIdx] // a prior block's entry to reuse, or nil for a fresh slot
	if lp == nil {
		lp = &types.Log{}
		entries[logIdx] = lp
	}
	lp.Address = addr
	lp.Topics = slices.Grow(lp.Topics[:0], numTopics)[:numTopics]
	dataCap := cap(lp.Data)
	lp.Data = slices.Grow(lp.Data[:0], dataSize)[:dataSize]
	a.reusableBytes += cap(lp.Data) - dataCap
	if cap(lp.Data) > maxReusableLogDataCap && dataCap <= maxReusableLogDataCap {
		a.oversized = append(a.oversized, logPos{ti, logIdx})
	}
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

func (a *logArena) markFilled(ti int) {
	if a.filledHi == 0 {
		a.filledLo, a.filledHi = ti, ti+1
		return
	}
	a.filledLo = min(a.filledLo, ti)
	a.filledHi = max(a.filledHi, ti+1)
}

// reset truncates what the transactions wrote, keeping the entries and their
// Topics/Data for alloc to reuse, up to the budget.
func (a *logArena) reset() {
	a.indexInBlock = 0
	all := a.byTx[:cap(a.byTx)]
	if a.reusableBytes > maxReusableLogBytes {
		a.drainOversized(all)
	}
	if a.reusableEntries > maxReusableLogEntries || a.reusableBytes > maxReusableLogBytes {
		a.trim()
		return
	}
	filled := all[a.filledLo:a.filledHi]
	for i := range filled {
		filled[i] = filled[i][:0]
	}
	a.filledLo, a.filledHi = 0, 0
}

// drainOversized frees Data past the per-entry cap until the arena is back
// inside its budget. It goes first because one such buffer holds as much as
// hundreds of ordinary entries. Newest first: transactions take their positions
// in order, so freeing the oldest frees what the next block asks for first.
func (a *logArena) drainOversized(all []types.Logs) {
	i := len(a.oversized) - 1
	for ; i >= 0 && a.reusableBytes > maxReusableLogBytes; i-- {
		p := a.oversized[i]
		entries := all[p.ti]
		lp := entries[:cap(entries)][p.idx]
		if lp == nil {
			continue
		}
		a.reusableBytes -= cap(lp.Data)
		lp.Data = nil
	}
	a.oversized = a.oversized[:i+1]
}

// forget removes a position from the eviction queue.
func (a *logArena) forget(p logPos) {
	for i := len(a.oversized) - 1; i >= 0; i-- {
		if a.oversized[i] == p {
			a.oversized = append(a.oversized[:i], a.oversized[i+1:]...)
			return
		}
	}
}

// trim truncates every transaction and drops what does not fit the budget,
// recounting what is kept.
func (a *logArena) trim() {
	entryCount, dataBytes := 0, 0
	all := a.byTx[:cap(a.byTx)]
	for i, txLogs := range all {
		entries := txLogs[:cap(txLogs)]
		if entryCount+cap(entries) > maxReusableLogEntries {
			all[i] = nil // the slots are retention even when every entry is nil
			continue
		}
		entryCount += cap(entries)
		for j, lp := range entries {
			if lp == nil {
				continue
			}
			data := cap(lp.Data)
			if data > maxReusableLogDataCap || dataBytes+data > maxReusableLogBytes {
				entries[j] = nil
				continue
			}
			dataBytes += data
		}
		all[i] = entries[:0]
	}
	a.reusableEntries, a.reusableBytes = entryCount, dataBytes
	a.filledLo, a.filledHi = 0, 0
	a.oversized = a.oversized[:0]
}

// revertLast drops the entry txIndex allocated last. Oversized Data goes with
// it: behind the length, only a transaction re-emitting that many logs could
// reach it again.
func (a *logArena) revertLast(txIndex int) {
	if txIndex+1 >= len(a.byTx) {
		panic(fmt.Sprintf("can't revert log index %v, max: %v", txIndex, len(a.byTx)-1))
	}
	txnLogs := a.byTx[txIndex+1]
	last := len(txnLogs) - 1
	if lp := txnLogs[last]; lp != nil && cap(lp.Data) > maxReusableLogDataCap {
		a.reusableBytes -= cap(lp.Data)
		lp.Data = nil
		a.forget(logPos{txIndex + 1, last})
	}
	a.byTx[txIndex+1] = txnLogs[:last] // revert 1 log
	if last == 0 {
		a.byTx = a.byTx[:len(a.byTx)-1] // revert txn
	}
	a.indexInBlock--
}

// forTx returns the entries of txIndex, owned by the arena.
func (a *logArena) forTx(txIndex int) types.Logs {
	if txIndex+1 >= len(a.byTx) {
		return nil
	}
	return a.byTx[txIndex+1]
}

// release drops the entries.
func (a *logArena) release() { *a = logArena{} }
