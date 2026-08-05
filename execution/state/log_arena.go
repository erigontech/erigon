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

// Log entries outlive the block that emitted them, so what one arena keeps for
// reuse must be capped in total: a burst of logs at a fresh tx index every block
// would otherwise add a high-water-mark buffer per block, forever. The budget
// holds any realistic block in full.
const (
	maxReusableLogEntries = 4096
	maxReusableLogBytes   = 4 * 1024 * 1024
	maxReusableLogDataCap = 64 * 1024
)

// logArena owns a block's log entries and keeps them for the next block to
// reuse. The executor resets once per transaction, so the budget is tracked as
// entries are allocated rather than recomputed by a full scan, and reset walks
// only the transactions that wrote.
type logArena struct {
	byTx               []types.Logs // by txIndex+1; index 0 is the pre-transaction system calls
	filledLo, filledHi int          // transactions written since the last reset, as [lo, hi); hi == 0 means none
	// What the entries retain for reuse, against the maxReusableLog budget.
	// Overcounting is safe: it only pulls in a trim, which recounts.
	reusableEntries, reusableBytes int
	oversized                      []logPos // entries that outgrew maxReusableLogDataCap, for reset to drop
	indexInBlock                   uint
}

// logPos addresses one entry: its transaction, then its place in that
// transaction. Entries never move, so a position stays valid until reset.
type logPos struct{ ti, idx int }

// alloc journals the allocation for revertLast, then reserves the next entry of
// txIndex and returns it sized for numTopics/dataSize. The caller must write
// every topic and every data byte; whatever it leaves unwritten is the previous
// block's. The entry is owned by the arena and reused by later blocks, so it
// must never be handed out without copying.
func (a *logArena) alloc(j *journal, addr common.Address, txIndex, numTopics, dataSize int) *types.Log {
	j.addLogChange(txIndex)
	ti := txIndex + 1
	if len(a.byTx) <= ti {
		a.byTx = slices.Grow(a.byTx, ti+1-len(a.byTx))[:ti+1]
	}
	byTx := a.byTx
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
	if cap(lp.Data) > maxReusableLogDataCap {
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
	if a.reusableEntries > maxReusableLogEntries || a.reusableBytes > maxReusableLogBytes {
		a.trim()
		return
	}
	all := a.byTx[:cap(a.byTx)]
	a.dropOversized(all)
	filled := all[a.filledLo:a.filledHi]
	for i := range filled {
		filled[i] = filled[i][:0]
	}
	a.filledLo, a.filledHi = 0, 0
}

// dropOversized frees the Data that outgrew the per-entry cap, keeping the entry
// and its Topics for reuse. A revert can put a different entry at a recorded
// position; freeing that one's Data instead is a lost buffer, not a leak.
func (a *logArena) dropOversized(all []types.Logs) {
	for _, p := range a.oversized {
		entries := all[p.ti]
		lp := entries[:cap(entries)][p.idx] // a revert can hide the entry behind the length
		if lp == nil {
			continue
		}
		a.reusableBytes -= cap(lp.Data)
		lp.Data = nil
	}
	a.oversized = a.oversized[:0]
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

// revertLast drops the entry txIndex allocated last.
func (a *logArena) revertLast(txIndex int) {
	if txIndex+1 >= len(a.byTx) {
		panic(fmt.Sprintf("can't revert log index %v, max: %v", txIndex, len(a.byTx)-1))
	}
	txnLogs := a.byTx[txIndex+1]
	a.byTx[txIndex+1] = txnLogs[:len(txnLogs)-1] // revert 1 log
	if len(a.byTx[txIndex+1]) == 0 {
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

// release drops the entries, so they are no longer reused.
func (a *logArena) release() { *a = logArena{} }
