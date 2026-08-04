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

// Log entries outlive the block that emitted them, so what one buffer keeps for
// reuse must be capped in total: a burst of logs at a fresh tx index every block
// would otherwise add a high-water-mark buffer per block, forever. The budget
// holds any realistic block in full.
const (
	maxReusableLogEntries = 4096
	maxReusableLogBytes   = 4 * 1024 * 1024
	maxReusableLogDataCap = 64 * 1024
)

// logBuffer holds a block's logs grouped by transaction and keeps the entries
// for the next block to reuse. The executor resets once per transaction, so the
// budget is tracked as entries are allocated rather than recomputed by a full
// scan, and reset walks only what the transaction wrote.
type logBuffer struct {
	groups             []types.Logs // by txIndex+1
	filledLo, filledHi int          // groups written since the last reset, as [lo, hi); hi == 0 means none
	// What the groups retain for reuse, against the maxReusableLog budget.
	// Overcounting is safe: it only pulls in a trim, which recounts.
	reusableEntries, reusableBytes int
	indexInBlock                   uint
}

// alloc journals the allocation for revertLast, then reserves the next entry of
// txIndex and returns it sized for numTopics/dataSize. The caller must write
// every topic and every data byte; whatever it leaves unwritten is the previous
// block's. The entry is owned by the buffer and reused by later blocks, so it
// must never be handed out without copying.
func (b *logBuffer) alloc(j *journal, addr common.Address, txIndex, numTopics, dataSize int) *types.Log {
	j.addLogChange(txIndex)
	ti := txIndex + 1
	if len(b.groups) <= ti {
		b.groups = slices.Grow(b.groups, ti+1-len(b.groups))[:ti+1]
	}
	logIdx := len(b.groups[ti])
	groupCap := cap(b.groups[ti])
	b.groups[ti] = slices.Grow(b.groups[ti], 1)[:logIdx+1]
	b.reusableEntries += cap(b.groups[ti]) - groupCap
	b.markFilled(ti)
	group := b.groups[ti]

	lp := group[logIdx] // a prior block's entry to reuse, or nil for a fresh slot
	if lp == nil {
		lp = &types.Log{}
		group[logIdx] = lp
	}
	lp.Address = addr
	lp.Topics = slices.Grow(lp.Topics[:0], numTopics)[:numTopics]
	dataCap := cap(lp.Data)
	lp.Data = slices.Grow(lp.Data[:0], dataSize)[:dataSize]
	b.reusableBytes += cap(lp.Data) - dataCap
	lp.Removed = false
	lp.TxHash, lp.BlockHash = common.Hash{}, common.Hash{}
	lp.TxIndex = hexutil.Uint(txIndex)
	lp.BlockNumber = 0 // non-consensus field, assigned by the caller
	// Block-wide, not per-tx: receipts.DeriveFields reads Logs[0].Index to
	// recover FirstLogIndexWithinBlock.
	lp.Index = hexutil.Uint(b.indexInBlock)
	b.indexInBlock++
	return lp
}

func (b *logBuffer) markFilled(ti int) {
	if b.filledHi == 0 {
		b.filledLo, b.filledHi = ti, ti+1
		return
	}
	b.filledLo = min(b.filledLo, ti)
	b.filledHi = max(b.filledHi, ti+1)
}

// reset truncates the groups, keeping entries and their Topics/Data for alloc to
// reuse, up to the budget. It walks only the groups written since the last
// reset, and those to cap: a reverted entry hides behind the length and is
// retained memory all the same.
func (b *logBuffer) reset() {
	b.indexInBlock = 0
	if b.reusableEntries > maxReusableLogEntries || b.reusableBytes > maxReusableLogBytes {
		b.trim()
		return
	}
	groups := b.groups[:cap(b.groups)]
	for i := b.filledLo; i < b.filledHi; i++ {
		entries := groups[i][:cap(groups[i])]
		for j, lp := range entries {
			if lp == nil || cap(lp.Data) <= maxReusableLogDataCap {
				continue
			}
			b.reusableBytes -= cap(lp.Data)
			entries[j] = nil
		}
		groups[i] = entries[:0]
	}
	b.filledLo, b.filledHi = 0, 0
}

// trim truncates every group and drops what does not fit the budget, recounting
// what is kept.
func (b *logBuffer) trim() {
	entryCount, dataBytes := 0, 0
	groups := b.groups[:cap(b.groups)]
	for i, group := range groups {
		entries := group[:cap(group)]
		if entryCount+cap(entries) > maxReusableLogEntries {
			groups[i] = nil // an all-nil group is retention too
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
		groups[i] = entries[:0]
	}
	b.reusableEntries, b.reusableBytes = entryCount, dataBytes
	b.filledLo, b.filledHi = 0, 0
}

// revertLast drops the entry txIndex allocated last.
func (b *logBuffer) revertLast(txIndex int) {
	if txIndex+1 >= len(b.groups) {
		panic(fmt.Sprintf("can't revert log index %v, max: %v", txIndex, len(b.groups)-1))
	}
	txnLogs := b.groups[txIndex+1]
	b.groups[txIndex+1] = txnLogs[:len(txnLogs)-1] // revert 1 log
	if len(b.groups[txIndex+1]) == 0 {
		b.groups = b.groups[:len(b.groups)-1] // revert txn
	}
	b.indexInBlock--
}

// forTx returns the entries of txIndex, owned by the buffer.
func (b *logBuffer) forTx(txIndex int) types.Logs {
	if txIndex+1 >= len(b.groups) {
		return nil
	}
	return b.groups[txIndex+1]
}

// release drops the buffers, so the entries are no longer reused.
func (b *logBuffer) release() { *b = logBuffer{} }
