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

// reset is the ownership boundary: everything handed out since the last one is
// live, and reset takes it back. What the arena holds is therefore one caller's
// unit of work — a transaction for the executor and the trace workers, a block
// for the assembler and the tooling, which reset once it is built.
const (
	// What one transaction's gas can buy, under the EIP-7825 cap.
	maxLogsPerTxn     = int(params.MaxTxnGasLimit / params.LogGas)     // 44739
	maxLogBytesPerTxn = int(params.MaxTxnGasLimit / params.LogDataGas) // 2MB

	// Fractions of those, because the run's array never shrinks below the budget:
	// the whole ceiling would park 7MB of entries per arena, while a tenth still
	// holds the p99 block of 1706 logs that a caller resetting per block keeps.
	maxRetainedLogSlots = maxLogsPerTxn / 10
	maxRetainedLogBytes = maxLogBytesPerTxn / 2

	// One large log must not take the whole budget and starve the small entries
	// real traffic is made of, and a cap much lower would throw away the reuse
	// that makes a block of large logs cheap.
	maxRetainedLogDataCap = maxRetainedLogBytes / 8
)

// logArena owns a block's log entries and recycles them in place, so what it
// holds follows the largest transaction rather than the widest block.
type logArena struct {
	// The block so far, in order — one transaction, for a caller that resets per
	// transaction. Past the run, the array keeps the slots reset took back, Topics
	// and Data included, for any later transaction to grow into.
	entries      types.Logs
	retainedData int // Data those slots hold, admitted against maxRetainedLogBytes
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

	// Points into the run's array, so it is only valid until the next alloc may
	// regrow it — the caller fills and notifies before allocating again.
	lp := &entries[logIdx]
	a.retainedData -= cap(lp.Data)
	lp.Address = addr
	lp.Topics = slices.Grow(lp.Topics[:0], numTopics)[:numTopics]
	lp.Data = slices.Grow(lp.Data[:0], dataSize)[:dataSize]
	a.retainedData += cap(lp.Data)
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

// keepData admits a slot's Data for reuse, dropping what the arena may not hold.
// Every slot passes through here on its way out of the run, reset and revert
// alike, so what lies past the run is always within budget.
func (a *logArena) keepData(lp *types.Log) {
	if d := cap(lp.Data); d > maxRetainedLogDataCap || a.retainedData > maxRetainedLogBytes {
		a.retainedData -= d
		lp.Data = nil
	}
}

// reset takes back what was written: the run goes empty and its slots stay for
// the next transaction. It walks only what the caller wrote, not the block's
// width.
func (a *logArena) reset() {
	a.indexInBlock = 0
	entries := a.entries
	for i := range entries {
		a.keepData(&entries[i])
	}
	if cap(entries) > maxRetainedLogSlots {
		a.entries, a.retainedData = nil, 0 // an outlier keeps its entries, not the array that held them
	} else {
		a.entries = entries[:0]
	}
}

// revertLast drops the entry allocated last. Its slot stays past the run, so the
// next alloc grows back into the same Topics and Data.
func (a *logArena) revertLast(txIndex int) {
	last := len(a.entries) - 1
	if last < 0 {
		panic(fmt.Sprintf("can't revert log of tx %d: none were emitted", txIndex))
	}
	a.keepData(&a.entries[last])
	a.entries = a.entries[:last]
	a.indexInBlock--
}

// forTx returns the entries txIndex emitted, owned by the arena. They are held
// in one run rather than grouped, and a transaction's own are the tail of it, so
// a transaction newer than the tail reads as empty and an older one panics.
func (a *logArena) forTx(txIndex int) types.Logs {
	entries := a.entries
	i := len(entries)
	if i > 0 && txIndex < int(entries[i-1].TxIndex) {
		// Newer than the tail is a transaction that emitted nothing, which is
		// how most of them end. Older is a caller reading what it has left:
		// answering empty would read as "emitted no logs", so say so instead.
		panic(fmt.Sprintf("logs of tx %d asked for, the run has reached tx %d",
			txIndex, int(entries[i-1].TxIndex)))
	}
	for i > 0 && int(entries[i-1].TxIndex) == txIndex {
		i--
	}
	return entries[i:]
}

func (a *logArena) release() { *a = logArena{} }
