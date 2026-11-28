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
	"sync"
	"unsafe"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/common/length"
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

	maxPooledLogBytes = maxLogBytesPerTxn

	// One large log must not take the whole budget and starve the small entries
	// real traffic is made of.
	maxPooledLogDataCap = maxPooledLogBytes / 2

	// Entries an arena keeps between resets: all a transaction can emit, so one
	// that fills its budget and reverts allocates nothing the next time.
	maxRetainedLogSlots = maxLogsPerTxn

	// Closed arenas kept for the next IntraBlockState, which executors build per
	// batch: one per worker of a 16-thread box. One smaller than a chunk is cheap
	// to rebuild and not worth a place.
	maxParkedLogArenas = 16
	maxParkedLogBytes  = 64 << 20

	maxLogChunkLen = 1024

	logSize    = int(unsafe.Sizeof(types.Log{}))
	logPtrSize = int(unsafe.Sizeof(&types.Log{}))
)

// logArena owns a block's log entries and recycles them in place, so what it
// holds follows the largest transaction rather than the widest block.
type logArena struct {
	entries      types.Logs  // the block so far, in order; past len, the released entries alloc reuses
	free         []types.Log // entries not handed out yet
	poolBytes    int         // Data the released entries hold
	chunkBytes   int         // chunks taken so far, unused tails included
	topicBytes   int         // Topics the entries hold
	indexInBlock uint
}

// alloc returns txIndex's next entry sized for numTopics/dataSize. The caller must write every topic and data
// byte; what it leaves unwritten is the previous transaction's. The arena owns
// the entry; callers must copy it before retaining it across a reset or truncate.
func (a *logArena) alloc(addr common.Address, txIndex, numTopics, dataSize int) *types.Log {
	if cap(a.entries) == 0 {
		idx := a.indexInBlock // set by ResumeLogIndexAt before the first log
		*a = logPark.take()
		a.indexInBlock = idx
	}
	logIdx := len(a.entries)
	if logIdx == cap(a.entries) {
		a.grow()
	}
	entries := a.entries[:logIdx+1]
	a.entries = entries

	lp := entries[logIdx]
	if lp == nil {
		lp = a.fresh(logIdx)
		entries[logIdx] = lp
	} else {
		a.poolBytes -= cap(lp.Data)
	}
	topics := slices.Grow(lp.Topics[:0], numTopics)
	a.topicBytes += (cap(topics) - cap(lp.Topics)) * length.Hash
	*lp = types.Log{
		Address: addr,
		Topics:  topics[:numTopics],
		Data:    slices.Grow(lp.Data[:0], dataSize)[:dataSize],
		TxIndex: hexutil.Uint(txIndex),
		// Block-wide, not per-tx: receipts.DeriveFields reads Logs[0].Index to
		// recover FirstLogIndexWithinBlock.
		Index: hexutil.Uint(a.indexInBlock),
	}
	a.indexInBlock++
	return lp
}

// grow doubles the run but stops once at maxRetainedLogSlots, so a transaction
// that fills its log budget leaves a run that reset keeps.
func (a *logArena) grow() {
	n := max(2*cap(a.entries), 1)
	if cap(a.entries) < maxRetainedLogSlots {
		n = min(n, maxRetainedLogSlots)
	}
	a.entries = append(make(types.Logs, 0, n), a.entries...)
}

// fresh takes a new entry from a chunk that doubles with the run, so an arena
// that logs once costs one entry and a large one costs a few chunks.
func (a *logArena) fresh(logIdx int) *types.Log {
	if len(a.free) == 0 {
		a.free = make([]types.Log, min(max(logIdx, 1), maxLogChunkLen))
		a.chunkBytes += len(a.free) * logSize
	}
	lp := &a.free[0]
	a.free = a.free[1:]
	return lp
}

// put releases an entry, dropping the Data the budget may not hold.
func (a *logArena) put(lp *types.Log) {
	if cap(lp.Data) > maxPooledLogDataCap || a.poolBytes+cap(lp.Data) > maxPooledLogBytes {
		lp.Data = nil
	}
	a.poolBytes += cap(lp.Data)
}

// reset takes back what was written: the entries stay in place for reuse and
// the block goes empty. It walks only what the caller wrote, not the block's width.
func (a *logArena) reset() {
	if cap(a.entries) > maxRetainedLogSlots {
		*a = logArena{} // an outlier keeps nothing
		return
	}
	a.indexInBlock = 0
	for _, lp := range a.entries {
		a.put(lp)
	}
	a.entries = a.entries[:0]
}

// truncate drops the entries past the first n, releasing them for reuse.
func (a *logArena) truncate(n int) {
	if n >= len(a.entries) {
		return
	}
	for _, lp := range a.entries[n:] {
		a.put(lp)
		a.indexInBlock--
	}
	a.entries = a.entries[:n]
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

// release parks the arena's entries for the next one and empties it.
func (a *logArena) release() {
	a.reset()
	logPark.put(a)
	*a = logArena{}
}

// heldBytes counts what a reset arena keeps alive: its run, chunks, Topics and Data.
func (a *logArena) heldBytes() int {
	return cap(a.entries)*logPtrSize + a.chunkBytes + a.topicBytes + a.poolBytes
}

type logParking struct {
	mu     sync.Mutex
	arenas []logArena
	bytes  int
}

var logPark logParking

func (p *logParking) put(a *logArena) {
	if cap(a.entries) < maxLogChunkLen {
		return
	}
	bytes := a.heldBytes()
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.arenas) < maxParkedLogArenas && p.bytes+bytes <= maxParkedLogBytes {
		p.arenas = append(p.arenas, *a)
		p.bytes += bytes
	}
}

func (p *logParking) take() logArena {
	p.mu.Lock()
	defer p.mu.Unlock()
	n := len(p.arenas) - 1
	if n < 0 {
		return logArena{}
	}
	arena := p.arenas[n]
	p.arenas[n] = logArena{}
	p.arenas = p.arenas[:n]
	p.bytes -= arena.heldBytes()
	return arena
}
