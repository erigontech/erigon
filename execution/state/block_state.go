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
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/types"
)

// BlockState is the block-level shared store that a block's transactions read and
// write through. It is thread-safe, holds no reader, and is created once per block by
// the executor; each transaction's state reads committed base through its own reader
// and the staged writes of lower transactions through this store. It is backed by the
// version map; the embedded field is the store's internals, used where the raw map
// still crosses a helper boundary.
//
// It also carries the ReceiptState: the block-wide accumulation (running log index,
// cumulative gas, log aggregate) that each per-transaction receipt is stamped against.
// A transaction emits its own logs into its per-transaction state, which the finalize
// path folds into the aggregate here in commit order. The parallel executor advances
// this only on the single-threaded apply/commit path, so it is never written by more
// than one goroutine and needs no lock.
type BlockState struct {
	*VersionMap
	receipts ReceiptState
}

// NewBlockState creates the per-block shared store, pre-populated from the block access
// list when one is present.
func NewBlockState(changes types.BlockAccessList) *BlockState {
	return &BlockState{VersionMap: NewVersionMap(changes)}
}

// Receipts returns the block-wide receipt/log accumulator.
func (bs *BlockState) Receipts() *ReceiptState { return &bs.receipts }

// ReceiptState accumulates the block-wide context each per-transaction receipt is
// stamped against — the running log index and cumulative gas — and owns the block-wide
// log aggregate that logs fold into. The per-transaction receipt builder produces
// tx-local values; ReceiptState contextualizes them into the block in commit order.
// Advanced on a single goroutine (the finalize/commit path), so it holds no lock.
type ReceiptState struct {
	logs                  logArena // block-wide ordered logs, indexed as transactions commit
	cumulativeGasUsed     uint64
	cumulativeBlobGasUsed uint64
}

// CommitLogs folds a transaction's logs into the block aggregate in commit order,
// assigning each its block-wide index, and returns the index the first log takes (the
// receipt's FirstLogIndexWithinBlock). txLogs carry transaction-local indices and are
// owned by the transaction's own arena, so the aggregate copies them; the caller may
// then reset that arena for the next transaction.
func (r *ReceiptState) CommitLogs(txLogs types.Logs) uint32 {
	first := uint32(r.logs.indexInBlock)
	for _, src := range txLogs {
		dst := r.logs.alloc(src.Address, int(src.TxIndex), len(src.Topics), len(src.Data))
		copy(dst.Topics, src.Topics)
		copy(dst.Data, src.Data)
		dst.Removed = src.Removed
		dst.TxHash, dst.BlockHash, dst.BlockNumber = src.TxHash, src.BlockHash, src.BlockNumber
	}
	return first
}

// LogIndex is the block-wide count of logs committed so far — the index the next
// transaction's first log will take.
func (r *ReceiptState) LogIndex() uint32 { return uint32(r.logs.indexInBlock) }

// StampReceipt gives a per-transaction receipt its place in the block: it numbers the
// receipt's logs from the block-wide index reached so far and records that as the
// receipt's FirstLogIndexWithinBlock. Call before the transaction's logs are folded in
// (via CommitLogs), so the index is the count of logs earlier transactions committed.
func (r *ReceiptState) StampReceipt(receipt *types.Receipt) {
	first := r.LogIndex()
	types.Logs(receipt.Logs).Reindex(first)
	receipt.FirstLogIndexWithinBlock = first
}

// Logs returns the block's aggregated logs in order. The entries are owned by the
// aggregate; a caller that holds them past the next reset must copy.
func (r *ReceiptState) Logs() types.Logs { return r.logs.entries }

// LogsRlpHash is the RLP hash of the block's aggregated logs.
func (r *ReceiptState) LogsRlpHash() common.Hash { return types.RlpHashLogs(r.logs.entries) }

// Reset empties the aggregate and the accumulators, taking numbering back to zero.
func (r *ReceiptState) Reset() {
	r.logs.reset()
	r.cumulativeGasUsed = 0
	r.cumulativeBlobGasUsed = 0
}

// ResumeLogIndexAt continues the block-wide log numbering at idx, for a caller that
// starts mid-block and hands in the count the skipped transactions left.
func (r *ReceiptState) ResumeLogIndexAt(idx uint32) { r.logs.indexInBlock = uint(idx) }

// Validate reports whether the transaction at txIdx is still valid against the current
// store: every value it read matches the version that now holds that cell. It is a pure
// predicate — it stamps nothing — so a caller may use it both to gate a mark and to
// re-check an already-committed tx without side effects.
func (bs *BlockState) Validate(txIdx int, io *VersionedIO) bool {
	return bs.ValidateVersion(txIdx, io,
		func(rv, wv Version) VersionValidity {
			if rv != wv {
				return VersionInvalid
			}
			return VersionValid
		}, false, "") == VersionValid
}

// Seal promotes the transaction's writes to Done and advances the seal frontier to
// txIdx in one in-order step. The caller guarantees txIdx is a regular (non
// system/block-end) transaction at the seal frontier.
func (bs *BlockState) Seal(txIdx int, writes *WriteSet) {
	bs.MarkWritesComplete(writes)
	bs.SealUpTo(txIdx)
}

// AppendTxLogs folds a transaction's own logs into the block aggregate in commit order.
// It is the BlockState-level shorthand for Receipts().CommitLogs when the caller does
// not need the returned first-log index.
func (bs *BlockState) AppendTxLogs(txLogs types.Logs) { bs.receipts.CommitLogs(txLogs) }

// BlockLogs returns the block's aggregated logs in order.
func (bs *BlockState) BlockLogs() types.Logs { return bs.receipts.Logs() }

// BlockLogsRlpHash is the RLP hash of the block's aggregated logs.
func (bs *BlockState) BlockLogsRlpHash() common.Hash { return bs.receipts.LogsRlpHash() }

// ResetBlockLogs empties the aggregate and takes the numbering back to zero.
func (bs *BlockState) ResetBlockLogs() { bs.receipts.Reset() }

// ResumeBlockLogIndexAt continues the aggregate's block-wide numbering at idx.
func (bs *BlockState) ResumeBlockLogIndexAt(idx uint32) { bs.receipts.ResumeLogIndexAt(idx) }
