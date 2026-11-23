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
// It also carries the block-wide log aggregate: a transaction emits its own logs into
// its per-transaction state, which the single-threaded finalize path folds into the
// aggregate here in commit order (the parallel executor aggregates through receipts and
// does not touch this, so the aggregate is only ever written on one goroutine).
type BlockState struct {
	*VersionMap
	logs logArena
}

// NewBlockState creates the per-block shared store, pre-populated from the block access
// list when one is present.
func NewBlockState(changes types.BlockAccessList) *BlockState {
	return &BlockState{VersionMap: NewVersionMap(changes)}
}

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

// AppendTxLogs folds a transaction's own logs into the block aggregate, assigning each
// its block-wide index in the order transactions commit. txLogs are owned by the
// transaction's per-tx arena, so the aggregate copies them and the caller may then
// reset that arena for the next transaction. Call only on the single-threaded finalize
// path (see the type doc): the aggregate is not safe for concurrent writers.
func (bs *BlockState) AppendTxLogs(txLogs types.Logs) {
	for _, src := range txLogs {
		dst := bs.logs.alloc(src.Address, int(src.TxIndex), len(src.Topics), len(src.Data))
		copy(dst.Topics, src.Topics)
		copy(dst.Data, src.Data)
		dst.Removed = src.Removed
		dst.TxHash, dst.BlockHash, dst.BlockNumber = src.TxHash, src.BlockHash, src.BlockNumber
	}
}

// BlockLogs returns the block's aggregated logs in order. The entries are owned by the
// aggregate; a caller that holds them past the next reset must copy.
func (bs *BlockState) BlockLogs() types.Logs { return bs.logs.entries }

// BlockLogsRlpHash is the RLP hash of the block's aggregated logs.
func (bs *BlockState) BlockLogsRlpHash() common.Hash { return types.RlpHashLogs(bs.logs.entries) }

// ResetBlockLogs empties the aggregate and takes the numbering back to zero.
func (bs *BlockState) ResetBlockLogs() { bs.logs.reset() }

// ResumeBlockLogIndexAt continues the aggregate's block-wide numbering at idx, for a
// caller that starts mid-block and hands in the count the skipped transactions left.
func (bs *BlockState) ResumeBlockLogIndexAt(idx uint32) { bs.logs.indexInBlock = uint(idx) }
