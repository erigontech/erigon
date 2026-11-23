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

import "github.com/erigontech/erigon/execution/types"

// BlockState is the block-level shared store that a block's transactions read and
// write through. It is thread-safe, holds no reader, and is created once per block by
// the executor; each transaction's state reads committed base through its own reader
// and the staged writes of lower transactions through this store. It is backed by the
// version map; the embedded field is the store's internals, used where the raw map
// still crosses a helper boundary.
type BlockState struct {
	*VersionMap
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
