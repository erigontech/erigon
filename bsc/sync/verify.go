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

package bscsync

import (
	"fmt"

	"github.com/erigontech/erigon/execution/types"
)

// verifyChain does structural-only validation (no EVM, no Parlia seal): each
// block links to its parent and its body matches the header roots. parent is the
// header preceding blocks[0] (nil is allowed only when blocks[0] is genesis's child
// and the caller has separately confirmed the parent).
func verifyChain(parent *types.Header, blocks []*types.Block) error {
	prev := parent
	for _, b := range blocks {
		h := b.HeaderNoCopy()
		if prev != nil {
			if h.Number.Uint64() != prev.Number.Uint64()+1 {
				return fmt.Errorf("non-contiguous block %d after %d", h.Number.Uint64(), prev.Number.Uint64())
			}
			if h.ParentHash != prev.Hash() {
				return fmt.Errorf("parent hash mismatch at %d: header parent %x != %x", h.Number.Uint64(), h.ParentHash, prev.Hash())
			}
		}
		if err := b.SanityCheck(); err != nil {
			return fmt.Errorf("sanity check at %d: %w", h.Number.Uint64(), err)
		}
		if err := b.HashCheck(true); err != nil {
			return fmt.Errorf("hash check at %d: %w", h.Number.Uint64(), err)
		}
		prev = h
	}
	return nil
}
