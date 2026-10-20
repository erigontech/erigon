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
package deriver

import (
	"context"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/eez/protocol"
)

// HashReader reads canonical hashes of the local chain.
type HashReader interface {
	// CanonicalHash returns the hash of the canonical block at number.
	CanonicalHash(ctx context.Context, number uint64) (common.Hash, bool, error)
	// CanonicalNumber returns the number of the canonical block with hash.
	CanonicalNumber(ctx context.Context, hash common.Hash) (uint64, bool, error)
}

// Range is the L2 blocks a settlement covers.
type Range struct {
	// Anchor is the block the applied run started from.
	Anchor uint64
	// From and To bound the blocks the settlement derives. A resumed batch
	// rewrites the Sync block at the anchor, so its range is that block.
	From, To uint64
	Resumed  bool
}

// SelectRange returns the blocks a settlement of blocks L2 blocks covers.
// Its anchor is the local block whose hash is the settlement's entry state,
// looked up within [cursor - blocks, cursor] since a competing batch can
// start below the cursor; it is the cursor when no such block exists.
func SelectRange(ctx context.Context, chain HashReader, cursor, blocks uint64, s *protocol.Settlement) (Range, error) {
	anchor := cursor
	n, ok, err := chain.CanonicalNumber(ctx, s.EntryState)
	if err != nil {
		return Range{}, err
	}
	if ok && n <= cursor && n >= cursor-min(cursor, blocks) {
		anchor = n
	}
	if s.Resumed() {
		return Range{Anchor: anchor, From: anchor, To: anchor, Resumed: true}, nil
	}
	return Range{Anchor: anchor, From: anchor + 1, To: anchor + blocks}, nil
}

// CursorAccepted reports whether the local block at the anchor can precede
// the settlement: it holds the root the applied run started from, or the
// settled root when the batch was derived already.
func CursorAccepted(anchorHash common.Hash, s *protocol.Settlement) bool {
	return anchorHash == s.EntryState || anchorHash == s.FinalState
}
