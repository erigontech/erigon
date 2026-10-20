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
	"context"
	"fmt"
	"slices"

	"github.com/erigontech/erigon/bsc/parlia"
	"github.com/erigontech/erigon/execution/types"
)

const ccbInitDepth = 64

// InitialiseCCB builds a chain builder from the stored chain ending at head. The root is the
// finalized block of head, or the block ccbInitDepth below head if that is older.
func InitialiseCCB(ctx context.Context, db parlia.Chain, verifier HeaderVerifier, head *types.Header) (*CanonicalChainBuilder, error) {
	headNum := head.Number.Uint64()
	snap, err := db.Snapshot(head.Hash(), headNum)
	if err != nil {
		return nil, fmt.Errorf("InitialiseCCB: head snapshot: %w", err)
	}
	rootNum := headNum - min(headNum, ccbInitDepth)
	if finalizedNum, _ := snap.Finalized(); snap.Attestation != nil && finalizedNum < rootNum {
		rootNum = finalizedNum
	}

	headers := []*types.Header{head}
	for h := head; h.Number.Uint64() > rootNum; {
		parent := db.GetHeader(h.ParentHash, h.Number.Uint64()-1)
		if parent == nil {
			return nil, fmt.Errorf("InitialiseCCB: %w: %d %x", parlia.ErrUnknownAncestor, h.Number.Uint64()-1, h.ParentHash)
		}
		headers = append(headers, parent)
		h = parent
	}
	slices.Reverse(headers)

	ccb, err := NewCanonicalChainBuilder(db, verifier, headers[0])
	if err != nil {
		return nil, err
	}
	if _, err := ccb.Connect(ctx, headers[1:]); err != nil {
		return nil, fmt.Errorf("InitialiseCCB: %w", err)
	}
	return ccb, nil
}
