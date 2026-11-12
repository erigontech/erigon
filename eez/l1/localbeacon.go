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

package l1

import (
	"context"
	"errors"
	"fmt"

	"github.com/erigontech/erigon/common"
)

// BeaconStore holds the blobs of the beacon node running in this process.
type BeaconStore interface {
	// ExecutionBlobs returns the blobs of the canonical beacon block whose
	// execution payload is the block with hash and timestamp time; found is
	// false while the store holds no such block or not its blobs.
	ExecutionBlobs(ctx context.Context, hash common.Hash, time uint64) (blobs [][]byte, found bool, err error)
}

// LocalBeacon is the BlobSource of the beacon node running in this process.
// It reads from next, when set, the blobs it does not serve and those it
// fails to read.
type LocalBeacon struct {
	store BeaconStore
	next  BlobSource
}

var _ BlobSource = (*LocalBeacon)(nil)

// NewLocalBeacon returns the BlobSource of store, backed by next when not
// nil.
func NewLocalBeacon(store BeaconStore, next BlobSource) *LocalBeacon {
	return &LocalBeacon{store: store, next: next}
}

// Blobs returns the blobs of hashes that the L1 block h carries.
func (b *LocalBeacon) Blobs(ctx context.Context, h *Header, hashes []common.Hash) ([][]byte, error) {
	out, err := b.local(ctx, h, hashes)
	if err == nil || b.next == nil {
		return out, err
	}
	out, nextErr := b.next.Blobs(ctx, h, hashes)
	if nextErr != nil && !errors.Is(err, ErrBlobUnavailable) {
		return nil, fmt.Errorf("%w; local beacon node: %w", nextErr, err)
	}
	return out, nextErr
}

// local returns the blobs of hashes that the local beacon node stores for
// the L1 block h.
func (b *LocalBeacon) local(ctx context.Context, h *Header, hashes []common.Hash) ([][]byte, error) {
	stored, found, err := b.store.ExecutionBlobs(ctx, h.Hash, h.Time)
	switch {
	case err != nil:
		return nil, fmt.Errorf("blobs of L1 block %d from the local beacon node: %w", h.Number, err)
	case !found:
		return nil, fmt.Errorf("%w: L1 block %d by the local beacon node", ErrBlobUnavailable, h.Number)
	}
	out, err := MatchBlobs(stored, hashes)
	if err != nil {
		return nil, fmt.Errorf("blobs of L1 block %d from the local beacon node: %w", h.Number, err)
	}
	return out, nil
}
