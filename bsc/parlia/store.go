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

package parlia

import (
	"fmt"
	"slices"

	lru "github.com/hashicorp/golang-lru/v2"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/types"
)

// SnapshotStore keeps recent snapshots in memory. On a miss it walks back through the
// header reader to a cached snapshot or to a checkpoint at least one epoch back, so a
// rebuild reads at most about two epochs of headers.
type SnapshotStore struct {
	config *chain.Config
	reader HeaderReader
	cache  *lru.Cache[common.Hash, *Snapshot]
}

func NewSnapshotStore(config *chain.Config, reader HeaderReader, size int) (*SnapshotStore, error) {
	cache, err := lru.New[common.Hash, *Snapshot](size)
	if err != nil {
		return nil, err
	}
	return &SnapshotStore{config: config, reader: reader, cache: cache}, nil
}

func (s *SnapshotStore) GetHeader(hash common.Hash, number uint64) *types.Header {
	return s.reader.GetHeader(hash, number)
}

func (s *SnapshotStore) Snapshot(hash common.Hash, number uint64) (*Snapshot, error) {
	var (
		headers []*types.Header
		snap    *Snapshot
	)
	for snap == nil {
		if cached, ok := s.cache.Get(hash); ok && cached.Number == number {
			snap = cached
			break
		}
		h := s.reader.GetHeader(hash, number)
		if h == nil {
			return nil, fmt.Errorf("%w: %d %x", ErrUnknownAncestor, number, hash)
		}
		if number%epochLength == checkpointOffset && len(headers) >= epochLength {
			epochHeader, err := s.ancestor(h, checkpointOffset)
			if err != nil {
				return nil, err
			}
			if snap, err = NewCheckpointSnapshot(s.config, h, epochHeader); err != nil {
				return nil, err
			}
			s.cache.Add(snap.Hash, snap)
			break
		}
		headers = append(headers, h)
		hash, number = h.ParentHash, number-1
	}

	for _, h := range slices.Backward(headers) {
		next, err := snap.Apply(s.config, []*types.Header{h}, s.reader)
		if err != nil {
			return nil, err
		}
		s.cache.Add(next.Hash, next)
		snap = next
	}
	return snap, nil
}

func (s *SnapshotStore) ancestor(h *types.Header, depth uint64) (*types.Header, error) {
	for range depth {
		parent := s.reader.GetHeader(h.ParentHash, h.Number.Uint64()-1)
		if parent == nil {
			return nil, fmt.Errorf("%w: %d %x", ErrUnknownAncestor, h.Number.Uint64()-1, h.ParentHash)
		}
		h = parent
	}
	return h, nil
}

func (s *SnapshotStore) Justified(hash common.Hash, number uint64) (*types.Header, error) {
	snap, err := s.Snapshot(hash, number)
	if err != nil {
		return nil, err
	}
	return s.attestedHeader(snap.Justified())
}

func (s *SnapshotStore) Finalized(hash common.Hash, number uint64) (*types.Header, error) {
	snap, err := s.Snapshot(hash, number)
	if err != nil {
		return nil, err
	}
	return s.attestedHeader(snap.Finalized())
}

func (s *SnapshotStore) attestedHeader(number uint64, hash common.Hash) (*types.Header, error) {
	if hash == (common.Hash{}) {
		return nil, fmt.Errorf("%w: no attestation yet", ErrUnknownAncestor)
	}
	h := s.reader.GetHeader(hash, number)
	if h == nil {
		return nil, fmt.Errorf("%w: %d %x", ErrUnknownAncestor, number, hash)
	}
	return h, nil
}
