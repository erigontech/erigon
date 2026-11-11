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

package l1_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/eez/internal/testfixtures"
	"github.com/erigontech/erigon/eez/l1"
)

// beaconStore holds blobs by execution block hash and records the
// timestamps it is asked with.
type beaconStore struct {
	blobs map[common.Hash][][]byte
	err   error
	times []uint64
}

func (s *beaconStore) ExecutionBlobs(_ context.Context, hash common.Hash, time uint64) ([][]byte, bool, error) {
	s.times = append(s.times, time)
	if s.err != nil {
		return nil, false, s.err
	}
	blobs, ok := s.blobs[hash]
	return blobs, ok, nil
}

func TestLocalBeacon(t *testing.T) {
	replay, err := testfixtures.LoadEEZDevReplay()
	require.NoError(t, err)
	b, other := replay.Batches[0], replay.Batches[1]
	h := &l1.Header{Number: uint64(b.L1Block.Number), Hash: b.L1Block.Hash, Time: uint64(b.L1Block.Timestamp)}
	hashes := b.Transaction.BlobVersionedHashes
	require.NotEmpty(t, hashes)
	ctx := context.Background()
	next := func() *countingBlobs { return &countingBlobs{blobs: b.Blobs()} }

	t.Run("served", func(t *testing.T) {
		store := &beaconStore{blobs: map[common.Hash][][]byte{h.Hash: b.Blobs()}}
		n := next()
		got, err := l1.NewLocalBeacon(store, n).Blobs(ctx, h, hashes)
		require.NoError(t, err)
		requireBlobs(t, hashes, got)
		require.Equal(t, []uint64{h.Time}, store.times)
		require.Zero(t, n.asked)
	})
	t.Run("served in the order asked", func(t *testing.T) {
		store := &beaconStore{blobs: map[common.Hash][][]byte{h.Hash: append(b.Blobs(), other.Blobs()...)}}
		asked := append(slices.Clone(other.Transaction.BlobVersionedHashes), hashes...)
		got, err := l1.NewLocalBeacon(store, nil).Blobs(ctx, h, asked)
		require.NoError(t, err)
		requireBlobs(t, asked, got)
	})
	t.Run("not stored", func(t *testing.T) {
		store := &beaconStore{}
		n := next()
		got, err := l1.NewLocalBeacon(store, n).Blobs(ctx, h, hashes)
		require.NoError(t, err)
		requireBlobs(t, hashes, got)
		require.Equal(t, len(hashes), n.asked)

		_, err = l1.NewLocalBeacon(store, nil).Blobs(ctx, h, hashes)
		require.ErrorIs(t, err, l1.ErrBlobUnavailable)
		require.NotErrorIs(t, err, l1.ErrBlobLost)
		require.ErrorContains(t, err, fmt.Sprintf("L1 block %d", h.Number))
	})
	t.Run("other blobs stored", func(t *testing.T) {
		store := &beaconStore{blobs: map[common.Hash][][]byte{h.Hash: other.Blobs()}}
		n := next()
		got, err := l1.NewLocalBeacon(store, n).Blobs(ctx, h, hashes)
		require.NoError(t, err)
		requireBlobs(t, hashes, got)
		require.Equal(t, len(hashes), n.asked)

		_, err = l1.NewLocalBeacon(store, nil).Blobs(ctx, h, hashes)
		require.ErrorIs(t, err, l1.ErrBlobUnavailable)
		require.NotErrorIs(t, err, l1.ErrBlobLost)
	})
	t.Run("store fails", func(t *testing.T) {
		failure := errors.New("index database closed")
		n := next()
		got, err := l1.NewLocalBeacon(&beaconStore{err: failure}, n).Blobs(ctx, h, hashes)
		require.NoError(t, err)
		requireBlobs(t, hashes, got)
		require.Equal(t, len(hashes), n.asked)

		_, err = l1.NewLocalBeacon(&beaconStore{err: failure}, nil).Blobs(ctx, h, hashes)
		require.ErrorIs(t, err, failure)
		require.NotErrorIs(t, err, l1.ErrBlobUnavailable)

		_, err = l1.NewLocalBeacon(&beaconStore{err: failure}, &countingBlobs{}).Blobs(ctx, h, hashes)
		require.ErrorIs(t, err, failure, "keeps why the local node failed")
		require.ErrorIs(t, err, l1.ErrBlobUnavailable)
	})
	t.Run("malformed blob stored", func(t *testing.T) {
		store := &beaconStore{blobs: map[common.Hash][][]byte{h.Hash: {make([]byte, 31)}}}
		n := next()
		got, err := l1.NewLocalBeacon(store, n).Blobs(ctx, h, hashes)
		require.NoError(t, err)
		requireBlobs(t, hashes, got)
		require.Equal(t, len(hashes), n.asked)

		_, err = l1.NewLocalBeacon(store, nil).Blobs(ctx, h, hashes)
		require.ErrorIs(t, err, l1.ErrInconsistent)
	})
}

// requireBlobs requires blobs to hash to hashes, in order.
func requireBlobs(t *testing.T, hashes []common.Hash, blobs [][]byte) {
	t.Helper()
	require.Len(t, blobs, len(hashes))
	for i, blob := range blobs {
		h, err := l1.VersionedHash(blob)
		require.NoError(t, err)
		require.Equal(t, hashes[i], h, "blob %d", i)
	}
}
