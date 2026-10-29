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
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/eez/internal/testfixtures"
	"github.com/erigontech/erigon/eez/l1"
)

// countingBlobs serves blobs and counts the blobs asked for.
type countingBlobs struct {
	blobs [][]byte
	asked int
}

func (c *countingBlobs) Blobs(_ context.Context, _ *l1.Header, hashes []common.Hash) ([][]byte, error) {
	c.asked += len(hashes)
	return l1.MatchBlobs(c.blobs, hashes)
}

func TestBlobDir(t *testing.T) {
	replay, err := testfixtures.LoadEEZDevReplay()
	require.NoError(t, err)
	next := &countingBlobs{}
	for _, b := range replay.Batches {
		next.blobs = append(next.blobs, b.Blobs()...)
	}
	dir := filepath.Join(t.TempDir(), "blobs")
	ctx := context.Background()
	b := replay.Batches[0]
	h := &l1.Header{Number: uint64(b.L1Block.Number), Time: uint64(b.L1Block.Timestamp)}
	hashes := b.Transaction.BlobVersionedHashes

	_, err = l1.NewBlobDir(dir, nil).Blobs(ctx, h, hashes)
	require.ErrorIs(t, err, l1.ErrBlobUnavailable)

	// Read through to next once, then from the directory.
	d := l1.NewBlobDir(dir, next)
	for range 2 {
		blobs, err := d.Blobs(ctx, h, hashes)
		require.NoError(t, err)
		require.Len(t, blobs, len(hashes))
		for i, blob := range blobs {
			hash, err := l1.VersionedHash(blob)
			require.NoError(t, err)
			require.Equal(t, hashes[i], hash)
		}
		require.Equal(t, len(hashes), next.asked)
	}
	for _, hash := range hashes {
		ok, err := d.Has(hash)
		require.NoError(t, err)
		require.True(t, ok)
	}
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	require.Len(t, entries, len(hashes))

	// A file whose content is not its name's blob.
	other := replay.Batches[1].Transaction.BlobVersionedHashes[0]
	require.NotEqual(t, hashes[0], other)
	content, err := os.ReadFile(filepath.Join(dir, hashes[0].Hex()+".blob"))
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(dir, other.Hex()+".blob"), content, 0o644))
	_, err = d.Blobs(ctx, h, []common.Hash{other})
	require.ErrorIs(t, err, l1.ErrInconsistent)
}
