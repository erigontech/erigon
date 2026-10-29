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

package tools_test

import (
	"path/filepath"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/dir"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/eez/config"
	"github.com/erigontech/erigon/eez/deriver/derivertest"
	"github.com/erigontech/erigon/eez/internal/testfixtures"
	"github.com/erigontech/erigon/eez/l1"
	"github.com/erigontech/erigon/eez/networks"
	"github.com/erigontech/erigon/eez/tools"
)

// TestMirror indexes eez.dev's first settlements in two runs and stores
// their blobs, then restores a removed blob once its source serves it.
func TestMirror(t *testing.T) {
	r, err := testfixtures.LoadEEZDevReplay()
	require.NoError(t, err)
	l2 := networks.Chiado699069.Config.L2.(*config.L2Config)
	chain := derivertest.ReplayL1(t, r, &l2.RollupX.L1Reference)
	scanner := l1.NewScanner(chain, l2.Registry, l2.RollupID)
	scanner.UseRollupX()
	var all derivertest.ReplayBlobs
	distinct := map[common.Hash]bool{}
	for _, b := range r.Batches {
		all = append(all, b.Blobs()...)
		for _, h := range b.Transaction.BlobVersionedHashes {
			distinct[h] = true
		}
	}
	root := t.TempDir()
	index := filepath.Join(root, "699069.jsonl")
	blobs := filepath.Join(root, "blobs")
	ctx, logger := t.Context(), log.New()

	half := uint64(r.Batches[6].L1Block.Number)
	last := uint64(r.Batches[len(r.Batches)-1].L1Block.Number)
	stats, err := tools.Mirror(ctx, scanner, l1.NewBlobDir(blobs, all), index, l2.RegistryDeployBlock, half, logger)
	require.NoError(t, err)
	require.Equal(t, 7, stats.Indexed)
	first := stats.Stored
	stats, err = tools.Mirror(ctx, scanner, l1.NewBlobDir(blobs, all), index, l2.RegistryDeployBlock, last, logger)
	require.NoError(t, err)
	require.Equal(t, len(r.Batches)-7, stats.Indexed)
	require.Equal(t, len(distinct), first+stats.Stored)
	require.Empty(t, stats.Unavailable)

	entries, err := tools.ReadIndex(index)
	require.NoError(t, err)
	require.Len(t, entries, len(r.Batches))
	for i, e := range entries {
		require.Equal(t, uint64(r.Batches[i].L1Block.Number), e.L1Block)
		require.Equal(t, r.Batches[i].L1Block.Hash, e.L1Hash)
		require.Equal(t, r.Batches[i].Transaction.BlobVersionedHashes, e.Blobs)
	}

	// Up to date: nothing indexed or stored.
	stats, err = tools.Mirror(ctx, scanner, l1.NewBlobDir(blobs, nil), index, l2.RegistryDeployBlock, last, logger)
	require.NoError(t, err)
	require.Zero(t, stats.Indexed)
	require.Zero(t, stats.Stored)
	require.Empty(t, stats.Unavailable)

	gone := entries[3].Blobs[0]
	require.NoError(t, dir.RemoveFile(filepath.Join(blobs, gone.Hex()+".blob")))
	stats, err = tools.Mirror(ctx, scanner, l1.NewBlobDir(blobs, nil), index, l2.RegistryDeployBlock, last, logger)
	require.NoError(t, err)
	var holding []tools.MirrorEntry
	for _, e := range entries {
		if slices.Contains(e.Blobs, gone) {
			holding = append(holding, e)
		}
	}
	require.Equal(t, holding, stats.Unavailable)
	stats, err = tools.Mirror(ctx, scanner, l1.NewBlobDir(blobs, all), index, l2.RegistryDeployBlock, last, logger)
	require.NoError(t, err)
	require.Equal(t, 1, stats.Stored)
	require.Empty(t, stats.Unavailable)
}
