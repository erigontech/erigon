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

package storage

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/seg"
	"github.com/erigontech/erigon/db/snaptype"
	"github.com/erigontech/erigon/db/snaptype2"
)

// TestChunkAlignedToBlock pins the 1000-boundary rounding semantics.
func TestChunkAlignedToBlock(t *testing.T) {
	t.Parallel()
	require.Equal(t, uint64(2_913_000), chunkAlignedToBlock(2_912_999), "toBlock+1 already a 1000-boundary returns toBlock+1")
	require.Equal(t, uint64(2_912_000), chunkAlignedToBlock(2_912_500), "non-aligned toBlock+1 returns previous 1000-boundary")
	require.Equal(t, uint64(2_912_000), chunkAlignedToBlock(2_912_000), "toBlock that is itself a 1000-multiple → next is 1 past → returns prior boundary")
	require.Equal(t, uint64(0), chunkAlignedToBlock(998), "toBlock+1 < 1000 returns 0 (no aligned cut available)")
	require.Equal(t, uint64(1_000), chunkAlignedToBlock(999), "toBlock=999 → toBlock+1=1000 → 1000-aligned")
}

// makeFakeSegFile writes a .seg with `count` uncompressed words at
// `path`. Used to fabricate straddle-file fixtures in unit tests. The
// content of each word is opaque — sliceStraddleSeg copies words
// by position, not by content, so the IndexBuilderFunc isn't
// exercised here (it would need real header RLP).
func makeFakeSegFile(t *testing.T, ctx context.Context, path, tmpDir string, count int) {
	t.Helper()
	cfg := seg.DefaultCfg
	c, err := seg.NewCompressor(ctx, "test-fake-seg", path, tmpDir, cfg, log.LvlError, log.New())
	require.NoError(t, err)
	defer c.Close()
	for i := 0; i < count; i++ {
		// Word content: ASCII "block-NNNNN". Differentiates positions
		// for sequential read verification.
		word := fmt.Appendf(nil, "block-%05d", i)
		require.NoError(t, c.AddUncompressedWord(word))
	}
	require.NoError(t, c.Compress())
}

// TestSliceStraddleSeg_TruncatesEntries pins the core slice semantic:
// after sliceStraddleSeg the new .seg file holds exactly the first N
// entries from the source, where N = newToBlock - oldFI.From. The
// remaining entries (representing blocks past toBlock) are gone.
//
// Block-snapshot files have 1 entry per block (for headers, bodies,
// senders). Fixture: 10000-entry source representing a 10K-block
// straddle file. Truncates to 7000 entries (the 002000-002007 range
// after a mode-B target inside the 002000-002010 file).
func TestSliceStraddleSeg_TruncatesEntries(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	snapDir := t.TempDir()
	tmpDir := t.TempDir()

	// Fixture: 10K-entry headers .seg over block range
	// [2_000_000, 2_010_000). One entry per block.
	oldFI := snaptype.FileInfo{
		Version: snaptype2.Headers.Versions().Current,
		From:    2_000_000,
		To:      2_010_000,
		Type:    snaptype2.Headers,
		Ext:     ".seg",
	}
	oldFI = oldFI.As(snaptype2.Headers)
	oldFI.Path = filepath.Join(snapDir, oldFI.Name())
	makeFakeSegFile(t, ctx, oldFI.Path, tmpDir, 10_000)

	// Truncate to newTo = 2_007_000 (3000 blocks dropped from the tail).
	newFI, written, err := sliceStraddleSeg(ctx, oldFI, 2_007_000, snapDir, tmpDir, log.New())
	require.NoError(t, err)
	require.Equal(t, uint64(7_000), written, "must write exactly 7000 entries (newTo - From)")
	require.FileExists(t, newFI.Path)

	dec, err := seg.NewDecompressor(newFI.Path)
	require.NoError(t, err)
	defer dec.Close()
	require.EqualValues(t, 7_000, dec.Count())

	// Spot-check first, middle, and last entries match the source's
	// positions 0, 3500, 6999.
	g := dec.MakeGetter()
	var buf []byte
	pos := 0
	for g.HasNext() {
		buf, _ = g.Next(buf[:0])
		switch pos {
		case 0, 3_500, 6_999:
			require.Equal(t, fmt.Sprintf("block-%05d", pos), string(buf),
				"entry %d content must match source position", pos)
		}
		pos++
	}
	require.Equal(t, 7_000, pos, "read all 7000 entries; no more")
}

// TestRebuildBodiesStraddleFile_RejectsWrongType pins that the
// bodies-specific entry point refuses a non-bodies FileInfo. Same
// pattern as headers.
func TestRebuildBodiesStraddleFile_RejectsWrongType(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	fi := snaptype.FileInfo{
		Version: snaptype2.Headers.Versions().Current,
		From:    2_000_000,
		To:      2_010_000,
		Type:    snaptype2.Headers, // wrong type for bodies entry
		Ext:     ".seg",
	}
	fi = fi.As(snaptype2.Headers)
	_, err := rebuildBodiesStraddleFile(ctx, fi, 2_007_000, t.TempDir(), t.TempDir(), nil, log.New())
	require.Error(t, err)
	require.Contains(t, err.Error(), "not Bodies")
}

// TestRebuildTransactionsStraddleFile_RejectsWrongType pins the
// type guard on the tx entry.
func TestRebuildTransactionsStraddleFile_RejectsWrongType(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	fi := snaptype.FileInfo{
		Version: snaptype2.Headers.Versions().Current,
		From:    2_000_000,
		To:      2_010_000,
		Type:    snaptype2.Bodies, // wrong type for tx entry
		Ext:     ".seg",
	}
	fi = fi.As(snaptype2.Bodies)
	_, err := rebuildTransactionsStraddleFile(ctx, fi, 2_007_000, t.TempDir(), t.TempDir(), nil, log.New())
	require.Error(t, err)
	require.Contains(t, err.Error(), "not Transactions")
}

// TestRebuildTransactionsStraddleFile_RejectsMissingRebuiltBodies
// pins the ordering contract: transactions rebuild requires the
// bodies file at the NEW range to exist. Without it (because bodies
// wasn't rebuilt first), the tx rebuild errors out with a clear
// message.
func TestRebuildTransactionsStraddleFile_RejectsMissingRebuiltBodies(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	snapDir := t.TempDir()
	tmpDir := t.TempDir()

	// Fake old transactions .seg fixture. The function reads bodies
	// BEFORE touching the source tx file, so the source contents
	// don't matter for this guard test — but the source path must
	// be visible to a Decompressor open. Build an empty seg.
	oldFI := snaptype.FileInfo{
		Version: snaptype2.Transactions.Versions().Current,
		From:    2_000_000,
		To:      2_010_000,
		Type:    snaptype2.Transactions,
		Ext:     ".seg",
	}
	oldFI = oldFI.As(snaptype2.Transactions)
	oldFI.Path = filepath.Join(snapDir, oldFI.Name())
	// Need at least one entry for seg.NewCompressor to produce a
	// valid file; the path-not-found check trips before we touch it.
	makeFakeSegFile(t, ctx, oldFI.Path, tmpDir, 1)

	_, err := rebuildTransactionsStraddleFile(ctx, oldFI, 2_007_000, snapDir, tmpDir, nil, log.New())
	require.Error(t, err)
	require.Contains(t, err.Error(), "rebuilt bodies file not found",
		"tx rebuild must fail explicitly when bodies hasn't been rebuilt first — order matters")
}

// TestSliceStraddleSeg_RejectsBadInputs pins guard-clause behavior.
func TestSliceStraddleSeg_RejectsBadInputs(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	makeFI := func(from, to uint64) snaptype.FileInfo {
		fi := snaptype.FileInfo{
			Version: snaptype2.Headers.Versions().Current,
			From:    from,
			To:      to,
			Type:    snaptype2.Headers,
			Ext:     ".seg",
		}
		return fi.As(snaptype2.Headers)
	}

	cases := []struct {
		name       string
		oldFI      snaptype.FileInfo
		newToBlock uint64
		wantErr    string
	}{
		{"newToBlock ≤ From", makeFI(2_000_000, 2_010_000), 2_000_000, "outside straddle"},
		{"newToBlock ≥ To", makeFI(2_000_000, 2_010_000), 2_010_000, "outside straddle"},
		{"newToBlock past To", makeFI(2_000_000, 2_010_000), 2_020_000, "outside straddle"},
		{"non-1000-aligned newToBlock", makeFI(2_000_000, 2_010_000), 2_005_500, "not aligned"},
		{"nil Type", snaptype.FileInfo{From: 2_000_000, To: 2_010_000}, 2_005_000, "nil Type"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, _, err := sliceStraddleSeg(ctx, tc.oldFI, tc.newToBlock, t.TempDir(), t.TempDir(), log.New())
			require.Error(t, err)
			require.Contains(t, err.Error(), tc.wantErr)
		})
	}
}
