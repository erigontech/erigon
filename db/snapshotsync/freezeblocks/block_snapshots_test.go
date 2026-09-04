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

package freezeblocks

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/memdb"
	"github.com/erigontech/erigon/db/snapcfg"
	"github.com/erigontech/erigon/db/snapshotsync"
	"github.com/erigontech/erigon/db/snapshotsync/blocksnapshots"
	"github.com/erigontech/erigon/db/snaptype"
	snaptype2 "github.com/erigontech/erigon/db/snaptype2"
	"github.com/erigontech/erigon/db/version"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/chain/networkname"
	"github.com/erigontech/erigon/execution/stagedsync/stages"
	"github.com/erigontech/erigon/node/ethconfig"
)

const testMergeLimit = snaptype.Erigon2MergeLimit

// blockFilesTxStub is a kv.Getter that also exposes a pinned block-files view,
// like a temporal tx does.
type blockFilesTxStub struct {
	kv.Getter
	view *blocksnapshots.View
}

func (s blockFilesTxStub) BlockFilesRoTx() *blocksnapshots.View { return s.view }

// TestBlockReaderPrefersTxBlockView proves step 3: when a tx exposes a pinned
// block-files view, the reader resolves segments through it — even one retired
// from the live set after the view was pinned. (The temporal tx that supplies
// this view in production is covered in the follow-up that enables it.)
func TestBlockReaderPrefersTxBlockView(t *testing.T) {
	logger := log.New()
	dir := t.TempDir()
	cfg := ethconfig.Defaults.Snapshot
	cfg.ChainName = networkname.Mainnet
	snapshots := blocksnapshots.NewRoSnapshots(cfg, dir, logger)
	defer snapshots.Close()

	ver := version.V1_0
	for _, typ := range snaptype2.BlockSnapshotTypes {
		createTestSegmentFile(t, 0, testMergeLimit, typ.Enum(), dir, ver, logger)
		createTestSegmentFile(t, testMergeLimit, 2*testMergeLimit, typ.Enum(), dir, ver, logger)
	}
	require.NoError(t, snapshots.OpenFolder())

	blockReader := NewBlockReader(snapshots, nil)

	// Pin a view, then retire the [0, mergeLimit) tx segment from the live set.
	tx := blockFilesTxStub{view: snapshots.View()}
	defer tx.view.Close()
	_, err := snapshots.RetireFilesBelow(snaptype2.Transactions, testMergeLimit+1, nil)
	require.NoError(t, err)

	const blk = testMergeLimit / 2 // inside the retired [0, mergeLimit) segment

	// The live set no longer resolves it...
	_, okLive, relLive := snapshots.BaseRoSnapshots.ViewSingleFile(snaptype2.Transactions, blk)
	relLive()
	require.False(t, okLive, "retired segment must be gone from the live set")

	// ...but a reader using the tx's pinned view still does.
	_, okTx, relTx := blockReader.viewSingleFile(tx, snaptype2.Transactions, blk)
	relTx()
	require.True(t, okTx, "reader must resolve the retired segment via the tx's pinned view")
}

// The minimal/full-node step: expire old transaction segments (handing their files to the
// seeder), keeping recent ones and the other block types.
func TestRetireMergedTransactionFilesBelow(t *testing.T) {
	logger := log.New()
	dir := t.TempDir()
	cfg := ethconfig.Defaults.Snapshot
	cfg.ChainName = networkname.Mainnet
	snapshots := blocksnapshots.NewRoSnapshots(cfg, dir, logger)
	defer snapshots.Close()

	ver := version.V1_0
	for _, typ := range snaptype2.BlockSnapshotTypes {
		createTestSegmentFile(t, 0, testMergeLimit, typ.Enum(), dir, ver, logger)
		createTestSegmentFile(t, testMergeLimit, 2*testMergeLimit, typ.Enum(), dir, ver, logger)
	}
	require.NoError(t, snapshots.OpenFolder())

	var deleted []string
	retired, err := snapshots.RetireFilesBelow(snaptype2.Transactions, testMergeLimit+testMergeLimit/2, func(files []string) error {
		deleted = append(deleted, files...)
		return nil
	})
	require.NoError(t, err)
	require.True(t, retired)

	// The seeder is told about the [0, mergeLimit) tx segment: its .seg + both indexes.
	require.ElementsMatch(t, []string{
		snaptype.SegmentFileName(ver, 0, testMergeLimit, snaptype2.Transactions.Enum()),
		snaptype.IdxFileName(ver, 0, testMergeLimit, snaptype2.Transactions.Enum().String()),
		snaptype.IdxFileName(ver, 0, testMergeLimit, snaptype2.Indexes.TxnHash2BlockNum.Name),
	}, deleted)

	// Gone from the live set...
	_, ok, rel := snapshots.BaseRoSnapshots.ViewSingleFile(snaptype2.Transactions, testMergeLimit/2)
	rel()
	require.False(t, ok, "retired tx segment must be gone from the live set")

	// ...the [mergeLimit, 2*mergeLimit) tx segment stays (its range ends at the cutoff)...
	_, ok, rel = snapshots.BaseRoSnapshots.ViewSingleFile(snaptype2.Transactions, testMergeLimit)
	rel()
	require.True(t, ok, "tx segment at/above the cutoff must be kept")

	// ...and headers of the same range are untouched.
	_, ok, rel = snapshots.BaseRoSnapshots.ViewSingleFile(snaptype2.Headers, testMergeLimit/2)
	rel()
	require.True(t, ok, "only transaction segments are retired")
}

// TestRemoveBlockTripleArtifacts_RemovesSegAndAccessors pins the
// atomic-cleanup helper called from dumpBlocksRange when a later
// triple member fails after earlier members have already been
// written. Every file the helper is told to sweep must be gone
// afterwards; files outside the committed slice must survive.
func TestRemoveBlockTripleArtifacts_RemovesSegAndAccessors(t *testing.T) {
	dir := t.TempDir()

	touched := func(fi snaptype.FileInfo) []string {
		files := []string{fi.Path}
		for _, idx := range fi.Type.IdxFileNames(fi.From, fi.To) {
			files = append(files, filepath.Join(fi.Dir(), idx))
		}
		for _, p := range files {
			require.NoError(t, os.WriteFile(p, []byte("fake"), 0644))
		}
		return files
	}

	hdrFI := snaptype2.Headers.FileInfo(dir, 0, 1000)
	bodyFI := snaptype2.Bodies.FileInfo(dir, 0, 1000)
	txFI := snaptype2.Transactions.FileInfo(dir, 0, 1000)

	hdrFiles := touched(hdrFI)
	bodyFiles := touched(bodyFI)
	txFiles := touched(txFI)

	// Simulate a transactions-side failure: headers + bodies were
	// committed; transactions never made it past dumpRange's own
	// cleanup so nothing of txFI is in `committed`.
	removeBlockTripleArtifacts([]snaptype.FileInfo{hdrFI, bodyFI})

	for _, p := range append(hdrFiles, bodyFiles...) {
		_, err := os.Stat(p)
		require.True(t, os.IsNotExist(err), "expected %s to be removed, got err=%v", p, err)
	}
	for _, p := range txFiles {
		_, err := os.Stat(p)
		require.NoError(t, err, "%s must not be touched — not in committed slice", p)
	}
}

func TestDumpRangeErrorsWhenRangeAlreadyClaimed(t *testing.T) {
	logger := log.New()
	dir := t.TempDir()
	cfg := ethconfig.Defaults.Snapshot
	cfg.ChainName = networkname.Mainnet
	snapshots := blocksnapshots.NewRoSnapshots(cfg, dir, logger)
	defer snapshots.Close()

	f := snaptype2.Headers.FileInfo(dir, 0, 1000)
	require.True(t, snapshots.TryAcquireRange(f.Type.Enum(), f.From, f.To))

	dumperCalled := false
	dumper := func(ctx context.Context, db kv.RoDB, chainConfig *chain.Config, blockFrom, blockTo uint64, firstKey firstKeyGetter, collector func(v []byte) error, workers int, lvl log.Lvl, logger log.Logger) (uint64, error) {
		dumperCalled = true
		return 0, errors.New("dumper must not run on a claimed range")
	}

	_, err := dumpRange(t.Context(), f, dumper, nil, nil, nil, dir, 1, log.LvlInfo, logger, &snapshots.BaseRoSnapshots)
	require.ErrorIs(t, err, snapshotsync.ErrRangeBuildInProgress)
	require.False(t, dumperCalled)
}

// TestDumpRangeRejectsShortDump pins the filename-vs-content contract
// for the one-entry-per-block types. A .seg is named from the requested
// range, so a dump that emits fewer entries than the range advertises
// lands a partial file under a full-range name — later reads trust the
// name and fail with "source ran out at entry N". The file must not
// survive, so retire can retry the range once the DB catches up.
func TestDumpRangeRejectsShortDump(t *testing.T) {
	for _, tc := range []struct {
		name string
		fi   snaptype.FileInfo
	}{
		{"headers", snaptype2.Headers.FileInfo(t.TempDir(), 0, 1000)},
		{"bodies", snaptype2.Bodies.FileInfo(t.TempDir(), 0, 1000)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := tc.fi
			// Emit 999 words for a 1000-block range — one short.
			dumper := func(ctx context.Context, db kv.RoDB, chainConfig *chain.Config, blockFrom, blockTo uint64, firstKey firstKeyGetter, collector func(v []byte) error, workers int, lvl log.Lvl, logger log.Logger) (uint64, error) {
				for i := uint64(0); i < blockTo-blockFrom-1; i++ {
					if err := collector([]byte{byte(i)}); err != nil {
						return 0, err
					}
				}
				return 0, nil
			}

			_, err := dumpRange(t.Context(), f, dumper, nil, nil, nil, filepath.Dir(f.Path), 1, log.LvlInfo, log.New(), nil)
			require.Error(t, err, "a short dump must not be accepted under a full-range filename")
			require.Contains(t, err.Error(), "sparse in requested range")

			_, statErr := os.Stat(f.Path)
			require.True(t, os.IsNotExist(statErr),
				"partial file must be removed so retire can retry the range, got err=%v", statErr)
		})
	}
}

// TestDumpRangePassesCompleteDumpToIndexing pins the complement of
// TestDumpRangeRejectsShortDump: a dump emitting exactly one entry per
// block clears the count gate. It then fails in index-building, which
// needs a salt this bare-tempdir fixture has no reason to provide — so
// the assertion is that the gate specifically did not reject it.
func TestDumpRangePassesCompleteDumpToIndexing(t *testing.T) {
	f := snaptype2.Headers.FileInfo(t.TempDir(), 0, 1000)
	dumper := func(ctx context.Context, db kv.RoDB, chainConfig *chain.Config, blockFrom, blockTo uint64, firstKey firstKeyGetter, collector func(v []byte) error, workers int, lvl log.Lvl, logger log.Logger) (uint64, error) {
		for i := uint64(0); i < blockTo-blockFrom; i++ {
			if err := collector([]byte{byte(i)}); err != nil {
				return 0, err
			}
		}
		return 0, nil
	}

	_, err := dumpRange(t.Context(), f, dumper, nil, nil, nil, filepath.Dir(f.Path), 1, log.LvlInfo, log.New(), nil)
	if err != nil {
		require.NotContains(t, err.Error(), "sparse in requested range",
			"a complete dump must clear the count gate")
	}
}

// TestDumpRangeAllowsShortTransactions pins that the per-block count
// rule does not apply to transactions: entries there are one per
// transaction, not one per block, so any count is legitimate. Same
// index-building caveat as above.
func TestDumpRangeAllowsShortTransactions(t *testing.T) {
	f := snaptype2.Transactions.FileInfo(t.TempDir(), 0, 1000)
	dumper := func(ctx context.Context, db kv.RoDB, chainConfig *chain.Config, blockFrom, blockTo uint64, firstKey firstKeyGetter, collector func(v []byte) error, workers int, lvl log.Lvl, logger log.Logger) (uint64, error) {
		return 0, collector([]byte{1})
	}

	_, err := dumpRange(t.Context(), f, dumper, nil, nil, nil, filepath.Dir(f.Path), 1, log.LvlInfo, log.New(), nil)
	if err != nil {
		require.NotContains(t, err.Error(), "sparse in requested range",
			"transactions emit one entry per txn, not per block — the count rule must not apply")
	}
}

// TestChooseRetireTailStart_NoV4OneReturnsChunkFrom pins the baseline:
// an empty snapDir (no v4 #1 present) returns chunkFrom unchanged so
// retire emits the standard chunk output.
func TestChooseRetireTailStart_NoV4OneReturnsChunkFrom(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	got := chooseRetireTailStart(dir, 3_491_000, 3_492_000)
	require.Equal(t, uint64(3_491_000), got, "no v4 #1 present — retire emits from chunkFrom")
}

// TestChooseRetireTailStart_V4OnePresentReturnsCut pins the core wire:
// when a v4 #1 headers .seg for the chunk exists on disk, retire's
// emit start moves to the v4 #1's To so the tail v4 #2 covers the
// complementary [cut, chunkEnd) range.
func TestChooseRetireTailStart_V4OnePresentReturnsCut(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	// Simulate mode-C emit output: v4 #1 headers file covering
	// [3491000, 3491691) — non-1000-aligned To triggers the v4 name.
	name := snaptype.FileNameV4(snaptype2.Headers.Versions().Current, 3_491_000, 3_491_691, "headers") + ".seg"
	require.NoError(t, os.WriteFile(filepath.Join(dir, name), []byte("stub"), 0o644))

	got := chooseRetireTailStart(dir, 3_491_000, 3_492_000)
	require.Equal(t, uint64(3_491_691), got, "v4 #1 present at [3491000, 3491691) — retire emits tail from cut")
}

// TestChooseRetireTailStart_WideV4OnePartialOverlapReturnsCut pins the
// cycle-15 signature: rebuildBlockStraddles produces a WIDE v4 #1
// whose From is far below the retire chunk (e.g. a merged straddle
// covering [3400000, 3498391) after mode-C truncation) and whose To
// falls INSIDE a later retire chunk. The old detector required
// info.From == chunkFrom and missed this case — retire tried to emit
// from chunkFrom, hit a DB with the pre-cut blocks pruned, and
// errored with "header missed in db".
func TestChooseRetireTailStart_WideV4OnePartialOverlapReturnsCut(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	// Wide v4 #1 spanning multiple chunks — covers [3400000, 3498391).
	// From is far below the retire chunk [3498000, 3499000); To lands
	// inside the chunk at 3498391. Retire must emit the tail
	// [3498391, 3499000).
	name := snaptype.FileNameV4(snaptype2.Headers.Versions().Current, 3_400_000, 3_498_391, "headers") + ".seg"
	require.NoError(t, os.WriteFile(filepath.Join(dir, name), []byte("stub"), 0o644))

	got := chooseRetireTailStart(dir, 3_498_000, 3_499_000)
	require.Equal(t, uint64(3_498_391), got,
		"wide v4 #1 with partial overlap must move emitFrom to v4 #1's To — pre-cut range was retired into v4 #1")
}

// TestChooseRetireTailStart_WideV4OneFullCoverageReturnsChunkEnd pins the
// full-coverage variant of the wide-v4 case: v4 #1's To reaches past
// chunkEnd, so retire has nothing to emit for this chunk. The
// per-chunk retire loop's `if emitFrom >= segEnd { skip }` branch
// relies on this returning chunkEnd (or higher).
func TestChooseRetireTailStart_WideV4OneFullCoverageReturnsChunkEnd(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	// Wide v4 #1 fully covers this chunk (and many others).
	name := snaptype.FileNameV4(snaptype2.Headers.Versions().Current, 3_400_000, 3_498_391, "headers") + ".seg"
	require.NoError(t, os.WriteFile(filepath.Join(dir, name), []byte("stub"), 0o644))

	// Retire chunk in the middle of the wide v4 #1's range.
	got := chooseRetireTailStart(dir, 3_450_000, 3_451_000)
	require.GreaterOrEqual(t, got, uint64(3_451_000),
		"chunk fully covered by wide v4 #1 — retire must skip (emitFrom >= chunkEnd)")
}

// TestChooseRetireTailStart_V4OneFullCoverageReturnsChunkEnd pins the
// degenerate case: v4 #1 already covers the whole chunk (its To ==
// chunkEnd). Retire has nothing to emit; DumpBlocks skips this chunk
// entirely. Returning chunkEnd signals that.
func TestChooseRetireTailStart_V4OneFullCoverageReturnsChunkEnd(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	// v4 #1 covers the whole chunk. Naming still uses v4 form to
	// force it into the detection path (real emitters wouldn't
	// produce a fully-aligned range as v4, but the detector must
	// handle the edge).
	name := snaptype.FileNameV4(snaptype2.Headers.Versions().Current, 3_491_000, 3_491_500, "headers") + ".seg"
	require.NoError(t, os.WriteFile(filepath.Join(dir, name), []byte("stub"), 0o644))
	name2 := snaptype.FileNameV4(snaptype2.Headers.Versions().Current, 3_491_000, 3_492_000, "headers") + ".seg"
	require.NoError(t, os.WriteFile(filepath.Join(dir, name2), []byte("stub"), 0o644))

	// maxCut is 3_492_000 which >= chunkEnd, retire skips.
	got := chooseRetireTailStart(dir, 3_491_000, 3_492_000)
	require.Equal(t, uint64(3_492_000), got, "v4 #1 covers whole chunk — retire has nothing to emit")
}

// TestDumpBlocks_FullyCoveredChunkProducesNothing pins the precondition
// of the retire spin: when a v4 #1 already covers the whole chunk,
// DumpBlocks emits no files. Paired with buildFiles reporting progress
// from produced files rather than from CanRetire, this is what lets the
// RetireBlocks loop terminate.
//
// Cycle 26 span: retire logged 1,630,704 identical
// "[snapshots:blocks:retire] Stat blocks=3.411.513" lines at ~445/s for a
// full hour after a mode-D unwind. FrozenBlocks() never advanced, so
// minBlockNum was recomputed identically each pass and CanRetire kept
// answering true — an exit condition that could never be reached. It
// also held buildingFiles=true throughout, which is what timed out the
// next setHead's quiescence wait.
func TestDumpBlocks_FullyCoveredChunkProducesNothing(t *testing.T) {
	logger := log.New()
	dir := t.TempDir()
	cfg := ethconfig.Defaults.Snapshot
	cfg.ChainName = networkname.Mainnet
	snapshots := blocksnapshots.NewRoSnapshots(cfg, dir, logger)
	defer snapshots.Close()
	require.NoError(t, snapshots.OpenFolder())

	// A v4 #1 covering the whole chunk — the post-mode-D-emit shape.
	name := snaptype.FileNameV4(snaptype2.Headers.Versions().Current, 3_491_000, 3_492_000, "headers") + ".seg"
	require.NoError(t, os.WriteFile(filepath.Join(dir, name), []byte("stub"), 0o644))

	db := memdb.NewTestDB(t, dbcfg.ChainDB)
	require.NoError(t, db.Update(t.Context(), func(tx kv.RwTx) error {
		// Past the chunk end so DumpBlocks' head-bound guard passes.
		return stages.SaveStageProgress(tx, stages.Bodies, 3_500_000)
	}))

	snCfg, ok := snapcfg.KnownCfg(networkname.Mainnet)
	require.True(t, ok)
	produced, err := DumpBlocks(t.Context(), 3_491_000, 3_492_000, nil, dir, dir, db, 1,
		log.LvlInfo, logger, NewBlockReader(snapshots, nil), snCfg, &snapshots.BaseRoSnapshots)
	require.NoError(t, err)
	require.Empty(t, produced,
		"chunk already covered by a v4 #1 must produce no files — retire has nothing to emit here")
}
