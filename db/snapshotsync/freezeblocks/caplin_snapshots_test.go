// Copyright 2024 The Erigon Authors
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
	"bytes"
	"encoding/binary"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/persistence/beacon_indicies"
	"github.com/erigontech/erigon/cl/persistence/format/snapshot_format"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/background"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/dbutils"
	"github.com/erigontech/erigon/db/kv/mdbx/mdbxtest"
	"github.com/erigontech/erigon/db/recsplit"
	"github.com/erigontech/erigon/db/seg"
	"github.com/erigontech/erigon/db/snapshotsync"
	"github.com/erigontech/erigon/db/snaptype"
	"github.com/erigontech/erigon/db/version"
	"github.com/erigontech/erigon/node/ethconfig"
)

// TestDumpBeaconBlocksNoPanic is a higher-level regression test: the production
// panic occurred inside DumpBeaconBlocks when chooseSegmentEnd was called with
// nil snCfg. This test uses toSlot=CaplinMergeLimit so the loop body executes
// (doesn't break early), exercising the exact code path that panicked.
func TestDumpBeaconBlocksNoPanic(t *testing.T) {
	db := mdbxtest.NewTestDB(t, dbcfg.ChainDB)
	dirs := datadir.New(t.TempDir())

	// toSlot == CaplinMergeLimit: toSlot-fromSlot is not < blocksPerFile,
	// so the loop reaches chooseSegmentEnd and dumpBeaconBlocksRange.
	// Before the fix this panicked; now it returns an error (no snap dir / empty db).
	var err error
	require.NotPanics(t, func() {
		err = DumpBeaconBlocks(t.Context(), db, 0, snaptype.CaplinMergeLimit, 0, dirs, 1, log.LvlDebug, log.New())
	})
	require.ErrorContains(t, err, "skipped too many blocks in a row")
}

// dumpBeaconBlocksRange indexes the segment it just compressed, which is only safe because
// Compress fsyncs and renames before returning.
func TestDumpBeaconBlocksRangeBuildsSegAndIdx(t *testing.T) {
	ctx := t.Context()
	db := mdbxtest.NewTestDB(t, dbcfg.ChainDB)
	dirs := datadir.New(t.TempDir())

	require.NoError(t, db.Update(ctx, func(tx kv.RwTx) error {
		var prevRoot common.Hash
		// One block every 500 slots keeps skippedInARow below its 1000 limit.
		for slot := uint64(0); slot < snaptype.CaplinMergeLimit; slot += 500 {
			var root common.Hash
			binary.BigEndian.PutUint64(root[:], slot+1)
			if err := beacon_indicies.MarkRootCanonical(ctx, tx, slot, root); err != nil {
				return err
			}
			if err := beacon_indicies.WriteParentBlockRoot(ctx, tx, root, prevRoot); err != nil {
				return err
			}
			if err := tx.Put(kv.BeaconBlocks, dbutils.BlockBodyKey(slot, root), root[:]); err != nil {
				return err
			}
			prevRoot = root
		}
		return nil
	}))

	require.NoError(t, dumpBeaconBlocksRange(ctx, db, 0, snaptype.CaplinMergeLimit, 1, dirs, 1, log.LvlDebug, log.New()))

	segName := snaptype.BeaconBlocks.FileName(version.ZeroVersion, 0, snaptype.CaplinMergeLimit)
	f, _, ok := snaptype.ParseFileName(dirs.Snap, segName)
	require.True(t, ok)

	d, err := seg.NewDecompressor(f.Path)
	require.NoError(t, err)
	defer d.Close()
	require.Equal(t, uint64(snaptype.CaplinMergeLimit), uint64(d.Count()))

	idxPaths, err := filepath.Glob(filepath.Join(dirs.Snap, "*.idx"))
	require.NoError(t, err)
	require.Len(t, idxPaths, 1)

	idx, err := recsplit.OpenIndex(idxPaths[0])
	require.NoError(t, err)
	defer idx.Close()
	require.Equal(t, uint64(snaptype.CaplinMergeLimit), idx.KeyCount())
}

// Pre-Deneb chains never freeze blobs, so the answer must be 0 regardless of visible segments.
func TestFrozenBlobsZeroBeforeDeneb(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	cfg.DenebForkEpoch = math.MaxUint64

	dirs := datadir.New(t.TempDir())
	sn := NewCaplinSnapshots(ethconfig.BlocksFreezing{ChainName: "mainnet"}, &cfg, dirs, log.New())
	t.Cleanup(sn.Close)

	require.Equal(t, uint64(0), sn.FrozenBlobs())
}

// FrozenBlobs is the blob-antiquation watermark, so it must report the end of the
// visible blob segments rather than a constant.
func TestFrozenBlobsReportsVisibleSegmentEnd(t *testing.T) {
	dirs := datadir.New(t.TempDir())
	writeEmptyBlobSidecarsSegment(t, dirs, 0, snaptype.CaplinMergeLimit)

	sn := NewCaplinSnapshots(ethconfig.BlocksFreezing{ChainName: "mainnet"}, &clparams.MainnetBeaconConfig, dirs, log.New())
	t.Cleanup(sn.Close)
	require.NoError(t, sn.OpenFolder())

	require.Equal(t, uint64(snaptype.CaplinMergeLimit), sn.FrozenBlobs())
}

// Blocks run ahead of blobs (blobs only start at Deneb) and the block tip is
// dumped before it is indexed, so the four watermarks legitimately disagree.
// The exact numbers below are the equivalence net for the BaseRoSnapshots embed:
// only IndicesMax may move, from To to To-1, dragging BlocksAvailable with it.
func TestCaplinWatermarks(t *testing.T) {
	const limit = snaptype.CaplinMergeLimit

	dirs := datadir.New(t.TempDir())
	writeEmptyCaplinSegment(t, dirs, snaptype.BeaconBlocks, 0, limit, true)
	writeEmptyCaplinSegment(t, dirs, snaptype.BeaconBlocks, limit, 2*limit, true)
	writeEmptyCaplinSegment(t, dirs, snaptype.BeaconBlocks, 2*limit, 3*limit, false)
	writeEmptyCaplinSegment(t, dirs, snaptype.BlobSidecars, 0, limit, true)

	sn := NewCaplinSnapshots(ethconfig.BlocksFreezing{ChainName: "mainnet"}, &clparams.MainnetBeaconConfig, dirs, log.New())
	t.Cleanup(sn.Close)
	require.NoError(t, sn.OpenFolder())

	view := sn.View()
	defer view.Close()
	require.Len(t, view.BeaconBlocks(), 2, "the unindexed tail segment must not be visible")
	require.Len(t, view.BlobSidecars(), 1)

	// dirty-backed: counts the unindexed tail, inclusive To-1.
	require.Equal(t, uint64(3*limit-1), sn.SegmentsMax())
	// visible-backed, inclusive To-1. BlocksAvailable is min(SegmentsMax, IndicesMax),
	// so it follows IndicesMax whenever the tip is dumped but not yet indexed.
	require.Equal(t, uint64(2*limit-1), sn.IndicesMax())
	require.Equal(t, uint64(2*limit-1), sn.BlocksAvailable())
	// visible-backed, exclusive To — cl/beacon/handler/blobs.go compares slot < FrozenBlobs().
	require.Equal(t, uint64(limit), sn.FrozenBlobs())
}

// The Deneb short-circuit wins over visible blob segments, so a pre-Deneb chain
// reports 0 even with blobs on disk.
func TestFrozenBlobsZeroBeforeDenebWithVisibleSegments(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	cfg.DenebForkEpoch = math.MaxUint64

	dirs := datadir.New(t.TempDir())
	writeEmptyBlobSidecarsSegment(t, dirs, 0, snaptype.CaplinMergeLimit)

	sn := NewCaplinSnapshots(ethconfig.BlocksFreezing{ChainName: "mainnet"}, &cfg, dirs, log.New())
	t.Cleanup(sn.Close)
	require.NoError(t, sn.OpenFolder())

	view := sn.View()
	defer view.Close()
	require.Len(t, view.BlobSidecars(), 1)
	require.Equal(t, uint64(0), sn.FrozenBlobs())
}

// A blob segment with one empty word per slot: enough for OpenFolder to make it
// visible, without needing a populated blob store.
func writeEmptyBlobSidecarsSegment(t *testing.T, dirs datadir.Dirs, from, to uint64) {
	t.Helper()
	writeEmptyCaplinSegment(t, dirs, snaptype.BlobSidecars, from, to, true)
}

// withIndex=false leaves the .seg on disk unindexed, which keeps it out of the
// visible generation while still counting as dirty.
func writeEmptyCaplinSegment(t *testing.T, dirs datadir.Dirs, sType snaptype.Type, from, to uint64, withIndex bool) {
	t.Helper()
	writeCaplinSegment(t, dirs, sType, from, to, withIndex, func(uint64) []byte { return nil })
}

// slotWord makes each word the slot it is stored at, so a read can be checked against
// the slot it asked for.
func slotWord(slot uint64) []byte {
	w := make([]byte, 8)
	binary.BigEndian.PutUint64(w, slot)
	return w
}

func writeCaplinSegment(t *testing.T, dirs datadir.Dirs, sType snaptype.Type, from, to uint64, withIndex bool, word func(slot uint64) []byte) {
	t.Helper()
	segName := sType.FileName(version.ZeroVersion, from, to)
	f, _, ok := snaptype.ParseFileName(dirs.Snap, segName)
	require.True(t, ok)

	c, err := seg.NewCompressor(t.Context(), "test "+sType.Name(), f.Path, dirs.Tmp, seg.DefaultCfg, log.LvlDebug, log.New())
	require.NoError(t, err)
	defer c.Close()
	for slot := from; slot < to; slot++ {
		require.NoError(t, c.AddWord(word(slot)))
	}
	require.NoError(t, c.Compress())

	if !withIndex {
		return
	}
	require.NoError(t, snapshotsync.BeaconSimpleIdx(t.Context(), f, 1, dirs.Tmp, &background.Progress{}, log.LvlDebug, log.New()))
}

// ReadHeader picks its segment through a pinned single-file view, so the word it decodes
// has to be the one stored at the slot asked for - and a slot with no block, or one past
// the last segment, has to come back empty rather than as a decode error.
func TestReadHeaderReadsSlotFromSegment(t *testing.T) {
	const limit = snaptype.CaplinMergeLimit
	const blockSlot = 42

	block := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.Phase0Version)
	block.Block.Slot = blockSlot
	block.Block.ProposerIndex = 9
	word := blockSnapshotWord(t, block)

	dirs := datadir.New(t.TempDir())
	writeCaplinSegment(t, dirs, snaptype.BeaconBlocks, 0, limit, true, func(slot uint64) []byte {
		if slot == blockSlot {
			return word
		}
		return nil
	})

	sn := NewCaplinSnapshots(ethconfig.BlocksFreezing{ChainName: "mainnet"}, &clparams.MainnetBeaconConfig, dirs, log.New())
	t.Cleanup(sn.Close)
	require.NoError(t, sn.OpenFolder())

	header, elBlockNumber, elBlockHash, err := sn.ReadHeader(blockSlot, nil)
	require.NoError(t, err)
	require.NotNil(t, header)
	require.Equal(t, uint64(blockSlot), header.Header.Slot)
	require.Equal(t, uint64(9), header.Header.ProposerIndex)
	require.Zero(t, elBlockNumber)
	require.Equal(t, common.Hash{}, elBlockHash)

	empty, _, _, err := sn.ReadHeader(blockSlot+1, nil)
	require.NoError(t, err)
	require.Nil(t, empty, "a slot with no block must not decode")

	beyond, _, _, err := sn.ReadHeader(limit, nil)
	require.NoError(t, err)
	require.Nil(t, beyond, "a slot past the last segment has no view to read")
}

func blockSnapshotWord(t *testing.T, block *cltypes.SignedBeaconBlock) []byte {
	t.Helper()
	var b bytes.Buffer
	enc, err := zstd.NewWriter(&b)
	require.NoError(t, err)
	_, err = snapshot_format.WriteBlockForSnapshot(enc, block, nil)
	require.NoError(t, err)
	require.NoError(t, enc.Close())
	return b.Bytes()
}

// A blob word is the concatenated SSZ of every sidecar at that slot, so ReadBlobSidecars
// has to split it on the fixed sidecar size and reject a word that is not a multiple of it.
func TestReadBlobSidecars(t *testing.T) {
	const limit = snaptype.CaplinMergeLimit
	const blobSlot = 7
	const truncatedSlot = 8

	encoded, err := blobSidecar(blobSlot, 3).EncodeSSZ(nil)
	require.NoError(t, err)
	require.Len(t, encoded, sidecarSSZSize)

	dirs := datadir.New(t.TempDir())
	writeCaplinSegment(t, dirs, snaptype.BlobSidecars, 0, limit, true, func(slot uint64) []byte {
		switch slot {
		case blobSlot:
			return encoded
		case truncatedSlot:
			return encoded[:len(encoded)-1]
		default:
			return nil
		}
	})

	sn := NewCaplinSnapshots(ethconfig.BlocksFreezing{ChainName: "mainnet"}, &clparams.MainnetBeaconConfig, dirs, log.New())
	t.Cleanup(sn.Close)
	require.NoError(t, sn.OpenFolder())

	sidecars, err := sn.ReadBlobSidecars(blobSlot)
	require.NoError(t, err)
	require.Len(t, sidecars, 1)
	require.Equal(t, uint64(3), sidecars[0].Index)
	require.Equal(t, uint64(blobSlot), sidecars[0].SignedBlockHeader.Header.Slot)

	_, err = sn.ReadBlobSidecars(truncatedSlot)
	require.ErrorContains(t, err, "invalid sidecar list length")

	empty, err := sn.ReadBlobSidecars(blobSlot + 2)
	require.NoError(t, err)
	require.Empty(t, empty, "a slot with no sidecars must not decode")

	beyond, err := sn.ReadBlobSidecars(limit)
	require.NoError(t, err)
	require.Empty(t, beyond, "a slot past the last segment has no view to read")
}

func blobSidecar(slot, index uint64) *cltypes.BlobSidecar {
	return &cltypes.BlobSidecar{
		Index:                    index,
		SignedBlockHeader:        &cltypes.SignedBeaconBlockHeader{Header: &cltypes.BeaconBlockHeader{Slot: slot}},
		CommitmentInclusionProof: solid.NewHashVector(cltypes.CommitmentBranchSize),
	}
}

// FrozenBlobs reads the visible generation OpenFolder republishes, so it must go
// through a pinned view rather than the raw slice. Meaningful under -race.
func TestFrozenBlobsConcurrentWithOpenFolder(t *testing.T) {
	dirs := datadir.New(t.TempDir())
	sn := NewCaplinSnapshots(ethconfig.BlocksFreezing{ChainName: "mainnet"}, &clparams.MainnetBeaconConfig, dirs, log.New())
	t.Cleanup(sn.Close)

	done := make(chan struct{})
	go func() {
		defer close(done)
		for range 100 {
			sn.FrozenBlobs()
		}
	}()
	var err error
	for range 100 {
		if err = sn.OpenFolder(); err != nil {
			break
		}
	}
	<-done
	require.NoError(t, err)
}

// Close unmaps every dirty segment, so the visible generation has to be dropped
// with it — a View() taken afterwards would hand out closed decompressors.
func TestCloseClearsVisibleSegments(t *testing.T) {
	dirs := datadir.New(t.TempDir())
	writeEmptyBlobSidecarsSegment(t, dirs, 0, snaptype.CaplinMergeLimit)

	sn := NewCaplinSnapshots(ethconfig.BlocksFreezing{ChainName: "mainnet"}, &clparams.MainnetBeaconConfig, dirs, log.New())
	closeOnce := sync.OnceFunc(sn.Close)
	t.Cleanup(closeOnce)
	require.NoError(t, sn.OpenFolder())
	require.Equal(t, uint64(snaptype.CaplinMergeLimit), sn.FrozenBlobs())

	closeOnce()

	view := sn.View()
	defer view.Close()
	require.Empty(t, view.BlobSidecars())
}

// A view pins the generation it read: a segment dropped from the set by a concurrent
// republish must stay mapped and readable until that view closes. Meaningful under -race.
//
// The drop is driven by excluding the segment from OpenList rather than unlinking it:
// Windows refuses to remove a file a reader still has mapped, and OpenList retires
// what is missing from the list as RetireReasonWasDeletedFromDisk, which is the same
// close-only generation path an on-disk removal takes.
func TestViewSurvivesConcurrentSegmentRemoval(t *testing.T) {
	const limit = snaptype.CaplinMergeLimit

	dirs := datadir.New(t.TempDir())
	writeCaplinSegment(t, dirs, snaptype.BeaconBlocks, 0, limit, true, slotWord)
	writeCaplinSegment(t, dirs, snaptype.BeaconBlocks, limit, 2*limit, true, slotWord)

	sn := NewCaplinSnapshots(ethconfig.BlocksFreezing{ChainName: "mainnet"}, &clparams.MainnetBeaconConfig, dirs, log.New())
	t.Cleanup(sn.Close)
	require.NoError(t, sn.OpenFolder())

	view := sn.View()
	defer view.Close()
	tail, ok := view.BeaconBlocksSegment(limit)
	require.True(t, ok)
	dropped := tail.Src().FileName()

	keep := make([]string, 0, 2)
	for _, f := range sn.SegFileNames(0, 2*limit) {
		if !strings.HasPrefix(f, strings.TrimSuffix(dropped, ".seg")) {
			keep = append(keep, f)
		}
	}
	require.NotEmpty(t, keep, "the first segment must survive the republish")

	var eg errgroup.Group
	eg.Go(func() error {
		return sn.OpenList(keep, false)
	})

	for slot := uint64(limit); slot < limit+2_000; slot++ {
		word, err := tail.Get(slot)
		require.NoError(t, err)
		require.Equal(t, slotWord(slot), word)
	}
	require.NoError(t, eg.Wait())

	word, err := tail.Get(limit)
	require.NoError(t, err)
	require.Equal(t, slotWord(limit), word)

	fresh := sn.View()
	defer fresh.Close()
	require.Len(t, fresh.BeaconBlocks(), 1, "the dropped segment must have left the visible set")
}

// A dump or a merge owns its range while it works, and its .seg lands before its index.
// BuildMissingIndices must leave a claimed range to its owner instead of racing it.
func TestBuildMissingIndicesSkipsClaimedRange(t *testing.T) {
	const limit = snaptype.CaplinMergeLimit

	dirs := datadir.New(t.TempDir())
	writeEmptyCaplinSegment(t, dirs, snaptype.BeaconBlocks, 0, limit, false)

	sn := NewCaplinSnapshots(ethconfig.BlocksFreezing{ChainName: "mainnet"}, &clparams.MainnetBeaconConfig, dirs, log.New())
	t.Cleanup(sn.Close)

	// The segment has to be dirty before the claim: openSegments skips a claimed range, so
	// claiming first would keep it out of the walk and never exercise the guard.
	require.NoError(t, sn.OpenFolder())
	require.True(t, sn.TryAcquireRange(snaptype.CaplinEnums.BeaconBlocks, 0, limit))
	require.NoError(t, sn.BuildMissingIndices(t.Context(), log.New()))
	require.Empty(t, caplinIdxFiles(t, dirs))

	sn.ReleaseRange(snaptype.CaplinEnums.BeaconBlocks, 0, limit)
	require.NoError(t, sn.BuildMissingIndices(t.Context(), log.New()))
	require.Len(t, caplinIdxFiles(t, dirs), 1)

	view := sn.View()
	defer view.Close()
	require.Len(t, view.BeaconBlocks(), 1, "a freshly indexed segment must be published")
}

// A corrupt .idx is not a missing one: the strict open rejects it, so the discovery open
// inside BuildMissingIndices has to be optimistic or the node can never repair itself.
func TestBuildMissingIndicesRebuildsCorruptIndex(t *testing.T) {
	const limit = snaptype.CaplinMergeLimit

	dirs := datadir.New(t.TempDir())
	writeEmptyCaplinSegment(t, dirs, snaptype.BeaconBlocks, 0, limit, true)
	idx := caplinIdxFiles(t, dirs)
	require.Len(t, idx, 1)
	require.NoError(t, os.WriteFile(idx[0], []byte("not an index"), 0o600))

	sn := NewCaplinSnapshots(ethconfig.BlocksFreezing{ChainName: "mainnet"}, &clparams.MainnetBeaconConfig, dirs, log.New())
	t.Cleanup(sn.Close)
	require.Error(t, sn.OpenFolder(), "a strict open must not accept a corrupt index")

	require.NoError(t, sn.BuildMissingIndices(t.Context(), log.New()))
	require.NoError(t, sn.OpenFolder())

	view := sn.View()
	defer view.Close()
	require.Len(t, view.BeaconBlocks(), 1, "the reindexed segment must be published")
}

// SegmentsMax is dirty-backed and drives the archive backfill stop condition, so a segment
// the backfill cannot walk back to must never reach the dirty set: caplin's own gap-filtered
// scan is what keeps it out.
func TestOpenFolderDropsSegmentsPastGap(t *testing.T) {
	const limit = snaptype.CaplinMergeLimit

	dirs := datadir.New(t.TempDir())
	writeEmptyCaplinSegment(t, dirs, snaptype.BeaconBlocks, 0, limit, true)
	writeEmptyCaplinSegment(t, dirs, snaptype.BeaconBlocks, 2*limit, 3*limit, true)

	sn := NewCaplinSnapshots(ethconfig.BlocksFreezing{ChainName: "mainnet"}, &clparams.MainnetBeaconConfig, dirs, log.New())
	t.Cleanup(sn.Close)
	require.NoError(t, sn.OpenFolder())

	require.Equal(t, uint64(limit-1), sn.SegmentsMax())

	view := sn.View()
	defer view.Close()
	require.Len(t, view.BeaconBlocks(), 1, "the segment past the gap must not be visible")
}

func caplinIdxFiles(t *testing.T, dirs datadir.Dirs) []string {
	t.Helper()
	idx, err := filepath.Glob(filepath.Join(dirs.Snap, "*beaconblocks*.idx"))
	require.NoError(t, err)
	return idx
}

// The antiquary and the history-download stage both call OpenFolder on the same
// CaplinSnapshots, so publishing must happen while dirtyLock is still held —
// recalcVisibleFiles walks the dirty btree a concurrent OpenList is mutating.
// Meaningful under -race.
func TestOpenListDirtyLockRace(t *testing.T) {
	dirs := datadir.New(t.TempDir())
	writeEmptyBlobSidecarsSegment(t, dirs, 0, snaptype.CaplinMergeLimit)

	sn := NewCaplinSnapshots(ethconfig.BlocksFreezing{ChainName: "mainnet"}, &clparams.MainnetBeaconConfig, dirs, log.New())
	t.Cleanup(sn.Close)
	require.NoError(t, sn.OpenFolder())

	files, _, err := snapshotsync.SegmentsCaplin(dirs.Snap)
	require.NoError(t, err)
	require.NotEmpty(t, files)
	list := make([]string, 0, len(files))
	for i := range files {
		_, fName := filepath.Split(files[i].Path)
		list = append(list, fName)
	}

	// One caller sees the segment, the other does not — the same divergence two
	// concurrent folder scans hit while the antiquary is dumping a new file.
	var eg errgroup.Group
	for _, l := range [][]string{list, nil} {
		eg.Go(func() error {
			for range 50 {
				if err := sn.OpenList(l, true); err != nil {
					return err
				}
			}
			return nil
		})
	}
	require.NoError(t, eg.Wait())
}

// BaseRoSnapshots.BuildMissedIndices is promoted onto CaplinSnapshots and reopens the
// folder after indexing. Go embedding has no virtual dispatch, so that reopen must not
// reach the unfiltered base scan: SegmentsMax drives the archive backfill stop condition
// and would then advance past a gap the backfill cannot walk back to.
func TestPromotedBuildMissedIndicesKeepsGapFilter(t *testing.T) {
	const limit = snaptype.CaplinMergeLimit

	dirs := datadir.New(t.TempDir())
	writeIndexSalt(t, dirs)
	writeEmptyCaplinSegment(t, dirs, snaptype.BeaconBlocks, 0, limit, false)
	writeEmptyCaplinSegment(t, dirs, snaptype.BeaconBlocks, 2*limit, 3*limit, true)

	sn := NewCaplinSnapshots(ethconfig.BlocksFreezing{ChainName: "mainnet", ProduceE2: true}, &clparams.MainnetBeaconConfig, dirs, log.New())
	t.Cleanup(sn.Close)
	require.NoError(t, sn.OpenFolder())
	require.Equal(t, uint64(limit-1), sn.SegmentsMax(), "the segment past the gap must not be dirty")

	require.NoError(t, sn.BuildMissedIndices(t.Context(), "test", nil, dirs, nil, log.New()))

	require.Equal(t, uint64(limit-1), sn.SegmentsMax(), "the reopen after indexing must keep the gap filter")

	view := sn.View()
	defer view.Close()
	require.Len(t, view.BeaconBlocks(), 1, "the segment past the gap must not be visible")
}

// Caplin's own BuildMissingIndices reopens through the gap-filtered scan, so it is a
// correct delegation target for the shadow above.
func TestCaplinBuildMissingIndicesKeepsGapFilter(t *testing.T) {
	const limit = snaptype.CaplinMergeLimit

	dirs := datadir.New(t.TempDir())
	writeIndexSalt(t, dirs)
	writeEmptyCaplinSegment(t, dirs, snaptype.BeaconBlocks, 0, limit, false)
	writeEmptyCaplinSegment(t, dirs, snaptype.BeaconBlocks, 2*limit, 3*limit, true)

	sn := NewCaplinSnapshots(ethconfig.BlocksFreezing{ChainName: "mainnet", ProduceE2: true}, &clparams.MainnetBeaconConfig, dirs, log.New())
	t.Cleanup(sn.Close)
	require.NoError(t, sn.BuildMissingIndices(t.Context(), log.New()))

	require.Equal(t, uint64(limit-1), sn.SegmentsMax())
	require.Len(t, caplinIdxFiles(t, dirs), 2, "both segments on disk must end up indexed")
}

func writeIndexSalt(t *testing.T, dirs datadir.Dirs) {
	t.Helper()
	salt := make([]byte, 4)
	binary.BigEndian.PutUint32(salt, 1)
	require.NoError(t, os.WriteFile(filepath.Join(dirs.Snap, "salt-blocks.txt"), salt, 0o600))
}
