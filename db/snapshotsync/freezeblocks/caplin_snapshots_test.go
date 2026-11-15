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
	"context"
	"encoding/binary"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/klauspost/compress/zstd"
	"github.com/spf13/afero"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/persistence/beacon_indicies"
	"github.com/erigontech/erigon/cl/persistence/blob_storage"
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
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/node/ethconfig"
)

type unavailableExecutionBlockReader struct{}

func (unavailableExecutionBlockReader) Transactions(uint64, common.Hash) (*solid.TransactionsSSZ, error) {
	return nil, snapshot_format.ErrExecutionBodyNotFound
}

func (unavailableExecutionBlockReader) Withdrawals(uint64, common.Hash) (*solid.ListSSZ[*cltypes.Withdrawal], error) {
	panic("Withdrawals should not be called after transactions are unavailable")
}

func (unavailableExecutionBlockReader) SetBeaconChainConfig(*clparams.BeaconChainConfig) {}

func (unavailableExecutionBlockReader) CacheBody(uint64, common.Hash, [][]byte, []*types.Withdrawal) {
}

// TestDumpBeaconBlocksNoPanic is a higher-level regression test: the production
// panic occurred inside DumpBeaconBlocks when chooseSegmentEnd was called with
// nil snCfg. This test uses toSlot=CaplinMergeLimit so the loop body executes
// (doesn't break early), exercising the exact code path that panicked.
func TestDumpBeaconBlocksNoPanic(t *testing.T) {
	db := mdbxtest.NewTestDB(t, dbcfg.ChainDB)
	dirs := datadir.New(t.TempDir())

	// toSlot == CaplinMergeLimit fills one segment, so the loop reaches dumpBeaconBlocksRange.
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
			if err := beacon_indicies.MarkRootCanonical(tx, slot, root); err != nil {
				return err
			}
			if err := beacon_indicies.WriteParentBlockRoot(tx, root, prevRoot); err != nil {
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

// Blocks run ahead of blobs (blobs only start at Deneb).
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

	// visible-backed, inclusive To-1: the unindexed tail is not counted, so the archive
	// backfill walks back over it.
	require.Equal(t, uint64(2*limit-1), sn.SegmentsMax())
	require.Equal(t, uint64(2*limit-1), sn.IndicesMax())
	require.Equal(t, uint64(2*limit-1), sn.BlocksAvailable())
	// visible-backed, exclusive To — cl/beacon/handler/blobs.go compares slot < FrozenBlobs().
	require.Equal(t, uint64(limit), sn.FrozenBlobs())
}

// The archive backfill takes SegmentsMax as the last slot readable from beacon-block
// snapshots, so blob segments past a block gap or past the last block segment must not raise it.
func TestCaplinSegmentsMaxIgnoresBlobSidecars(t *testing.T) {
	const limit = snaptype.CaplinMergeLimit

	for _, tc := range []struct {
		name   string
		blocks [][2]uint64
		blobs  [][2]uint64
	}{
		{
			name:   "blobs past a block gap",
			blocks: [][2]uint64{{0, limit}, {2 * limit, 3 * limit}},
			blobs:  [][2]uint64{{0, limit}, {limit, 2 * limit}, {2 * limit, 3 * limit}},
		},
		{
			name:   "blobs past the last block segment",
			blocks: [][2]uint64{{0, limit}},
			blobs:  [][2]uint64{{0, 2 * limit}},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dirs := datadir.New(t.TempDir())
			for _, r := range tc.blocks {
				writeEmptyCaplinSegment(t, dirs, snaptype.BeaconBlocks, r[0], r[1], true)
			}
			for _, r := range tc.blobs {
				writeEmptyCaplinSegment(t, dirs, snaptype.BlobSidecars, r[0], r[1], true)
			}

			sn := NewCaplinSnapshots(ethconfig.BlocksFreezing{ChainName: "mainnet"}, &clparams.MainnetBeaconConfig, dirs, log.New())
			t.Cleanup(sn.Close)
			require.NoError(t, sn.OpenFolder())

			require.Equal(t, uint64(limit-1), sn.SegmentsMax())
		})
	}
}

// A hole in the beacon-block files below the first indexed one hides every block segment, so
// readers fall back to MDBX and the archive backfill walks down through the hole. Unindexed files
// with no hole below the visible set do not: the index build after the backfill makes them visible.
func TestCaplinSegmentsMaxAnchorsAtLowestSegment(t *testing.T) {
	const limit = snaptype.CaplinMergeLimit

	for _, tc := range []struct {
		name      string
		indexed   [][2]uint64
		unindexed [][2]uint64
		want      uint64
	}{
		{
			name:      "unindexed segment below a gap",
			indexed:   [][2]uint64{{2 * limit, 3 * limit}},
			unindexed: [][2]uint64{{0, limit}},
			want:      0,
		},
		{
			name:      "unindexed segment directly below",
			indexed:   [][2]uint64{{limit, 2 * limit}},
			unindexed: [][2]uint64{{0, limit}},
			want:      2*limit - 1,
		},
		{
			name:      "two unindexed segments directly below",
			indexed:   [][2]uint64{{2 * limit, 3 * limit}},
			unindexed: [][2]uint64{{0, limit}, {limit, 2 * limit}},
			want:      3*limit - 1,
		},
		{
			name:      "unindexed segment covered by an indexed one of the same start",
			indexed:   [][2]uint64{{0, 2 * limit}},
			unindexed: [][2]uint64{{0, limit}},
			want:      2*limit - 1,
		},
		{
			name:    "lowest segment above slot zero",
			indexed: [][2]uint64{{limit, 2 * limit}},
			want:    2*limit - 1,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dirs := datadir.New(t.TempDir())
			for _, r := range tc.indexed {
				writeEmptyCaplinSegment(t, dirs, snaptype.BeaconBlocks, r[0], r[1], true)
			}
			for _, r := range tc.unindexed {
				writeEmptyCaplinSegment(t, dirs, snaptype.BeaconBlocks, r[0], r[1], false)
			}

			sn := NewCaplinSnapshots(ethconfig.BlocksFreezing{ChainName: "mainnet"}, &clparams.MainnetBeaconConfig, dirs, log.New())
			t.Cleanup(sn.Close)
			require.NoError(t, sn.OpenFolder())

			require.Equal(t, tc.want, sn.SegmentsMax())
			require.Equal(t, tc.want, sn.BlocksAvailable())
		})
	}
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

func TestBeaconSnapshotReaderPropagatesUnavailableExecutionBody(t *testing.T) {
	const slot = 42

	db := mdbxtest.NewTestDB(t, dbcfg.ChainDB)
	block := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.BellatrixVersion)
	block.Block.Slot = slot
	block.Block.Body.ExecutionPayload.BlockNumber = 42
	block.Block.Body.ExecutionPayload.BlockHash = common.Hash{0x42}
	root, err := block.Block.HashSSZ()
	require.NoError(t, err)
	require.NoError(t, db.Update(t.Context(), func(tx kv.RwTx) error {
		return beacon_indicies.WriteBeaconBlockAndIndicies(tx, block, true)
	}))

	dirs := datadir.New(t.TempDir())
	snapshots := NewCaplinSnapshots(ethconfig.BlocksFreezing{ChainName: "mainnet"}, &clparams.MainnetBeaconConfig, dirs, log.New())
	t.Cleanup(snapshots.Close)
	require.NoError(t, snapshots.OpenFolder())
	reader := NewBeaconSnapshotReader(snapshots, unavailableExecutionBlockReader{}, &clparams.MainnetBeaconConfig)

	require.NoError(t, db.View(t.Context(), func(tx kv.Tx) error {
		byRoot, err := reader.ReadBlockByRoot(t.Context(), tx, root)
		require.ErrorIs(t, err, snapshot_format.ErrExecutionBodyNotFound)
		require.Nil(t, byRoot)

		bySlot, err := reader.ReadBlockBySlot(t.Context(), tx, slot)
		require.ErrorIs(t, err, snapshot_format.ErrExecutionBodyNotFound)
		require.Nil(t, bySlot)
		return nil
	}))
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

// OpenFolder opens every typed segment, as EL's does: a covered segment and one past a gap
// enter the dirty set, and visibility hides them.
func TestOpenFolderOpensEveryBeaconBlocksSegment(t *testing.T) {
	const limit = snaptype.CaplinMergeLimit

	dirs := datadir.New(t.TempDir())
	writeEmptyCaplinSegment(t, dirs, snaptype.BeaconBlocks, 0, limit, true)
	writeEmptyCaplinSegment(t, dirs, snaptype.BeaconBlocks, 0, 2*limit, true)
	writeEmptyCaplinSegment(t, dirs, snaptype.BeaconBlocks, 3*limit, 4*limit, true)

	sn := NewCaplinSnapshots(ethconfig.BlocksFreezing{ChainName: "mainnet"}, &clparams.MainnetBeaconConfig, dirs, log.New())
	t.Cleanup(sn.Close)
	require.NoError(t, sn.OpenFolder())

	var dirty int
	sn.WalkDirtySegments(snaptype.CaplinEnums.BeaconBlocks, func(*snapshotsync.DirtySegment) bool {
		dirty++
		return true
	})
	require.Equal(t, 3, dirty)
	require.Equal(t, uint64(2*limit-1), sn.SegmentsMax())

	view := sn.View()
	defer view.Close()
	require.Len(t, view.BeaconBlocks(), 1)
}

// Two versions of one range both enter the dirty set, and visibility serves the newer one, as
// for EL.
func TestOpenFolderServesTheNewerVersionOfARange(t *testing.T) {
	const limit = snaptype.CaplinMergeLimit

	dirs := datadir.New(t.TempDir())
	writeEmptyCaplinSegment(t, dirs, snaptype.BeaconBlocks, 0, limit, true)
	older := snaptype.BeaconBlocks.FileName(version.V1_0, 0, limit)
	newer := snaptype.BeaconBlocks.FileName(version.ZeroVersion, 0, limit)
	olderPrefix, newerPrefix := strings.SplitN(older, "-", 2)[0]+"-", strings.SplitN(newer, "-", 2)[0]+"-"
	require.NotEqual(t, olderPrefix, newerPrefix)
	files, err := filepath.Glob(filepath.Join(dirs.Snap, newerPrefix+"*beaconblocks*"))
	require.NoError(t, err)
	require.Len(t, files, 2)
	for _, f := range files {
		data, err := os.ReadFile(f)
		require.NoError(t, err)
		name := olderPrefix + strings.TrimPrefix(filepath.Base(f), newerPrefix)
		require.NoError(t, os.WriteFile(filepath.Join(dirs.Snap, name), data, 0o644))
	}

	sn := NewCaplinSnapshots(ethconfig.BlocksFreezing{ChainName: "mainnet"}, &clparams.MainnetBeaconConfig, dirs, log.New())
	t.Cleanup(sn.Close)
	require.NoError(t, sn.OpenFolder())

	var dirty int
	sn.WalkDirtySegments(snaptype.CaplinEnums.BeaconBlocks, func(*snapshotsync.DirtySegment) bool {
		dirty++
		return true
	})
	require.Equal(t, 2, dirty)

	view := sn.View()
	defer view.Close()
	require.Len(t, view.BeaconBlocks(), 1)
	require.Equal(t, newer, view.BeaconBlocks()[0].Src().FileName())
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

	files, err := snapshotsync.AllTypedSegments(dirs.Snap, sn.Types())
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

func TestIndexMissedConcurrentWithOpenFolder(t *testing.T) {
	dirs := datadir.New(t.TempDir())
	writeEmptyCaplinSegment(t, dirs, snaptype.BeaconBlocks, 0, snaptype.CaplinMergeLimit, false)

	sn := NewCaplinSnapshots(ethconfig.BlocksFreezing{ChainName: "mainnet"}, &clparams.MainnetBeaconConfig, dirs, log.New())
	t.Cleanup(sn.Close)
	files, err := snapshotsync.AllTypedSegments(dirs.Snap, sn.Types())
	require.NoError(t, err)
	list := make([]string, 0, len(files))
	for i := range files {
		_, fName := filepath.Split(files[i].Path)
		list = append(list, fName)
	}
	require.NoError(t, sn.OpenList(list, true))

	var eg errgroup.Group
	eg.Go(func() error {
		return sn.IndexMissed(t.Context(), "caplin", dirs.Tmp, nil, log.New())
	})
	eg.Go(func() error {
		for range 200 {
			_ = sn.OpenFolder()
		}
		return nil
	})
	require.NoError(t, eg.Wait())
}

func TestCaplinBuildMissingIndicesIndexesPastGap(t *testing.T) {
	const limit = snaptype.CaplinMergeLimit

	dirs := datadir.New(t.TempDir())
	writeEmptyCaplinSegment(t, dirs, snaptype.BeaconBlocks, 0, limit, false)
	writeEmptyCaplinSegment(t, dirs, snaptype.BeaconBlocks, 2*limit, 3*limit, false)

	sn := NewCaplinSnapshots(ethconfig.BlocksFreezing{ChainName: "mainnet", ProduceE2: true}, &clparams.MainnetBeaconConfig, dirs, log.New())
	t.Cleanup(sn.Close)
	require.NoError(t, sn.BuildMissingIndices(t.Context(), log.New()))

	require.Len(t, caplinIdxFiles(t, dirs), 2)
	require.Equal(t, uint64(limit-1), sn.SegmentsMax())
}

// Without a count from the blocks, a slot whose count row is missing from the blob store
// reads as blobless and freezes as an empty entry.
func TestDumpBlobSidecarsRangeRequiresBlobCount(t *testing.T) {
	db := mdbxtest.NewTestDB(t, dbcfg.ChainDB)
	dirs := datadir.New(t.TempDir())
	store := blob_storage.NewBlobStore(db, afero.NewMemMapFs())

	err := DumpBlobSidecarsRange(t.Context(), db, store, 0, snaptype.CaplinMergeLimit, 0, dirs, 1, nil, log.LvlDebug, log.New())
	require.ErrorContains(t, err, "blob count")

	written, err := filepath.Glob(filepath.Join(dirs.Snap, "*blobsidecars*"))
	require.NoError(t, err)
	require.Empty(t, written)
}

// A slot outside every frozen block segment reads as no block, the same as an empty slot, so
// counting it as blobless would freeze a hole wherever the blob store is pruned.
func TestBlobCountFromBlocksRejectsSlotsWithoutBlockSegment(t *testing.T) {
	db := mdbxtest.NewTestDB(t, dbcfg.ChainDB)
	dirs := datadir.New(t.TempDir())
	writeCaplinSegment(t, dirs, snaptype.BeaconBlocks, 20_000, 30_000, true, func(uint64) []byte { return nil })
	sn := NewCaplinSnapshots(ethconfig.BlocksFreezing{ChainName: "mainnet"}, &clparams.MainnetBeaconConfig, dirs, log.New())
	t.Cleanup(sn.Close)
	require.NoError(t, sn.OpenFolder())
	require.Equal(t, uint64(30_000), sn.VisibleSegmentsMaxTo(snaptype.BeaconBlocks.Enum()))

	blobCount := BlobCountFromBlocks(t.Context(), sn, NewBeaconSnapshotReader(sn, nil, &clparams.MainnetBeaconConfig))
	require.NoError(t, db.View(t.Context(), func(tx kv.Tx) error {
		n, _, err := blobCount(tx, 25_000)
		require.NoError(t, err, "an empty slot inside a frozen block segment is blobless")
		require.Zero(t, n)
		return nil
	}))

	store := blob_storage.NewBlobStore(db, afero.NewMemMapFs())
	err := DumpBlobSidecarsRange(t.Context(), db, store, 10_000, 20_000, 0, dirs, 1, blobCount, log.LvlDebug, log.New())
	require.ErrorContains(t, err, "no frozen beacon block segment")
}

func TestOpenBeaconBlocksIgnoresDamagedBlobSegments(t *testing.T) {
	dirs := datadir.New(t.TempDir())
	writeCaplinSegment(t, dirs, snaptype.BeaconBlocks, 20_000, 30_000, true, func(uint64) []byte { return nil })
	blobSeg := snaptype.BlobSidecars.FileName(version.ZeroVersion, 20_000, 30_000)
	blobIdx := snaptype.IdxFileName(version.V1_1, 20_000, 30_000, snaptype.CaplinIndexes.BlobSidecarSlot.Name)
	for _, name := range []string{blobSeg, blobIdx} {
		require.NoError(t, os.WriteFile(filepath.Join(dirs.Snap, name), bytes.Repeat([]byte{0xff}, 31), 0o644))
	}

	sn := NewCaplinSnapshots(ethconfig.BlocksFreezing{ChainName: "mainnet"}, &clparams.MainnetBeaconConfig, dirs, log.New())
	t.Cleanup(sn.Close)
	require.NoError(t, sn.OpenBeaconBlocks())
	require.Equal(t, uint64(30_000), sn.VisibleSegmentsMaxTo(snaptype.BeaconBlocks.Enum()))
	require.Zero(t, sn.VisibleSegmentsMaxTo(snaptype.BlobSidecars.Enum()))
}

func TestRemoveBlobIndexAndTorrents(t *testing.T) {
	dirs := datadir.New(filepath.Join(t.TempDir(), "dd[1]"))
	idx := func(v version.Version, from, to uint64) string {
		return snaptype.IdxFileName(v, from, to, snaptype.CaplinIndexes.BlobSidecarSlot.Name)
	}
	seg := snaptype.BlobSidecars.FileName(version.V1_1, 30_000, 40_000)
	removed := []string{seg + ".torrent", idx(version.V1_1, 30_000, 40_000), idx(version.V1_1, 30_000, 40_000) + ".torrent", idx(version.V1_0, 30_000, 40_000)}
	kept := []string{
		seg,
		snaptype.BlobSidecars.FileName(version.V1_1, 40_000, 50_000) + ".torrent",
		idx(version.V1_1, 40_000, 50_000),
		snaptype.BeaconBlocks.FileName(version.V1_1, 30_000, 40_000) + ".torrent",
	}
	for _, name := range append(removed, kept...) {
		require.NoError(t, os.WriteFile(filepath.Join(dirs.Snap, name), []byte{1}, 0o644))
	}

	require.NoError(t, removeBlobIndexAndTorrents(dirs.Snap, 30_000, 40_000))

	entries, err := os.ReadDir(dirs.Snap)
	require.NoError(t, err)
	var left []string
	for _, e := range entries {
		if !e.IsDir() {
			left = append(left, e.Name())
		}
	}
	require.ElementsMatch(t, kept, left)
}

func TestRetireBlobsBelowDropsSegmentsUnderTheFloor(t *testing.T) {
	const limit = snaptype.CaplinMergeLimit
	for _, tc := range []struct {
		name        string
		floor       uint64
		wantRetired int
		wantFirst   uint64
	}{
		{name: "zero floor keeps everything", floor: 0, wantRetired: 0, wantFirst: 0},
		{name: "floor inside the first segment keeps it", floor: limit - 1, wantRetired: 0, wantFirst: 0},
		{name: "floor at a segment end retires it", floor: limit, wantRetired: 1, wantFirst: limit},
		{name: "floor inside the second segment", floor: limit + 1, wantRetired: 1, wantFirst: limit},
		{name: "floor past the tip keeps the newest segment", floor: 10 * limit, wantRetired: 2, wantFirst: 2 * limit},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dirs := datadir.New(t.TempDir())
			for i := range uint64(3) {
				writeEmptyBlobSidecarsSegment(t, dirs, i*limit, (i+1)*limit)
			}
			sn := NewCaplinSnapshots(ethconfig.BlocksFreezing{ChainName: "mainnet"}, &clparams.MainnetBeaconConfig, dirs, log.New())
			t.Cleanup(sn.Close)
			require.NoError(t, sn.OpenFolder())

			var deleted []string
			_, err := sn.RetireBlobsBelow(tc.floor, func(paths []string) error {
				deleted = append(deleted, paths...)
				return nil
			})
			require.NoError(t, err)

			view := sn.View()
			defer view.Close()
			segs := view.BlobSidecars()
			require.Len(t, segs, 3-tc.wantRetired)
			require.Equal(t, tc.wantFirst, segs[0].From())
			require.Equal(t, uint64(3*limit), sn.FrozenBlobs())
			if tc.wantRetired == 0 {
				require.Empty(t, deleted)
			} else {
				require.NotEmpty(t, deleted)
				for _, path := range deleted {
					require.False(t, strings.Contains(filepath.Base(path), "beaconblocks"))
				}
			}
		})
	}
}

func TestRetireBlobsBelowWaitsForOverlapCleanup(t *testing.T) {
	const limit = snaptype.CaplinMergeLimit
	dirs := datadir.New(t.TempDir())
	writeEmptyBlobSidecarsSegment(t, dirs, 0, limit)
	writeEmptyBlobSidecarsSegment(t, dirs, limit, 2*limit)
	writeEmptyBlobSidecarsSegment(t, dirs, 0, 2*limit)
	writeEmptyBlobSidecarsSegment(t, dirs, 2*limit, 3*limit)
	sn := NewCaplinSnapshots(ethconfig.BlocksFreezing{ChainName: "mainnet"}, &clparams.MainnetBeaconConfig, dirs, log.New())
	t.Cleanup(sn.Close)
	require.NoError(t, sn.OpenFolder())

	retired, err := sn.RetireBlobsBelow(10*limit, func([]string) error {
		t.Fatal("no file may be handed to the seeder while superseded segments remain")
		return nil
	})
	require.NoError(t, err)
	require.False(t, retired)
	require.Equal(t, uint64(3*limit), sn.FrozenBlobs())
}

func TestBlobCountFromBlocksFollowsTheChildsPayloadStatus(t *testing.T) {
	const slot = 7
	const unfrozen = snaptype.CaplinMergeLimit + 1
	epochEnd := func(slot uint64) uint64 { return slot/32*32 + 31 }
	for _, tc := range []struct {
		name      string
		onChain   bool
		stored    bool
		childSlot uint64
		childRoot common.Hash
		finalized uint64
		parent    common.Hash
		expected  uint64
		err       string
	}{
		{"payload skipped", false, false, slot + 3, common.Hash{}, 0, common.Hash{}, 0, ""},
		{"payload skipped, its blobs stored", false, true, slot + 3, common.Hash{}, 0, common.Hash{}, 0, ""},
		{"payload on chain", true, false, slot + 3, common.Hash{}, 0, common.Hash{}, 2, "blob storage count mismatch at slot 7"},
		{"payload skipped by a block that is not canonical", false, true, slot + 3, common.Hash{0xee}, 0, common.Hash{}, 0, "block at slot 10 is not canonical"},
		{"payload skipped by a finalized child not frozen yet", false, true, unfrozen, common.Hash{}, epochEnd(unfrozen + 32), common.Hash{}, 0, ""},
		{"payload skipped by a child after the finalized checkpoint", false, true, unfrozen, common.Hash{}, epochEnd(unfrozen), common.Hash{}, 2, ""},
		{"next block builds on another parent", false, true, slot + 3, common.Hash{}, 0, common.Hash{0xaa}, 2, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := clparams.MainnetBeaconConfig
			cfg.GloasForkEpoch = 0
			block := cltypes.NewSignedBeaconBlock(&cfg, clparams.GloasVersion)
			block.Block.Slot = slot
			bid := block.Block.Body.GetSignedExecutionPayloadBid().Message
			bid.BlockHash = common.Hash{0x01}
			for range 2 {
				block.GetBlobKzgCommitments().Append(&cltypes.KZGCommitment{})
			}
			root, err := block.Block.HashSSZ()
			require.NoError(t, err)
			child := cltypes.NewSignedBeaconBlock(&cfg, clparams.GloasVersion)
			child.Block.Slot = tc.childSlot
			child.Block.ParentRoot = root
			if tc.parent != (common.Hash{}) {
				child.Block.ParentRoot = tc.parent
			}
			child.Block.Body.GetSignedExecutionPayloadBid().Message.ParentBlockHash = common.Hash{0x02}
			if tc.onChain {
				child.Block.Body.GetSignedExecutionPayloadBid().Message.ParentBlockHash = bid.BlockHash
			}

			dirs := datadir.New(t.TempDir())
			writeCaplinSegment(t, dirs, snaptype.BeaconBlocks, 0, snaptype.CaplinMergeLimit, true, func(s uint64) []byte {
				switch s {
				case slot:
					return blockSnapshotWord(t, block)
				case tc.childSlot:
					return blockSnapshotWord(t, child)
				}
				return nil
			})
			sn := NewCaplinSnapshots(ethconfig.BlocksFreezing{ChainName: "mainnet"}, &cfg, dirs, log.New())
			t.Cleanup(sn.Close)
			require.NoError(t, sn.OpenFolder())

			db := mdbxtest.NewTestDB(t, dbcfg.ChainDB)
			childRoot, err := child.Block.HashSSZ()
			require.NoError(t, err)
			if tc.childRoot != (common.Hash{}) {
				childRoot = tc.childRoot
			}
			require.NoError(t, db.Update(t.Context(), func(tx kv.RwTx) error {
				require.NoError(t, beacon_indicies.MarkRootCanonical(tx, slot, root))
				require.NoError(t, beacon_indicies.WriteHighestFinalized(tx, tc.finalized))
				if tc.childSlot < snaptype.CaplinMergeLimit {
					return beacon_indicies.MarkRootCanonical(tx, tc.childSlot, childRoot)
				}
				return beacon_indicies.WriteBeaconBlockAndIndicies(tx, child, true)
			}))

			blobCount := BlobCountFromBlocks(t.Context(), sn, NewBeaconSnapshotReader(sn, nil, &cfg))
			var n uint64
			var skipped bool
			err = db.View(t.Context(), func(tx kv.Tx) (err error) {
				n, skipped, err = blobCount(tx, slot)
				return err
			})
			if tc.childRoot != (common.Hash{}) {
				require.ErrorContains(t, err, tc.err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.expected, n)
			require.Equal(t, tc.expected == 0, skipped)

			store := blob_storage.NewBlobStore(db, afero.NewMemMapFs())
			if tc.stored {
				require.NoError(t, store.WriteBlobSidecars(t.Context(), root, []*cltypes.BlobSidecar{blobSidecar(slot, 0), blobSidecar(slot, 1)}))
			}
			err = DumpBlobSidecarsRange(t.Context(), db, store, 0, snaptype.CaplinMergeLimit, 0, dirs, 1, blobCount, log.LvlDebug, log.New())
			if tc.err != "" {
				require.ErrorContains(t, err, tc.err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.expected > 0, len(dumpedBlobWord(t, dirs, slot)) > 0)
		})
	}
}

func dumpedBlobWord(t *testing.T, dirs datadir.Dirs, slot uint64) []byte {
	t.Helper()
	f, _, ok := snaptype.ParseFileName(dirs.Snap, snaptype.BlobSidecars.FileName(version.ZeroVersion, 0, snaptype.CaplinMergeLimit))
	require.True(t, ok)
	d, err := seg.NewDecompressor(f.Path)
	require.NoError(t, err)
	defer d.Close()
	g := d.MakeGetter()
	for range slot {
		g.Skip()
	}
	word, _ := g.Next(nil)
	return word
}

func TestDumpBlobSidecarsRangeStopsWhenTheFrozenWordIsNotTheCanonicalBlock(t *testing.T) {
	const slot = 7
	cfg := clparams.MainnetBeaconConfig
	cfg.GloasForkEpoch = 0
	block := cltypes.NewSignedBeaconBlock(&cfg, clparams.GloasVersion)
	block.Block.Slot = slot
	block.Block.Body.GetSignedExecutionPayloadBid().Message.BlockHash = common.Hash{0x01}
	for range 2 {
		block.GetBlobKzgCommitments().Append(&cltypes.KZGCommitment{})
	}
	root, err := block.Block.HashSSZ()
	require.NoError(t, err)
	child := cltypes.NewSignedBeaconBlock(&cfg, clparams.GloasVersion)
	child.Block.Slot = slot + 1
	child.Block.ParentRoot = root
	child.Block.Body.GetSignedExecutionPayloadBid().Message.ParentBlockHash = common.Hash{0x02}
	childRoot, err := child.Block.HashSSZ()
	require.NoError(t, err)
	other := cltypes.NewSignedBeaconBlock(&cfg, clparams.DenebVersion)
	other.Block.Slot = slot - 1
	otherRoot, err := other.Block.HashSSZ()
	require.NoError(t, err)
	gloasOther := cltypes.NewSignedBeaconBlock(&cfg, clparams.GloasVersion)
	gloasOther.Block.Slot = slot - 1
	for range 2 {
		gloasOther.GetBlobKzgCommitments().Append(&cltypes.KZGCommitment{})
	}
	gloasOtherRoot, err := gloasOther.Block.HashSSZ()
	require.NoError(t, err)
	blobless := cltypes.NewSignedBeaconBlock(&cfg, clparams.DenebVersion)
	blobless.Block.Slot = slot
	blobless.Block.Body.ExecutionPayload.Transactions = solid.NewTransactionsSSZFromTransactions([][]byte{{0x01}})
	deneb := cltypes.NewSignedBeaconBlock(&cfg, clparams.DenebVersion)
	deneb.Block.Slot = slot
	deneb.Block.Body.ExecutionPayload.Transactions = solid.NewTransactionsSSZFromTransactions([][]byte{{0x01}})
	for range 2 {
		deneb.GetBlobKzgCommitments().Append(&cltypes.KZGCommitment{})
	}
	denebRoot, err := deneb.Block.HashSSZ()
	require.NoError(t, err)
	denebBodyRoot, err := deneb.Block.Body.HashSSZ()
	require.NoError(t, err)
	for _, tc := range []struct {
		name      string
		word      []byte
		canonical common.Hash
		err       string
	}{
		{"canonical block missing from the beacon segment", nil, root, "blob storage count mismatch at slot 7: 0 != 2"},
		{"frozen block not canonical at its slot", blockSnapshotWord(t, block), common.Hash{0xee}, "block at slot 7 is not canonical"},
		{"frozen block of another slot", blockSnapshotWord(t, other), otherRoot, "blob storage count mismatch at slot 7: 0 != 2"},
		{"frozen Gloas block of another slot, canonical by root", blockSnapshotWord(t, gloasOther), gloasOtherRoot, "block at slot 7 is not canonical"},
		{"frozen blobless Deneb block not canonical at its slot", blockSnapshotWord(t, blobless), common.Hash{0xee}, "blob storage count mismatch at slot 7: 0 != 2"},
		{"frozen Deneb word whose body lost its commitments", withBodyRoot(t, blockSnapshotWord(t, blobless), denebBodyRoot), denebRoot, "blob storage count mismatch at slot 7: 0 != 2"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dirs := datadir.New(t.TempDir())
			writeCaplinSegment(t, dirs, snaptype.BeaconBlocks, 0, snaptype.CaplinMergeLimit, true, func(s uint64) []byte {
				switch {
				case s == slot:
					return tc.word
				case s == slot+1:
					return blockSnapshotWord(t, child)
				}
				return nil
			})
			sn := NewCaplinSnapshots(ethconfig.BlocksFreezing{ChainName: "mainnet"}, &cfg, dirs, log.New())
			t.Cleanup(sn.Close)
			require.NoError(t, sn.OpenFolder())

			db := mdbxtest.NewTestDB(t, dbcfg.ChainDB)
			require.NoError(t, db.Update(t.Context(), func(tx kv.RwTx) error {
				require.NoError(t, beacon_indicies.MarkRootCanonical(tx, slot, tc.canonical))
				return beacon_indicies.MarkRootCanonical(tx, slot+1, childRoot)
			}))
			store := blob_storage.NewBlobStore(db, afero.NewMemMapFs())
			require.NoError(t, store.WriteBlobSidecars(t.Context(), tc.canonical, []*cltypes.BlobSidecar{blobSidecar(slot, 0), blobSidecar(slot, 1)}))

			blobCount := BlobCountFromBlocks(t.Context(), sn, NewBeaconSnapshotReader(sn, nil, &cfg))
			err := DumpBlobSidecarsRange(t.Context(), db, store, 0, snaptype.CaplinMergeLimit, 0, dirs, 1, blobCount, log.LvlDebug, log.New())
			require.ErrorContains(t, err, tc.err)
		})
	}
}

func withBodyRoot(t *testing.T, word []byte, bodyRoot common.Hash) []byte {
	t.Helper()
	dec, err := zstd.NewReader(nil)
	require.NoError(t, err)
	raw, err := dec.DecodeAll(word, nil)
	dec.Close()
	require.NoError(t, err)
	copy(raw[1:1+len(bodyRoot)], bodyRoot[:])
	var b bytes.Buffer
	enc, err := zstd.NewWriter(&b)
	require.NoError(t, err)
	_, err = enc.Write(raw)
	require.NoError(t, err)
	require.NoError(t, enc.Close())
	return b.Bytes()
}

func TestBlobCountFromBlocksStopsOnCancel(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	dirs := datadir.New(t.TempDir())
	writeCaplinSegment(t, dirs, snaptype.BeaconBlocks, 0, snaptype.CaplinMergeLimit, true, func(uint64) []byte { return nil })
	sn := NewCaplinSnapshots(ethconfig.BlocksFreezing{ChainName: "mainnet"}, &cfg, dirs, log.New())
	t.Cleanup(sn.Close)
	require.NoError(t, sn.OpenFolder())
	db := mdbxtest.NewTestDB(t, dbcfg.ChainDB)

	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	blobCount := BlobCountFromBlocks(ctx, sn, NewBeaconSnapshotReader(sn, nil, &cfg))
	err := db.View(t.Context(), func(tx kv.Tx) error {
		_, _, err := blobCount(tx, 7)
		return err
	})
	require.ErrorIs(t, err, context.Canceled)
}

func TestBlobCountFromBlocksDumpsACanonicalDenebBlockWithTransactions(t *testing.T) {
	const slot = 7
	cfg := clparams.MainnetBeaconConfig
	block := cltypes.NewSignedBeaconBlock(&cfg, clparams.DenebVersion)
	block.Block.Slot = slot
	block.Block.Body.ExecutionPayload.Transactions = solid.NewTransactionsSSZFromTransactions([][]byte{{0x01, 0x02}})
	for range 2 {
		block.GetBlobKzgCommitments().Append(&cltypes.KZGCommitment{})
	}
	root, err := block.Block.HashSSZ()
	require.NoError(t, err)
	blobless := cltypes.NewSignedBeaconBlock(&cfg, clparams.DenebVersion)
	blobless.Block.Slot = slot + 1
	blobless.Block.Body.ExecutionPayload.Transactions = solid.NewTransactionsSSZFromTransactions([][]byte{{0x03}})
	blobLessRoot, err := blobless.Block.HashSSZ()
	require.NoError(t, err)

	dirs := datadir.New(t.TempDir())
	writeCaplinSegment(t, dirs, snaptype.BeaconBlocks, 0, snaptype.CaplinMergeLimit, true, func(s uint64) []byte {
		switch s {
		case slot:
			return blockSnapshotWord(t, block)
		case slot + 1:
			return blockSnapshotWord(t, blobless)
		}
		return nil
	})
	sn := NewCaplinSnapshots(ethconfig.BlocksFreezing{ChainName: "mainnet"}, &cfg, dirs, log.New())
	t.Cleanup(sn.Close)
	require.NoError(t, sn.OpenFolder())

	db := mdbxtest.NewTestDB(t, dbcfg.ChainDB)
	require.NoError(t, db.Update(t.Context(), func(tx kv.RwTx) error {
		require.NoError(t, beacon_indicies.MarkRootCanonical(tx, slot+1, blobLessRoot))
		return beacon_indicies.MarkRootCanonical(tx, slot, root)
	}))
	store := blob_storage.NewBlobStore(db, afero.NewMemMapFs())
	require.NoError(t, store.WriteBlobSidecars(t.Context(), root, []*cltypes.BlobSidecar{blobSidecar(slot, 0), blobSidecar(slot, 1)}))

	blobCount := BlobCountFromBlocks(t.Context(), sn, NewBeaconSnapshotReader(sn, nil, &cfg))
	require.NoError(t, DumpBlobSidecarsRange(t.Context(), db, store, 0, snaptype.CaplinMergeLimit, 0, dirs, 1, blobCount, log.LvlDebug, log.New()))
	require.NotEmpty(t, dumpedBlobWord(t, dirs, slot))
	require.Empty(t, dumpedBlobWord(t, dirs, slot+1))
}

func TestBlobCountFromBlocksRejectsAPreGloasChildOfAGloasBlock(t *testing.T) {
	const slot = 7
	cfg := clparams.MainnetBeaconConfig
	cfg.GloasForkEpoch = 0
	block := cltypes.NewSignedBeaconBlock(&cfg, clparams.GloasVersion)
	block.Block.Slot = slot
	for range 2 {
		block.GetBlobKzgCommitments().Append(&cltypes.KZGCommitment{})
	}
	root, err := block.Block.HashSSZ()
	require.NoError(t, err)
	child := cltypes.NewSignedBeaconBlock(&cfg, clparams.DenebVersion)
	child.Block.Slot = slot + 1
	child.Block.ParentRoot = root

	dirs := datadir.New(t.TempDir())
	writeCaplinSegment(t, dirs, snaptype.BeaconBlocks, 0, snaptype.CaplinMergeLimit, true, func(s uint64) []byte {
		switch s {
		case slot:
			return blockSnapshotWord(t, block)
		case slot + 1:
			return blockSnapshotWord(t, child)
		}
		return nil
	})
	sn := NewCaplinSnapshots(ethconfig.BlocksFreezing{ChainName: "mainnet"}, &cfg, dirs, log.New())
	t.Cleanup(sn.Close)
	require.NoError(t, sn.OpenFolder())
	db := mdbxtest.NewTestDB(t, dbcfg.ChainDB)
	require.NoError(t, db.Update(t.Context(), func(tx kv.RwTx) error {
		return beacon_indicies.MarkRootCanonical(tx, slot, root)
	}))

	blobCount := BlobCountFromBlocks(t.Context(), sn, NewBeaconSnapshotReader(sn, nil, &cfg))
	require.NotPanics(t, func() {
		err = db.View(t.Context(), func(tx kv.Tx) error {
			_, _, countErr := blobCount(tx, slot)
			return countErr
		})
	})
	require.ErrorContains(t, err, "block at slot 8: pre-Gloas block after a Gloas block")
}
