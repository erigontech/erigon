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
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/memdb"
	"github.com/erigontech/erigon/db/kv/temporal"
	"github.com/erigontech/erigon/db/snapshotsync"
	"github.com/erigontech/erigon/db/snapshotsync/blocksnapshots"
	"github.com/erigontech/erigon/db/snaptype"
	snaptype2 "github.com/erigontech/erigon/db/snaptype2"
	"github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/db/version"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/chain/networkname"
	"github.com/erigontech/erigon/node/ethconfig"
)

// TestRemoveBlockSnapshotsBelow pins the minimal-mode behavior: aged, fully
// merged transaction files below the retention floor are unlinked, while headers
// and bodies (kept so FillDBFromSnapshots computes head TD from genesis) and any
// segment at/above the floor are kept.
func TestRemoveBlockSnapshotsBelow(t *testing.T) {
	logger := log.New()
	dir := t.TempDir()
	db := memdb.NewTestDB(t, dbcfg.ChainDB)
	cfg := ethconfig.Defaults.Snapshot
	cfg.ChainName = networkname.Mainnet
	snapshots := blocksnapshots.NewRoSnapshots(cfg, dir, logger)
	defer snapshots.Close()

	ver := version.V1_0
	const mergeLimit uint64 = snaptype.Erigon2MergeLimit
	for _, typ := range snaptype2.BlockSnapshotTypes {
		createTestSegmentFile(t, 0, mergeLimit, typ.Enum(), dir, ver, logger)
		createTestSegmentFile(t, mergeLimit, 2*mergeLimit, typ.Enum(), dir, ver, logger)
	}
	require.NoError(t, snapshots.OpenFolder())
	require.Equal(t, 2*mergeLimit-1, snapshots.SegmentsMax())

	blockReader := NewBlockReader(snapshots, nil)
	br := &BlockRetire{db: db, blockReader: blockReader, logger: logger}

	var seederDeleted []string
	onDelete := func(l []string) error {
		seederDeleted = append(seederDeleted, l...)
		return nil
	}

	deleted, err := br.RemoveBlockSnapshotsBelow(t.Context(), mergeLimit, onDelete)
	require.NoError(t, err)
	require.True(t, deleted)

	// Only the below-floor transactions segment is removed.
	txBelow := filepath.Join(dir, snaptype.SegmentFileName(ver, 0, mergeLimit, snaptype2.Transactions.Enum()))
	require.NoFileExists(t, txBelow)
	require.Equal(t, []string{filepath.Base(txBelow)}, seederDeleted)

	// Headers/bodies below the floor are kept; every type at/above the floor stays.
	for _, typ := range []snaptype.Type{snaptype2.Headers, snaptype2.Bodies} {
		below := filepath.Join(dir, snaptype.SegmentFileName(ver, 0, mergeLimit, typ.Enum()))
		require.FileExistsf(t, below, "%s below floor must be kept", typ.Enum())
	}
	for _, typ := range snaptype2.BlockSnapshotTypes {
		above := filepath.Join(dir, snaptype.SegmentFileName(ver, mergeLimit, 2*mergeLimit, typ.Enum()))
		require.FileExistsf(t, above, "%s at/above floor must remain", typ.Enum())
	}
	require.Equal(t, 2*mergeLimit-1, snapshots.SegmentsMax())
}

const testMergeLimit = snaptype.Erigon2MergeLimit

// newBlocksTemporalDB builds a temporal.DB carrying real block snapshots (two
// merged segments per type at [0,mergeLimit) and [mergeLimit,2*mergeLimit)) plus
// the BlockRetire that removes them.
func newBlocksTemporalDB(t *testing.T) (context.Context, datadir.Dirs, *blocksnapshots.RoSnapshots, *temporal.DB, *BlockRetire) {
	t.Helper()
	logger := log.New()
	dirs := datadir.New(t.TempDir())
	ctx := t.Context()

	ver := version.V1_0
	for _, typ := range snaptype2.BlockSnapshotTypes {
		createTestSegmentFile(t, 0, testMergeLimit, typ.Enum(), dirs.Snap, ver, logger)
		createTestSegmentFile(t, testMergeLimit, 2*testMergeLimit, typ.Enum(), dirs.Snap, ver, logger)
	}

	cfg := ethconfig.Defaults.Snapshot
	cfg.ChainName = networkname.Mainnet
	snapshots := blocksnapshots.NewRoSnapshots(cfg, dirs.Snap, logger)
	t.Cleanup(snapshots.Close)
	require.NoError(t, snapshots.OpenFolder())

	rawDB := memdb.NewTestDB(t, dbcfg.ChainDB)
	agg := state.NewTest(dirs).MustOpen(ctx, rawDB)
	require.NoError(t, agg.OpenFolder())
	t.Cleanup(agg.Close)
	tdb, err := temporal.New(rawDB, agg, snapshots)
	require.NoError(t, err)

	br := &BlockRetire{db: rawDB, blockReader: NewBlockReader(snapshots, nil), logger: logger}
	return ctx, dirs, snapshots, tdb, br
}

// TestTemporalTxDefersBlockFileUnlink proves step 2: a block file retired while a
// temporal tx is open is not physically unlinked until that tx closes, because
// the tx pins the block-files view (blocktx) for its whole lifetime — the same
// reclamation watermark that already governs state files.
func TestTemporalTxDefersBlockFileUnlink(t *testing.T) {
	ctx, dirs, _, tdb, br := newBlocksTemporalDB(t)

	txBelow := filepath.Join(dirs.Snap, snaptype.SegmentFileName(version.V1_0, 0, testMergeLimit, snaptype2.Transactions.Enum()))
	require.FileExists(t, txBelow)

	// Open a temporal tx: it pins the block-files view for its lifetime.
	tx, err := tdb.BeginTemporalRo(ctx)
	require.NoError(t, err)
	defer tx.Rollback()

	deleted, err := br.RemoveBlockSnapshotsBelow(ctx, testMergeLimit, nil)
	require.NoError(t, err)
	require.True(t, deleted)

	// The open tx pins the outgoing generation, so the retired file survives.
	require.FileExists(t, txBelow, "block file must survive while a temporal tx pins the view")

	// Closing the tx drains the generation; reclamation then unlinks the file.
	tx.Rollback()
	require.NoFileExists(t, txBelow, "block file must be unlinked once the temporal tx closes")
}

// TestBlockReaderReadsThroughTemporalTxView proves step 3: reads go through the
// tx-pinned view. A segment retired while the tx is open vanishes from the live
// snapshot set, yet the still-open tx keeps resolving it through its pinned view.
func TestBlockReaderReadsThroughTemporalTxView(t *testing.T) {
	ctx, _, snapshots, tdb, br := newBlocksTemporalDB(t)
	blockReader := br.blockReader.(*BlockReader)

	const blockInRemovedSegment = testMergeLimit / 2 // inside [0, mergeLimit)

	tx, err := tdb.BeginTemporalRo(ctx)
	require.NoError(t, err)
	defer tx.Rollback()

	_, ok, release := blockReader.viewSingleFile(tx, snaptype2.Transactions, blockInRemovedSegment)
	release()
	require.True(t, ok, "segment must be resolvable through the tx before removal")

	deleted, err := br.RemoveBlockSnapshotsBelow(ctx, testMergeLimit, nil)
	require.NoError(t, err)
	require.True(t, deleted)

	// The live set no longer has the retired segment...
	_, okLive, releaseLive := snapshots.BaseRoSnapshots.ViewSingleFile(snaptype2.Transactions, blockInRemovedSegment)
	releaseLive()
	require.False(t, okLive, "retired segment must be gone from the live snapshot set")

	// ...but the still-open tx keeps reading it through its pinned view.
	_, okTx, releaseTx := blockReader.viewSingleFile(tx, snaptype2.Transactions, blockInRemovedSegment)
	releaseTx()
	require.True(t, okTx, "temporal tx must keep resolving the retired segment via its pinned view")
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
