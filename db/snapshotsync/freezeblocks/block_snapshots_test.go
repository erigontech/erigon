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
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/memdb"
	"github.com/erigontech/erigon/db/snapshotsync"
	"github.com/erigontech/erigon/db/snapshotsync/blocksnapshots"
	"github.com/erigontech/erigon/db/snaptype"
	snaptype2 "github.com/erigontech/erigon/db/snaptype2"
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
