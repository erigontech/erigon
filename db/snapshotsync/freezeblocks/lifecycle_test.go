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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/common/background"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/mdbx/mdbxtest"
	"github.com/erigontech/erigon/db/snapshotsync"
	"github.com/erigontech/erigon/db/snapshotsync/blocksnapshots"
	"github.com/erigontech/erigon/db/snaptype"
	snaptype2 "github.com/erigontech/erigon/db/snaptype2"
	"github.com/erigontech/erigon/db/version"
	chainspec "github.com/erigontech/erigon/execution/chain/spec"
	"github.com/erigontech/erigon/node/ethconfig"
)

type nopSeeder struct{}

func (nopSeeder) Seed(context.Context, []string) error   { return nil }
func (nopSeeder) Delete(context.Context, []string) error { return nil }

// #26992 round 2: MergeBlocks reports a skipped range as merged, so BuildFiles loops. Main: fails (nil segment crash, #26988).
func TestMergeBlocksReportsNoMergeForASkippedRange(t *testing.T) {
	logger := log.New()
	dirs := datadir.New(t.TempDir())
	dir := dirs.Snap
	for _, typ := range []snaptype.Type{snaptype2.Headers, snaptype2.Bodies} {
		createTestSegmentFile(t, 0, 200_000, typ.Enum(), dir, version.V1_0, logger)
	}
	createTestSegmentFile(t, 0, 100_000, snaptype2.Transactions.Enum(), dir, version.V1_0, logger)
	createTestSegmentFile(t, 100_000, 200_000, snaptype2.Transactions.Enum(), dir, snaptype2.Transactions.Versions().Current, logger)
	for from := uint64(100_000); from < 200_000; from += 10_000 {
		createTestSegmentFile(t, from, from+10_000, snaptype2.Transactions.Enum(), dir, version.V1_0, logger)
	}
	cfg := ethconfig.Defaults
	cfg.Snapshot.ChainName = chainspec.Mainnet.Config.ChainName
	snapshots := blocksnapshots.NewRoSnapshots(cfg.Snapshot, dir, logger)
	defer snapshots.Close()
	require.NoError(t, snapshots.OpenFolder())

	db := mdbxtest.NewTestDB(t, dbcfg.ChainDB)
	br := NewBlockRetire(t.Context(), 1, dirs, NewBlockReader(snapshots), nil, db, chainspec.Mainnet.Config, &cfg, nil, nil, logger)
	defer br.Close()

	merged, err := br.MergeBlocks(t.Context(), log.LvlInfo, nopSeeder{})
	require.NoError(t, err)
	merged2, err := br.MergeBlocks(t.Context(), log.LvlInfo, nopSeeder{})
	require.NoError(t, err)
	if merged && merged2 {
		t.Error("MergeBlocks reports merged=true on every call while nothing is merged")
	}

	// BuildFiles loops while buildFiles returns true; it re-proposes the same skipped range each pass.
	for range 3 {
		again, err := br.buildFiles(t.Context(), br.blockReader.FrozenBlocks(), 0, log.LvlInfo, nopSeeder{})
		require.NoError(t, err)
		require.False(t, again, "buildFiles reports work done for a range Merge skipped, so BuildFiles never exits")
	}
}

// Copilot on #26992: a blob prune around a segment whose index is being built moves FrozenBlobs back. Main: passes.
func TestRetireBlobsBelowWaitsForAnIndexBuild(t *testing.T) {
	const limit = snaptype.CaplinMergeLimit
	dirs := datadir.New(t.TempDir())
	writeEmptyCaplinSegment(t, dirs, snaptype.BlobSidecars, 0, limit, false)
	writeEmptyBlobSidecarsSegment(t, dirs, 0, 2*limit)
	writeEmptyBlobSidecarsSegment(t, dirs, 2*limit, 3*limit)
	sn := NewCaplinSnapshots(ethconfig.BlocksFreezing{ChainName: "mainnet"}, &clparams.MainnetBeaconConfig, dirs, log.New())
	t.Cleanup(sn.Close)
	require.NoError(t, sn.OpenFolder())
	require.Equal(t, uint64(3*limit), sn.FrozenBlobs())
	require.True(t, sn.TryAcquireRange(snaptype.BlobSidecars.Enum(), 0, limit)) // what claimIndexJobs does

	_, err := sn.RetireBlobsBelow(10*limit, func([]string) error { return nil })
	require.NoError(t, err)

	f, _, ok := snaptype.ParseFileName(dirs.Snap, snaptype.BlobSidecars.FileName(version.ZeroVersion, 0, limit))
	require.True(t, ok)
	require.NoError(t, snapshotsync.BeaconSimpleIdx(t.Context(), f, 1, dirs.Tmp, &background.Progress{}, log.LvlDebug, log.New()))
	sn.ReleaseRange(snaptype.BlobSidecars.Enum(), 0, limit)
	require.NoError(t, sn.OpenFolder())
	require.Equal(t, uint64(3*limit), sn.FrozenBlobs())
}
