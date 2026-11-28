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

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/mdbx/mdbxtest"
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

// A merge range that is skipped is not reported as merged, so BuildFiles stops.
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
