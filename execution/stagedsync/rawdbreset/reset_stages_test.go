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

package rawdbreset_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/temporal/temporaltest"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/db/snapshotsync/freezeblocks"
	"github.com/erigontech/erigon/execution/chain/networkname"
	"github.com/erigontech/erigon/execution/stagedsync/rawdbreset"
	"github.com/erigontech/erigon/execution/stagedsync/stages"
	"github.com/erigontech/erigon/node/ethconfig"
)

// newEmptyBlockReader returns a BlockReader backed by an empty RoSnapshots
// instance, so FrozenBlocks() == 0 and the reset helper takes its
// "no-snapshots, skip FillDBFromSnapshots" path. FillDBFromSnapshots itself
// is exercised by stage_header --reset / stage_exec --reset integration
// paths and is out of scope for this unit test.
func newEmptyBlockReader(t *testing.T, dirs datadir.Dirs, logger log.Logger) *freezeblocks.BlockReader {
	t.Helper()
	snaps := freezeblocks.NewRoSnapshots(ethconfig.BlocksFreezing{ChainName: networkname.Mainnet}, dirs.Snap, logger)
	t.Cleanup(snaps.Close)
	return freezeblocks.NewBlockReader(snaps, nil)
}

// TestResetCanonicalAndRefillFromSnapshots_ClearsStaleSidechainPointers
// verifies the fix for a stale-canonical-pointer leak observed on hoodi
// snapshotters running release/3.4: a sidechain block was once canonical from
// CL's POV and was committed into kv.HeaderCanonical by a successful
// forkchoice update; subsequent reorg-to-real-canonical FCUs failed on
// execution (pre-#21157 unwind bug), the tx rolled back, and the sidechain
// hash stayed in kv.HeaderCanonical. integration reset_state cleared MDBX
// domain state but did NOT touch the canonical-hash mapping, so forward
// catchup after restart re-applied the sidechain block as canonical and
// re-introduced the phantom.
//
// Pre-fix, ResetCanonicalAndRefillFromSnapshots did not exist (compile
// error) and ResetState left kv.HeaderCanonical untouched. With the fix,
// ResetCanonicalAndRefillFromSnapshots wipes the entire kv.HeaderCanonical
// table, clears Headers/BlockHashes/Bodies/Senders/Snapshots stage progress
// and (when frozen blocks are present) hands re-population off to
// FillDBFromSnapshots. The next forkchoice update from CL then drives
// canonical assignments for the post-tip range fresh, with no chance for
// stale sidechain pointers to survive.
func TestResetCanonicalAndRefillFromSnapshots_ClearsStaleSidechainPointers(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dirs := datadir.New(t.TempDir())
	db := temporaltest.NewTestDB(t, dirs)
	logger := log.New()
	br := newEmptyBlockReader(t, dirs, logger)

	const sideTipHeight = uint64(110)
	staleHashAt105 := common.Hash{0x99}

	err := db.Update(ctx, func(tx kv.RwTx) error {
		for h := uint64(0); h <= sideTipHeight; h++ {
			if err := rawdb.WriteCanonicalHash(tx, common.Hash{byte(h)}, h); err != nil {
				return err
			}
		}
		if err := rawdb.WriteCanonicalHash(tx, staleHashAt105, 105); err != nil {
			return err
		}
		if err := rawdb.WriteHeadHeaderHash(tx, common.Hash{byte(sideTipHeight)}); err != nil {
			return err
		}
		for _, st := range []stages.SyncStage{stages.Headers, stages.BlockHashes, stages.Bodies, stages.Senders, stages.Snapshots} {
			if err := stages.SaveStageProgress(tx, st, sideTipHeight); err != nil {
				return err
			}
		}
		return nil
	})
	require.NoError(t, err)

	err = db.View(ctx, func(tx kv.Tx) error {
		h, errRead := rawdb.ReadCanonicalHash(tx, 105)
		require.NoError(t, errRead)
		require.Equal(t, staleHashAt105, h, "stale entry must be present before reset")
		return nil
	})
	require.NoError(t, err)

	require.NoError(t, rawdbreset.ResetCanonicalAndRefillFromSnapshots(ctx, db, dirs, br, logger))

	err = db.View(ctx, func(tx kv.Tx) error {
		for h := uint64(0); h <= sideTipHeight; h++ {
			hash, errRead := rawdb.ReadCanonicalHash(tx, h)
			require.NoError(t, errRead)
			require.Equal(t, common.Hash{}, hash, "canonical hash at %d must be cleared", h)
		}
		for _, st := range []stages.SyncStage{stages.Headers, stages.BlockHashes, stages.Bodies, stages.Senders, stages.Snapshots} {
			progress, errRead := stages.GetStageProgress(tx, st)
			require.NoError(t, errRead)
			require.Zero(t, progress, "%s stage progress must be reset to 0 so FillDBFromSnapshots can re-advance it on the next start", st)
		}
		return nil
	})
	require.NoError(t, err)
}

// TestResetCanonicalAndRefillFromSnapshots_NoOpOnEmptyDB exercises the
// idempotency guarantee: calling on a fresh db with no canonical entries
// and no frozen blocks must succeed and leave everything empty.
func TestResetCanonicalAndRefillFromSnapshots_NoOpOnEmptyDB(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dirs := datadir.New(t.TempDir())
	db := temporaltest.NewTestDB(t, dirs)
	logger := log.New()
	br := newEmptyBlockReader(t, dirs, logger)

	require.NoError(t, rawdbreset.ResetCanonicalAndRefillFromSnapshots(ctx, db, dirs, br, logger))

	err := db.View(ctx, func(tx kv.Tx) error {
		for _, st := range []stages.SyncStage{stages.Headers, stages.BlockHashes, stages.Bodies, stages.Senders, stages.Snapshots} {
			progress, errRead := stages.GetStageProgress(tx, st)
			require.NoError(t, errRead)
			require.Zero(t, progress, "%s stage progress must be zero on empty db", st)
		}
		return nil
	})
	require.NoError(t, err)
}
