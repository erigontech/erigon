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
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/temporal/temporaltest"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/execution/stagedsync/rawdbreset"
	"github.com/erigontech/erigon/execution/stagedsync/stages"
)

// TestResetCanonicalAboveTip_ClearsStaleSidechainPointers verifies the fix for
// a stale-canonical-pointer leak observed on hoodi snapshotters running
// release/3.4: a sidechain block was once canonical from CL's POV and was
// committed into kv.HeaderCanonical by a successful forkchoice update;
// subsequent reorg-to-real-canonical FCUs failed on execution (pre-#21157
// unwind bug), the tx rolled back, and the sidechain hash stayed in
// kv.HeaderCanonical. integration reset_state cleared MDBX domain state but
// did NOT touch the canonical-hash mapping, so forward catchup after restart
// re-applied the sidechain block as canonical and re-introduced the phantom.
//
// Pre-fix, ResetCanonicalAboveTip did not exist (compile error) and ResetState
// left kv.HeaderCanonical untouched above the snapshot tip. With the fix,
// ResetCanonicalAboveTip truncates kv.HeaderCanonical from snapshotTip+1
// forward, truncates TD likewise, caps Headers/BlockHashes/Bodies/Senders
// stage progress at the snapshot tip, and re-anchors HeadHeaderHash to the
// tip's canonical hash. The next forkchoice update from CL then drives
// canonical assignments fresh, with no chance for stale sidechain pointers
// to survive.
func TestResetCanonicalAboveTip_ClearsStaleSidechainPointers(t *testing.T) {
	ctx := context.Background()
	dirs := datadir.New(t.TempDir())
	db := temporaltest.NewTestDB(t, dirs)

	const snapshotTip = uint64(100)
	const stalePastTip = uint64(110)
	snapshotTipHash := common.Hash{0xaa}
	staleHashAt105 := common.Hash{0x99}

	// Seed: canonical hash at snapshot tip + canonical entries above it,
	// including one explicit non-zero "stale sidechain" pointer at 105 so the
	// assertion below distinguishes "missing" from "zero".
	err := db.Update(ctx, func(tx kv.RwTx) error {
		if err := rawdb.WriteCanonicalHash(tx, snapshotTipHash, snapshotTip); err != nil {
			return err
		}
		for h := snapshotTip + 1; h <= stalePastTip; h++ {
			if err := rawdb.WriteCanonicalHash(tx, common.Hash{byte(h)}, h); err != nil {
				return err
			}
		}
		if err := rawdb.WriteCanonicalHash(tx, staleHashAt105, 105); err != nil {
			return err
		}
		if err := rawdb.WriteHeadHeaderHash(tx, common.Hash{byte(stalePastTip)}); err != nil {
			return err
		}
		// simulate Headers/BlockHashes/Bodies/Senders stages being above tip
		for _, st := range []stages.SyncStage{stages.Headers, stages.BlockHashes, stages.Bodies, stages.Senders} {
			if err := stages.SaveStageProgress(tx, st, stalePastTip); err != nil {
				return err
			}
		}
		return nil
	})
	require.NoError(t, err)

	// Pre-check: stale canonical entry visible (documents the bug).
	err = db.View(ctx, func(tx kv.Tx) error {
		h, errRead := rawdb.ReadCanonicalHash(tx, 105)
		require.NoError(t, errRead)
		require.Equal(t, staleHashAt105, h, "stale entry must be present before reset")
		return nil
	})
	require.NoError(t, err)

	require.NoError(t, rawdbreset.ResetCanonicalAboveTip(ctx, db, snapshotTip))

	err = db.View(ctx, func(tx kv.Tx) error {
		for h := snapshotTip + 1; h <= stalePastTip; h++ {
			hash, errRead := rawdb.ReadCanonicalHash(tx, h)
			require.NoError(t, errRead)
			require.Equal(t, common.Hash{}, hash, "canonical hash at %d must be cleared", h)
		}
		tipHash, errRead := rawdb.ReadCanonicalHash(tx, snapshotTip)
		require.NoError(t, errRead)
		require.Equal(t, snapshotTipHash, tipHash, "canonical hash at snapshot tip must be preserved")

		head := rawdb.ReadHeadHeaderHash(tx)
		require.Equal(t, snapshotTipHash, head, "HeadHeaderHash must be re-anchored to snapshot tip")

		for _, st := range []stages.SyncStage{stages.Headers, stages.BlockHashes, stages.Bodies, stages.Senders} {
			progress, errRead := stages.GetStageProgress(tx, st)
			require.NoError(t, errRead)
			require.Equal(t, snapshotTip, progress, "%s stage progress must be capped at snapshot tip", st)
		}
		return nil
	})
	require.NoError(t, err)
}

// TestResetCanonicalAboveTip_NoOpWhenAlreadyAtTip exercises the idempotency
// guarantee: calling on an already-clean db must succeed without altering
// already-correct stage progress at-or-below the tip.
func TestResetCanonicalAboveTip_NoOpWhenAlreadyAtTip(t *testing.T) {
	ctx := context.Background()
	dirs := datadir.New(t.TempDir())
	db := temporaltest.NewTestDB(t, dirs)

	const snapshotTip = uint64(50)
	snapshotTipHash := common.Hash{0x42}

	err := db.Update(ctx, func(tx kv.RwTx) error {
		if err := rawdb.WriteCanonicalHash(tx, snapshotTipHash, snapshotTip); err != nil {
			return err
		}
		if err := stages.SaveStageProgress(tx, stages.Headers, snapshotTip); err != nil {
			return err
		}
		return nil
	})
	require.NoError(t, err)

	require.NoError(t, rawdbreset.ResetCanonicalAboveTip(ctx, db, snapshotTip))

	err = db.View(ctx, func(tx kv.Tx) error {
		h, errRead := rawdb.ReadCanonicalHash(tx, snapshotTip)
		require.NoError(t, errRead)
		require.Equal(t, snapshotTipHash, h)
		p, errRead := stages.GetStageProgress(tx, stages.Headers)
		require.NoError(t, errRead)
		require.Equal(t, snapshotTip, p, "stage progress at tip must not be moved down")
		return nil
	})
	require.NoError(t, err)
}
