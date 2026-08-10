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
	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/temporal/temporaltest"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/db/snapshotsync/freezeblocks"
	dbstate "github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/execution/cache"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/stagedsync/rawdbreset"
	"github.com/erigontech/erigon/execution/stagedsync/stages"
)

func resetCacheTestDB(t *testing.T) (kv.TemporalRwDB, *dbstate.Aggregator) {
	t.Helper()
	previous := dbg.UseStateCache
	dbg.SetUseStateCache(true)
	t.Cleanup(func() { dbg.SetUseStateCache(previous) })

	db := temporaltest.NewTestDBWithStepSize(t, datadir.New(t.TempDir()), 16)
	hasAgg, ok := db.(dbstate.HasAgg)
	require.True(t, ok)
	agg, ok := hasAgg.Agg().(*dbstate.Aggregator)
	require.True(t, ok)
	return db, agg
}

func cacheGenerations(t *testing.T, db kv.TemporalRwDB) (cache.Generation, cache.Generation, *commitment.BranchCache) {
	t.Helper()
	tx, err := db.BeginTemporalRo(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()

	stateVersion, err := rawdb.GetStateVersion(tx)
	require.NoError(t, err)
	debug := tx.Debug()
	stateGeneration := cache.StateGeneration(
		stateVersion,
		debug.TxNumsInFiles(kv.AccountsDomain),
		debug.TxNumsInFiles(kv.StorageDomain),
		debug.TxNumsInFiles(kv.CodeDomain),
	)
	branchGeneration := cache.BranchGeneration(stateVersion, debug.TxNumsInFiles(kv.CommitmentDomain))
	provider, ok := tx.AggTx().(commitment.BranchCacheProvider)
	require.True(t, ok)
	return stateGeneration, branchGeneration, provider.BranchCache()
}

func stateVersion(t *testing.T, db kv.TemporalRwDB) uint64 {
	t.Helper()
	tx, err := db.BeginRo(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()

	version, err := rawdb.GetStateVersion(tx)
	require.NoError(t, err)
	return version
}

func TestResetExecAdvancesStateVersion(t *testing.T) {
	db, _ := resetCacheTestDB(t)
	require.NoError(t, db.Update(t.Context(), func(tx kv.RwTx) error {
		_, err := rawdb.IncrementStateVersion(tx)
		return err
	}))
	before := stateVersion(t, db)

	require.NoError(t, rawdbreset.ResetExec(t.Context(), db))

	require.Equal(t, before+1, stateVersion(t, db))
}

func TestResetExecResetsBoundStateCache(t *testing.T) {
	db, agg := resetCacheTestDB(t)
	stateGeneration, _, _ := cacheGenerations(t, db)

	stateCache := cache.NewStateCache(1<<20, 1<<20, 1<<20, 1<<20)
	t.Cleanup(stateCache.Close)
	agg.BindStateCache(stateCache)
	publisher := stateCache.Publisher()
	publisher.Initialize(stateGeneration)
	publication := publisher.Begin()
	key := []byte{0x01}
	publication.Publish(stateGeneration, []cache.Update{{
		Domain: kv.AccountsDomain,
		Key:    key,
		Value:  []byte{0xaa},
	}}, false)
	oldView := stateCache.View(stateGeneration)
	_, ok := oldView.Get(kv.AccountsDomain, key)
	require.True(t, ok, "precondition: state entry is cached")

	require.NoError(t, rawdbreset.ResetExec(t.Context(), db))

	_, ok = oldView.Get(kv.AccountsDomain, key)
	require.False(t, ok, "reset must revoke views of the pre-reset state")
	oldView.Fill(kv.AccountsDomain, key, []byte{0xbb}, 0)
	publication = publisher.Begin()
	publication.Publish(stateGeneration, nil, false)
	_, ok = stateCache.View(stateGeneration).Get(kv.AccountsDomain, key)
	require.False(t, ok, "the same numeric generation must not expose or accept pre-reset state")
}

func TestResetExecResetsBranchCacheGeneration(t *testing.T) {
	db, _ := resetCacheTestDB(t)
	_, branchGeneration, branchCache := cacheGenerations(t, db)
	require.NotNil(t, branchCache)

	publisher := branchCache.Publisher()
	publisher.Initialize(branchGeneration)
	key := []byte{0x01}
	oldView := branchCache.View(branchGeneration)
	oldView.Fill(key, []byte{0xaa}, 0)
	_, _, ok := oldView.Get(key)
	require.True(t, ok, "precondition: branch entry is cached")

	require.NoError(t, rawdbreset.ResetExec(t.Context(), db))

	oldView.Fill(key, []byte{0xbb}, 0)
	publication := publisher.Begin()
	publication.Publish(branchGeneration, nil, false, nil)
	_, _, ok = branchCache.View(branchGeneration).Get(key)
	require.False(t, ok, "a pre-reset view must not refill the reset branch generation")
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
	br := freezeblocks.NewBlockReader(db.(freezeblocks.HasBlockFiles).DebugBlockFiles(), nil)

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
	br := freezeblocks.NewBlockReader(db.(freezeblocks.HasBlockFiles).DebugBlockFiles(), nil)

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
