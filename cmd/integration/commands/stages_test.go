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

package commands

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/temporal/temporaltest"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/execution/cache"
	"github.com/erigontech/erigon/execution/commitment"
)

func branchGeneration(t *testing.T, tx kv.TemporalTx) cache.Generation {
	t.Helper()
	stateVersion, err := rawdb.GetStateVersion(tx)
	require.NoError(t, err)
	return cache.BranchGeneration(stateVersion, tx.Debug().TxNumsInFiles(kv.CommitmentDomain))
}

func TestCommitExecUnwindDoesNotRepublishDiscardedBranches(t *testing.T) {
	previous := dbg.UseStateCache
	dbg.SetUseStateCache(true)
	t.Cleanup(func() { dbg.SetUseStateCache(previous) })

	ctx := t.Context()
	logger := log.New()
	db := temporaltest.NewTestDBWithStepSize(t, datadir.New(t.TempDir()), 100)

	seedTx, err := db.BeginTemporalRw(ctx)
	require.NoError(t, err)
	defer seedTx.Rollback()
	seedDomains, err := execctx.NewSharedDomains(ctx, seedTx, logger)
	require.NoError(t, err)
	seedDomains.SetCanonicalCaches(nil)
	require.NoError(t, seedDomains.Commit(ctx, seedTx))
	seedDomains.Close()

	unwindTx, err := db.BeginTemporalRw(ctx)
	require.NoError(t, err)
	defer unwindTx.Rollback()
	unwindDomains, err := execctx.NewSharedDomains(ctx, unwindTx, logger)
	require.NoError(t, err)
	unwindDomains.SetCanonicalCaches(nil)

	provider, ok := unwindTx.AggTx().(commitment.BranchCacheProvider)
	require.True(t, ok)
	branchCache := provider.BranchCache()
	require.NotNil(t, branchCache)
	discardedKey := []byte{0xa0, 0xb0}
	oldView := branchCache.View(branchGeneration(t, unwindTx))
	oldView.Fill(discardedKey, []byte("discarded-fork"), 1)
	_, _, ok = oldView.Get(discardedKey)
	require.True(t, ok, "precondition: discarded branch is cached")

	var diffs [kv.DomainLen][]kv.DomainEntryDiff
	unwindDomains.Unwind(0, &diffs)
	require.NoError(t, commitExecUnwind(ctx, unwindDomains, unwindTx))
	unwindDomains.Close()

	nextTx, err := db.BeginTemporalRw(ctx)
	require.NoError(t, err)
	defer nextTx.Rollback()
	nextDomains, err := execctx.NewSharedDomains(ctx, nextTx, logger)
	require.NoError(t, err)
	nextDomains.SetCanonicalCaches(nil)
	require.NoError(t, nextDomains.Commit(ctx, nextTx))
	nextDomains.Close()

	readTx, err := db.BeginTemporalRo(ctx)
	require.NoError(t, err)
	defer readTx.Rollback()
	_, _, ok = branchCache.View(branchGeneration(t, readTx)).Get(discardedKey)
	require.False(t, ok, "a later commit must not republish a branch discarded by the unwind")
}
