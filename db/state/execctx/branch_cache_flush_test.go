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

package execctx_test

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/execution/commitment"
)

type commitErrorTx struct {
	kv.TemporalRwTx
	err error
}

func (tx *commitErrorTx) Commit() error { return tx.err }

// Use Commit (not Flush) so the rebuilt branch refreshes the BranchCache entry.
func TestBranchCacheCommitRefreshesAfterReadThrough(t *testing.T) {
	stepSize := uint64(100)
	db := newTestDb(t, stepSize)
	ctx := t.Context()
	logger := log.New()

	key := []byte{0x0a, 0x0b}

	writeCommit := func(val []byte, step uint64, prev []byte, readFirst bool) {
		rwTx, err := db.BeginTemporalRw(ctx)
		require.NoError(t, err)
		defer rwTx.Rollback()
		sd, err := execctx.NewSharedDomains(ctx, rwTx, logger)
		require.NoError(t, err)
		defer sd.Close()

		if readFirst {
			got, _, err := sd.GetLatest(kv.CommitmentDomain, rwTx, key)
			require.NoError(t, err)
			require.Equal(t, prev, got)
		}
		require.NoError(t, sd.DomainPut(kv.CommitmentDomain, rwTx, key, val, step, prev))
		require.NoError(t, sd.Commit(ctx, rwTx))
		sd.Close()
	}

	writeCommit([]byte("v1-branch-bytes"), 1, nil, false)
	writeCommit([]byte("v2-branch-bytes"), 2, []byte("v1-branch-bytes"), true)

	rwTx, err := db.BeginTemporalRw(ctx)
	require.NoError(t, err)
	defer rwTx.Rollback()
	sd, err := execctx.NewSharedDomains(ctx, rwTx, logger)
	require.NoError(t, err)
	defer sd.Close()

	v, _, err := sd.GetLatest(kv.CommitmentDomain, rwTx, key)
	require.NoError(t, err)
	require.Equal(t, []byte("v2-branch-bytes"), v, "fresh SD must read the latest committed branch, not the stale read-through entry")
}

func TestSpeculativeUnwindDetachesWithoutChangingBranchCache(t *testing.T) {
	db := newTestDb(t, 100)
	ctx := t.Context()

	roTx, err := db.BeginTemporalRo(ctx)
	require.NoError(t, err)
	defer roTx.Rollback()

	sd, err := execctx.NewSharedDomains(ctx, roTx, log.New())
	require.NoError(t, err)
	defer sd.Close()

	provider, ok := roTx.AggTx().(commitment.BranchCacheProvider)
	require.True(t, ok)
	branchCache := provider.BranchCache()
	require.NotNil(t, branchCache)

	stateVersion, err := rawdb.GetStateVersion(roTx)
	require.NoError(t, err)
	branchCache.Publisher().Initialize(stateVersion)

	key := []byte{0xa0, 0xb0}
	published := branchCache.View(stateVersion)
	published.Fill(key, []byte("canonical-cache-only"), 1)

	sd.Unwind(50, nil)

	value, _, ok := published.Get(key)
	require.True(t, ok, "a speculative unwind must not mutate the process-global branch generation")
	require.Equal(t, []byte("canonical-cache-only"), value)

	value, _, err = sd.GetLatest(kv.CommitmentDomain, roTx, key)
	require.NoError(t, err)
	require.Empty(t, value, "the rewound SharedDomains must detach from the canonical branch generation")
}

func TestCanonicalUnwindClearsBranchCacheOnlyAfterCommit(t *testing.T) {
	db := newTestDb(t, 100)
	ctx := t.Context()
	logger := log.New()
	key := []byte{0xa0, 0xb0}

	seedTx, err := db.BeginTemporalRw(ctx)
	require.NoError(t, err)
	defer seedTx.Rollback()
	seedSD, err := execctx.NewSharedDomains(ctx, seedTx, logger)
	require.NoError(t, err)
	require.NoError(t, seedSD.DomainPut(kv.CommitmentDomain, seedTx, key, []byte("durable"), 1, nil))
	require.NoError(t, seedSD.Commit(ctx, seedTx))
	seedSD.Close()

	unwindTx, err := db.BeginTemporalRw(ctx)
	require.NoError(t, err)
	defer unwindTx.Rollback()
	unwindSD, err := execctx.NewSharedDomains(ctx, unwindTx, logger)
	require.NoError(t, err)
	defer unwindSD.Close()

	provider, ok := unwindTx.AggTx().(commitment.BranchCacheProvider)
	require.True(t, ok)
	branchCache := provider.BranchCache()
	stateVersion, err := rawdb.GetStateVersion(unwindTx)
	require.NoError(t, err)
	oldView := branchCache.View(stateVersion)
	cacheOnlyKey := []byte{0xa0, 0xc0}
	oldView.Fill(cacheOnlyKey, []byte("discarded-fork"), 2)

	var diffs [kv.DomainLen][]kv.DomainEntryDiff
	unwindSD.Unwind(0, &diffs)
	value, _, ok := oldView.Get(cacheOnlyKey)
	require.True(t, ok, "the cache must keep serving the still-durable version before Commit")
	require.Equal(t, []byte("discarded-fork"), value)
	require.NoError(t, unwindSD.Commit(ctx, unwindTx))

	_, _, ok = oldView.Get(cacheOnlyKey)
	require.False(t, ok, "committing the unwind must revoke views of the discarded version")

	readTx, err := db.BeginTemporalRo(ctx)
	require.NoError(t, err)
	defer readTx.Rollback()
	newStateVersion, err := rawdb.GetStateVersion(readTx)
	require.NoError(t, err)
	_, _, ok = branchCache.View(newStateVersion).Get(cacheOnlyKey)
	require.False(t, ok, "the unwound generation must not retain a cache-only discarded branch")
}

func TestFailedCommitRestoresBranchCacheGeneration(t *testing.T) {
	db := newTestDb(t, 100)
	ctx := t.Context()
	logger := log.New()

	seedTx, err := db.BeginTemporalRw(ctx)
	require.NoError(t, err)
	defer seedTx.Rollback()
	seedSD, err := execctx.NewSharedDomains(ctx, seedTx, logger)
	require.NoError(t, err)
	require.NoError(t, seedSD.Commit(ctx, seedTx))
	seedSD.Close()

	rwTx, err := db.BeginTemporalRw(ctx)
	require.NoError(t, err)
	defer rwTx.Rollback()
	sd, err := execctx.NewSharedDomains(ctx, rwTx, logger)
	require.NoError(t, err)
	defer sd.Close()

	provider, ok := rwTx.AggTx().(commitment.BranchCacheProvider)
	require.True(t, ok)
	branchCache := provider.BranchCache()
	stateVersion, err := rawdb.GetStateVersion(rwTx)
	require.NoError(t, err)
	view := branchCache.View(stateVersion)
	key := []byte{0xa0, 0xb0}
	view.Fill(key, []byte("durable"), 1)

	sentinel := errors.New("injected commit failure")
	err = sd.Commit(ctx, &commitErrorTx{TemporalRwTx: rwTx, err: sentinel})
	require.ErrorIs(t, err, sentinel)

	value, _, ok := view.Get(key)
	require.True(t, ok, "a failed database commit must restore the previous branch generation")
	require.Equal(t, []byte("durable"), value)
}
