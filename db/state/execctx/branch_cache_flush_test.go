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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/state/execctx"
)

// TestBranchCacheFlushInvalidatesStaleEntries verifies that Flush invalidates
// BranchCache entries for commitment keys it writes, so that subsequent reads
// after ClearRam hit the DB instead of serving stale cached values.
func TestBranchCacheFlushInvalidatesStaleEntries(t *testing.T) {
	stepSize := uint64(100)
	db := newTestDb(t, stepSize)
	ctx := t.Context()
	logger := log.New()

	key := []byte{0x0a, 0x0b}

	// Batch 1: write v1, flush+commit (simulating integration-path from-0 loop).
	rwTx, err := db.BeginTemporalRw(ctx)
	require.NoError(t, err)
	defer rwTx.Rollback() //nolint:gocritic
	sd, err := execctx.NewSharedDomains(ctx, rwTx, logger)
	require.NoError(t, err)

	require.NoError(t, sd.DomainPut(kv.CommitmentDomain, rwTx, key, []byte("v1"), 1, nil))
	require.NoError(t, sd.Flush(ctx, rwTx))
	sd.ClearRam(true)
	require.NoError(t, rwTx.Commit())

	// Batch 2: read v1 (populates BranchCache via read-through), write v2, flush+commit.
	rwTx, err = db.BeginTemporalRw(ctx)
	require.NoError(t, err)
	defer rwTx.Rollback() //nolint:gocritic
	sd2, err := execctx.NewSharedDomains(ctx, rwTx, logger)
	require.NoError(t, err)

	got, _, err := sd2.GetLatest(kv.CommitmentDomain, rwTx, key)
	require.NoError(t, err)
	require.Equal(t, []byte("v1"), got)

	require.NoError(t, sd2.DomainPut(kv.CommitmentDomain, rwTx, key, []byte("v2"), 2, []byte("v1")))
	require.NoError(t, sd2.Flush(ctx, rwTx))
	sd2.ClearRam(true)
	require.NoError(t, rwTx.Commit())

	// Batch 3: read must return v2, not v1 (stale cache).
	rwTx, err = db.BeginTemporalRw(ctx)
	require.NoError(t, err)
	defer rwTx.Rollback()
	sd3, err := execctx.NewSharedDomains(ctx, rwTx, logger)
	require.NoError(t, err)
	defer sd3.Close()

	got, _, err = sd3.GetLatest(kv.CommitmentDomain, rwTx, key)
	require.NoError(t, err)
	require.Equal(t, []byte("v2"), got, "Flush must invalidate BranchCache so reads return the latest value, not stale v1")
}

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
