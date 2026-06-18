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
