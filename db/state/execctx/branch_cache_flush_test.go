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
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/execution/commitment"
)

type commitmentWrite struct {
	txNum uint64
	value []byte
}

func writeCommitmentRows(t *testing.T, db kv.TemporalRwDB, key, prev []byte, writes ...commitmentWrite) {
	t.Helper()
	rwTx, err := db.BeginTemporalRw(t.Context())
	require.NoError(t, err)
	defer rwTx.Rollback()
	sd, err := execctx.NewSharedDomains(t.Context(), rwTx, log.New())
	require.NoError(t, err)
	defer sd.Close()
	for _, write := range writes {
		require.NoError(t, sd.DomainPut(kv.CommitmentDomain, rwTx, key, write.value, write.txNum, prev))
		prev = write.value
	}
	require.NoError(t, sd.Commit(t.Context(), rwTx))
	sd.Close()
	require.NoError(t, rwTx.Commit())
}

func writeAggregationGuard(t *testing.T, db kv.TemporalRwDB, txNum uint64) {
	t.Helper()
	rwTx, err := db.BeginTemporalRw(t.Context())
	require.NoError(t, err)
	defer rwTx.Rollback()
	sd, err := execctx.NewSharedDomains(t.Context(), rwTx, log.New())
	require.NoError(t, err)
	defer sd.Close()
	require.NoError(t, sd.DomainPut(kv.AccountsDomain, rwTx, []byte{0x01}, encAccount(1), txNum, nil))
	require.NoError(t, sd.Commit(t.Context(), rwTx))
	sd.Close()
	require.NoError(t, rwTx.Commit())
}

func commitmentFileFixture(t *testing.T, stepSize uint64) (kv.TemporalRwDB, []byte, []byte) {
	t.Helper()
	db := newTestDb(t, stepSize)
	key := []byte{0x0a, 0x0c}
	value := []byte{0, 0, 0, 0, 1}
	writeCommitmentRows(t, db, key, nil, commitmentWrite{txNum: 5, value: value})
	writeAggregationGuard(t, db, 20)
	require.NoError(t, db.(state.HasAgg).Agg().(*state.Aggregator).BuildFiles(stepSize))
	return db, key, value
}

func TestGetLatestHonorsStagedUnwindBound(t *testing.T) {
	const stepSize = uint64(16)
	db, key, frozenValue := commitmentFileFixture(t, stepSize)
	stepOneValue := []byte{0, 0, 0, 0, 2}
	deadForkValue := []byte{0, 0, 0, 0, 3}
	writeCommitmentRows(t, db, key, frozenValue, commitmentWrite{txNum: 20, value: stepOneValue}, commitmentWrite{txNum: 40, value: deadForkValue})
	roTx, err := db.BeginTemporalRo(t.Context())
	require.NoError(t, err)
	defer roTx.Rollback()
	sd, err := execctx.NewSharedDomains(t.Context(), roTx, log.New())
	require.NoError(t, err)
	defer sd.Close()
	stepBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(stepBytes, ^uint64(1))
	var diffs [kv.DomainLen][]kv.DomainEntryDiff
	diffs[kv.CommitmentDomain] = []kv.DomainEntryDiff{{Key: string(key) + string(stepBytes), Value: nil}}
	sd.Unwind(32, &diffs)
	got, _, err := sd.GetLatest(kv.CommitmentDomain, roTx, key)
	require.NoError(t, err)
	require.Equal(t, stepOneValue, got)
}

func TestBoundedGetLatestDoesNotPopulateBranchCache(t *testing.T) {
	const stepSize = uint64(16)
	db, key, frozenValue := commitmentFileFixture(t, stepSize)
	stepOneValue := []byte{0, 0, 0, 0, 2}
	deadForkValue := []byte{0, 0, 0, 0, 3}
	writeCommitmentRows(t, db, key, frozenValue, commitmentWrite{txNum: 20, value: stepOneValue}, commitmentWrite{txNum: 40, value: deadForkValue})
	roTx, err := db.BeginTemporalRo(t.Context())
	require.NoError(t, err)
	defer roTx.Rollback()
	branchCache := roTx.AggTx().(commitment.BranchCacheProvider).BranchCache()
	branchCache.Clear()
	sd, err := execctx.NewSharedDomains(t.Context(), roTx, log.New())
	require.NoError(t, err)
	defer sd.Close()
	stepBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(stepBytes, ^uint64(1))
	var diffs [kv.DomainLen][]kv.DomainEntryDiff
	diffs[kv.CommitmentDomain] = []kv.DomainEntryDiff{{Key: string(key) + string(stepBytes), Value: nil}}
	sd.Unwind(32, &diffs)
	_, _, err = sd.GetLatest(kv.CommitmentDomain, roTx, key)
	require.NoError(t, err)
	_, _, ok := branchCache.Get(key)
	require.False(t, ok)
}

func TestSnapshotBranchCacheEntrySurvivesLegalUnwind(t *testing.T) {
	const stepSize = uint64(16)
	db, key, frozenValue := commitmentFileFixture(t, stepSize)
	roTx, err := db.BeginTemporalRo(t.Context())
	require.NoError(t, err)
	defer roTx.Rollback()
	branchCache := roTx.AggTx().(commitment.BranchCacheProvider).BranchCache()
	branchCache.Clear()
	sd, err := execctx.NewSharedDomains(t.Context(), roTx, log.New())
	require.NoError(t, err)
	defer sd.Close()
	require.Equal(t, stepSize, roTx.Debug().TxNumsInFiles(kv.CommitmentDomain))
	got, step, err := sd.GetLatest(kv.CommitmentDomain, roTx, key)
	require.NoError(t, err)
	require.Equal(t, frozenValue, got)
	require.Equal(t, kv.Step(1), step)
	branchCache.Unwind(20)
	got, _, ok := branchCache.Get(key)
	require.True(t, ok)
	require.Equal(t, frozenValue, got)
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
