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

package execctx_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/db/state/kvmetrics"
)

func mergedReads(t *testing.T, dm *kvmetrics.DomainMetrics) int64 {
	t.Helper()
	return dm.SnapshotDomain(kv.AccountsDomain).DbReadCount
}

func batchOfReads(n int64) *kvmetrics.DomainMetrics {
	dm := kvmetrics.NewDomainMetrics()
	dm.Domains[kv.AccountsDomain] = &kvmetrics.DomainIOMetrics{DbReadCount: n}
	return dm
}

func sharedDomainsForMetrics(t *testing.T) *execctx.SharedDomains {
	t.Helper()
	db := newTestDb(t, 16)
	ctx := context.Background()

	tx, err := db.BeginTemporalRo(ctx)
	require.NoError(t, err)
	t.Cleanup(tx.Rollback)

	sd, err := execctx.NewSharedDomains(ctx, tx, log.New())
	require.NoError(t, err)
	t.Cleanup(sd.Close)
	return sd
}

func TestMergeMetricsSeparatesNonExecSources(t *testing.T) {
	prev := dbg.KVReadLevelledMetrics
	t.Cleanup(func() { dbg.KVReadLevelledMetrics = prev })
	dbg.EnableKVReadLevelledMetrics()

	sd := sharedDomainsForMetrics(t)

	sd.MergeMetrics(kvmetrics.SourceExec, batchOfReads(5))
	sd.MergeMetrics(kvmetrics.SourceCommitment, batchOfReads(3))
	sd.MergeMetrics(kvmetrics.SourceWarmup, batchOfReads(2))

	require.Equal(t, int64(10), mergedReads(t, sd.Metrics()),
		"every source lands in the aggregate")
	require.Equal(t, int64(5), mergedReads(t, sd.NonExecMetrics()),
		"commitment and warmup are held apart from execution's reads")
}

func TestMergeMetricsSkipsNonExecSplitWithoutReadMetrics(t *testing.T) {
	prev := dbg.KVReadLevelledMetrics
	t.Cleanup(func() { dbg.KVReadLevelledMetrics = prev })
	dbg.KVReadLevelledMetrics = false

	sd := sharedDomainsForMetrics(t)

	sd.MergeMetrics(kvmetrics.SourceCommitment, batchOfReads(3))
	sd.MergeMetrics(kvmetrics.SourceWarmup, batchOfReads(2))

	require.Equal(t, int64(5), mergedReads(t, sd.Metrics()),
		"the per-batch log aggregate is unconditional")
	require.Zero(t, mergedReads(t, sd.NonExecMetrics()),
		"nothing reads the split when the counters are off, so it must not pay the lock")
}
