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

package state_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/db/state/execctx"
)

// commitmentReadsMeteredThroughCompute drives the whole chain a live node uses -- ComputeCommitment
// into TrieContext into the state reader -- and reports whether the commitment domain shows up in
// the SharedDomains read accumulator at all. On a v2 node it does; the v3 arm reports nothing.
func commitmentReadsMeteredThroughCompute(t *testing.T, edgeRecords, parallel bool) int64 {
	t.Helper()
	previous := dbg.KVReadLevelledMetrics
	dbg.KVReadLevelledMetrics = true
	t.Cleanup(func() { dbg.KVReadLevelledMetrics = previous })

	db, agg := newAcceptanceDB(t, 16, 2)
	agg.ForTestEdgeRecordsInCommitment(kv.CommitmentDomain, edgeRecords)

	var total int64
	applyBatch := func(batch []acceptanceEntry, txNum uint64) {
		tx, err := db.BeginTemporalRw(t.Context())
		require.NoError(t, err)
		defer tx.Rollback()
		require.NoError(t, rawdbv3.TxNums.Append(tx, txNum, txNum))
		domains, err := execctx.NewSharedDomains(t.Context(), tx, log.New())
		require.NoError(t, err)
		defer domains.Close()
		domains.EnableTrieWarmup(true)
		if parallel {
			domains.EnableParaTrieDB(db)
		}
		for i := range batch {
			entry := &batch[i]
			previousValue, _, err := domains.GetLatest(entry.domain, tx, entry.key)
			require.NoError(t, err)
			require.NoError(t, domains.DomainPut(entry.domain, tx, entry.key, entry.value, txNum, previousValue))
		}
		_, err = domains.ComputeCommitment(t.Context(), tx, true, txNum, txNum, "warmup-metrics", nil)
		require.NoError(t, err)
		if entry, ok := domains.Metrics().Domains[kv.CommitmentDomain]; ok {
			total += entry.CacheReadCount + entry.DbReadCount + entry.FileReadCount
		}
		require.NoError(t, domains.Commit(t.Context(), tx))
		require.NoError(t, tx.Commit())
	}
	for batchNumber, batch := range acceptanceBatches() {
		applyBatch(batch, uint64(batchNumber+1))
	}
	return total
}

// Both record formats must meter their commitment reads. v3 never reaches getLatest, so if the
// record path does not meter itself the whole domain reads zero and the arms cannot be compared.
func TestCommitmentReadsAreMeteredInBothFormats(t *testing.T) {
	for _, parallel := range []bool{false, true} {
		name := "serial"
		if parallel {
			name = "parallel"
		}
		t.Run(name, func(t *testing.T) {
			legacy := commitmentReadsMeteredThroughCompute(t, false, parallel)
			require.Positive(t, legacy, "v2 commitment reads must be metered")
			edge := commitmentReadsMeteredThroughCompute(t, true, parallel)
			require.Positive(t, edge, "v3 commitment record reads must be metered")
		})
	}
}
