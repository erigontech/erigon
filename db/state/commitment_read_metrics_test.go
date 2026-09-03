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
	"math"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/state/kvmetrics"
)

// A v3 record read never reaches getLatest, so unless it meters itself every
// kv_read_count{domain="commitment"} series reads zero on a v3 node and the arms cannot be
// compared at all.
func TestCommitmentV3RecordReadsAreMetered(t *testing.T) {
	previous := dbg.KVReadLevelledMetrics
	dbg.KVReadLevelledMetrics = true
	t.Cleanup(func() { dbg.KVReadLevelledMetrics = previous })

	db, agg := newAcceptanceDB(t, 1, 2)
	agg.ForTestEdgeRecordsInCommitment(kv.CommitmentDomain, true)
	batches := acceptanceBatches()
	for batchNumber, batch := range batches {
		applyAcceptanceBatch(t, db, batch, uint64(batchNumber+1))
	}
	byNode := nodeKeysFromRecords(nonEmptyAcceptanceRecords(allAcceptanceRecords(t, db)))
	require.NotEmpty(t, byNode)

	readAll := func() *kvmetrics.DomainIOMetrics {
		tx, err := db.BeginTemporalRo(t.Context())
		require.NoError(t, err)
		defer tx.Rollback()
		reader, ok := tx.AggTx().(commitmentRecordReader)
		require.True(t, ok)
		wm := kvmetrics.NewDomainMetrics()
		for nodeKey := range byNode {
			_, _, _, err := reader.ReadCommitmentRecords(tx, []byte(nodeKey), 0, false, math.MaxUint64, wm)
			require.NoError(t, err)
		}
		entry, ok := wm.Domains[kv.CommitmentDomain]
		require.True(t, ok, "commitment domain must appear in the read metrics")
		return entry
	}

	fromDB := readAll()
	require.Positive(t, fromDB.DbReadCount, "records still in chaindata must count as db reads")

	require.NoError(t, agg.BuildFiles(db, 4, unboundedFinalityCtx))
	require.NoError(t, agg.MergeLoop(t.Context()))
	fromFiles := readAll()
	require.Positive(t, fromFiles.FileReadCount, "records served from .kv must count as file reads")
}

// With the env flag off the read path must not meter at all: the accumulator is per-worker and
// touching it costs a timestamp and a map lookup on every record.
func TestCommitmentV3RecordReadsSkipMeteringWhenDisabled(t *testing.T) {
	previous := dbg.KVReadLevelledMetrics
	dbg.KVReadLevelledMetrics = false
	t.Cleanup(func() { dbg.KVReadLevelledMetrics = previous })

	db, agg := newAcceptanceDB(t, 1, 2)
	agg.ForTestEdgeRecordsInCommitment(kv.CommitmentDomain, true)
	for batchNumber, batch := range acceptanceBatches() {
		applyAcceptanceBatch(t, db, batch, uint64(batchNumber+1))
	}
	byNode := nodeKeysFromRecords(nonEmptyAcceptanceRecords(allAcceptanceRecords(t, db)))

	tx, err := db.BeginTemporalRo(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()
	reader, ok := tx.AggTx().(commitmentRecordReader)
	require.True(t, ok)
	wm := kvmetrics.NewDomainMetrics()
	for nodeKey := range byNode {
		_, _, _, err := reader.ReadCommitmentRecords(tx, []byte(nodeKey), 0, false, math.MaxUint64, wm)
		require.NoError(t, err)
	}
	require.Empty(t, wm.Domains, "no domain should be metered with KV_READ_METRICS off")
}
