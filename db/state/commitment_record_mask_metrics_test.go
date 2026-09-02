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
	"math/bits"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/diagnostics/metrics"
)

func counterDelta(name string) func() uint64 {
	c := metrics.GetOrCreateCounter(name)
	start := c.GetValueUint64()
	return func() uint64 { return c.GetValueUint64() - start }
}

type recordWalkDeltas struct {
	reads, consulted, scanned func() uint64
}

func recordWalkCounters(walk string) recordWalkDeltas {
	return recordWalkDeltas{
		reads:     counterDelta(`domain_commitment_record_reads{walk="` + walk + `"}`),
		consulted: counterDelta(`domain_commitment_record_files_consulted{walk="` + walk + `"}`),
		scanned:   counterDelta(`domain_commitment_record_files_scanned{walk="` + walk + `"}`),
	}
}

func acceptanceFilesFixture(t *testing.T) (kv.TemporalRwDB, *state.Aggregator, map[string]map[int][]byte, int) {
	t.Helper()
	db, agg := newAcceptanceDB(t, 1, 2)
	agg.ForTestEdgeRecordsInCommitment(kv.CommitmentDomain, true)
	for batchNumber, batch := range acceptanceBatches() {
		applyAcceptanceBatch(t, db, batch, uint64(batchNumber+1))
	}
	byNode := nodeKeysFromRecords(nonEmptyAcceptanceRecords(allAcceptanceRecords(t, db)))
	require.NotEmpty(t, byNode)
	require.NoError(t, agg.BuildFiles(4))
	files := agg.FilesAmount()[kv.CommitmentDomain]
	require.Positive(t, files, "the records must have moved into .kv files")
	return db, agg, byNode, files
}

func TestCommitmentV3RecordWalkIsCountedByOutcome(t *testing.T) {
	db, _, byNode, files := acceptanceFilesFixture(t)

	tx, err := db.BeginTemporalRo(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()
	reader, ok := tx.AggTx().(commitmentRecordReader)
	require.True(t, ok)

	checked := 0
	for nodeKey := range byNode {
		exhausted, satisfied := recordWalkCounters("exhausted"), recordWalkCounters("satisfied")

		_, present, _, err := reader.ReadCommitmentRecords(tx, []byte(nodeKey), 0, false, math.MaxUint64, nil)
		require.NoError(t, err)
		if bits.OnesCount16(present) == 16 {
			continue
		}
		require.EqualValues(t, 1, exhausted.reads())
		require.Zero(t, satisfied.reads())
		require.EqualValues(t, files, exhausted.consulted(),
			"a read wanting all 16 children keeps walking every file for the absent ones")

		_, _, _, err = reader.ReadCommitmentRecords(tx, []byte(nodeKey), present, true, math.MaxUint64, nil)
		require.NoError(t, err)
		require.EqualValues(t, 1, satisfied.reads())
		require.EqualValues(t, 1, exhausted.reads(), "a read that found every wanted child is not exhausted")
		require.LessOrEqual(t, satisfied.consulted(), exhausted.consulted())
		require.LessOrEqual(t, satisfied.scanned(), satisfied.consulted(), "a file is scanned only after it was consulted")
		checked++
	}
	require.Positive(t, checked, "at least one node with fewer than 16 children must exist")
}

func TestCommitmentV3UnknownMaskNodeReadExhaustsTheFileWalk(t *testing.T) {
	db, _, byNode, files := acceptanceFilesFixture(t)

	tx, err := db.BeginTemporalRo(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()
	sd, err := execctx.NewSharedDomains(t.Context(), tx, log.New())
	require.NoError(t, err)
	defer sd.Close()

	checked := 0
	for nodeKey := range byNode {
		unknownNodeReads := counterDelta(`domain_commitment_node_reads{mask="unknown"}`)
		knownNodeReads := counterDelta(`domain_commitment_node_reads{mask="known"}`)
		exhausted, satisfied := recordWalkCounters("exhausted"), recordWalkCounters("satisfied")

		_, present, _, err := sd.ReadCommitmentRecords(tx, []byte(nodeKey), 0, false, nil)
		require.NoError(t, err)
		if bits.OnesCount16(present) == 16 {
			continue
		}
		require.EqualValues(t, 1, unknownNodeReads())
		require.EqualValues(t, 1, exhausted.reads(), "the trie forwards a maskless read as a narrowed known one; the walk still runs out of files")
		require.EqualValues(t, files, exhausted.consulted())

		_, _, _, err = sd.ReadCommitmentRecords(tx, []byte(nodeKey), 0, false, nil)
		require.NoError(t, err)
		require.EqualValues(t, 2, exhausted.reads(), "a warm branch cache does not help: the absent children are never in it")
		require.EqualValues(t, 2*files, exhausted.consulted())

		_, _, _, err = sd.ReadCommitmentRecords(tx, []byte(nodeKey), present, true, nil)
		require.NoError(t, err)
		require.EqualValues(t, 1, knownNodeReads())
		require.Zero(t, satisfied.reads(), "a known mask after the cache fill is served without reaching the aggregator")
		require.EqualValues(t, 2, exhausted.reads())
		checked++
	}
	require.Positive(t, checked)
}
