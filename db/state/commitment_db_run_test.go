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

	"github.com/erigontech/erigon/db/kv"
)

type commitmentRecordReader interface {
	ReadCommitmentRecords(roTx kv.Tx, nodeKey []byte, mask uint16, maskKnown bool, maxTxNum uint64) ([16][]byte, uint16, kv.Step, error)
}

// nodeKeysFromRecords groups a flat record view by parent: a record key is its node key plus one
// child byte, so the parent falls out of the key itself.
func nodeKeysFromRecords(records map[string][]byte) map[string]map[int][]byte {
	byNode := make(map[string]map[int][]byte)
	for key, value := range records {
		if len(key) < 2 {
			continue
		}
		last := key[len(key)-1]
		if last < 0x80 || last > 0x8f {
			continue
		}
		nodeKey := key[:len(key)-1]
		children, ok := byNode[nodeKey]
		if !ok {
			children = make(map[int][]byte)
			byNode[nodeKey] = children
		}
		children[int(last&0x0f)] = value
	}
	return byNode
}

func requireNodeRecordsMatch(t *testing.T, db kv.TemporalRoDB, byNode map[string]map[int][]byte) {
	t.Helper()
	tx, err := db.BeginTemporalRo(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()
	reader, ok := tx.AggTx().(commitmentRecordReader)
	require.True(t, ok, "AggregatorRoTx must expose ReadCommitmentRecords")

	checked := 0
	for nodeKey, want := range byNode {
		var wantMask uint16
		for nibble := range want {
			wantMask |= uint16(1) << nibble
		}

		got, present, _, err := reader.ReadCommitmentRecords(tx, []byte(nodeKey), 0, false, math.MaxUint64)
		require.NoErrorf(t, err, "unbounded read of node %x", nodeKey)
		require.Equalf(t, wantMask, present, "present mask for node %x", nodeKey)
		for nibble := range 16 {
			if want[nibble] == nil {
				require.Emptyf(t, got[nibble], "node %x nibble %d must be absent", nodeKey, nibble)
				continue
			}
			require.Equalf(t, want[nibble], got[nibble], "node %x nibble %d", nodeKey, nibble)
		}

		// A caller that already knows the mask must get the same bytes for the slots it asked for
		// and nothing for the rest.
		masked := wantMask &^ (wantMask & -wantMask)
		gotMasked, presentMasked, _, err := reader.ReadCommitmentRecords(tx, []byte(nodeKey), masked, true, math.MaxUint64)
		require.NoErrorf(t, err, "masked read of node %x", nodeKey)
		require.Equalf(t, masked, presentMasked, "masked present for node %x", nodeKey)
		for bitset := masked; bitset != 0; {
			bit := bitset & -bitset
			nibble := bits.TrailingZeros16(bit)
			require.Equalf(t, want[nibble], gotMasked[nibble], "masked node %x nibble %d", nodeKey, nibble)
			bitset ^= bit
		}
		checked++
	}
	require.Positive(t, checked, "no v3 nodes to check")
}

// The db read walks a node's children with one cursor run rather than a seek per nibble. It has to
// return exactly what a flat latest-view of the domain holds, both while every record is still in
// chaindata and once files hold the older ones.
func TestCommitmentV3DbRecordsMatchLatestView(t *testing.T) {
	db, agg := newAcceptanceDB(t, 1, 2)
	agg.ForTestEdgeRecordsInCommitment(kv.CommitmentDomain, true)

	batches := acceptanceBatches()
	for batchNumber, batch := range batches[:2] {
		applyAcceptanceBatch(t, db, batch, uint64(batchNumber+1))
	}
	t.Run("db only", func(t *testing.T) {
		requireNodeRecordsMatch(t, db, nodeKeysFromRecords(nonEmptyAcceptanceRecords(allAcceptanceRecords(t, db))))
	})

	// Freeze the first batches into files, then write over some of them: the run now has to reject
	// the db records the files already superseded and keep the ones that are newer.
	require.NoError(t, agg.BuildFiles(2))
	for batchNumber, batch := range batches[2:] {
		applyAcceptanceBatch(t, db, batch, uint64(len(batches)-1+batchNumber+1))
	}
	t.Run("files and db", func(t *testing.T) {
		requireNodeRecordsMatch(t, db, nodeKeysFromRecords(nonEmptyAcceptanceRecords(allAcceptanceRecords(t, db))))
	})

	require.NoError(t, agg.MergeLoop(t.Context()))
	t.Run("after merge", func(t *testing.T) {
		requireNodeRecordsMatch(t, db, nodeKeysFromRecords(nonEmptyAcceptanceRecords(allAcceptanceRecords(t, db))))
	})
}
