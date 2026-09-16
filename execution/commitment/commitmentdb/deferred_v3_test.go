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

package commitmentdb

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/etl"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/commitment/nibbles"
)

func TestTrieContextV3OverlayIncludesLatestRecord(t *testing.T) {
	collector := etl.NewCollector(t.Name(), t.TempDir(), etl.NewSortableBuffer(1), log.Root())
	defer collector.Close()

	reader := &testStateReader{}
	ctx := &TrieContext{
		stateReader:    reader,
		stepSize:       1,
		localCollector: collector,
		localWrites:    make(map[string][]byte),
		edgeRecords:    true,
	}

	nodePath := []byte{1, 2, 3}
	nodeKey := nibbles.EncodeKeyV3(nodePath)
	recordKey := nibbles.ChildKeyV3(nodeKey, 7)
	first := make([]byte, 1+20)
	first[0] = 1
	latest := make([]byte, 1+20)
	latest[0] = 1
	for i := range latest[1:] {
		latest[i+1] = byte(i + 1)
	}
	require.NoError(t, ctx.PutBranch(recordKey, first, nil))
	require.NoError(t, ctx.PutBranch(recordKey, latest, nil))

	branch, _, err := ctx.Branch(nibbles.HexToCompact(nodePath))
	require.NoError(t, err)

	var records [16][]byte
	records[7] = latest
	want, err := commitment.SynthesizeBranchRow(0, false, records, 1<<7, nil)
	require.NoError(t, err)
	require.Equal(t, []byte(want.Data), branch)
}

// An empty record on the very first key: the row buffer is still nil there, and append(nil[:0])
// of an empty slice yields nil, not an empty slice. DomainPut rejects a nil value outright, so
// getting this wrong kills the node rather than corrupting anything quietly.
func TestLoadLatestCollectorRecordsKeepsFirstEmptyRecordNonNil(t *testing.T) {
	collector := etl.NewCollector(t.Name(), t.TempDir(), etl.NewSortableBuffer(1), log.Root())
	defer collector.Close()

	tombstoneKey := []byte{0x10, 0x80 | 1}
	laterKey := []byte{0x10, 0x80 | 2}
	require.NoError(t, collector.Collect(tombstoneKey, []byte{}))
	require.NoError(t, collector.Collect(laterKey, []byte{5}))

	var got [][]byte
	err := loadLatestCollectorRecords(collector, func(_, v []byte) error {
		got = append(got, cloneBytesPreserveNil(v))
		return nil
	})
	require.NoError(t, err)
	require.Len(t, got, 2)
	require.NotNil(t, got[0], "an empty record must stay non-nil: DomainPut refuses a nil value")
	require.Empty(t, got[0])
	require.Equal(t, []byte{5}, got[1])
}

func TestLoadLatestCollectorRecordsResolvesEachRecord(t *testing.T) {
	collector := etl.NewCollector(t.Name(), t.TempDir(), etl.NewSortableBuffer(1), log.Root())
	defer collector.Close()

	key := []byte{0x10, 0x80 | 3}
	tombstoneKey := []byte{0x10, 0x80 | 4}
	otherKey := []byte{0x10, 0x80 | 5}
	require.NoError(t, collector.Collect(key, []byte{1}))
	require.NoError(t, collector.Collect(key, []byte{2, 3}))
	require.NoError(t, collector.Collect(tombstoneKey, []byte{4}))
	require.NoError(t, collector.Collect(tombstoneKey, []byte{}))
	require.NoError(t, collector.Collect(otherKey, []byte{4}))
	// A long row followed by a short one under the same key: the reused row buffer must report the
	// short value's length, not carry the tail of the long one.
	shrinkKey := []byte{0x10, 0x80 | 6}
	require.NoError(t, collector.Collect(shrinkKey, []byte{9, 9, 9, 9, 9, 9}))
	require.NoError(t, collector.Collect(shrinkKey, []byte{7}))

	var got []struct {
		key []byte
		val []byte
	}
	err := loadLatestCollectorRecords(collector, func(k, v []byte) error {
		got = append(got, struct {
			key []byte
			val []byte
		}{bytes.Clone(k), cloneBytesPreserveNil(v)})
		return nil
	})
	require.NoError(t, err)
	require.Len(t, got, 4)
	require.Equal(t, key, got[0].key)
	require.Equal(t, []byte{2, 3}, got[0].val)
	require.Equal(t, tombstoneKey, got[1].key)
	require.NotNil(t, got[1].val)
	require.Empty(t, got[1].val)
	require.Equal(t, otherKey, got[2].key)
	require.Equal(t, []byte{4}, got[2].val)
	require.Equal(t, shrinkKey, got[3].key)
	require.Equal(t, []byte{7}, got[3].val)
}
