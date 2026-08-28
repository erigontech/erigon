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

package state

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/background"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/config3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/state/statecfg"
	"github.com/erigontech/erigon/execution/commitment/nibbles"
)

func TestScanCommitmentRecordRunKeepsTombstoneAheadOfOlderFile(t *testing.T) {
	t.Parallel()

	nodeKey := nibbles.EncodeKeyV3([]byte{2, 4})
	childKey := nibbles.ChildKeyV3(nodeKey, 7)
	wanted := uint16(1 << 7)
	newest := []commitmentRecordTestEntry{{key: childKey, val: []byte{}}}
	older := []commitmentRecordTestEntry{{key: childKey, val: []byte{0x99}}}
	var records [16][]byte

	present, err := scanCommitmentRecordRun(nodeKey, wanted, 0, &records, commitmentRecordTestSeek(newest, new(int), new(int)))
	require.NoError(t, err)
	require.Equal(t, wanted, present)
	require.NotNil(t, records[7])
	require.Empty(t, records[7])

	present, err = scanCommitmentRecordRun(nodeKey, wanted, present, &records, commitmentRecordTestSeek(older, new(int), new(int)))
	require.NoError(t, err)
	require.Equal(t, wanted, present)
	require.Empty(t, records[7], "an older file must not resurrect a deleted child")
}

func mergeCommitmentTombstoneInput(t *testing.T, value []byte) []byte {
	t.Helper()

	db, domain := testDbAndDomainOfStep(t, statecfg.Schema.CommitmentDomain, 1, log.New())
	defer db.Close()
	defer domain.Close()

	tx, err := db.BeginRw(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()

	key := []byte{0x10, 0x80}
	domainTx := domain.beginForTests()
	writer := domainTx.NewWriter()
	require.NoError(t, writer.PutWithPrev(key, value, 0, nil))
	require.NoError(t, writer.Flush(t.Context(), tx))
	writer.Close()
	domainTx.Close()

	for step := range kv.Step(4) {
		require.NoError(t, domain.collateBuildIntegrate(t.Context(), step, tx, background.NewProgressSet()))
	}

	domainTx = domain.beginForTests()
	ranges := domainTx.findMergeRange(domainTx.files.EndTxNum(), config3.DefaultStepsInFrozenFile, config3.DefaultStepsInFrozenFile)
	require.True(t, ranges.values.needMerge)
	require.Equal(t, uint64(0), ranges.values.from)
	require.Equal(t, uint64(4), ranges.values.to)
	valuesOut, indexOut, historyOut := domainTx.staticFilesInRange(ranges)
	valuesIn, indexIn, historyIn, err := domainTx.mergeFiles(t.Context(), valuesOut, indexOut, historyOut, ranges, nil, true, background.NewProgressSet())
	require.NoError(t, err)
	domainTx.Close()
	require.NotNil(t, valuesIn)
	t.Cleanup(func() {
		valuesIn.closeFilesAndRemove()
		if indexIn != nil {
			indexIn.closeFilesAndRemove()
		}
		if historyIn != nil {
			historyIn.closeFilesAndRemove()
		}
	})

	reader := domain.dataReader(valuesIn.decompressor)
	reader.Reset(0)
	if !reader.HasNext() {
		return nil
	}
	gotKey, _ := reader.Next(nil)
	gotValue, _ := reader.Next(nil)
	require.False(t, reader.HasNext())
	require.Equal(t, key, gotKey)
	return gotValue
}

func TestCommitmentTombstoneBottomMerge(t *testing.T) {
	t.Run("legacy four-byte delete is copied", func(t *testing.T) {
		value := []byte{0x00, 0x01, 0x00, 0x00}
		got := mergeCommitmentTombstoneInput(t, value)
		require.Equal(t, value, got, "bottom-most merge copied legacy delete record %x", got)
	})

	t.Run("zero-length tombstone is dropped", func(t *testing.T) {
		got := mergeCommitmentTombstoneInput(t, []byte{})
		require.Nil(t, got, "merge.go must drop the zero-length tombstone, got %x", got)
	})
}
