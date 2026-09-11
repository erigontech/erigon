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
	"bytes"
	"encoding/binary"
	"sort"
	"testing"

	"github.com/c2h5oh/datasize"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/mdbx"
	"github.com/erigontech/erigon/db/kv/mdbx/mdbxtest"
	"github.com/erigontech/erigon/db/kv/temporal"
	statepkg "github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/db/state/changeset"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/execution/commitment/nibbles"
)

type commitmentV3Update struct {
	txNum uint64
	key   []byte
	value []byte
}

func newCommitmentV3TestDB(t *testing.T, stepSize uint64) kv.TemporalRwDB {
	t.Helper()
	logger := log.New()
	dirs := datadir.New(t.TempDir())
	rawDB := mdbxtest.InMem(t, mdbx.New(dbcfg.ChainDB, logger), dirs.Chaindata).
		GrowthStep(32 * datasize.MB).MapSize(2 * datasize.GB).MustOpen()
	t.Cleanup(rawDB.Close)
	agg := statepkg.NewTest(dirs).StepSize(stepSize).Logger(logger).MustOpen(t.Context())
	t.Cleanup(agg.Close)
	require.NoError(t, agg.OpenFolder(rawDB))
	db, err := temporal.New(rawDB, agg, nil)
	require.NoError(t, err)
	t.Cleanup(db.Close)
	return db
}

func beginCommitmentV3Domains(t *testing.T, db kv.TemporalRwDB) (kv.TemporalRwTx, *execctx.SharedDomains) {
	t.Helper()
	tx, err := db.BeginTemporalRw(t.Context())
	require.NoError(t, err)
	t.Cleanup(tx.Rollback)
	domains, err := execctx.NewSharedDomains(t.Context(), tx, log.New())
	require.NoError(t, err)
	domains.GetCommitmentContext().SetCommitmentEdgeRecords(true)
	return tx, domains
}

func commitmentV3EdgeKey(path []byte, nibble byte) []byte {
	return nibbles.ChildKeyV3(nibbles.EncodeKeyV3(path), nibble)
}

func commitmentV3Record(seed byte) []byte {
	record := make([]byte, 35)
	record[0] = 0x10
	for i := 1; i < len(record); i++ {
		record[i] = seed + byte(i)
	}
	return record
}

func diffKey(key []byte, step kv.Step) string {
	fullKey := make([]byte, 0, len(key)+8)
	fullKey = append(fullKey, key...)
	var stepBytes [8]byte
	binary.BigEndian.PutUint64(stepBytes[:], ^uint64(step))
	fullKey = append(fullKey, stepBytes[:]...)
	return string(fullKey)
}

func denseBundledRow() []byte {
	const mask uint16 = 0xffff
	row := make([]byte, 0, 4+16*(2+32))
	row = binary.BigEndian.AppendUint16(row, mask)
	row = binary.BigEndian.AppendUint16(row, mask)
	for nibble := range 16 {
		row = append(row, 0x08, 0x20)
		for i := range 32 {
			row = append(row, byte(nibble+i))
		}
	}
	return row
}

func serializedDiff(key, value []byte, step kv.Step) []byte {
	diff := &kv.DomainDiff{}
	diff.DomainUpdate(key, step, value)
	return changeset.SerializeDiffSet(diff.GetDiffSet(), nil)
}

func TestCommitmentV3DomainPutRecordsPreviousEdgeValue(t *testing.T) {
	stepSize := uint64(100)
	db := newCommitmentV3TestDB(t, stepSize)
	key := commitmentV3EdgeKey([]byte{1, 2, 3, 4}, 7)
	oldValue := commitmentV3Record(0x10)
	newValue := commitmentV3Record(0x40)

	tx, domains := beginCommitmentV3Domains(t, db)
	require.NoError(t, domains.DomainPut(kv.CommitmentDomain, tx, key, oldValue, 1, nil))
	require.NoError(t, domains.Flush(t.Context(), tx))
	domains.Close()
	require.NoError(t, tx.Commit())

	tx, domains = beginCommitmentV3Domains(t, db)
	defer tx.Rollback()
	defer domains.Close()
	cs := &changeset.StateChangeSet{}
	domains.SetChangesetAccumulator(cs)
	prev, _, err := domains.GetLatest(kv.CommitmentDomain, tx, key)
	require.NoError(t, err)
	require.Equal(t, oldValue, prev)
	require.NoError(t, domains.DomainPut(kv.CommitmentDomain, tx, key, newValue, 2, prev))

	diffs := cs.Diffs[kv.CommitmentDomain].GetDiffSet()
	require.Len(t, diffs, 1)
	require.Equal(t, diffKey(key, 0), diffs[0].Key)
	require.Equal(t, oldValue, diffs[0].Value)
	require.NotEqual(t, newValue, diffs[0].Value)
}

func TestCommitmentV3ChangesetBytesStayWithinBundledRowBudget(t *testing.T) {
	path := []byte{1, 2, 3, 4}
	nodeKey := nibbles.EncodeKeyV3(path)
	edgeKey := commitmentV3EdgeKey(path, 7)
	edgeValue := commitmentV3Record(0x10)
	rowValue := denseBundledRow()

	edgeBytes := serializedDiff(edgeKey, edgeValue, 0)
	rowBytes := serializedDiff(nodeKey, rowValue, 0)
	require.LessOrEqual(t, len(edgeBytes), len(rowBytes),
		"one changed child must not exceed the full bundled row changeset: edge=%d row=%d", len(edgeBytes), len(rowBytes))

	var edgeDiff kv.DomainDiff
	for nibble := range 16 {
		edgeDiff.DomainUpdate(commitmentV3EdgeKey(path, byte(nibble)), 0, edgeValue)
	}
	rowDiff := &kv.DomainDiff{}
	rowDiff.DomainUpdate(nodeKey, 0, rowValue)
	require.LessOrEqual(t, len(edgeDiff.GetDiffSet()), len(rowDiff.GetDiffSet())*16,
		"edge changeset entry count exceeded the 16-child record multiplier")
}

func unwindCommitmentV3Updates(t *testing.T, stepSize, unwindTo uint64, baseline map[string][]byte, updates []commitmentV3Update) map[string][]byte {
	t.Helper()
	db := newCommitmentV3TestDB(t, stepSize)

	tx, domains := beginCommitmentV3Domains(t, db)
	for key, value := range baseline {
		require.NoError(t, domains.DomainPut(kv.CommitmentDomain, tx, []byte(key), value, 1, nil))
	}
	require.NoError(t, domains.Flush(t.Context(), tx))
	domains.Close()
	require.NoError(t, tx.Commit())

	tx, domains = beginCommitmentV3Domains(t, db)
	current := make(map[string][]byte, len(baseline))
	for key, value := range baseline {
		current[key] = bytes.Clone(value)
	}
	blockAccumulators := make(map[uint64]*changeset.StateChangeSet)
	for _, update := range updates {
		if _, ok := blockAccumulators[update.txNum]; !ok {
			acc := &changeset.StateChangeSet{}
			blockAccumulators[update.txNum] = acc
			domains.SetChangesetAccumulator(acc)
		}
		prev := current[string(update.key)]
		require.NotEmpty(t, prev, "test updates must target an existing edge record")
		require.NoError(t, domains.DomainPut(kv.CommitmentDomain, tx, update.key, update.value, update.txNum, nil))
		current[string(update.key)] = bytes.Clone(update.value)
	}
	blockChanges := make(map[uint64][]kv.DomainEntryDiff, len(blockAccumulators))
	for txNum, acc := range blockAccumulators {
		blockChanges[txNum] = acc.Diffs[kv.CommitmentDomain].GetDiffSet()
	}
	require.NoError(t, domains.Flush(t.Context(), tx))
	domains.Close()
	require.NoError(t, tx.Commit())

	tx, err := db.BeginTemporalRw(t.Context())
	require.NoError(t, err)
	t.Cleanup(tx.Rollback)
	domains, err = execctx.NewSharedDomains(t.Context(), tx, log.New())
	require.NoError(t, err)
	var txNums []uint64
	for txNum := range blockChanges {
		if txNum >= unwindTo {
			txNums = append(txNums, txNum)
		}
	}
	sort.Slice(txNums, func(i, j int) bool { return txNums[i] > txNums[j] })
	var merged []kv.DomainEntryDiff
	for _, txNum := range txNums {
		if merged == nil {
			merged = blockChanges[txNum]
		} else {
			merged = changeset.MergeDiffSets(merged, blockChanges[txNum])
		}
	}
	var diffs [kv.DomainLen][]kv.DomainEntryDiff
	diffs[kv.CommitmentDomain] = merged
	domains.Unwind(unwindTo, &diffs)
	require.NoError(t, domains.Flush(t.Context(), tx))
	domains.Close()
	require.NoError(t, tx.Commit())

	tx, err = db.BeginTemporalRw(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()
	return readCommitmentValues(t, tx, baseline)
}

func readCommitmentValues(t *testing.T, tx kv.TemporalRwTx, expected map[string][]byte) map[string][]byte {
	t.Helper()
	got := make(map[string][]byte, len(expected))
	for key := range expected {
		value, _, err := tx.GetLatest(kv.CommitmentDomain, []byte(key), kv.GetLatestOptions{})
		require.NoError(t, err)
		got[key] = bytes.Clone(value)
	}
	return got
}

func TestCommitmentV3UnwindRestoresChangedEdgeRecords(t *testing.T) {
	path := []byte{2, 4, 6, 8}
	keys := make([][]byte, 4)
	baseline := make(map[string][]byte, len(keys))
	for nibble := range 4 {
		key := commitmentV3EdgeKey(path, byte(nibble))
		keys[nibble] = key
		baseline[string(key)] = commitmentV3Record(byte(0x10 + nibble*8))
	}

	tests := []struct {
		name    string
		updates []commitmentV3Update
	}{
		{
			name: "one child",
			updates: []commitmentV3Update{
				{txNum: 2, key: keys[0], value: commitmentV3Record(0x90)},
			},
		},
		{
			name: "several children",
			updates: []commitmentV3Update{
				{txNum: 2, key: keys[0], value: commitmentV3Record(0xa0)},
				{txNum: 2, key: keys[1], value: commitmentV3Record(0xb0)},
			},
		},
		{
			name: "whole node",
			updates: []commitmentV3Update{
				{txNum: 2, key: keys[0], value: commitmentV3Record(0xc0)},
				{txNum: 2, key: keys[1], value: commitmentV3Record(0xd0)},
				{txNum: 2, key: keys[2], value: commitmentV3Record(0xe0)},
				{txNum: 2, key: keys[3], value: commitmentV3Record(0xf0)},
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := unwindCommitmentV3Updates(t, 100, 2, baseline, test.updates)
			for key, want := range baseline {
				require.Equal(t, want, got[key], "unwind changed edge record %x", key)
			}
		})
	}
}

func TestCommitmentV3UnwindAcrossStepBoundary(t *testing.T) {
	path := []byte{9, 8, 7, 6}
	key := commitmentV3EdgeKey(path, 3)
	baseline := map[string][]byte{string(key): commitmentV3Record(0x10)}
	updates := []commitmentV3Update{
		{txNum: 2, key: key, value: commitmentV3Record(0x20)},
		{txNum: 105, key: key, value: commitmentV3Record(0x30)},
	}

	got := unwindCommitmentV3Updates(t, 100, 50, baseline, updates)
	require.Equal(t, commitmentV3Record(0x20), got[string(key)],
		"unwind across a step boundary must restore the last pre-boundary edge record")
}
