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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/commitment/eip8297"
)

func TestPBinR21ResumeZeroStartPreservesUntouchedBaseRows(t *testing.T) {
	pbinR21CheckResumeRows(t, 8, 0, 8, 16, [][2]uint64{{0, 16}, {16, 24}}, nil)
}

func TestPBinR21ResumeNonzeroSpanPreservesUntouchedBaseRows(t *testing.T) {
	pbinR21CheckResumeRows(t, 1, 8, 16, 24, [][2]uint64{{0, 8}, {8, 24}, {24, 32}}, [][2]uint64{{0, 8}, {8, 12}, {12, 16}})
}

func TestPBinR21ResumeAtRangeLastStamp(t *testing.T) {
	pbinR21CheckResumeRows(t, 1, 8, 23, 24, [][2]uint64{{0, 8}, {8, 24}, {24, 32}}, [][2]uint64{{0, 8}, {8, 24}})
}

func pbinR21CheckResumeRows(t *testing.T, step, initialFrom, baseTx, endTx uint64, sourceRanges, baseMerges [][2]uint64) {
	t.Helper()
	selectPBinLeafStreamHash(t)
	baseDB, baseAgg := testDbAndAggregatorv3(t, step)
	sourceDB, sourceAgg := testDbAndAggregatorv3(t, step)
	targetDB, targetAgg := testDbAndAggregatorv3(t, step)
	freshDB, freshAgg := testDbAndAggregatorv3(t, step)
	initial := []pbinLeafStreamAccount{
		{address: pbinLeafStreamAddress(0x11), nonce: 1, balance: 1, slots: map[string][]byte{string([]byte{1}): {1}}},
		{address: pbinLeafStreamAddress(0x22), nonce: 1, balance: 1},
		{address: pbinLeafStreamAddress(0x33), nonce: 3, balance: 3},
	}
	updated := []pbinLeafStreamAccount{
		{address: pbinLeafStreamAddress(0x11), nonce: 2, balance: 2, slots: map[string][]byte{string([]byte{1}): {2}}},
		{address: pbinLeafStreamAddress(0x22), nonce: 2, balance: 2},
	}
	for _, db := range []kv.TemporalRwDB{baseDB, sourceDB, targetDB, freshDB} {
		writePBinLeafStreamRange(t, db, initialFrom, initialFrom+8, initial)
	}
	for _, db := range []kv.TemporalRwDB{sourceDB, targetDB, freshDB} {
		writePBinLeafStreamStates(t, db, baseTx, updated)
	}
	writePBinLeafStreamStates(t, targetDB, endTx, updated)
	require.NoError(t, baseAgg.BuildFiles2(t.Context(), baseDB, 0, kv.Step(baseTx/step), unboundedFinalityCtx, false))
	require.NoError(t, sourceAgg.BuildFiles2(t.Context(), sourceDB, 0, kv.Step(endTx/step), unboundedFinalityCtx, false))
	require.NoError(t, freshAgg.BuildFiles2(t.Context(), freshDB, 0, kv.Step(endTx/step), unboundedFinalityCtx, false))
	require.NoError(t, targetAgg.BuildFiles2(t.Context(), targetDB, 0, kv.Step(sourceRanges[len(sourceRanges)-1][1]/step), unboundedFinalityCtx, false))
	baseAgg.WaitForFiles()
	sourceAgg.WaitForFiles()
	freshAgg.WaitForFiles()
	targetAgg.WaitForFiles()
	convertPBinBetween(t, baseDB, baseAgg, targetDB, targetAgg, baseTx)
	for _, bounds := range baseMerges {
		require.NoError(t, targetAgg.MergePBinRange(t.Context(), kv.CommitmentDomain, bounds[0], bounds[1]))
	}
	freshRoot := convertPBinAt(t, freshDB, freshAgg, endTx)
	resumeRoot := pbinR21ResumeAt(t, sourceDB, sourceAgg, targetDB, targetAgg, baseTx, endTx, sourceRanges)
	resumeRows := pbinCommitmentRows(t, targetAgg)
	freshRows := pbinCommitmentRows(t, freshAgg)
	delete(resumeRows, string(commitment.KeyCommitmentState))
	delete(freshRows, string(commitment.KeyCommitmentState))
	require.Equal(t, freshRoot, resumeRoot)
	require.Equal(t, len(freshRows), len(resumeRows), "resume spanning range must preserve untouched base rows")
	require.Equal(t, freshRows, resumeRows)
}

func TestPBinR21ZeroStartDropsDeletionTombstones(t *testing.T) {
	selectPBinLeafStreamHash(t)
	baseDB, baseAgg := testDbAndAggregatorv3(t, 8)
	sourceDB, sourceAgg := testDbAndAggregatorv3(t, 8)
	targetDB, targetAgg := testDbAndAggregatorv3(t, 8)
	freshDB, freshAgg := testDbAndAggregatorv3(t, 8)
	initial := []pbinLeafStreamAccount{
		{address: pbinLeafStreamAddress(0x11), nonce: 1, balance: 1, slots: map[string][]byte{string([]byte{1}): {1}}},
		{address: pbinLeafStreamAddress(0x22), nonce: 1, balance: 1},
		{address: pbinLeafStreamAddress(0x33), nonce: 1, balance: 1},
	}
	updated := []pbinLeafStreamAccount{
		{address: initial[1].address, nonce: 2, balance: 2},
		{address: initial[2].address, nonce: 2, balance: 2},
	}
	for _, db := range []kv.TemporalRwDB{baseDB, sourceDB, targetDB, freshDB} {
		writePBinLeafStreamRange(t, db, 0, 8, initial)
		deletePBinResumeKey(t, db, kv.AccountsDomain, initial[2].address, 7)
	}
	for _, db := range []kv.TemporalRwDB{sourceDB, freshDB} {
		deletePBinResumeKey(t, db, kv.AccountsDomain, initial[0].address, 8)
		deletePBinResumeKey(t, db, kv.StorageDomain, append(bytes.Clone(initial[0].address), 1), 8)
		writePBinLeafStreamStates(t, db, 8, updated)
	}
	writePBinLeafStreamStates(t, targetDB, 8, updated)
	writePBinLeafStreamStates(t, targetDB, 16, updated)
	require.NoError(t, baseAgg.BuildFiles2(t.Context(), baseDB, 0, 1, unboundedFinalityCtx, false))
	require.NoError(t, sourceAgg.BuildFiles2(t.Context(), sourceDB, 0, 2, unboundedFinalityCtx, false))
	require.NoError(t, freshAgg.BuildFiles2(t.Context(), freshDB, 0, 2, unboundedFinalityCtx, false))
	require.NoError(t, targetAgg.BuildFiles2(t.Context(), targetDB, 0, 3, unboundedFinalityCtx, false))
	targetAgg.WaitForFiles()
	convertPBinBetween(t, baseDB, baseAgg, targetDB, targetAgg, 8)
	resumeRoot := resumePBinAtWithRanges(t, sourceDB, sourceAgg, targetDB, targetAgg, 16, [][2]uint64{{0, 16}, {16, 24}})
	freshRoot := convertPBinAt(t, freshDB, freshAgg, 16)
	require.Equal(t, freshRoot, resumeRoot)
	got, want := pbinCommitmentRows(t, targetAgg), pbinCommitmentRows(t, freshAgg)
	delete(got, string(commitment.KeyCommitmentState))
	delete(want, string(commitment.KeyCommitmentState))
	require.Equal(t, want, got, "deletion, update and recreation must match fresh conversion")
	requireResumeStateFile(t, targetAgg, 16, 24)
	var tombstones int
	for _, value := range pbinRangeRows(t, targetAgg, 0, 16) {
		if len(value) == 0 {
			tombstones++
		}
	}
	require.Zero(t, tombstones, "zero-start output must drop deletion tombstones after merging base rows")
}

func pbinR21ResumeAt(t *testing.T, sourceDB kv.TemporalRwDB, sourceAgg *state.Aggregator, targetDB kv.TemporalRwDB, targetAgg *state.Aggregator, baseTxNum, endTxNum uint64, sourceRanges [][2]uint64) common.Hash {
	t.Helper()
	sourceTx, err := sourceDB.BeginTemporalRo(t.Context())
	require.NoError(t, err)
	defer sourceTx.Rollback()
	targetTx, err := targetDB.BeginTemporalRw(t.Context())
	require.NoError(t, err)
	defer targetTx.Rollback()
	root, err := state.ConvertPBin(t.Context(), state.PBinConvertOptions{
		SourceAggregator: sourceAgg,
		SourceTx:         sourceTx,
		TargetAggregator: targetAgg,
		TargetTx:         targetTx,
		TargetDB:         targetDB,
		TargetDomain:     kv.CommitmentDomain,
		EndTxNum:         endTxNum,
		Hash:             eip8297.HashBytes,
		Workers:          4,
		Resume:           &state.PBinResumeOptions{BaseTxNum: baseTxNum, SourceRanges: sourceRanges, SourceTxNum: endTxNum, Workers: 4, PostCancun: true},
	})
	require.NoError(t, err)
	return root
}
