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
	"github.com/erigontech/erigon/db/seg"
	"github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/commitment/eip8297"
)

func TestPBinResumeMatchesFreshConversion(t *testing.T) {
	selectPBinLeafStreamHash(t)
	baseDB, baseAgg := testDbAndAggregatorv3(t, pbinLeafStreamStepSize)
	resumeDB, resumeAgg := testDbAndAggregatorv3(t, pbinLeafStreamStepSize)
	targetDB, targetAgg := testDbAndAggregatorv3(t, pbinLeafStreamStepSize)
	freshDB, freshAgg := testDbAndAggregatorv3(t, pbinLeafStreamStepSize)
	initial := []pbinLeafStreamAccount{
		{address: pbinLeafStreamAddress(0x11), nonce: 1, balance: 1, slots: map[string][]byte{string([]byte{1}): {1}}},
		{address: pbinLeafStreamAddress(0x22), nonce: 1, balance: 1},
		{address: pbinLeafStreamAddress(0x33), nonce: 3, balance: 3},
	}
	updated := []pbinLeafStreamAccount{{address: pbinLeafStreamAddress(0x11), nonce: 2, balance: 2, slots: map[string][]byte{string([]byte{1}): {2}}}, {address: pbinLeafStreamAddress(0x22), nonce: 2, balance: 2}}
	future := []pbinLeafStreamAccount{{address: pbinLeafStreamAddress(0x11), nonce: 2, balance: 2, code: []byte{1}, codeWritten: true, slots: map[string][]byte{string([]byte{1}): {2}}}, {address: pbinLeafStreamAddress(0x22), nonce: 2, balance: 2}}
	writePBinLeafStreamRange(t, baseDB, 0, pbinLeafStreamStepSize, initial)
	require.NoError(t, baseAgg.BuildFiles2(t.Context(), baseDB, 0, 1, unboundedFinalityCtx, false))
	writePBinLeafStreamRange(t, targetDB, 0, pbinLeafStreamStepSize, initial)
	writePBinLeafStreamStates(t, targetDB, pbinLeafStreamStepSize, updated)
	writePBinLeafStreamStates(t, targetDB, 2*pbinLeafStreamStepSize, future)
	require.NoError(t, targetAgg.BuildFiles2(t.Context(), targetDB, 0, 3, unboundedFinalityCtx, false))
	for _, db := range []kv.TemporalRwDB{resumeDB, freshDB} {
		writePBinLeafStreamRange(t, db, 0, pbinLeafStreamStepSize, initial)
		writePBinLeafStreamStates(t, db, pbinLeafStreamStepSize, updated)
	}
	require.NoError(t, resumeAgg.BuildFiles2(t.Context(), resumeDB, 0, 2, unboundedFinalityCtx, false))
	require.NoError(t, freshAgg.BuildFiles2(t.Context(), freshDB, 0, 2, unboundedFinalityCtx, false))
	resumeAgg.WaitForFiles()
	freshAgg.WaitForFiles()
	baseRoot := convertPBinBetween(t, baseDB, baseAgg, targetDB, targetAgg, 8)
	require.NotEqual(t, common.Hash{}, baseRoot)
	targetAgg.WaitForFiles()
	at := targetAgg.BeginFilesRo()
	value, found, start, end, stateErr := at.DebugGetLatestFromFiles(kv.CommitmentDomain, commitment.KeyCommitmentState, ^uint64(0))
	require.NoError(t, stateErr)
	require.True(t, found)
	require.Equal(t, uint64(8), start)
	require.Equal(t, uint64(16), end)
	require.NotEmpty(t, value)
	at.Close()
	resumeRoot := resumePBinAt(t, resumeDB, resumeAgg, targetDB, targetAgg, 16)
	freshRoot := convertPBinAt(t, freshDB, freshAgg, 16)
	resumeRows := pbinCommitmentRows(t, targetAgg)
	freshRows := pbinCommitmentRows(t, freshAgg)
	delete(resumeRows, string(commitment.KeyCommitmentState))
	delete(freshRows, string(commitment.KeyCommitmentState))
	require.Equal(t, freshRoot, resumeRoot)
	require.Equal(t, freshRows, resumeRows)
	requireResumeStateFile(t, targetAgg, 16, 24)
}

func convertPBinBetween(t *testing.T, sourceDB kv.TemporalRwDB, sourceAgg *state.Aggregator, targetDB kv.TemporalRwDB, targetAgg *state.Aggregator, endTxNum uint64) common.Hash {
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
	})
	require.NoError(t, err)
	return root
}

func TestPBinResumeEmptyGapKeepsBaseRoot(t *testing.T) {
	selectPBinLeafStreamHash(t)
	baseDB, baseAgg := testDbAndAggregatorv3(t, pbinLeafStreamStepSize)
	targetDB, targetAgg := testDbAndAggregatorv3(t, pbinLeafStreamStepSize)
	account := []pbinLeafStreamAccount{{address: pbinLeafStreamAddress(0x11), nonce: 1, balance: 1}}
	targetFuture := []pbinLeafStreamAccount{{address: pbinLeafStreamAddress(0x11), nonce: 2, balance: 2}}
	writePBinLeafStreamRange(t, baseDB, 0, pbinLeafStreamStepSize, account)
	require.NoError(t, baseAgg.BuildFiles2(t.Context(), baseDB, 0, 1, unboundedFinalityCtx, false))
	writePBinLeafStreamRange(t, targetDB, 0, pbinLeafStreamStepSize, account)
	writePBinLeafStreamStates(t, targetDB, pbinLeafStreamStepSize, targetFuture)
	writePBinLeafStreamStates(t, targetDB, 2*pbinLeafStreamStepSize, targetFuture)
	require.NoError(t, targetAgg.BuildFiles2(t.Context(), targetDB, 0, 3, unboundedFinalityCtx, false))
	targetAgg.WaitForFiles()
	baseRoot := convertPBinBetween(t, baseDB, baseAgg, targetDB, targetAgg, 8)
	resumeRoot := resumePBinAt(t, baseDB, baseAgg, targetDB, targetAgg, 16)
	require.Equal(t, baseRoot, resumeRoot)
	requireResumeStateFile(t, targetAgg, 16, 24)
	emptyRows := pbinRangeRows(t, targetAgg, 16, 24)
	require.Len(t, emptyRows, 1)
	_, ok := emptyRows[string(commitment.KeyCommitmentState)]
	require.True(t, ok)
	rows := pbinCommitmentRows(t, targetAgg)
	for key, value := range rows {
		if key == string(commitment.KeyCommitmentState) {
			continue
		}
		require.NotEmpty(t, value)
	}
}

func TestPBinResumeDeletedSubtreeKeepsTombstones(t *testing.T) {
	selectPBinLeafStreamHash(t)
	baseDB, baseAgg := testDbAndAggregatorv3(t, pbinLeafStreamStepSize)
	resumeDB, resumeAgg := testDbAndAggregatorv3(t, pbinLeafStreamStepSize)
	targetDB, targetAgg := testDbAndAggregatorv3(t, pbinLeafStreamStepSize)
	account := []pbinLeafStreamAccount{{address: pbinLeafStreamAddress(0x11), nonce: 1, balance: 1, slots: map[string][]byte{string([]byte{1}): {1}}}}
	writePBinLeafStreamRange(t, baseDB, 0, pbinLeafStreamStepSize, account)
	require.NoError(t, baseAgg.BuildFiles2(t.Context(), baseDB, 0, 1, unboundedFinalityCtx, false))
	writePBinLeafStreamRange(t, resumeDB, 0, pbinLeafStreamStepSize, account)
	deletePBinResumeAccount(t, resumeDB, account[0].address, pbinLeafStreamStepSize)
	deletePBinResumeStorage(t, resumeDB, account[0].address, []byte{1}, pbinLeafStreamStepSize)
	require.NoError(t, resumeAgg.BuildFiles2(t.Context(), resumeDB, 0, 2, unboundedFinalityCtx, false))
	writePBinLeafStreamRange(t, targetDB, 0, pbinLeafStreamStepSize, account)
	writePBinLeafStreamStates(t, targetDB, pbinLeafStreamStepSize, account)
	writePBinLeafStreamStates(t, targetDB, 2*pbinLeafStreamStepSize, account)
	require.NoError(t, targetAgg.BuildFiles2(t.Context(), targetDB, 0, 3, unboundedFinalityCtx, false))
	targetAgg.WaitForFiles()
	baseRoot := convertPBinBetween(t, baseDB, baseAgg, targetDB, targetAgg, 8)
	resumeRoot := resumePBinAt(t, resumeDB, resumeAgg, targetDB, targetAgg, 16)
	require.NotEqual(t, baseRoot, resumeRoot)
	require.Equal(t, eip8297.EmptyTreeHash, resumeRoot)
	requireResumeStateFile(t, targetAgg, 16, 24)
	var cache eip8297.DigestCache
	headerPrefix := cache.AccountHeaderStem(account[0].address)
	storagePrefix := cache.AccountStoragePrefix(account[0].address)
	rows := pbinRangeRows(t, targetAgg, 16, 24)
	var tombstones int
	for key, value := range rows {
		if bytes.HasPrefix([]byte(key), headerPrefix) || bytes.HasPrefix([]byte(key), storagePrefix) {
			require.Empty(t, value, "deleted subtree row %x is not a tombstone", key)
			tombstones++
		}
	}
	require.NotZero(t, tombstones)
	latestRows := pbinCommitmentRows(t, targetAgg)
	for key := range latestRows {
		require.False(t, bytes.HasPrefix([]byte(key), headerPrefix) || bytes.HasPrefix([]byte(key), storagePrefix), "deleted subtree row %x remains visible", key)
	}
}

func convertPBinAt(t *testing.T, db kv.TemporalRwDB, agg *state.Aggregator, endTxNum uint64) common.Hash {
	t.Helper()
	sourceTx, err := db.BeginTemporalRo(t.Context())
	require.NoError(t, err)
	defer sourceTx.Rollback()
	targetTx, err := db.BeginTemporalRw(t.Context())
	require.NoError(t, err)
	defer targetTx.Rollback()
	root, err := state.ConvertPBin(t.Context(), state.PBinConvertOptions{
		SourceAggregator: agg,
		SourceTx:         sourceTx,
		TargetAggregator: agg,
		TargetTx:         targetTx,
		TargetDomain:     kv.CommitmentDomain,
		EndTxNum:         endTxNum,
		Hash:             eip8297.HashBytes,
	})
	require.NoError(t, err)
	return root
}

func resumePBinAt(t *testing.T, sourceDB kv.TemporalRwDB, sourceAgg *state.Aggregator, targetDB kv.TemporalRwDB, targetAgg *state.Aggregator, endTxNum uint64) common.Hash {
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
		Resume:           &state.PBinResumeOptions{BaseTxNum: 8, SourceTxNum: endTxNum, Workers: 4, PostCancun: true},
	})
	require.NoError(t, err)
	return root
}

func pbinCommitmentRows(t *testing.T, agg *state.Aggregator) map[string][]byte {
	t.Helper()
	at := agg.BeginFilesRo()
	defer at.Close()
	iter, err := at.DebugRangeLatestFromFiles(kv.CommitmentDomain, nil, nil, kv.Unlim)
	require.NoError(t, err)
	defer iter.Close()
	rows := make(map[string][]byte)
	for iter.HasNext() {
		key, value, err := iter.Next()
		require.NoError(t, err)
		rows[string(key)] = append([]byte(nil), value...)
	}
	return rows
}

func pbinRangeRows(t *testing.T, agg *state.Aggregator, wantStart, wantEnd uint64) map[string][]byte {
	t.Helper()
	at := agg.BeginFilesRo()
	defer at.Close()
	rows := make(map[string][]byte)
	for _, file := range at.Files(kv.CommitmentDomain) {
		if file.StartRootNum() != wantStart || file.EndRootNum() != wantEnd {
			continue
		}
		decompressor, err := seg.NewDecompressor(file.Fullpath())
		require.NoError(t, err)
		reader := seg.NewReader(decompressor.MakeGetter(), seg.CompressNone)
		for reader.HasNext() {
			key, _ := reader.Next(nil)
			value, _ := reader.Next(nil)
			rows[string(key)] = append([]byte(nil), value...)
		}
		decompressor.Close()
	}
	return rows
}

func requireResumeStateFile(t *testing.T, agg *state.Aggregator, wantStart, wantEnd uint64) {
	t.Helper()
	at := agg.BeginFilesRo()
	defer at.Close()
	var foundRange bool
	for _, file := range at.Files(kv.CommitmentDomain) {
		if file.StartRootNum() == wantStart && file.EndRootNum() == wantEnd {
			foundRange = true
		}
	}
	require.True(t, foundRange)
	value, found, start, end, err := at.DebugGetLatestFromFiles(kv.CommitmentDomain, commitment.KeyCommitmentState, ^uint64(0))
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, wantStart, start)
	require.Equal(t, wantEnd, end)
	require.NotEmpty(t, value)
}
