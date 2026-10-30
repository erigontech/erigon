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
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/dir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/seg"
	"github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/commitment/eip8297"
	"github.com/stretchr/testify/require"
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
	resumeRoot := resumePBinAtWithRanges(t, resumeDB, resumeAgg, targetDB, targetAgg, 16, [][2]uint64{{0, 8}, {8, 16}, {16, 24}})
	freshRoot := convertPBinAt(t, freshDB, freshAgg, 16)
	resumeRows := pbinCommitmentRows(t, targetAgg)
	freshRows := pbinCommitmentRows(t, freshAgg)
	delete(resumeRows, string(commitment.KeyCommitmentState))
	delete(freshRows, string(commitment.KeyCommitmentState))
	require.Equal(t, freshRoot, resumeRoot)
	require.Equal(t, freshRows, resumeRows)
	deltaRows := pbinRangeRows(t, targetAgg, 8, 16)
	delete(deltaRows, string(commitment.KeyCommitmentState))
	require.NotEmpty(t, deltaRows, "gap rows must be placed in the source range containing their stamps")
	requireResumeStateFile(t, targetAgg, 16, 24)
}

func convertPBin(t *testing.T, sourceDB kv.TemporalRwDB, sourceAgg *state.Aggregator, targetDB kv.TemporalRwDB, targetAgg *state.Aggregator, endTxNum uint64, configure func(*state.PBinConvertOptions)) common.Hash {
	t.Helper()
	sourceTx, err := sourceDB.BeginTemporalRo(t.Context())
	require.NoError(t, err)
	defer sourceTx.Rollback()
	targetTx, err := targetDB.BeginTemporalRw(t.Context())
	require.NoError(t, err)
	defer targetTx.Rollback()
	options := state.PBinConvertOptions{
		SourceAggregator: sourceAgg,
		SourceTx:         sourceTx,
		TargetAggregator: targetAgg,
		TargetTx:         targetTx,
		TargetDomain:     kv.CommitmentDomain,
		EndTxNum:         endTxNum,
		Hash:             eip8297.HashBytes,
	}
	if configure != nil {
		configure(&options)
	}
	root, err := state.ConvertPBin(t.Context(), options)
	require.NoError(t, err)
	return root
}

func convertPBinBetween(t *testing.T, sourceDB kv.TemporalRwDB, sourceAgg *state.Aggregator, targetDB kv.TemporalRwDB, targetAgg *state.Aggregator, endTxNum uint64) common.Hash {
	return convertPBin(t, sourceDB, sourceAgg, targetDB, targetAgg, endTxNum, func(options *state.PBinConvertOptions) {
		options.TargetDB = targetDB
	})
}

func convertPBinAt(t *testing.T, db kv.TemporalRwDB, agg *state.Aggregator, endTxNum uint64) common.Hash {
	return convertPBin(t, db, agg, db, agg, endTxNum, nil)
}

func resumePBinAtWithRanges(t *testing.T, sourceDB kv.TemporalRwDB, sourceAgg *state.Aggregator, targetDB kv.TemporalRwDB, targetAgg *state.Aggregator, endTxNum uint64, sourceRanges [][2]uint64) common.Hash {
	return convertPBin(t, sourceDB, sourceAgg, targetDB, targetAgg, endTxNum, func(options *state.PBinConvertOptions) {
		options.TargetDB = targetDB
		options.Workers = 4
		options.Resume = &state.PBinResumeOptions{BaseTxNum: 8, SourceRanges: sourceRanges, SourceTxNum: endTxNum, Workers: 4, PostCancun: true}
	})
}

func resumePBinAtWithAbsent(t *testing.T, sourceDB kv.TemporalRwDB, sourceAgg *state.Aggregator, targetDB kv.TemporalRwDB, targetAgg *state.Aggregator, endTxNum uint64, probe state.PBinAbsentProbeFunc) common.Hash {
	return convertPBin(t, sourceDB, sourceAgg, targetDB, targetAgg, endTxNum, func(options *state.PBinConvertOptions) {
		options.TargetDB = targetDB
		options.RangeWriterLimits = &state.PBinRangeWriterLimits{AbsentProbe: probe}
		options.Workers = 4
		options.Resume = &state.PBinResumeOptions{BaseTxNum: 8, SourceTxNum: endTxNum, Workers: 4, PostCancun: true}
	})
}

func resumePBinAtWithBase(t *testing.T, sourceDB kv.TemporalRwDB, sourceAgg *state.Aggregator, targetDB kv.TemporalRwDB, targetAgg *state.Aggregator, baseTxNum, endTxNum uint64, sourceRanges [][2]uint64) common.Hash {
	return convertPBin(t, sourceDB, sourceAgg, targetDB, targetAgg, endTxNum, func(options *state.PBinConvertOptions) {
		options.TargetDB = targetDB
		options.Workers = 4
		options.Resume = &state.PBinResumeOptions{BaseTxNum: baseTxNum, SourceRanges: sourceRanges, SourceTxNum: endTxNum, Workers: 4, PostCancun: true}
	})
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
	deletePBinResumeKey(t, resumeDB, kv.AccountsDomain, account[0].address, pbinLeafStreamStepSize)
	deletePBinResumeKey(t, resumeDB, kv.StorageDomain, append(bytes.Clone(account[0].address), 1), pbinLeafStreamStepSize)
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

func resumePBinAt(t *testing.T, sourceDB kv.TemporalRwDB, sourceAgg *state.Aggregator, targetDB kv.TemporalRwDB, targetAgg *state.Aggregator, endTxNum uint64) common.Hash {
	return resumePBinAtWithRanges(t, sourceDB, sourceAgg, targetDB, targetAgg, endTxNum, nil)
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

func TestPBinResumeRejectsBasePastY(t *testing.T) {
	selectPBinLeafStreamHash(t)
	baseDB, baseAgg := testDbAndAggregatorv3(t, 8)
	targetDB, targetAgg := testDbAndAggregatorv3(t, 8)
	initial := []pbinLeafStreamAccount{{address: append(make([]byte, 19), 1), nonce: 1, balance: 1}}
	writePBinLeafStreamRange(t, baseDB, 0, 8, initial)
	require.NoError(t, baseAgg.BuildFiles2(t.Context(), baseDB, 0, 1, unboundedFinalityCtx, false))
	baseAgg.WaitForFiles()
	writePBinLeafStreamRange(t, targetDB, 0, 32, initial)
	require.NoError(t, targetAgg.BuildFiles2(t.Context(), targetDB, 0, 4, unboundedFinalityCtx, false))
	targetAgg.WaitForFiles()
	convertPBinBetween(t, baseDB, baseAgg, targetDB, targetAgg, 8)
	resumePBinAt(t, baseDB, baseAgg, targetDB, targetAgg, 24)
	sourceTx, err := baseDB.BeginTemporalRo(t.Context())
	require.NoError(t, err)
	defer sourceTx.Rollback()
	targetTx, err := targetDB.BeginTemporalRw(t.Context())
	require.NoError(t, err)
	defer targetTx.Rollback()
	root, err := state.ConvertPBin(t.Context(), state.PBinConvertOptions{
		SourceAggregator: baseAgg,
		SourceTx:         sourceTx,
		TargetAggregator: targetAgg,
		TargetTx:         targetTx,
		TargetDB:         targetDB,
		TargetDomain:     kv.CommitmentDomain,
		EndTxNum:         16,
		Hash:             eip8297.HashBytes,
		Workers:          4,
		Resume:           &state.PBinResumeOptions{BaseTxNum: 8, SourceTxNum: 16, PostCancun: true, Workers: 4},
	})
	require.Error(t, err, "resume accepted base state at 24 beyond Y=16: err=%v root=%x", err, root)
}

func TestPBinResumeConsultsBaseRowsForAbsent(t *testing.T) {
	selectPBinLeafStreamHash(t)
	baseDB, baseAgg := testDbAndAggregatorv3(t, pbinLeafStreamStepSize)
	sourceDB, sourceAgg := testDbAndAggregatorv3(t, pbinLeafStreamStepSize)
	targetDB, targetAgg := testDbAndAggregatorv3(t, pbinLeafStreamStepSize)
	base := []pbinLeafStreamAccount{
		{address: pbinLeafStreamAddress(0x11), nonce: 1, balance: 1},
		{address: pbinLeafStreamAddress(0x22), nonce: 1, balance: 1},
		{address: pbinLeafStreamAddress(0x33), nonce: 1, balance: 1},
	}
	updated := []pbinLeafStreamAccount{
		{address: pbinLeafStreamAddress(0x11), nonce: 2, balance: 2},
		{address: pbinLeafStreamAddress(0x22), nonce: 2, balance: 2},
		{address: pbinLeafStreamAddress(0x44), nonce: 1, balance: 1},
	}
	writePBinLeafStreamRange(t, baseDB, 0, 8, base)
	require.NoError(t, baseAgg.BuildFiles2(t.Context(), baseDB, 0, 1, unboundedFinalityCtx, false))
	baseAgg.WaitForFiles()
	writePBinLeafStreamRange(t, sourceDB, 0, 8, base)
	writePBinLeafStreamStates(t, sourceDB, 8, updated)
	require.NoError(t, sourceAgg.BuildFiles2(t.Context(), sourceDB, 0, 2, unboundedFinalityCtx, false))
	sourceAgg.WaitForFiles()
	writePBinLeafStreamRange(t, targetDB, 0, 8, base)
	writePBinLeafStreamStates(t, targetDB, 8, updated)
	writePBinLeafStreamStates(t, targetDB, 16, updated)
	require.NoError(t, targetAgg.BuildFiles2(t.Context(), targetDB, 0, 3, unboundedFinalityCtx, false))
	targetAgg.WaitForFiles()
	convertPBinBetween(t, baseDB, baseAgg, targetDB, targetAgg, 8)
	rows := pbinCommitmentRows(t, targetAgg)
	var baseKey []byte
	for key := range rows {
		if key != string(commitment.KeyCommitmentState) {
			baseKey = []byte(key)
			break
		}
	}
	require.NotEmpty(t, baseKey)
	var result atomic.Int32
	probe := func(isAbsent func([]byte) bool) {
		if isAbsent(baseKey) {
			result.Store(1)
			return
		}
		result.Store(2)
	}
	resumePBinAtWithAbsent(t, sourceDB, sourceAgg, targetDB, targetAgg, 16, probe)
	require.Equal(t, int32(2), result.Load(), "resume must consult an existing base row through Absent")
}

func TestPBinResumeRejectsFilesPastBase(t *testing.T) {
	selectPBinLeafStreamHash(t)
	baseDB, baseAgg := testDbAndAggregatorv3(t, 8)
	targetDB, targetAgg := testDbAndAggregatorv3(t, 8)
	initial := []pbinLeafStreamAccount{{address: pbinLeafStreamAddress(1), nonce: 1, balance: 1}}
	writePBinLeafStreamRange(t, baseDB, 0, 24, initial)
	require.NoError(t, baseAgg.BuildFiles2(t.Context(), baseDB, 0, 3, unboundedFinalityCtx, false))
	baseAgg.WaitForFiles()
	writePBinLeafStreamRange(t, targetDB, 0, 48, initial)
	require.NoError(t, targetAgg.BuildFiles2(t.Context(), targetDB, 0, 6, unboundedFinalityCtx, false))
	targetAgg.WaitForFiles()
	convertPBinBetween(t, baseDB, baseAgg, targetDB, targetAgg, 24)
	var files []string
	require.NoError(t, filepath.WalkDir(targetAgg.Dirs().Snap, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !entry.IsDir() && strings.Contains(entry.Name(), "commitment.3-4.") {
			files = append(files, path)
		}
		return nil
	}))
	require.NotEmpty(t, files)
	for _, file := range files {
		destination := strings.Replace(file, "commitment.3-4.", "commitment.4-5.", 1)
		require.NoError(t, dir.RemoveAll(destination))
		require.NoError(t, os.Link(file, destination))
	}
	require.NoError(t, targetAgg.ReloadFiles())
	at := targetAgg.BeginFilesRo()
	var future bool
	for _, file := range at.Files(kv.CommitmentDomain) {
		if file.StartRootNum() == 32 && file.EndRootNum() == 40 {
			future = true
		}
	}
	at.Close()
	require.True(t, future, "the target must expose the extra binary file [32,40)")
	require.NotEmpty(t, pbinRangeRows(t, targetAgg, 32, 40), "the file past X must contain rows")
	_, err := state.NewPBinRangeWriterForResume(targetAgg, kv.CommitmentDomain, 24, 48)
	require.Error(t, err, "resume accepted binary file [32,40) past base X=24 because it is before Y=48; got %v", err)
}

func TestPBinResumeRejectsCopiedFilesPastBase(t *testing.T) {
	for _, baseTx := range []uint64{23, 24} {
		t.Run(fmt.Sprint(baseTx), func(t *testing.T) {
			fromStep := baseTx / 8
			baseRange := fmt.Sprintf("commitment.%d-%d.", fromStep, fromStep+1)
			futureRange := fmt.Sprintf("commitment.%d-%d.", fromStep+1, fromStep+2)
			futureStart, futureEnd := (fromStep+1)*8, (fromStep+2)*8
			selectPBinLeafStreamHash(t)
			baseDB, baseAgg := testDbAndAggregatorv3(t, 8)
			targetDB, targetAgg := testDbAndAggregatorv3(t, 8)
			initial := []pbinLeafStreamAccount{{address: pbinLeafStreamAddress(1), nonce: 1, balance: 1}}
			writePBinLeafStreamRange(t, baseDB, 0, 24, initial)
			require.NoError(t, baseAgg.BuildFiles2(t.Context(), baseDB, 0, 3, unboundedFinalityCtx, false))
			baseAgg.WaitForFiles()
			writePBinLeafStreamRange(t, targetDB, 0, 48, initial)
			require.NoError(t, targetAgg.BuildFiles2(t.Context(), targetDB, 0, 6, unboundedFinalityCtx, false))
			targetAgg.WaitForFiles()
			convertPBinBetween(t, baseDB, baseAgg, targetDB, targetAgg, baseTx)
			var files []string
			require.NoError(t, filepath.WalkDir(targetAgg.Dirs().Snap, func(path string, entry os.DirEntry, err error) error {
				if err != nil {
					return err
				}
				if !entry.IsDir() && strings.Contains(entry.Name(), baseRange) {
					files = append(files, path)
				}
				return nil
			}))
			require.NotEmpty(t, files)
			for _, file := range files {
				destination := strings.Replace(file, baseRange, futureRange, 1)
				require.NoError(t, dir.RemoveAll(destination))
				data, err := os.ReadFile(file)
				require.NoError(t, err)
				require.NoError(t, os.WriteFile(destination, data, 0o644))
			}
			require.NoError(t, targetAgg.ReloadFiles())
			at := targetAgg.BeginFilesRo()
			var future bool
			for _, file := range at.Files(kv.CommitmentDomain) {
				if file.StartRootNum() == futureStart && file.EndRootNum() == futureEnd {
					future = true
				}
			}
			at.Close()
			require.True(t, future, "the target must expose the extra binary file [%d,%d)", futureStart, futureEnd)
			require.NotEmpty(t, pbinRangeRows(t, targetAgg, futureStart, futureEnd), "the file past X must contain rows")
			_, err := state.NewPBinRangeWriterForResume(targetAgg, kv.CommitmentDomain, baseTx, 48)
			require.Error(t, err, "resume accepted copied binary file [%d,%d) past base X=%d; got %v", futureStart, futureEnd, baseTx, err)
		})
	}
}

func TestPBinResumeZeroStartPreservesUntouchedBaseRows(t *testing.T) {
	pbinCheckResumeRows(t, 8, 0, 8, 16, [][2]uint64{{0, 16}, {16, 24}}, nil)
}

func TestPBinResumeNonzeroSpanPreservesUntouchedBaseRows(t *testing.T) {
	covered := pbinCheckResumeRows(t, 1, 8, 16, 24, [][2]uint64{{0, 8}, {8, 24}, {24, 32}}, [][2]uint64{{0, 8}, {8, 12}, {12, 16}})
	for _, base := range []string{"-commitment.8-12.kv", "-commitment.8-12.kvi", "-commitment.12-16.kv", "-commitment.12-16.kvi"} {
		require.True(t, slices.ContainsFunc(covered, func(path string) bool { return strings.HasSuffix(path, base) }), "base file %s under output range [8,24) must exist before the resume", base)
	}
}

func TestPBinResumeAtRangeLastStamp(t *testing.T) {
	pbinCheckResumeRows(t, 1, 8, 23, 25, [][2]uint64{{0, 8}, {8, 24}, {24, 32}}, [][2]uint64{{0, 8}, {8, 24}})
}

func pbinCheckResumeRows(t *testing.T, step, initialFrom, baseTx, endTx uint64, sourceRanges, baseMerges [][2]uint64) []string {
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
		writePBinLeafStreamStates(t, db, baseTx+1, updated)
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
	covered := pbinFilesInsideRanges(t, targetAgg, sourceRanges)
	var aggregatorMerges atomic.Int32
	targetAgg.OnFilesChange(func([]string) { aggregatorMerges.Add(1) }, func([]string) {})
	resumeRoot := resumePBinAtWithBase(t, sourceDB, sourceAgg, targetDB, targetAgg, baseTx, endTx, sourceRanges)
	require.Zero(t, aggregatorMerges.Load(), "each resume range must be merged once, inside the range writer")
	for _, path := range covered {
		_, err := os.Stat(path)
		require.ErrorIs(t, err, os.ErrNotExist, "base file %s under a merged resume range must be removed", filepath.Base(path))
	}
	leftovers, err := filepath.Glob(filepath.Join(targetAgg.Dirs().Tmp, "pbin-resume-delta-*"))
	require.NoError(t, err)
	require.Empty(t, leftovers, "resume delta files must be removed after the merge")
	resumeRows := pbinCommitmentRows(t, targetAgg)
	freshRows := pbinCommitmentRows(t, freshAgg)
	delete(resumeRows, string(commitment.KeyCommitmentState))
	delete(freshRows, string(commitment.KeyCommitmentState))
	require.Equal(t, freshRoot, resumeRoot)
	require.Equal(t, len(freshRows), len(resumeRows), "resume spanning range must preserve untouched base rows")
	require.Equal(t, freshRows, resumeRows)
	return covered
}

func pbinFilesInsideRanges(t *testing.T, agg *state.Aggregator, ranges [][2]uint64) []string {
	t.Helper()
	pattern := regexp.MustCompile(`-commitment\.(\d+)-(\d+)\.(kv|kvi|bt|kvei)$`)
	entries, err := os.ReadDir(agg.Dirs().SnapDomain)
	require.NoError(t, err)
	var paths []string
	for _, entry := range entries {
		match := pattern.FindStringSubmatch(entry.Name())
		if match == nil {
			continue
		}
		from, err := strconv.ParseUint(match[1], 10, 64)
		require.NoError(t, err)
		to, err := strconv.ParseUint(match[2], 10, 64)
		require.NoError(t, err)
		from, to = from*agg.StepSize(), to*agg.StepSize()
		for _, r := range ranges {
			if r[0] <= from && to <= r[1] && (r[0] != from || r[1] != to) {
				paths = append(paths, filepath.Join(agg.Dirs().SnapDomain, entry.Name()))
			}
		}
	}
	return paths
}

func TestPBinResumeZeroStartDropsDeletionTombstones(t *testing.T) {
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
