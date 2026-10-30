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
	"sort"
	"testing"

	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/commitment/eip8297"
	"github.com/erigontech/erigon/execution/commitment/v3/pbt"
	"github.com/erigontech/erigon/internal/commitmenttest/temporal"
	"github.com/stretchr/testify/require"
)

func TestPBinVerifyStreamsRowsWithoutPointLookups(t *testing.T) {
	selectPBinBinOnlyHash(t)
	db, agg := temporal.Open(t, 8)
	writePBinConvertStateN(t, db, 1024)
	require.NoError(t, agg.BuildFiles2(t.Context(), db, 0, 1, unboundedFinalityCtx, false))
	agg.WaitForFiles()
	convertPBinAt(t, db, agg, 7)
	verifyTx, err := db.BeginTemporalRo(t.Context())
	require.NoError(t, err)
	defer verifyTx.Rollback()
	stats := new(pbt.VerifyStats)
	_, err = state.VerifyPBinDomainRootWithStats(t.Context(), verifyTx, agg, kv.CommitmentDomain, stats)
	require.NoError(t, err)
	require.Zero(t, stats.PointLookups.Load(), "C-d must not use point lookups")
	require.Zero(t, stats.ExactRangeReads.Load(), "verification must not use exact range reads")
	require.Positive(t, stats.RangeReads.Load())
	require.Equal(t, stats.RangeTasks.Load(), stats.RangeReads.Load(), "each verification task must use one range read")
	require.Positive(t, stats.TaskRowsLoaded.Load())
	require.Equal(t, stats.TaskRowsLoaded.Load(), stats.TaskRowsReleased.Load(), "every verification task must release its rows")
	require.Zero(t, stats.UpperRowsAtStart.Load(), "upper rows must be verified after task rows are released")
	require.Equal(t, stats.RangeTaskReads.Load(), stats.RangeReadsAtOffset.Load(), "every verification task must start at a planned file offset")
	require.LessOrEqual(t, stats.PeakRows.Load(), stats.TaskWorkers.Load()*stats.MaxTaskRows.Load(), "verification row memory exceeded the worker bound")
}

func TestPBinVerifyDataKeepsOnlyUpperRows(t *testing.T) {
	selectPBinBinOnlyHash(t)
	db, agg := temporal.Open(t, 8)
	writePBinConvertStateN(t, db, 1024)
	require.NoError(t, agg.BuildFiles2(t.Context(), db, 0, 1, unboundedFinalityCtx, false))
	agg.WaitForFiles()
	convertPBinAt(t, db, agg, 7)
	tx, err := db.BeginTemporalRo(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()
	at := agg.BeginFilesRo()
	defer at.Close()
	iter, err := at.DebugRangeLatestFromFiles(kv.CommitmentDomain, nil, nil, kv.Unlim)
	require.NoError(t, err)
	bucketRows := 0
	for iter.HasNext() {
		key, _, nextErr := iter.Next()
		require.NoError(t, nextErr)
		if path, decodeErr := eip8297.DecodeBitPath(key); decodeErr == nil && path.BitLen == 264 {
			bucketRows++
		}
	}
	iter.Close()
	require.Positive(t, bucketRows)
	_, _, upperRows, err := at.DebugRangeLatestVerifyData(tx, kv.CommitmentDomain, pbt.GlobalRootKey(), 16)
	require.NoError(t, err)
	for key := range upperRows {
		if key == string(pbt.GlobalRootKey()) {
			continue
		}
		path, decodeErr := eip8297.DecodeBitPath([]byte(key))
		require.NoError(t, decodeErr)
		require.Less(t, path.BitLen, int16(16), "row %x must be read by its task, not preloaded", key)
	}
}

func TestPBinVerifySerialManyStorageSlots(t *testing.T) {
	selectPBinBinOnlyHash(t)
	db, agg := temporal.Open(t, 8)
	writePBinConvertStateManyStorageSlots(t, db)
	require.NoError(t, agg.BuildFiles2(t.Context(), db, 0, 1, unboundedFinalityCtx, false))
	agg.WaitForFiles()
	convertPBinAt(t, db, agg, 7)

	verifyTx, err := db.BeginTemporalRo(t.Context())
	require.NoError(t, err)
	defer verifyTx.Rollback()
	previousWorkers := dbg.PBTWorkers
	dbg.PBTWorkers = 1
	t.Cleanup(func() { dbg.PBTWorkers = previousWorkers })
	_, err = state.VerifyPBinDomainRoot(t.Context(), verifyTx, agg, kv.CommitmentDomain)
	require.NoError(t, err)
}

func TestPBinVerifySerialUsesPlannedRangeOffsets(t *testing.T) {
	selectPBinBinOnlyHash(t)
	db, agg := temporal.Open(t, 8)
	writePBinConvertStateN(t, db, 1024)
	require.NoError(t, agg.BuildFiles2(t.Context(), db, 0, 1, unboundedFinalityCtx, false))
	agg.WaitForFiles()
	convertPBinAt(t, db, agg, 7)
	verifyTx, err := db.BeginTemporalRo(t.Context())
	require.NoError(t, err)
	defer verifyTx.Rollback()
	previousWorkers := dbg.PBTWorkers
	dbg.PBTWorkers = 1
	t.Cleanup(func() { dbg.PBTWorkers = previousWorkers })
	stats := new(pbt.VerifyStats)
	_, err = state.VerifyPBinDomainRootWithStats(t.Context(), verifyTx, agg, kv.CommitmentDomain, stats)
	require.NoError(t, err)
	require.Zero(t, stats.ExactRangeReads.Load(), "serial verification must not use exact range reads")
	require.Positive(t, stats.RangeTaskReads.Load())
	require.Equal(t, stats.RangeTaskReads.Load(), stats.RangeReadsAtOffset.Load(), "every serial verification task must start at a planned file offset")
}

func TestPBinVerifyRejectsUnreferencedRowInsideChainRange(t *testing.T) {
	verifyPBinRejectsUnreferencedRow(t, pbinUnreferencedRowInsideChainRange)
}

func TestPBinVerifyRejectsUnreferencedRowInChainGap(t *testing.T) {
	verifyPBinRejectsUnreferencedRow(t, pbinUnreferencedRowInChainGap)
}

func TestPBinVerifyRejectsUnreferencedRowInEmptyZone(t *testing.T) {
	verifyPBinRejectsUnreferencedRow(t, pbinUnreferencedRowInEmptyZone)
}

func TestPBinVerifyRejectsUnreferencedRowAboveChainDepth(t *testing.T) {
	verifyPBinRejectsUnreferencedRow(t, pbinUnreferencedRowAboveChainDepth)
}

type pbinUnreferencedRowSelector func(*testing.T, map[string][]byte) ([]byte, []byte)

func verifyPBinRejectsUnreferencedRow(t *testing.T, selectRow pbinUnreferencedRowSelector) {
	t.Helper()
	selectPBinBinOnlyHash(t)
	db, agg := temporal.Open(t, 8)
	writePBinConvertStateN(t, db, 1024)
	require.NoError(t, agg.BuildFiles2(t.Context(), db, 0, 1, unboundedFinalityCtx, false))
	agg.WaitForFiles()
	convertPBinAt(t, db, agg, 7)
	rows := pbinCommitmentRows(t, agg)
	extraKey, extraValue := selectRow(t, rows)
	require.NotEmpty(t, extraKey)
	badTx, err := db.BeginTemporalRw(t.Context())
	require.NoError(t, err)
	defer badTx.Rollback()
	cfg := commitment.DefaultTrieConfig()
	cfg.Variant = commitment.VariantBinPatriciaTrie
	cfg.EnableTrieWarmup = false
	domains, err := execctx.NewSharedDomains(t.Context(), badTx, log.Root(), execctx.WithTrieConfig(cfg), execctx.WithoutCommitmentSeek())
	require.NoError(t, err)
	require.NoError(t, domains.DomainPut(kv.CommitmentDomain, badTx, extraKey, extraValue, 9, nil))
	require.NoError(t, domains.Flush(t.Context(), badTx))
	require.NoError(t, badTx.Commit())
	domains.Close()
	verifyTx, err := db.BeginTemporalRo(t.Context())
	require.NoError(t, err)
	defer verifyTx.Rollback()
	_, err = state.VerifyPBinDomainRoot(t.Context(), verifyTx, agg, kv.CommitmentDomain)
	require.ErrorContains(t, err, "unreferenced verification row")
}

func pbinSortedRowKeys(t *testing.T, rows map[string][]byte) []string {
	t.Helper()
	keys := make([]string, 0, len(rows))
	for key, value := range rows {
		if len(value) == 0 || commitment.IsCommitmentStateKey([]byte(key)) {
			continue
		}
		if _, err := eip8297.DecodeBitPath([]byte(key)); err != nil {
			continue
		}
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func pbinUnreferencedRowInsideChainRange(t *testing.T, rows map[string][]byte) ([]byte, []byte) {
	for _, key := range pbinSortedRowKeys(t, rows) {
		path, err := eip8297.DecodeBitPath([]byte(key))
		require.NoError(t, err)
		if path.BitLen <= 16 {
			continue
		}
		candidate := path
		candidate.SetBitAt(16, 1-candidate.Bit(16))
		candidateKey, err := pbt.EncodeRowKey(&candidate)
		require.NoError(t, err)
		if _, exists := rows[string(candidateKey)]; !exists {
			return candidateKey, bytes.Clone(rows[key])
		}
	}
	t.Fatal("no deterministic row candidate inside a chain range")
	return nil, nil
}

func pbinUnreferencedRowInChainGap(t *testing.T, rows map[string][]byte) ([]byte, []byte) {
	taskPrefixes := make(map[string]map[string]struct{})
	keys := pbinSortedRowKeys(t, rows)
	for _, key := range keys {
		path, err := eip8297.DecodeBitPath([]byte(key))
		require.NoError(t, err)
		if path.BitLen < 16 {
			continue
		}
		parent := path.Slice(0, 12)
		task := path.Slice(0, 16)
		parentKey, err := pbt.EncodeRowKey(&parent)
		require.NoError(t, err)
		taskKey, err := pbt.EncodeRowKey(&task)
		require.NoError(t, err)
		if taskPrefixes[string(parentKey)] == nil {
			taskPrefixes[string(parentKey)] = make(map[string]struct{})
		}
		taskPrefixes[string(parentKey)][string(taskKey)] = struct{}{}
	}
	for _, key := range keys {
		path, err := eip8297.DecodeBitPath([]byte(key))
		require.NoError(t, err)
		if path.BitLen < 16 {
			continue
		}
		parent := path.Slice(0, 12)
		parentKey, err := pbt.EncodeRowKey(&parent)
		require.NoError(t, err)
		prefixes := taskPrefixes[string(parentKey)]
		if len(prefixes) < 2 {
			continue
		}
		for bit := int16(12); bit < 16; bit++ {
			candidate := path
			candidate.SetBitAt(bit, 1-candidate.Bit(bit))
			task := candidate.Slice(0, 16)
			taskKey, err := pbt.EncodeRowKey(&task)
			require.NoError(t, err)
			if _, exists := prefixes[string(taskKey)]; exists {
				continue
			}
			candidateKey, err := pbt.EncodeRowKey(&candidate)
			require.NoError(t, err)
			if _, exists := rows[string(candidateKey)]; !exists {
				return candidateKey, bytes.Clone(rows[key])
			}
		}
	}
	t.Fatal("no deterministic row candidate in a chain gap")
	return nil, nil
}

func pbinUnreferencedRowInEmptyZone(t *testing.T, rows map[string][]byte) ([]byte, []byte) {
	for _, key := range pbinSortedRowKeys(t, rows) {
		path, err := eip8297.DecodeBitPath([]byte(key))
		require.NoError(t, err)
		if path.BitLen < 16 {
			continue
		}
		candidate := path
		for bit := range 8 {
			candidate.SetBitAt(int16(bit), uint64((0x05>>uint(7-bit))&1))
		}
		candidateKey, err := pbt.EncodeRowKey(&candidate)
		require.NoError(t, err)
		if _, exists := rows[string(candidateKey)]; !exists {
			return candidateKey, bytes.Clone(rows[key])
		}
	}
	t.Fatal("no deterministic row candidate in an empty zone")
	return nil, nil
}

func pbinUnreferencedRowAboveChainDepth(t *testing.T, rows map[string][]byte) ([]byte, []byte) {
	keys := pbinSortedRowKeys(t, rows)
	require.NotEmpty(t, keys)
	value := bytes.Clone(rows[keys[0]])
	for bitLen := int16(12); bitLen >= 4; bitLen -= 4 {
		for candidateValue := 0; candidateValue < 1<<bitLen; candidateValue++ {
			var candidate eip8297.Bitpath
			candidate.BitLen = bitLen
			for bit := int16(0); bit < bitLen; bit++ {
				candidate.SetBitAt(bit, uint64(candidateValue>>uint(bitLen-1-bit)&1))
			}
			candidateKey, err := pbt.EncodeRowKey(&candidate)
			require.NoError(t, err)
			if _, exists := rows[string(candidateKey)]; !exists {
				return candidateKey, value
			}
		}
	}
	t.Fatal("no deterministic row candidate above chain depth")
	return nil, nil
}

func TestPBinVerifyCountsActualRootPointReads(t *testing.T) {
	selectPBinBinOnlyHash(t)
	db, agg := temporal.Open(t, 8)
	writePBinConvertStateN(t, db, 1024)
	require.NoError(t, agg.BuildFiles2(t.Context(), db, 0, 1, unboundedFinalityCtx, false))
	agg.WaitForFiles()
	convertPBinAt(t, db, agg, 7)
	verifyTx, err := db.BeginTemporalRo(t.Context())
	require.NoError(t, err)
	defer verifyTx.Rollback()
	counted := &pbinCountingTx{TemporalTx: verifyTx}
	stats := new(pbt.VerifyStats)
	_, err = state.VerifyPBinDomainRootWithStats(t.Context(), counted, agg, kv.CommitmentDomain, stats)
	require.NoError(t, err)
	require.Zero(t, len(counted.reads), "C-d made point reads of %x while stats reported %d", counted.reads, stats.PointLookups.Load())
}

type pbinCountingTx struct {
	kv.TemporalTx
	reads [][]byte
}

func (tx *pbinCountingTx) Pin() kv.TemporalFilesPin {
	return tx.TemporalTx.(interface{ Pin() kv.TemporalFilesPin }).Pin()
}

func (tx *pbinCountingTx) GetLatest(domain kv.Domain, key []byte, opts kv.GetLatestOptions) ([]byte, kv.Step, error) {
	if domain == kv.CommitmentDomain && bytes.Equal(key, pbt.GlobalRootKey()) {
		tx.reads = append(tx.reads, bytes.Clone(key))
	}
	return tx.TemporalTx.GetLatest(domain, key, opts)
}

func TestPBinVerifyStorageRowsStayBounded(t *testing.T) {
	const (
		maxLiveRows  = 1024
		maxLiveBytes = 1 << 20
	)
	selectPBinBinOnlyHash(t)
	db, agg := temporal.Open(t, 8)
	writePBinConvertStateN(t, db, 4096)
	require.NoError(t, agg.BuildFiles2(t.Context(), db, 0, 1, unboundedFinalityCtx, false))
	agg.WaitForFiles()
	convertPBinAt(t, db, agg, 7)
	verifyTx, err := db.BeginTemporalRo(t.Context())
	require.NoError(t, err)
	defer verifyTx.Rollback()
	stats := new(pbt.VerifyStats)
	_, err = state.VerifyPBinDomainRootWithStats(t.Context(), verifyTx, agg, kv.CommitmentDomain, stats)
	require.NoError(t, err)
	require.Greater(t, stats.RangeRows.Load(), uint64(1))
	require.Less(t, stats.PeakRows.Load(), uint64(maxLiveRows),
		"verification retained too many rows: peak=%d bound=%d", stats.PeakRows.Load(), maxLiveRows)
	require.Less(t, stats.PeakBytes.Load(), uint64(maxLiveBytes),
		"verification retained too many bytes: peak=%d bound=%d", stats.PeakBytes.Load(), maxLiveBytes)
}
