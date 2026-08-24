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
	"context"
	"encoding/binary"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/background"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/recsplit/multiencseq"
	"github.com/erigontech/erigon/db/seg"
)

// writePairedHistoryStepFileForTest writes an (.ef, .v) pair at the given
// paths using the History's compression settings and page size — matching
// what History.collate produces. Each key gets its own txN sequence + one
// value per txN. Keys must be in strict ascending order.
//
// baseTxN is the multiencseq baseNum (typically step*stepSize).
func writePairedHistoryStepFileForTest(t *testing.T, ctx context.Context, h *History, efPath, vPath string, baseTxN uint64, entries []struct {
	Key    []byte
	TxNs   []uint64
	Values [][]byte
}) {
	t.Helper()
	require := require.New(t)

	cfg := seg.DefaultCfg
	cfg.MinPatternScore = 1
	cfg.Workers = 1

	efComp, err := seg.NewCompressor(ctx, "fixture-ef", efPath, h.dirs.Tmp, cfg, log.LvlDebug, h.logger)
	require.NoError(err)
	defer efComp.Close()
	efW := seg.NewWriter(efComp, h.InvertedIndex.Compression)

	pageSize := 1
	vComp, err := seg.NewCompressor(ctx, "fixture-v", vPath, h.dirs.Tmp, cfg.WithValuesOnCompressedPage(pageSize), log.LvlDebug, h.logger)
	require.NoError(err)
	defer vComp.Close()
	vW := seg.NewPagedWriter(ctx, seg.NewWriter(vComp, h.Compression), true, 1)

	for _, e := range entries {
		require.Len(e.Values, len(e.TxNs), "test fixture: values must be same length as txNs")
		require.NotEmpty(e.TxNs, "test fixture: empty txN list not allowed")
		maxTxN := e.TxNs[len(e.TxNs)-1]
		builder := multiencseq.NewBuilder(baseTxN, uint64(len(e.TxNs)), maxTxN)
		for _, txN := range e.TxNs {
			builder.AddOffset(txN)
		}
		builder.Build()
		_, err = efW.Write(e.Key)
		require.NoError(err)
		_, err = efW.Write(builder.AppendBytes(nil))
		require.NoError(err)
		for i, txN := range e.TxNs {
			hk := make([]byte, 8+len(e.Key))
			binary.BigEndian.PutUint64(hk, txN)
			copy(hk[8:], e.Key)
			require.NoError(vW.Add(hk, e.Values[i]))
		}
	}
	require.NoError(efComp.Compress())
	require.NoError(vW.Flush())
	require.NoError(vW.Compress())
}

// readPairedHistoryStepFileForTest is the inverse of the writer above.
// Returns a per-key map of (txNs, values) — key-collision-free because
// .ef enforces ascending unique keys.
func readPairedHistoryStepFileForTest(t *testing.T, efPath, vPath string, efCompression, vCompression seg.FileCompression, baseTxN uint64) (keys [][]byte, txNs [][]uint64, values [][][]byte) {
	t.Helper()
	c, err := openHistoryEFVCursor(efPath, vPath, efCompression, vCompression, baseTxN)
	require.NoError(t, err)
	defer c.Close()
	for c.hasKey {
		keys = append(keys, append([]byte(nil), c.key...))
		txNsCopy := append([]uint64(nil), c.txNs...)
		txNs = append(txNs, txNsCopy)
		valsCopy := make([][]byte, len(c.values))
		for i, v := range c.values {
			valsCopy[i] = append([]byte(nil), v...)
		}
		values = append(values, valsCopy)
		require.NoError(t, c.advance())
	}
	return
}

// TestHistory_mergeV4AndMDBXHistoryFiles_DisjointKeys pins the simplest
// merge case: v4 covers keys {a, c}, MDBX covers keys {b, d}. Merged
// output must interleave them in key order (a, b, c, d) with each key's
// original txNs preserved.
func TestHistory_mergeV4AndMDBXHistoryFiles_DisjointKeys(t *testing.T) {
	t.Parallel()
	_, agg := testDbAndAggregatorv3(t, 100)
	ctx := t.Context()
	h := agg.d[kv.AccountsDomain].History

	const baseTxN = uint64(0)
	tmp := t.TempDir()
	v4EF := tmp + "/v4.ef"
	v4V := tmp + "/v4.v"
	mdbxEF := tmp + "/mdbx.ef"
	mdbxV := tmp + "/mdbx.v"
	outEF := tmp + "/out.ef"
	outV := tmp + "/out.v"

	writePairedHistoryStepFileForTest(t, ctx, h, v4EF, v4V, baseTxN, []struct {
		Key    []byte
		TxNs   []uint64
		Values [][]byte
	}{
		{Key: []byte("a"), TxNs: []uint64{10}, Values: [][]byte{[]byte("va-10")}},
		{Key: []byte("c"), TxNs: []uint64{20}, Values: [][]byte{[]byte("vc-20")}},
	})
	writePairedHistoryStepFileForTest(t, ctx, h, mdbxEF, mdbxV, baseTxN, []struct {
		Key    []byte
		TxNs   []uint64
		Values [][]byte
	}{
		{Key: []byte("b"), TxNs: []uint64{30}, Values: [][]byte{[]byte("vb-30")}},
		{Key: []byte("d"), TxNs: []uint64{40}, Values: [][]byte{[]byte("vd-40")}},
	})

	require.NoError(t, h.mergeV4AndMDBXHistoryFiles(ctx, baseTxN, v4EF, v4V, mdbxEF, mdbxV, outEF, outV))

	keys, txNs, values := readPairedHistoryStepFileForTest(t, outEF, outV, h.InvertedIndex.Compression, h.Compression, baseTxN)
	require.Equal(t, [][]byte{[]byte("a"), []byte("b"), []byte("c"), []byte("d")}, keys)
	require.Equal(t, [][]uint64{{10}, {30}, {20}, {40}}, txNs)
	require.Equal(t, [][][]byte{
		{[]byte("va-10")},
		{[]byte("vb-30")},
		{[]byte("vc-20")},
		{[]byte("vd-40")},
	}, values)
}

// TestHistory_mergeV4AndMDBXHistoryFiles_SameKeyConcatenatesTxNs pins the
// central invariant: when the same key appears on both sides, the merged
// entry's txN sequence is v4's txNs concatenated with MDBX's txNs (v4 first
// because its txN range is earlier than MDBX's). Values follow the same
// order.
func TestHistory_mergeV4AndMDBXHistoryFiles_SameKeyConcatenatesTxNs(t *testing.T) {
	t.Parallel()
	_, agg := testDbAndAggregatorv3(t, 100)
	ctx := t.Context()
	h := agg.d[kv.AccountsDomain].History

	const baseTxN = uint64(0)
	tmp := t.TempDir()
	v4EF := tmp + "/v4.ef"
	v4V := tmp + "/v4.v"
	mdbxEF := tmp + "/mdbx.ef"
	mdbxV := tmp + "/mdbx.v"
	outEF := tmp + "/out.ef"
	outV := tmp + "/out.v"

	// v4 side: key K has 2 history entries at txN 10, 20 (first-half of step)
	writePairedHistoryStepFileForTest(t, ctx, h, v4EF, v4V, baseTxN, []struct {
		Key    []byte
		TxNs   []uint64
		Values [][]byte
	}{
		{Key: []byte("K"), TxNs: []uint64{10, 20}, Values: [][]byte{[]byte("v-10"), []byte("v-20")}},
	})
	// MDBX side: same key K has 2 history entries at txN 30, 40 (second-half of step)
	writePairedHistoryStepFileForTest(t, ctx, h, mdbxEF, mdbxV, baseTxN, []struct {
		Key    []byte
		TxNs   []uint64
		Values [][]byte
	}{
		{Key: []byte("K"), TxNs: []uint64{30, 40}, Values: [][]byte{[]byte("v-30"), []byte("v-40")}},
	})

	require.NoError(t, h.mergeV4AndMDBXHistoryFiles(ctx, baseTxN, v4EF, v4V, mdbxEF, mdbxV, outEF, outV))

	keys, txNs, values := readPairedHistoryStepFileForTest(t, outEF, outV, h.InvertedIndex.Compression, h.Compression, baseTxN)
	require.Equal(t, [][]byte{[]byte("K")}, keys, "single key with concatenated sequence")
	require.Equal(t, [][]uint64{{10, 20, 30, 40}}, txNs, "v4 txNs first, then MDBX txNs")
	require.Equal(t, [][][]byte{{[]byte("v-10"), []byte("v-20"), []byte("v-30"), []byte("v-40")}}, values)
}

// TestHistory_mergeV4AndMDBXHistoryFiles_OverlapDedupesToMDBX: when v4 and
// MDBX share (key, txN) pairs — the expected shape after multi-iter mode-C
// because WipeWritableShadowPast only prunes history at txN > lastTxN,
// leaving txN <= lastTxN entries intact in both v4 and MDBX — the merge
// drops v4's overlapping tail and keeps MDBX's values. Deterministic
// execution guarantees the values would be identical anyway, so either
// side wins; preferring MDBX matches the state-side priority-merge
// convention (mergedStepSources in db/state/step_source.go where source[0]
// = MDBX takes priority on duplicate keys).
func TestHistory_mergeV4AndMDBXHistoryFiles_OverlapDedupesToMDBX(t *testing.T) {
	t.Parallel()
	_, agg := testDbAndAggregatorv3(t, 100)
	ctx := t.Context()
	h := agg.d[kv.AccountsDomain].History

	const baseTxN = uint64(0)
	tmp := t.TempDir()
	v4EF := tmp + "/v4.ef"
	v4V := tmp + "/v4.v"
	mdbxEF := tmp + "/mdbx.ef"
	mdbxV := tmp + "/mdbx.v"
	outEF := tmp + "/out.ef"
	outV := tmp + "/out.v"

	// v4 covers txN 10, 20 for K.
	writePairedHistoryStepFileForTest(t, ctx, h, v4EF, v4V, baseTxN, []struct {
		Key    []byte
		TxNs   []uint64
		Values [][]byte
	}{
		{Key: []byte("K"), TxNs: []uint64{10, 20}, Values: [][]byte{[]byte("v4-10"), []byte("v4-20")}},
	})
	// MDBX covers txN 20, 30 — 20 overlaps v4.
	writePairedHistoryStepFileForTest(t, ctx, h, mdbxEF, mdbxV, baseTxN, []struct {
		Key    []byte
		TxNs   []uint64
		Values [][]byte
	}{
		{Key: []byte("K"), TxNs: []uint64{20, 30}, Values: [][]byte{[]byte("mdbx-20"), []byte("mdbx-30")}},
	})

	require.NoError(t, h.mergeV4AndMDBXHistoryFiles(ctx, baseTxN, v4EF, v4V, mdbxEF, mdbxV, outEF, outV))

	keys, txNs, values := readPairedHistoryStepFileForTest(t, outEF, outV, h.InvertedIndex.Compression, h.Compression, baseTxN)
	require.Equal(t, [][]byte{[]byte("K")}, keys)
	require.Equal(t, [][]uint64{{10, 20, 30}}, txNs, "overlapping txN=20 present once, v4-only txN=10 kept, MDBX-only txN=30 kept")
	// MDBX wins on the overlap.
	require.Equal(t, [][][]byte{{[]byte("v4-10"), []byte("mdbx-20"), []byte("mdbx-30")}}, values)
}

// TestHistory_buildFiles_MergesV4WhenPresent pins the wire contract
// of stage 8c: History.buildFiles must call mergeV4IntoStepFile between
// Compress and Decompressor.Open so the accessor (.vi) indexes the
// MERGED content, not the MDBX-only tail. Constructs a collation-like
// pair with just one key K, then adds a v4 pair for the same step but a
// different key L. After buildFiles, the .ef must contain BOTH keys and
// the .vi must resolve entries for both.
func TestHistory_buildFiles_MergesV4WhenPresent(t *testing.T) {
	t.Parallel()
	_, agg := testDbAndAggregatorv3(t, 100)
	ctx := t.Context()
	h := agg.d[kv.AccountsDomain].History

	const step = kv.Step(7)
	baseTxN := uint64(step) * h.stepSize

	// Build a v4 (.v, .ef) at the aggregator's v4 naming for step 7,
	// covering the first-half range only. Contents: key L at txN
	// baseTxN+5, baseTxN+6.
	v4EFPath, v4VPath := buildV4HistoryPairForTest(t, ctx, agg, kv.AccountsDomain, []byte("L-key-long-enough-for-recsplit"), baseTxN, []uint64{baseTxN + 5, baseTxN + 6})
	// Register the v4 in dirtyFiles so v4FilesForStep(7) finds it.
	v4VDec, err := seg.NewDecompressor(v4VPath)
	require.NoError(t, err)
	t.Cleanup(v4VDec.Close)
	v4VItem := newFilesItem(baseTxN, baseTxN+7)
	v4VItem.decompressor = v4VDec
	h.dirtyFiles.Set(v4VItem)

	v4EFDec, err := seg.NewDecompressor(v4EFPath)
	require.NoError(t, err)
	t.Cleanup(v4EFDec.Close)
	v4EFItem := newFilesItem(baseTxN, baseTxN+7)
	v4EFItem.decompressor = v4EFDec
	h.InvertedIndex.dirtyFiles.Set(v4EFItem)

	// Build a synthetic "MDBX-only" collation via HistoryCollation with a
	// key K in the second-half range. We fabricate the compressors that
	// buildFiles will Compress + read back.
	collVPath := h.vNewFilePath(step, step+1)
	collEFPath := h.efNewFilePath(step, step+1)

	cfg := seg.DefaultCfg
	cfg.MinPatternScore = 1
	cfg.Workers = 1

	efComp, err := seg.NewCompressor(ctx, "coll-ef", collEFPath, h.dirs.Tmp, cfg, log.LvlDebug, h.logger)
	require.NoError(t, err)
	efWriter := seg.NewWriter(efComp, h.InvertedIndex.Compression)

	vComp, err := seg.NewCompressor(ctx, "coll-v", collVPath, h.dirs.Tmp, cfg.WithValuesOnCompressedPage(1), log.LvlDebug, h.logger)
	require.NoError(t, err)
	vWriter := seg.NewPagedWriter(ctx, seg.NewWriter(vComp, h.Compression), true, 1)

	// Write MDBX-only entry for key K at txN baseTxN+50, baseTxN+60.
	kKey := []byte("K-key-long-enough-for-recsplit")
	kTxNs := []uint64{baseTxN + 50, baseTxN + 60}
	builder := multiencseq.NewBuilder(baseTxN, uint64(len(kTxNs)), kTxNs[len(kTxNs)-1])
	for _, txN := range kTxNs {
		builder.AddOffset(txN)
	}
	builder.Build()
	_, err = efWriter.Write(kKey)
	require.NoError(t, err)
	_, err = efWriter.Write(builder.AppendBytes(nil))
	require.NoError(t, err)
	for _, txN := range kTxNs {
		hk := make([]byte, 8+len(kKey))
		binary.BigEndian.PutUint64(hk, txN)
		copy(hk[8:], kKey)
		require.NoError(t, vWriter.Add(hk, []byte("kv-value")))
	}
	require.NoError(t, vWriter.Flush())

	coll := HistoryCollation{
		efHistoryComp: efWriter, // buildFiles will Compress this
		efHistoryPath: collEFPath,
		efBaseTxNum:   baseTxN,
		historyComp:   vWriter, // buildFiles will Compress this
		historyPath:   collVPath,
	}

	// Now run buildFiles — this should Compress, run mergeV4IntoStepFile,
	// then build accessors over the MERGED content.
	files, err := h.buildFiles(ctx, step, coll, background.NewProgressSet())
	require.NoError(t, err)
	defer files.CleanupOnError()

	// Inspect the merged .ef: must contain BOTH keys in ascending order.
	efReader := h.InvertedIndex.dataReader(files.efHistoryDecomp)
	efReader.Reset(0)
	var mergedKeys [][]byte
	for efReader.HasNext() {
		k, _ := efReader.Next(nil)
		mergedKeys = append(mergedKeys, append([]byte(nil), k...))
		require.True(t, efReader.HasNext(), "key without paired seq")
		_, _ = efReader.Next(nil)
	}
	// Alphabetical: K-key < L-key, so the merged order is [K, L].
	require.Equal(t, [][]byte{kKey, []byte("L-key-long-enough-for-recsplit")}, mergedKeys)
}

// TestHistory_mergeV4IntoStepFile_NoV4Present pins the no-op branch:
// when h.v4FilesForStep(step) is empty, the merge is a no-op and the
// original files are untouched.
func TestHistory_mergeV4IntoStepFile_NoV4Present(t *testing.T) {
	t.Parallel()
	_, agg := testDbAndAggregatorv3(t, 100)
	ctx := t.Context()
	h := agg.d[kv.AccountsDomain].History

	const step = kv.Step(5)
	baseTxN := uint64(step) * h.stepSize
	tmp := t.TempDir()
	vPath := tmp + "/final.v"
	efPath := tmp + "/final.ef"

	writePairedHistoryStepFileForTest(t, ctx, h, efPath, vPath, baseTxN, []struct {
		Key    []byte
		TxNs   []uint64
		Values [][]byte
	}{
		{Key: []byte("K"), TxNs: []uint64{baseTxN + 5}, Values: [][]byte{[]byte("val")}},
	})

	beforeStat, err := os.Stat(vPath)
	require.NoError(t, err)

	require.NoError(t, h.mergeV4IntoStepFile(ctx, step, vPath, efPath), "no-op when no v4")

	afterStat, err := os.Stat(vPath)
	require.NoError(t, err)
	require.Equal(t, beforeStat.Size(), afterStat.Size(), "file unchanged when no v4")
	require.Equal(t, beforeStat.ModTime(), afterStat.ModTime(), "file mtime unchanged when no v4")
}
