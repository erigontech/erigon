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

// TestHistory_mergeV4AndMDBXHistoryFiles_OverlappingTxNsError: if v4 and
// MDBX share the same (key, txN), the merge must error rather than emit
// duplicates. The mode-C emit + wipeWritableShadowPast invariant makes
// this impossible in practice, but the primitive defends against
// accidental violations that would silently corrupt the history file.
func TestHistory_mergeV4AndMDBXHistoryFiles_OverlappingTxNsError(t *testing.T) {
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

	// v4 covers txN 20 for K.
	writePairedHistoryStepFileForTest(t, ctx, h, v4EF, v4V, baseTxN, []struct {
		Key    []byte
		TxNs   []uint64
		Values [][]byte
	}{
		{Key: []byte("K"), TxNs: []uint64{20}, Values: [][]byte{[]byte("v4")}},
	})
	// MDBX also covers txN 20 (invalid — WipeWritableShadowPast should
	// have removed it), plus 30.
	writePairedHistoryStepFileForTest(t, ctx, h, mdbxEF, mdbxV, baseTxN, []struct {
		Key    []byte
		TxNs   []uint64
		Values [][]byte
	}{
		{Key: []byte("K"), TxNs: []uint64{20, 30}, Values: [][]byte{[]byte("mdbx-20"), []byte("mdbx-30")}},
	})

	err := h.mergeV4AndMDBXHistoryFiles(ctx, baseTxN, v4EF, v4V, mdbxEF, mdbxV, outEF, outV)
	require.Error(t, err)
	require.Contains(t, err.Error(), "txN range overlap")
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
