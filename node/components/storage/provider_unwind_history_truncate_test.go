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

package storage

import (
	"encoding/binary"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/recsplit/multiencseq"
	"github.com/erigontech/erigon/db/seg"
)

// writeStraddlerFixture builds a synthetic (.ef, .v) pair with a single key K
// whose history has state changes at every txN in the txNums slice. Values
// are the big-endian encoding of each txN (arbitrary, but distinguishable).
//
// baseTxN is the base for the multiencseq sequence — analogous to
// item.startTxNum in production files. All txNums must be >= baseTxN.
func writeStraddlerFixture(t *testing.T, tmpDir, efPath, vPath string, key []byte, baseTxN uint64, txNums []uint64) {
	t.Helper()
	require := require.New(t)
	ctx := t.Context()
	logger := log.New()

	cfg := seg.DefaultCfg
	cfg.MinPatternScore = 1
	cfg.Workers = 1

	efComp, err := seg.NewCompressor(ctx, "fixture ef", efPath, tmpDir, cfg, log.LvlDebug, logger)
	require.NoError(err)
	defer efComp.Close()
	efWriter := seg.NewWriter(efComp, seg.CompressNone)

	pageSize := 1
	vComp, err := seg.NewCompressor(ctx, "fixture v", vPath, tmpDir, cfg.WithValuesOnCompressedPage(pageSize), log.LvlDebug, logger)
	require.NoError(err)
	defer vComp.Close()
	vWriter := seg.NewPagedWriter(ctx, seg.NewWriter(vComp, seg.CompressNone), true, 1)

	// Build the sequence
	require.NotEmpty(txNums)
	maxTxN := txNums[len(txNums)-1]
	builder := multiencseq.NewBuilder(baseTxN, uint64(len(txNums)), maxTxN)
	for _, txN := range txNums {
		builder.AddOffset(txN)
	}
	builder.Build()
	seqBytes := builder.AppendBytes(nil)

	// Write (key, seqBytes) to .ef
	_, err = efWriter.Write(key)
	require.NoError(err)
	_, err = efWriter.Write(seqBytes)
	require.NoError(err)
	require.NoError(efComp.Compress())

	// Write one paged entry per txN to .v
	for _, txN := range txNums {
		hk := make([]byte, 8+len(key))
		binary.BigEndian.PutUint64(hk, txN)
		copy(hk[8:], key)
		val := make([]byte, 8)
		binary.BigEndian.PutUint64(val, txN)
		require.NoError(vWriter.Add(hk, val))
	}
	require.NoError(vWriter.Flush())
	require.NoError(vWriter.Compress())
}

// readStraddlerFile decodes the (.ef, .v) pair produced by
// writeStraddlerFixture / TruncateStraddlerHistoryFile and returns
// (kept-txNums, kept-values) for the single fixture key.
func readStraddlerFile(t *testing.T, efPath, vPath string, baseTxN uint64) (txNums []uint64, values [][]byte) {
	t.Helper()
	require := require.New(t)

	efDec, err := seg.NewDecompressor(efPath)
	require.NoError(err)
	defer efDec.Close()
	vDec, err := seg.NewDecompressor(vPath)
	require.NoError(err)
	defer vDec.Close()

	pageValuesCount := vDec.CompressedPageValuesCount()
	if pageValuesCount == 0 {
		pageValuesCount = 1
	}

	efR := seg.NewReader(efDec.MakeGetter(), seg.CompressNone)
	efR.Reset(0)
	vR := seg.NewPagedReader(seg.NewReader(vDec.MakeGetter(), seg.CompressNone), pageValuesCount, true)
	vR.Reset(0)

	for efR.HasNext() {
		_, _ = efR.Next(nil)
		require.True(efR.HasNext(), "ef truncated: key without seq")
		seqBytes, _ := efR.Next(nil)

		seq := multiencseq.ReadMultiEncSeq(baseTxN, seqBytes)
		it := seq.Iterator(0)
		for it.HasNext() {
			txN, err := it.Next()
			require.NoError(err)
			txNums = append(txNums, txN)

			require.True(vR.HasNext(), "v truncated: seq txN %d has no matching value", txN)
			val, _ := vR.Next(nil)
			values = append(values, append([]byte(nil), val...))
		}
	}
	return txNums, values
}

// TestTruncateStraddlerHistoryFile_FiltersHistoryEntries — RED first.
// Builds a synthetic .ef/.v pair with entries at txN=[100,200,300,400].
// Calls TruncateStraddlerHistoryFile with targetTxN=250.
// Asserts the new pair contains only txN=[100,200] and their paired values.
func TestTruncateStraddlerHistoryFile_FiltersHistoryEntries(t *testing.T) {
	t.Parallel()

	tmp := t.TempDir()
	oldEF := filepath.Join(tmp, "old.ef")
	oldV := filepath.Join(tmp, "old.v")
	newEF := filepath.Join(tmp, "new.ef")
	newV := filepath.Join(tmp, "new.v")

	key := []byte("k")
	baseTxN := uint64(0)
	writeStraddlerFixture(t, tmp, oldEF, oldV, key, baseTxN, []uint64{100, 200, 300, 400})

	err := TruncateStraddlerHistoryFile(t.Context(), oldEF, oldV, newEF, newV,
		baseTxN, 250 /* targetTxN */, seg.CompressNone, seg.CompressNone, tmp, log.New())
	require.NoError(t, err)

	txNums, values := readStraddlerFile(t, newEF, newV, baseTxN)
	require.Equal(t, []uint64{100, 200}, txNums)
	require.Len(t, values, 2)

	// Values are BE-encoded txNums per the fixture builder.
	require.Equal(t, uint64(100), binary.BigEndian.Uint64(values[0]))
	require.Equal(t, uint64(200), binary.BigEndian.Uint64(values[1]))
}

// TestTruncateStraddlerHistoryFile_EmptyResult — targetTxN below every source
// entry; the output must be valid empty (.ef, .v) files.
func TestTruncateStraddlerHistoryFile_EmptyResult(t *testing.T) {
	t.Parallel()

	tmp := t.TempDir()
	oldEF := filepath.Join(tmp, "old.ef")
	oldV := filepath.Join(tmp, "old.v")
	newEF := filepath.Join(tmp, "new.ef")
	newV := filepath.Join(tmp, "new.v")

	key := []byte("k")
	baseTxN := uint64(0)
	writeStraddlerFixture(t, tmp, oldEF, oldV, key, baseTxN, []uint64{100, 200, 300, 400})

	err := TruncateStraddlerHistoryFile(t.Context(), oldEF, oldV, newEF, newV,
		baseTxN, 50 /* targetTxN */, seg.CompressNone, seg.CompressNone, tmp, log.New())
	require.NoError(t, err)

	txNums, values := readStraddlerFile(t, newEF, newV, baseTxN)
	require.Empty(t, txNums)
	require.Empty(t, values)
}

// writeMultiKeyFixture is like writeStraddlerFixture but writes N keys, each
// with its own txNum list. Keys are written in ascending sort order (as .ef
// requires); the .v file's paged entries preserve (key, txN) ordering to
// match .ef iteration.
func writeMultiKeyFixture(t *testing.T, tmpDir, efPath, vPath string, baseTxN uint64, entries []struct {
	Key    []byte
	TxNums []uint64
}) {
	t.Helper()
	require := require.New(t)
	ctx := t.Context()
	logger := log.New()

	cfg := seg.DefaultCfg
	cfg.MinPatternScore = 1
	cfg.Workers = 1

	efComp, err := seg.NewCompressor(ctx, "mk-fixture ef", efPath, tmpDir, cfg, log.LvlDebug, logger)
	require.NoError(err)
	defer efComp.Close()
	efWriter := seg.NewWriter(efComp, seg.CompressNone)

	pageSize := 1
	vComp, err := seg.NewCompressor(ctx, "mk-fixture v", vPath, tmpDir, cfg.WithValuesOnCompressedPage(pageSize), log.LvlDebug, logger)
	require.NoError(err)
	defer vComp.Close()
	vWriter := seg.NewPagedWriter(ctx, seg.NewWriter(vComp, seg.CompressNone), true, 1)

	for _, e := range entries {
		require.NotEmpty(e.TxNums)
		maxTxN := e.TxNums[len(e.TxNums)-1]
		builder := multiencseq.NewBuilder(baseTxN, uint64(len(e.TxNums)), maxTxN)
		for _, txN := range e.TxNums {
			builder.AddOffset(txN)
		}
		builder.Build()
		seqBytes := builder.AppendBytes(nil)

		_, err = efWriter.Write(e.Key)
		require.NoError(err)
		_, err = efWriter.Write(seqBytes)
		require.NoError(err)

		for _, txN := range e.TxNums {
			hk := make([]byte, 8+len(e.Key))
			binary.BigEndian.PutUint64(hk, txN)
			copy(hk[8:], e.Key)
			val := make([]byte, 8)
			binary.BigEndian.PutUint64(val, txN)
			require.NoError(vWriter.Add(hk, val))
		}
	}
	require.NoError(efComp.Compress())
	require.NoError(vWriter.Flush())
	require.NoError(vWriter.Compress())
}

// readMultiKeyFile is like readStraddlerFile but returns per-key results.
func readMultiKeyFile(t *testing.T, efPath, vPath string, baseTxN uint64) map[string][]uint64 {
	t.Helper()
	require := require.New(t)

	efDec, err := seg.NewDecompressor(efPath)
	require.NoError(err)
	defer efDec.Close()
	vDec, err := seg.NewDecompressor(vPath)
	require.NoError(err)
	defer vDec.Close()

	pageValuesCount := vDec.CompressedPageValuesCount()
	if pageValuesCount == 0 {
		pageValuesCount = 1
	}

	efR := seg.NewReader(efDec.MakeGetter(), seg.CompressNone)
	efR.Reset(0)
	vR := seg.NewPagedReader(seg.NewReader(vDec.MakeGetter(), seg.CompressNone), pageValuesCount, true)
	vR.Reset(0)

	out := make(map[string][]uint64)
	for efR.HasNext() {
		key, _ := efR.Next(nil)
		require.True(efR.HasNext())
		seqBytes, _ := efR.Next(nil)

		seq := multiencseq.ReadMultiEncSeq(baseTxN, seqBytes)
		it := seq.Iterator(0)
		var txNs []uint64
		for it.HasNext() {
			txN, err := it.Next()
			require.NoError(err)
			txNs = append(txNs, txN)
			// consume the paired value so ordering stays sane
			require.True(vR.HasNext(), "v truncated at key=%x txN=%d", key, txN)
			_, _ = vR.Next(nil)
		}
		out[string(key)] = txNs
	}
	return out
}

// TestTruncateStraddlerHistoryFile_MultipleKeys — .ef with keys k1, k2, k3
// each carrying a different mix of pre/post targetTxN entries. Verifies
// per-key filtering, correct dropping of keys whose sequence empties out,
// and .v/.ef alignment across key boundaries.
func TestTruncateStraddlerHistoryFile_MultipleKeys(t *testing.T) {
	t.Parallel()

	tmp := t.TempDir()
	oldEF := filepath.Join(tmp, "old.ef")
	oldV := filepath.Join(tmp, "old.v")
	newEF := filepath.Join(tmp, "new.ef")
	newV := filepath.Join(tmp, "new.v")

	entries := []struct {
		Key    []byte
		TxNums []uint64
	}{
		{Key: []byte("k1"), TxNums: []uint64{100, 200, 300}}, // 100,200 kept (300 dropped)
		{Key: []byte("k2"), TxNums: []uint64{400, 500}},      // fully dropped
		{Key: []byte("k3"), TxNums: []uint64{150, 249}},      // both kept (249 <= 249)
		{Key: []byte("k4"), TxNums: []uint64{249, 251}},      // 249 kept, 251 dropped
	}
	writeMultiKeyFixture(t, tmp, oldEF, oldV, 0, entries)

	// targetTxN=249: filter is inclusive (keep txN <= 249).
	err := TruncateStraddlerHistoryFile(t.Context(), oldEF, oldV, newEF, newV,
		0, 249, seg.CompressNone, seg.CompressNone, tmp, log.New())
	require.NoError(t, err)

	got := readMultiKeyFile(t, newEF, newV, 0)
	require.Equal(t, map[string][]uint64{
		"k1": {100, 200},
		"k3": {150, 249},
		"k4": {249},
	}, got, "k2 must be absent (all its entries were dropped)")
}

// TestTruncateStraddlerHistoryFile_KeepAll — targetTxN >= max source entry;
// the output equals the input.
func TestTruncateStraddlerHistoryFile_KeepAll(t *testing.T) {
	t.Parallel()

	tmp := t.TempDir()
	oldEF := filepath.Join(tmp, "old.ef")
	oldV := filepath.Join(tmp, "old.v")
	newEF := filepath.Join(tmp, "new.ef")
	newV := filepath.Join(tmp, "new.v")

	key := []byte("k")
	baseTxN := uint64(0)
	writeStraddlerFixture(t, tmp, oldEF, oldV, key, baseTxN, []uint64{100, 200, 300, 400})

	err := TruncateStraddlerHistoryFile(t.Context(), oldEF, oldV, newEF, newV,
		baseTxN, 500 /* targetTxN */, seg.CompressNone, seg.CompressNone, tmp, log.New())
	require.NoError(t, err)

	txNums, _ := readStraddlerFile(t, newEF, newV, baseTxN)
	require.Equal(t, []uint64{100, 200, 300, 400}, txNums)
}
