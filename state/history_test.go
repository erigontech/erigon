/*
   Copyright 2022 Erigon contributors

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package state

import (
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/ledgerwatch/erigon-lib/common/background"
	"github.com/ledgerwatch/erigon-lib/common/hexutility"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/iter"
	"github.com/ledgerwatch/erigon-lib/kv/mdbx"
	"github.com/ledgerwatch/erigon-lib/kv/order"
	"github.com/ledgerwatch/erigon-lib/recsplit"
	"github.com/ledgerwatch/erigon-lib/recsplit/eliasfano32"
	"github.com/ledgerwatch/log/v3"
	"github.com/stretchr/testify/require"
	btree2 "github.com/tidwall/btree"
)

func testDbAndHistory(tb testing.TB, largeValues bool, logger log.Logger) (string, kv.RwDB, *History) {
	tb.Helper()
	path := tb.TempDir()
	keysTable := "AccountKeys"
	indexTable := "AccountIndex"
	valsTable := "AccountVals"
	settingsTable := "Settings"
	db := mdbx.NewMDBX(logger).InMem(path).WithTableCfg(func(defaultBuckets kv.TableCfg) kv.TableCfg {
		return kv.TableCfg{
			keysTable:     kv.TableCfgItem{Flags: kv.DupSort},
			indexTable:    kv.TableCfgItem{Flags: kv.DupSort},
			valsTable:     kv.TableCfgItem{Flags: kv.DupSort},
			settingsTable: kv.TableCfgItem{},
		}
	}).MustOpen()
	h, err := NewHistory(path, path, 16, "hist", keysTable, indexTable, valsTable, false, nil, false, logger)
	require.NoError(tb, err)
	h.DisableFsync()
	tb.Cleanup(db.Close)
	tb.Cleanup(h.Close)
	return path, db, h
}

func TestHistoryCollationBuild(t *testing.T) {
	logger := log.New()
	logEvery := time.NewTicker(30 * time.Second)
	defer logEvery.Stop()
	ctx := context.Background()

	test := func(t *testing.T, h *History, db kv.RwDB) {
		t.Helper()
		require := require.New(t)
		tx, err := db.BeginRw(ctx)
		require.NoError(err)
		defer tx.Rollback()
		h.SetTx(tx)
		h.StartWrites()
		defer h.FinishWrites()

		h.SetTxNum(2)
		err = h.AddPrevValue([]byte("key1"), nil, nil)
		require.NoError(err)

		h.SetTxNum(3)
		err = h.AddPrevValue([]byte("key2"), nil, nil)
		require.NoError(err)

		h.SetTxNum(6)
		err = h.AddPrevValue([]byte("key1"), nil, []byte("value1.1"))
		require.NoError(err)
		err = h.AddPrevValue([]byte("key2"), nil, []byte("value2.1"))
		require.NoError(err)

		flusher := h.Rotate()

		h.SetTxNum(7)
		err = h.AddPrevValue([]byte("key2"), nil, []byte("value2.2"))
		require.NoError(err)
		err = h.AddPrevValue([]byte("key3"), nil, nil)
		require.NoError(err)

		err = flusher.Flush(ctx, tx)
		require.NoError(err)

		err = h.Rotate().Flush(ctx, tx)
		require.NoError(err)

		c, err := h.collate(0, 0, 8, tx)
		require.NoError(err)
		require.True(strings.HasSuffix(c.historyPath, "hist.0-1.v"))
		require.Equal(6, c.historyCount)
		require.Equal(3, len(c.indexBitmaps))
		require.Equal([]uint64{7}, c.indexBitmaps["key3"].ToArray())
		require.Equal([]uint64{3, 6, 7}, c.indexBitmaps["key2"].ToArray())
		require.Equal([]uint64{2, 6}, c.indexBitmaps["key1"].ToArray())

		sf, err := h.buildFiles(ctx, 0, c, background.NewProgressSet())
		require.NoError(err)
		defer sf.Close()
		var valWords []string
		g := sf.historyDecomp.MakeGetter()
		g.Reset(0)
		for g.HasNext() {
			w, _ := g.Next(nil)
			valWords = append(valWords, string(w))
		}
		require.Equal([]string{"", "value1.1", "", "value2.1", "value2.2", ""}, valWords)
		require.Equal(6, int(sf.historyIdx.KeyCount()))
		g = sf.efHistoryDecomp.MakeGetter()
		g.Reset(0)
		var keyWords []string
		var intArrs [][]uint64
		for g.HasNext() {
			w, _ := g.Next(nil)
			keyWords = append(keyWords, string(w))
			w, _ = g.Next(w[:0])
			ef, _ := eliasfano32.ReadEliasFano(w)
			ints, err := iter.ToU64Arr(ef.Iterator())
			require.NoError(err)
			intArrs = append(intArrs, ints)
		}
		require.Equal([]string{"key1", "key2", "key3"}, keyWords)
		require.Equal([][]uint64{{2, 6}, {3, 6, 7}, {7}}, intArrs)
		r := recsplit.NewIndexReader(sf.efHistoryIdx)
		for i := 0; i < len(keyWords); i++ {
			offset := r.Lookup([]byte(keyWords[i]))
			g.Reset(offset)
			w, _ := g.Next(nil)
			require.Equal(keyWords[i], string(w))
		}
		r = recsplit.NewIndexReader(sf.historyIdx)
		g = sf.historyDecomp.MakeGetter()
		var vi int
		for i := 0; i < len(keyWords); i++ {
			ints := intArrs[i]
			for j := 0; j < len(ints); j++ {
				var txKey [8]byte
				binary.BigEndian.PutUint64(txKey[:], ints[j])
				offset := r.Lookup2(txKey[:], []byte(keyWords[i]))
				g.Reset(offset)
				w, _ := g.Next(nil)
				require.Equal(valWords[vi], string(w))
				vi++
			}
		}
	}
	t.Run("large_values", func(t *testing.T) {
		_, db, h := testDbAndHistory(t, true, logger)
		test(t, h, db)
	})
	t.Run("small_values", func(t *testing.T) {
		_, db, h := testDbAndHistory(t, false, logger)
		test(t, h, db)
	})
}

func TestHistoryAfterPrune(t *testing.T) {
	logger := log.New()
	logEvery := time.NewTicker(30 * time.Second)
	defer logEvery.Stop()
	ctx := context.Background()
	test := func(t *testing.T, h *History, db kv.RwDB) {
		t.Helper()
		require := require.New(t)
		tx, err := db.BeginRw(ctx)
		require.NoError(err)
		defer tx.Rollback()
		h.SetTx(tx)
		h.StartWrites()
		defer h.FinishWrites()

		h.SetTxNum(2)
		err = h.AddPrevValue([]byte("key1"), nil, nil)
		require.NoError(err)

		h.SetTxNum(3)
		err = h.AddPrevValue([]byte("key2"), nil, nil)
		require.NoError(err)

		h.SetTxNum(6)
		err = h.AddPrevValue([]byte("key1"), nil, []byte("value1.1"))
		require.NoError(err)
		err = h.AddPrevValue([]byte("key2"), nil, []byte("value2.1"))
		require.NoError(err)

		h.SetTxNum(7)
		err = h.AddPrevValue([]byte("key2"), nil, []byte("value2.2"))
		require.NoError(err)
		err = h.AddPrevValue([]byte("key3"), nil, nil)
		require.NoError(err)

		err = h.Rotate().Flush(ctx, tx)
		require.NoError(err)

		c, err := h.collate(0, 0, 16, tx)
		require.NoError(err)

		sf, err := h.buildFiles(ctx, 0, c, background.NewProgressSet())
		require.NoError(err)

		h.integrateFiles(sf, 0, 16)

		err = h.prune(ctx, 0, 16, math.MaxUint64, logEvery)
		require.NoError(err)
		h.SetTx(tx)

		for _, table := range []string{h.indexKeysTable, h.historyValsTable, h.indexTable} {
			var cur kv.Cursor
			cur, err = tx.Cursor(table)
			require.NoError(err)
			defer cur.Close()
			var k []byte
			k, _, err = cur.First()
			require.NoError(err)
			require.Nil(k, table)
		}
	}
	t.Run("large_values", func(t *testing.T) {
		_, db, h := testDbAndHistory(t, true, logger)
		test(t, h, db)
	})
	t.Run("small_values", func(t *testing.T) {
		_, db, h := testDbAndHistory(t, false, logger)
		test(t, h, db)
	})
}

func filledHistory(tb testing.TB, largeValues bool, logger log.Logger) (string, kv.RwDB, *History, uint64) {
	tb.Helper()
	path, db, h := testDbAndHistory(tb, largeValues, logger)
	ctx := context.Background()
	tx, err := db.BeginRw(ctx)
	require.NoError(tb, err)
	defer tx.Rollback()
	h.SetTx(tx)
	h.StartWrites()
	defer h.FinishWrites()

	txs := uint64(1000)
	// keys are encodings of numbers 1..31
	// each key changes value on every txNum which is multiple of the key
	var prevVal [32][]byte
	var flusher flusher
	for txNum := uint64(1); txNum <= txs; txNum++ {
		h.SetTxNum(txNum)
		for keyNum := uint64(1); keyNum <= uint64(31); keyNum++ {
			if txNum%keyNum == 0 {
				valNum := txNum / keyNum
				var k [8]byte
				var v [8]byte
				binary.BigEndian.PutUint64(k[:], keyNum)
				binary.BigEndian.PutUint64(v[:], valNum)
				k[0] = 1   //mark key to simplify debug
				v[0] = 255 //mark value to simplify debug
				err = h.AddPrevValue(k[:], nil, prevVal[keyNum])
				require.NoError(tb, err)
				prevVal[keyNum] = v[:]
			}
		}
		if flusher != nil {
			err = flusher.Flush(ctx, tx)
			require.NoError(tb, err)
			flusher = nil
		}
		if txNum%10 == 0 {
			flusher = h.Rotate()
		}
	}
	if flusher != nil {
		err = flusher.Flush(ctx, tx)
		require.NoError(tb, err)
	}
	err = h.Rotate().Flush(ctx, tx)
	require.NoError(tb, err)
	err = tx.Commit()
	require.NoError(tb, err)

	return path, db, h, txs
}

func checkHistoryHistory(t *testing.T, h *History, txs uint64) {
	t.Helper()
	// Check the history
	hc := h.MakeContext()
	defer hc.Close()

	for txNum := uint64(0); txNum <= txs; txNum++ {
		for keyNum := uint64(1); keyNum <= uint64(31); keyNum++ {
			valNum := txNum / keyNum
			var k [8]byte
			var v [8]byte
			label := fmt.Sprintf("txNum=%d, keyNum=%d", txNum, keyNum)
			//fmt.Printf("label=%s\n", label)
			binary.BigEndian.PutUint64(k[:], keyNum)
			binary.BigEndian.PutUint64(v[:], valNum)
			k[0], v[0] = 0x01, 0xff
			val, ok, err := hc.GetNoState(k[:], txNum+1)
			//require.Equal(t, ok, txNum < 976)
			if ok {
				require.NoError(t, err, label)
				if txNum >= keyNum {
					require.Equal(t, v[:], val, label)
				} else {
					require.Equal(t, []byte{}, val, label)
				}
			}
		}
	}
}

func TestHistoryHistory(t *testing.T) {
	logger := log.New()
	logEvery := time.NewTicker(30 * time.Second)
	defer logEvery.Stop()
	ctx := context.Background()
	test := func(t *testing.T, h *History, db kv.RwDB, txs uint64) {
		t.Helper()
		require := require.New(t)
		tx, err := db.BeginRw(ctx)
		require.NoError(err)
		h.SetTx(tx)
		defer tx.Rollback()

		// Leave the last 2 aggregation steps un-collated
		for step := uint64(0); step < txs/h.aggregationStep-1; step++ {
			func() {
				c, err := h.collate(step, step*h.aggregationStep, (step+1)*h.aggregationStep, tx)
				require.NoError(err)
				sf, err := h.buildFiles(ctx, step, c, background.NewProgressSet())
				require.NoError(err)
				h.integrateFiles(sf, step*h.aggregationStep, (step+1)*h.aggregationStep)
				err = h.prune(ctx, step*h.aggregationStep, (step+1)*h.aggregationStep, math.MaxUint64, logEvery)
				require.NoError(err)
			}()
		}
		checkHistoryHistory(t, h, txs)
	}
	t.Run("large_values", func(t *testing.T) {
		_, db, h, txs := filledHistory(t, true, logger)
		test(t, h, db, txs)
	})
	t.Run("small_values", func(t *testing.T) {
		_, db, h, txs := filledHistory(t, false, logger)
		test(t, h, db, txs)
	})

}

func collateAndMergeHistory(tb testing.TB, db kv.RwDB, h *History, txs uint64) {
	tb.Helper()
	require := require.New(tb)

	logEvery := time.NewTicker(30 * time.Second)
	defer logEvery.Stop()
	ctx := context.Background()
	tx, err := db.BeginRwNosync(ctx)
	require.NoError(err)
	h.SetTx(tx)
	defer tx.Rollback()

	// Leave the last 2 aggregation steps un-collated
	for step := uint64(0); step < txs/h.aggregationStep-1; step++ {
		c, err := h.collate(step, step*h.aggregationStep, (step+1)*h.aggregationStep, tx)
		require.NoError(err)
		sf, err := h.buildFiles(ctx, step, c, background.NewProgressSet())
		require.NoError(err)
		h.integrateFiles(sf, step*h.aggregationStep, (step+1)*h.aggregationStep)
		err = h.prune(ctx, step*h.aggregationStep, (step+1)*h.aggregationStep, math.MaxUint64, logEvery)
		require.NoError(err)
	}

	var r HistoryRanges
	maxEndTxNum := h.endTxNumMinimax()

	maxSpan := h.aggregationStep * StepsInBiggestFile

	for {
		if stop := func() bool {
			hc := h.MakeContext()
			defer hc.Close()
			r = h.findMergeRange(maxEndTxNum, maxSpan)
			if !r.any() {
				return true
			}
			indexOuts, historyOuts, _, err := hc.staticFilesInRange(r)
			require.NoError(err)
			indexIn, historyIn, err := h.mergeFiles(ctx, indexOuts, historyOuts, r, 1, background.NewProgressSet())
			require.NoError(err)
			h.integrateMergedFiles(indexOuts, historyOuts, indexIn, historyIn)
			return false
		}(); stop {
			break
		}
	}

	hc := h.MakeContext()
	defer hc.Close()
	err = hc.BuildOptionalMissedIndices(ctx)
	require.NoError(err)

	err = tx.Commit()
	require.NoError(err)
}

func TestHistoryMergeFiles(t *testing.T) {
	logger := log.New()
	test := func(t *testing.T, h *History, db kv.RwDB, txs uint64) {
		t.Helper()
		collateAndMergeHistory(t, db, h, txs)
		checkHistoryHistory(t, h, txs)
	}

	t.Run("large_values", func(t *testing.T) {
		_, db, h, txs := filledHistory(t, true, logger)
		test(t, h, db, txs)
	})
	t.Run("small_values", func(t *testing.T) {
		_, db, h, txs := filledHistory(t, false, logger)
		test(t, h, db, txs)
	})
}

func TestHistoryScanFiles(t *testing.T) {
	logger := log.New()
	logEvery := time.NewTicker(30 * time.Second)
	defer logEvery.Stop()
	test := func(t *testing.T, h *History, db kv.RwDB, txs uint64) {
		t.Helper()
		require := require.New(t)

		collateAndMergeHistory(t, db, h, txs)
		// Recreate domain and re-scan the files
		txNum := h.txNum
		require.NoError(h.OpenFolder())
		h.SetTxNum(txNum)
		// Check the history
		checkHistoryHistory(t, h, txs)
	}

	t.Run("large_values", func(t *testing.T) {
		_, db, h, txs := filledHistory(t, true, logger)
		test(t, h, db, txs)
	})
	t.Run("small_values", func(t *testing.T) {
		_, db, h, txs := filledHistory(t, false, logger)
		test(t, h, db, txs)
	})
}

func TestIterateChanged(t *testing.T) {
	logger := log.New()
	logEvery := time.NewTicker(30 * time.Second)
	defer logEvery.Stop()
	ctx := context.Background()

	test := func(t *testing.T, h *History, db kv.RwDB, txs uint64) {
		t.Helper()
		require := require.New(t)

		collateAndMergeHistory(t, db, h, txs)

		tx, err := db.BeginRo(ctx)
		require.NoError(err)
		defer tx.Rollback()
		var keys, vals []string
		ic := h.MakeContext()
		defer ic.Close()

		it, err := ic.HistoryRange(2, 20, order.Asc, -1, tx)
		require.NoError(err)
		for it.HasNext() {
			k, v, err := it.Next()
			require.NoError(err)
			keys = append(keys, fmt.Sprintf("%x", k))
			vals = append(vals, fmt.Sprintf("%x", v))
		}
		require.Equal([]string{
			"0100000000000001",
			"0100000000000002",
			"0100000000000003",
			"0100000000000004",
			"0100000000000005",
			"0100000000000006",
			"0100000000000007",
			"0100000000000008",
			"0100000000000009",
			"010000000000000a",
			"010000000000000b",
			"010000000000000c",
			"010000000000000d",
			"010000000000000e",
			"010000000000000f",
			"0100000000000010",
			"0100000000000011",
			"0100000000000012",
			"0100000000000013"}, keys)
		require.Equal([]string{
			"ff00000000000001",
			"",
			"",
			"",
			"",
			"",
			"",
			"",
			"",
			"",
			"",
			"",
			"",
			"",
			"",
			"",
			"",
			"",
			""}, vals)
		it, err = ic.HistoryRange(995, 1000, order.Asc, -1, tx)
		require.NoError(err)
		keys, vals = keys[:0], vals[:0]
		for it.HasNext() {
			k, v, err := it.Next()
			require.NoError(err)
			keys = append(keys, fmt.Sprintf("%x", k))
			vals = append(vals, fmt.Sprintf("%x", v))
		}
		require.Equal([]string{
			"0100000000000001",
			"0100000000000002",
			"0100000000000003",
			"0100000000000004",
			"0100000000000005",
			"0100000000000006",
			"0100000000000009",
			"010000000000000c",
			"010000000000001b",
		}, keys)

		require.Equal([]string{
			"ff000000000003e2",
			"ff000000000001f1",
			"ff0000000000014b",
			"ff000000000000f8",
			"ff000000000000c6",
			"ff000000000000a5",
			"ff0000000000006e",
			"ff00000000000052",
			"ff00000000000024"}, vals)

		// no upper bound
		it, err = ic.HistoryRange(995, -1, order.Asc, -1, tx)
		require.NoError(err)
		keys, vals = keys[:0], vals[:0]
		for it.HasNext() {
			k, v, err := it.Next()
			require.NoError(err)
			keys = append(keys, fmt.Sprintf("%x", k))
			vals = append(vals, fmt.Sprintf("%x", v))
		}
		require.Equal([]string{"0100000000000001", "0100000000000002", "0100000000000003", "0100000000000004", "0100000000000005", "0100000000000006", "0100000000000008", "0100000000000009", "010000000000000a", "010000000000000c", "0100000000000014", "0100000000000019", "010000000000001b"}, keys)
		require.Equal([]string{"ff000000000003e2", "ff000000000001f1", "ff0000000000014b", "ff000000000000f8", "ff000000000000c6", "ff000000000000a5", "ff0000000000007c", "ff0000000000006e", "ff00000000000063", "ff00000000000052", "ff00000000000031", "ff00000000000027", "ff00000000000024"}, vals)

		// no upper bound, limit=2
		it, err = ic.HistoryRange(995, -1, order.Asc, 2, tx)
		require.NoError(err)
		keys, vals = keys[:0], vals[:0]
		for it.HasNext() {
			k, v, err := it.Next()
			require.NoError(err)
			keys = append(keys, fmt.Sprintf("%x", k))
			vals = append(vals, fmt.Sprintf("%x", v))
		}
		require.Equal([]string{"0100000000000001", "0100000000000002"}, keys)
		require.Equal([]string{"ff000000000003e2", "ff000000000001f1"}, vals)

		// no lower bound, limit=2
		it, err = ic.HistoryRange(-1, 1000, order.Asc, 2, tx)
		require.NoError(err)
		keys, vals = keys[:0], vals[:0]
		for it.HasNext() {
			k, v, err := it.Next()
			require.NoError(err)
			keys = append(keys, fmt.Sprintf("%x", k))
			vals = append(vals, fmt.Sprintf("%x", v))
		}
		require.Equal([]string{"0100000000000001", "0100000000000002"}, keys)
		require.Equal([]string{"ff000000000003cf", "ff000000000001e7"}, vals)
	}
	t.Run("large_values", func(t *testing.T) {
		_, db, h, txs := filledHistory(t, true, logger)
		test(t, h, db, txs)
	})
	t.Run("small_values", func(t *testing.T) {
		_, db, h, txs := filledHistory(t, false, logger)
		test(t, h, db, txs)
	})
}

func TestIterateChanged2(t *testing.T) {
	logger := log.New()
	logEvery := time.NewTicker(30 * time.Second)
	defer logEvery.Stop()
	ctx := context.Background()

	test := func(t *testing.T, h *History, db kv.RwDB, txs uint64) {
		t.Helper()
		roTx, err := db.BeginRo(ctx)
		require.NoError(t, err)
		defer roTx.Rollback()

		type testCase struct {
			k, v  string
			txNum uint64
		}
		testCases := []testCase{
			{txNum: 0, k: "0100000000000001", v: ""},
			{txNum: 900, k: "0100000000000001", v: "ff00000000000383"},
			{txNum: 1000, k: "0100000000000001", v: "ff000000000003e7"},
		}
		var keys, vals []string
		t.Run("before merge", func(t *testing.T) {
			hc, require := h.MakeContext(), require.New(t)
			defer hc.Close()

			it, err := hc.HistoryRange(2, 20, order.Asc, -1, roTx)
			require.NoError(err)
			for it.HasNext() {
				k, v, err := it.Next()
				require.NoError(err)
				keys = append(keys, fmt.Sprintf("%x", k))
				vals = append(vals, fmt.Sprintf("%x", v))
			}
			require.NoError(err)
			require.Equal([]string{
				"0100000000000001",
				"0100000000000002",
				"0100000000000003",
				"0100000000000004",
				"0100000000000005",
				"0100000000000006",
				"0100000000000007",
				"0100000000000008",
				"0100000000000009",
				"010000000000000a",
				"010000000000000b",
				"010000000000000c",
				"010000000000000d",
				"010000000000000e",
				"010000000000000f",
				"0100000000000010",
				"0100000000000011",
				"0100000000000012",
				"0100000000000013"}, keys)
			require.Equal([]string{
				"ff00000000000001",
				"",
				"",
				"",
				"",
				"",
				"",
				"",
				"",
				"",
				"",
				"",
				"",
				"",
				"",
				"",
				"",
				"",
				""}, vals)
			keys, vals = keys[:0], vals[:0]

			it, err = hc.HistoryRange(995, 1000, order.Asc, -1, roTx)
			require.NoError(err)
			for it.HasNext() {
				k, v, err := it.Next()
				require.NoError(err)
				keys = append(keys, fmt.Sprintf("%x", k))
				vals = append(vals, fmt.Sprintf("%x", v))
			}
			require.NoError(err)
			require.Equal([]string{
				"0100000000000001",
				"0100000000000002",
				"0100000000000003",
				"0100000000000004",
				"0100000000000005",
				"0100000000000006",
				"0100000000000009",
				"010000000000000c",
				"010000000000001b",
			}, keys)

			require.Equal([]string{
				"ff000000000003e2",
				"ff000000000001f1",
				"ff0000000000014b",
				"ff000000000000f8",
				"ff000000000000c6",
				"ff000000000000a5",
				"ff0000000000006e",
				"ff00000000000052",
				"ff00000000000024"}, vals)

			// single Get test-cases
			tx, err := db.BeginRo(ctx)
			require.NoError(err)
			defer tx.Rollback()

			v, ok, err := hc.GetNoStateWithRecent(hexutility.MustDecodeHex("0100000000000001"), 900, tx)
			require.NoError(err)
			require.True(ok)
			require.Equal(hexutility.MustDecodeHex("ff00000000000383"), v)
			v, ok, err = hc.GetNoStateWithRecent(hexutility.MustDecodeHex("0100000000000001"), 0, tx)
			require.NoError(err)
			require.True(ok)
			require.Equal([]byte{}, v)
			v, ok, err = hc.GetNoStateWithRecent(hexutility.MustDecodeHex("0100000000000001"), 1000, tx)
			require.NoError(err)
			require.True(ok)
			require.Equal(hexutility.MustDecodeHex("ff000000000003e7"), v)
			_ = testCases
		})
		t.Run("after merge", func(t *testing.T) {
			collateAndMergeHistory(t, db, h, txs)
			hc, require := h.MakeContext(), require.New(t)
			defer hc.Close()

			keys = keys[:0]
			it, err := hc.HistoryRange(2, 20, order.Asc, -1, roTx)
			require.NoError(err)
			for it.HasNext() {
				k, _, err := it.Next()
				require.NoError(err)
				keys = append(keys, fmt.Sprintf("%x", k))
			}
			require.NoError(err)
			require.Equal([]string{
				"0100000000000001",
				"0100000000000002",
				"0100000000000003",
				"0100000000000004",
				"0100000000000005",
				"0100000000000006",
				"0100000000000007",
				"0100000000000008",
				"0100000000000009",
				"010000000000000a",
				"010000000000000b",
				"010000000000000c",
				"010000000000000d",
				"010000000000000e",
				"010000000000000f",
				"0100000000000010",
				"0100000000000011",
				"0100000000000012",
				"0100000000000013"}, keys)

			// single Get test-cases
			tx, err := db.BeginRo(ctx)
			require.NoError(err)
			defer tx.Rollback()

			v, ok, err := hc.GetNoStateWithRecent(hexutility.MustDecodeHex("0100000000000001"), 900, tx)
			require.NoError(err)
			require.True(ok)
			require.Equal(hexutility.MustDecodeHex("ff00000000000383"), v)
			v, ok, err = hc.GetNoStateWithRecent(hexutility.MustDecodeHex("0100000000000001"), 0, tx)
			require.NoError(err)
			require.True(ok)
			require.Equal([]byte{}, v)
			v, ok, err = hc.GetNoStateWithRecent(hexutility.MustDecodeHex("0100000000000001"), 1000, tx)
			require.NoError(err)
			require.True(ok)
			require.Equal(hexutility.MustDecodeHex("ff000000000003e7"), v)
		})
	}
	t.Run("large_values", func(t *testing.T) {
		_, db, h, txs := filledHistory(t, true, logger)
		test(t, h, db, txs)
	})
	t.Run("small_values", func(t *testing.T) {
		_, db, h, txs := filledHistory(t, false, logger)
		test(t, h, db, txs)
	})
}

func TestScanStaticFilesH(t *testing.T) {
	logger := log.New()
	h := &History{InvertedIndex: &InvertedIndex{filenameBase: "test", aggregationStep: 1, logger: logger},
		files:  btree2.NewBTreeG[*filesItem](filesItemLess),
		logger: logger,
	}
	files := []string{
		"test.0-1.v",
		"test.1-2.v",
		"test.0-4.v",
		"test.2-3.v",
		"test.3-4.v",
		"test.4-5.v",
	}
	h.scanStateFiles(files)
	require.Equal(t, 6, h.files.Len())

	h.files.Clear()
	h.integrityFileExtensions = []string{"kv"}
	h.scanStateFiles(files)
	require.Equal(t, 0, h.files.Len())

}
