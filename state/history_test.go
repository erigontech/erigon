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
	"testing/fstest"
	"time"

	"github.com/google/btree"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/mdbx"
	"github.com/ledgerwatch/erigon-lib/recsplit"
	"github.com/ledgerwatch/erigon-lib/recsplit/eliasfano32"
	"github.com/ledgerwatch/log/v3"
	"github.com/stretchr/testify/require"
)

func testDbAndHistory(tb testing.TB) (string, kv.RwDB, *History) {
	tb.Helper()
	path := tb.TempDir()
	logger := log.New()
	keysTable := "Keys"
	indexTable := "Index"
	valsTable := "Vals"
	settingsTable := "Settings"
	db := mdbx.NewMDBX(logger).Path(path).WithTableCfg(func(defaultBuckets kv.TableCfg) kv.TableCfg {
		return kv.TableCfg{
			keysTable:     kv.TableCfgItem{Flags: kv.DupSort},
			indexTable:    kv.TableCfgItem{Flags: kv.DupSort},
			valsTable:     kv.TableCfgItem{},
			settingsTable: kv.TableCfgItem{},
		}
	}).MustOpen()
	ii, err := NewHistory(path, path, 16 /* aggregationStep */, "hist" /* filenameBase */, keysTable, indexTable, valsTable, settingsTable, false /* compressVals */)
	require.NoError(tb, err)
	tb.Cleanup(db.Close)
	tb.Cleanup(ii.Close)
	return path, db, ii
}

func TestHistoryCollationBuild(t *testing.T) {
	logEvery := time.NewTicker(30 * time.Second)
	defer logEvery.Stop()
	require := require.New(t)
	_, db, h := testDbAndHistory(t)
	ctx := context.Background()
	tx, err := db.BeginRw(ctx)
	require.NoError(err)
	defer tx.Rollback()
	h.SetTx(tx)
	h.StartWrites("")
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

	err = h.Rotate().Flush(tx)
	require.NoError(err)

	c, err := h.collate(0, 0, 8, tx, logEvery)
	require.NoError(err)
	require.True(strings.HasSuffix(c.historyPath, "hist.0-1.v"))
	require.Equal(6, c.historyCount)
	require.Equal(3, len(c.indexBitmaps))
	require.Equal([]uint64{7}, c.indexBitmaps["key3"].ToArray())
	require.Equal([]uint64{3, 6, 7}, c.indexBitmaps["key2"].ToArray())
	require.Equal([]uint64{2, 6}, c.indexBitmaps["key1"].ToArray())

	sf, err := h.buildFiles(ctx, 0, c)
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
		var ints []uint64
		it := ef.Iterator()
		for it.HasNext() {
			ints = append(ints, it.Next())
		}
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

func TestHistoryAfterPrune(t *testing.T) {
	logEvery := time.NewTicker(30 * time.Second)
	defer logEvery.Stop()
	_, db, h := testDbAndHistory(t)
	ctx := context.Background()
	tx, err := db.BeginRw(ctx)
	require.NoError(t, err)
	defer tx.Rollback()
	h.SetTx(tx)
	h.StartWrites("")
	defer h.FinishWrites()

	h.SetTxNum(2)
	err = h.AddPrevValue([]byte("key1"), nil, nil)
	require.NoError(t, err)

	h.SetTxNum(3)
	err = h.AddPrevValue([]byte("key2"), nil, nil)
	require.NoError(t, err)

	h.SetTxNum(6)
	err = h.AddPrevValue([]byte("key1"), nil, []byte("value1.1"))
	require.NoError(t, err)
	err = h.AddPrevValue([]byte("key2"), nil, []byte("value2.1"))
	require.NoError(t, err)

	h.SetTxNum(7)
	err = h.AddPrevValue([]byte("key2"), nil, []byte("value2.2"))
	require.NoError(t, err)
	err = h.AddPrevValue([]byte("key3"), nil, nil)
	require.NoError(t, err)

	err = h.Rotate().Flush(tx)
	require.NoError(t, err)

	c, err := h.collate(0, 0, 16, tx, logEvery)
	require.NoError(t, err)

	sf, err := h.buildFiles(ctx, 0, c)
	require.NoError(t, err)
	defer sf.Close()

	h.integrateFiles(sf, 0, 16)

	err = h.prune(ctx, 0, 16, math.MaxUint64, logEvery)
	require.NoError(t, err)
	h.SetTx(tx)

	for _, table := range []string{h.indexKeysTable, h.historyValsTable, h.indexTable} {
		var cur kv.Cursor
		cur, err = tx.Cursor(table)
		require.NoError(t, err)
		defer cur.Close()
		var k []byte
		k, _, err = cur.First()
		require.NoError(t, err)
		require.Nil(t, k, table)
	}
}

func filledHistory(tb testing.TB) (string, kv.RwDB, *History, uint64) {
	tb.Helper()
	path, db, h := testDbAndHistory(tb)
	ctx := context.Background()
	tx, err := db.BeginRw(ctx)
	require.NoError(tb, err)
	defer tx.Rollback()
	h.SetTx(tx)
	h.StartWrites("")
	defer h.FinishWrites()

	txs := uint64(1000)
	// keys are encodings of numbers 1..31
	// each key changes value on every txNum which is multiple of the key
	var prevVal [32][]byte
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
		if txNum%10 == 0 {
			err = h.Rotate().Flush(tx)
			require.NoError(tb, err)
		}
	}
	err = h.Rotate().Flush(tx)
	require.NoError(tb, err)
	err = tx.Commit()
	require.NoError(tb, err)
	return path, db, h, txs
}

func checkHistoryHistory(t *testing.T, db kv.RwDB, h *History, txs uint64) {
	t.Helper()
	// Check the history
	hc := h.MakeContext()
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
	logEvery := time.NewTicker(30 * time.Second)
	defer logEvery.Stop()
	_, db, h, txs := filledHistory(t)
	ctx := context.Background()
	tx, err := db.BeginRw(ctx)
	require.NoError(t, err)
	h.SetTx(tx)
	defer tx.Rollback()

	// Leave the last 2 aggregation steps un-collated
	for step := uint64(0); step < txs/h.aggregationStep-1; step++ {
		func() {
			c, err := h.collate(step, step*h.aggregationStep, (step+1)*h.aggregationStep, tx, logEvery)
			require.NoError(t, err)
			sf, err := h.buildFiles(ctx, step, c)
			require.NoError(t, err)
			h.integrateFiles(sf, step*h.aggregationStep, (step+1)*h.aggregationStep)
			err = h.prune(ctx, step*h.aggregationStep, (step+1)*h.aggregationStep, math.MaxUint64, logEvery)
			require.NoError(t, err)
		}()
	}
	checkHistoryHistory(t, db, h, txs)
}

func collateAndMergeHistory(tb testing.TB, db kv.RwDB, h *History, txs uint64) {
	tb.Helper()
	logEvery := time.NewTicker(30 * time.Second)
	defer logEvery.Stop()
	ctx := context.Background()
	tx, err := db.BeginRw(ctx)
	require.NoError(tb, err)
	h.SetTx(tx)
	defer tx.Rollback()
	// Leave the last 2 aggregation steps un-collated
	for step := uint64(0); step < txs/h.aggregationStep-1; step++ {
		func() {
			c, err := h.collate(step, step*h.aggregationStep, (step+1)*h.aggregationStep, tx, logEvery)
			require.NoError(tb, err)
			sf, err := h.buildFiles(ctx, step, c)
			require.NoError(tb, err)
			h.integrateFiles(sf, step*h.aggregationStep, (step+1)*h.aggregationStep)
			err = h.prune(ctx, step*h.aggregationStep, (step+1)*h.aggregationStep, math.MaxUint64, logEvery)
			require.NoError(tb, err)
			var r HistoryRanges
			maxEndTxNum := h.endTxNumMinimax()
			maxSpan := uint64(16 * 16)
			for r = h.findMergeRange(maxEndTxNum, maxSpan); r.any(); r = h.findMergeRange(maxEndTxNum, maxSpan) {
				indexOuts, historyOuts, _ := h.staticFilesInRange(r)
				indexIn, historyIn, err := h.mergeFiles(ctx, indexOuts, historyOuts, r, 1)
				require.NoError(tb, err)
				h.integrateMergedFiles(indexOuts, historyOuts, indexIn, historyIn)
				err = h.deleteFiles(indexOuts, historyOuts)
				require.NoError(tb, err)
			}
		}()
	}
	err = tx.Commit()
	require.NoError(tb, err)
}

func TestHistoryMergeFiles(t *testing.T) {
	_, db, h, txs := filledHistory(t)

	collateAndMergeHistory(t, db, h, txs)
	checkHistoryHistory(t, db, h, txs)
}

func TestHistoryScanFiles(t *testing.T) {
	path, db, h, txs := filledHistory(t)
	var err error

	collateAndMergeHistory(t, db, h, txs)
	// Recreate domain and re-scan the files
	txNum := h.txNum
	h.Close()
	h, err = NewHistory(path, path, h.aggregationStep, h.filenameBase, h.indexKeysTable, h.indexTable, h.historyValsTable, h.settingsTable, h.compressVals)
	require.NoError(t, err)
	defer h.Close()
	h.SetTxNum(txNum)
	// Check the history
	checkHistoryHistory(t, db, h, txs)
}

func TestIterateChanged(t *testing.T) {
	_, db, h, txs := filledHistory(t)
	collateAndMergeHistory(t, db, h, txs)
	ctx := context.Background()

	roTx, err := db.BeginRo(ctx)
	require.NoError(t, err)
	defer roTx.Rollback()
	var keys, vals []string
	ic := h.MakeContext()
	ic.SetTx(roTx)
	it := ic.IterateChanged(2, 20, roTx)
	defer it.Close()
	for it.HasNext() {
		k, v := it.Next(nil, nil)
		keys = append(keys, fmt.Sprintf("%x", k))
		vals = append(vals, fmt.Sprintf("%x", v))
	}
	it.Close()
	require.Equal(t, []string{
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
	require.Equal(t, []string{
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
	it = ic.IterateChanged(995, 1000, roTx)
	keys, vals = keys[:0], vals[:0]
	for it.HasNext() {
		k, v := it.Next(nil, nil)
		keys = append(keys, fmt.Sprintf("%x", k))
		vals = append(vals, fmt.Sprintf("%x", v))
	}
	it.Close()
	require.Equal(t, []string{
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

	require.Equal(t, []string{
		"ff000000000003e2",
		"ff000000000001f1",
		"ff0000000000014b",
		"ff000000000000f8",
		"ff000000000000c6",
		"ff000000000000a5",
		"ff0000000000006e",
		"ff00000000000052",
		"ff00000000000024"}, vals)
}

func TestScanStaticFilesH(t *testing.T) {
	ii := &History{InvertedIndex: &InvertedIndex{filenameBase: "test", aggregationStep: 1},
		files: btree.NewG[*filesItem](32, filesItemLess),
	}
	ffs := fstest.MapFS{
		"test.0-1.v": {},
		"test.1-2.v": {},
		"test.0-4.v": {},
		"test.2-3.v": {},
		"test.3-4.v": {},
		"test.4-5.v": {},
	}
	files, err := ffs.ReadDir(".")
	require.NoError(t, err)
	ii.scanStateFiles(files)
	var found []string
	ii.files.Ascend(func(i *filesItem) bool {
		found = append(found, fmt.Sprintf("%d-%d", i.startTxNum, i.endTxNum))
		return true
	})
	require.Equal(t, 2, len(found))
	require.Equal(t, "0-4", found[0])
	require.Equal(t, "4-5", found[1])
}
