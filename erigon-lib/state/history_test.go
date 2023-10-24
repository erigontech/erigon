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

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/datadir"

	"github.com/ledgerwatch/log/v3"
	"github.com/stretchr/testify/require"
	btree2 "github.com/tidwall/btree"

	"github.com/ledgerwatch/erigon-lib/common/background"
	"github.com/ledgerwatch/erigon-lib/common/hexutility"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/iter"
	"github.com/ledgerwatch/erigon-lib/kv/mdbx"
	"github.com/ledgerwatch/erigon-lib/kv/order"
	"github.com/ledgerwatch/erigon-lib/recsplit"
	"github.com/ledgerwatch/erigon-lib/recsplit/eliasfano32"
)

func testDbAndHistory(tb testing.TB, largeValues bool, logger log.Logger) (kv.RwDB, *History) {
	tb.Helper()
	dirs := datadir.New(tb.TempDir())
	keysTable := "AccountKeys"
	indexTable := "AccountIndex"
	valsTable := "AccountVals"
	settingsTable := "Settings"
	db := mdbx.NewMDBX(logger).InMem(dirs.SnapDomain).WithTableCfg(func(defaultBuckets kv.TableCfg) kv.TableCfg {
		return kv.TableCfg{
			keysTable:     kv.TableCfgItem{Flags: kv.DupSort},
			indexTable:    kv.TableCfgItem{Flags: kv.DupSort},
			valsTable:     kv.TableCfgItem{Flags: kv.DupSort},
			settingsTable: kv.TableCfgItem{},
		}
	}).MustOpen()
	//TODO: tests will fail if set histCfg.compression = CompressKeys | CompressValues
	salt := uint32(1)
	cfg := histCfg{
		iiCfg:             iiCfg{salt: &salt, dirs: dirs},
		withLocalityIndex: false, withExistenceIndex: true, compression: CompressNone, historyLargeValues: largeValues,
	}
	h, err := NewHistory(cfg, 16, "hist", keysTable, indexTable, valsTable, nil, logger)
	require.NoError(tb, err)
	h.DisableFsync()
	tb.Cleanup(db.Close)
	tb.Cleanup(h.Close)
	return db, h
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
		hc := h.MakeContext()
		defer hc.Close()
		hc.StartWrites()
		defer hc.FinishWrites()

		hc.SetTxNum(2)
		err = hc.AddPrevValue([]byte("key1"), nil, nil)
		require.NoError(err)

		hc.SetTxNum(3)
		err = hc.AddPrevValue([]byte("key2"), nil, nil)
		require.NoError(err)

		hc.SetTxNum(6)
		err = hc.AddPrevValue([]byte("key1"), nil, []byte("value1.1"))
		require.NoError(err)
		err = hc.AddPrevValue([]byte("key2"), nil, []byte("value2.1"))
		require.NoError(err)

		flusher := hc.Rotate()

		hc.SetTxNum(7)
		err = hc.AddPrevValue([]byte("key2"), nil, []byte("value2.2"))
		require.NoError(err)
		err = hc.AddPrevValue([]byte("key3"), nil, nil)
		require.NoError(err)

		err = flusher.Flush(ctx, tx)
		require.NoError(err)

		err = hc.Rotate().Flush(ctx, tx)
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
		db, h := testDbAndHistory(t, true, logger)
		test(t, h, db)
	})
	t.Run("small_values", func(t *testing.T) {
		db, h := testDbAndHistory(t, false, logger)
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
		hc := h.MakeContext()
		defer hc.Close()
		hc.StartWrites()
		defer hc.FinishWrites()

		hc.SetTxNum(2)
		err = hc.AddPrevValue([]byte("key1"), nil, nil)
		require.NoError(err)

		hc.SetTxNum(3)
		err = hc.AddPrevValue([]byte("key2"), nil, nil)
		require.NoError(err)

		hc.SetTxNum(6)
		err = hc.AddPrevValue([]byte("key1"), nil, []byte("value1.1"))
		require.NoError(err)
		err = hc.AddPrevValue([]byte("key2"), nil, []byte("value2.1"))
		require.NoError(err)

		hc.SetTxNum(7)
		err = hc.AddPrevValue([]byte("key2"), nil, []byte("value2.2"))
		require.NoError(err)
		err = hc.AddPrevValue([]byte("key3"), nil, nil)
		require.NoError(err)

		err = hc.Rotate().Flush(ctx, tx)
		require.NoError(err)

		c, err := h.collate(0, 0, 16, tx)
		require.NoError(err)

		sf, err := h.buildFiles(ctx, 0, c, background.NewProgressSet())
		require.NoError(err)

		h.integrateFiles(sf, 0, 16)
		hc.Close()

		hc = h.MakeContext()
		err = hc.Prune(ctx, tx, 0, 16, math.MaxUint64, logEvery)
		hc.Close()

		require.NoError(err)

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
		db, h := testDbAndHistory(t, true, logger)
		test(t, h, db)
	})
	t.Run("small_values", func(t *testing.T) {
		db, h := testDbAndHistory(t, false, logger)
		test(t, h, db)
	})
}

func filledHistory(tb testing.TB, largeValues bool, logger log.Logger) (kv.RwDB, *History, uint64) {
	tb.Helper()
	db, h := testDbAndHistory(tb, largeValues, logger)
	ctx := context.Background()
	tx, err := db.BeginRw(ctx)
	require.NoError(tb, err)
	defer tx.Rollback()
	hc := h.MakeContext()
	defer hc.Close()
	hc.StartWrites()
	defer hc.FinishWrites()

	txs := uint64(1000)
	// keys are encodings of numbers 1..31
	// each key changes value on every txNum which is multiple of the key
	var prevVal [32][]byte
	var flusher flusher
	for txNum := uint64(1); txNum <= txs; txNum++ {
		hc.SetTxNum(txNum)
		for keyNum := uint64(1); keyNum <= uint64(31); keyNum++ {
			if txNum%keyNum == 0 {
				valNum := txNum / keyNum
				var k [8]byte
				var v [8]byte
				binary.BigEndian.PutUint64(k[:], keyNum)
				binary.BigEndian.PutUint64(v[:], valNum)
				k[0] = 1   //mark key to simplify debug
				v[0] = 255 //mark value to simplify debug
				err = hc.AddPrevValue(k[:], nil, prevVal[keyNum])
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
			flusher = hc.Rotate()
		}
	}
	if flusher != nil {
		err = flusher.Flush(ctx, tx)
		require.NoError(tb, err)
	}
	err = hc.Rotate().Flush(ctx, tx)
	require.NoError(tb, err)
	err = tx.Commit()
	require.NoError(tb, err)

	return db, h, txs
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
		defer tx.Rollback()

		// Leave the last 2 aggregation steps un-collated
		for step := uint64(0); step < txs/h.aggregationStep-1; step++ {
			func() {
				c, err := h.collate(step, step*h.aggregationStep, (step+1)*h.aggregationStep, tx)
				require.NoError(err)
				sf, err := h.buildFiles(ctx, step, c, background.NewProgressSet())
				require.NoError(err)
				h.integrateFiles(sf, step*h.aggregationStep, (step+1)*h.aggregationStep)

				hc := h.MakeContext()
				err = hc.Prune(ctx, tx, step*h.aggregationStep, (step+1)*h.aggregationStep, math.MaxUint64, logEvery)
				hc.Close()
				require.NoError(err)
			}()
		}
		checkHistoryHistory(t, h, txs)
	}
	t.Run("large_values", func(t *testing.T) {
		db, h, txs := filledHistory(t, true, logger)
		test(t, h, db, txs)
	})
	t.Run("small_values", func(t *testing.T) {
		db, h, txs := filledHistory(t, false, logger)
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
	defer tx.Rollback()

	// Leave the last 2 aggregation steps un-collated
	for step := uint64(0); step < txs/h.aggregationStep-1; step++ {
		c, err := h.collate(step, step*h.aggregationStep, (step+1)*h.aggregationStep, tx)
		require.NoError(err)
		sf, err := h.buildFiles(ctx, step, c, background.NewProgressSet())
		require.NoError(err)
		h.integrateFiles(sf, step*h.aggregationStep, (step+1)*h.aggregationStep)

		hc := h.MakeContext()
		err = hc.Prune(ctx, tx, step*h.aggregationStep, (step+1)*h.aggregationStep, math.MaxUint64, logEvery)
		hc.Close()
		require.NoError(err)
	}

	var r HistoryRanges
	maxEndTxNum := h.endTxNumMinimax()

	maxSpan := h.aggregationStep * StepsInColdFile

	for {
		if stop := func() bool {
			hc := h.MakeContext()
			defer hc.Close()
			r = hc.findMergeRange(maxEndTxNum, maxSpan)
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
	err = hc.ic.BuildOptionalMissedIndices(ctx, background.NewProgressSet())
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
		db, h, txs := filledHistory(t, true, logger)
		test(t, h, db, txs)
	})
	t.Run("small_values", func(t *testing.T) {
		db, h, txs := filledHistory(t, false, logger)
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
		hc := h.MakeContext()
		defer hc.Close()
		// Recreate domain and re-scan the files
		txNum := hc.ic.txNum
		require.NoError(h.OpenFolder())
		hc.SetTxNum(txNum)
		// Check the history
		checkHistoryHistory(t, h, txs)
	}

	t.Run("large_values", func(t *testing.T) {
		db, h, txs := filledHistory(t, true, logger)
		test(t, h, db, txs)
		db.Close()
	})
	t.Run("small_values", func(t *testing.T) {
		db, h, txs := filledHistory(t, false, logger)
		test(t, h, db, txs)
		db.Close()
	})
}

func TestHistory_UnwindExperiment(t *testing.T) {
	db, h := testDbAndHistory(t, false, log.New())

	hc := h.MakeContext()
	defer hc.Close()
	hc.StartWrites()
	defer hc.FinishWrites()

	key := common.FromHex("deadbeef")
	loc := common.FromHex("1ceb00da")
	var prevVal []byte
	for i := 0; i < 8; i++ {
		hc.SetTxNum(uint64(1 << i))
		err := hc.AddPrevValue(key, loc, prevVal)
		require.NoError(t, err)
		prevVal = []byte("d1ce" + fmt.Sprintf("%x", i))
	}
	err := db.Update(context.Background(), func(tx kv.RwTx) error {
		return hc.Rotate().Flush(context.Background(), tx)
	})
	require.NoError(t, err)

	tx, err := db.BeginRo(context.Background())
	require.NoError(t, err)
	defer tx.Rollback()

	for i := 0; i < 32; i++ {
		toRest, needDelete, err := hc.ifUnwindKey(common.Append(key, loc), uint64(i), tx)
		fmt.Printf("i=%d tx %d toRest=%v, needDelete=%v\n", i, i, toRest, needDelete)
		require.NoError(t, err)
		if i > 1 {
			require.NotNil(t, toRest)
			require.True(t, needDelete)
			if 0 == (i&i - 1) {
				require.Equal(t, uint64(i>>1), toRest.TxNum)
				require.Equal(t, []byte("d1ce"+fmt.Sprintf("%x", i>>1)), toRest.Value)
			}
		} else {
			require.Nil(t, toRest)
			require.True(t, needDelete)
		}
	}
}

func TestHistory_IfUnwindKey(t *testing.T) {
	db, h := testDbAndHistory(t, false, log.New())

	hc := h.MakeContext()
	defer hc.Close()
	hc.StartWrites()

	rwTx, err := db.BeginRw(context.Background())
	require.NoError(t, err)
	defer rwTx.Rollback()

	// Add some test data
	key := common.FromHex("1ceb00da")
	var val []byte
	for i := uint64(1); i <= 5; i++ {
		hc.SetTxNum(i)
		hc.AddPrevValue(key, nil, val)
		val = []byte(fmt.Sprintf("value_%d", i))
	}
	err = hc.Rotate().Flush(context.Background(), rwTx)
	require.NoError(t, err)
	hc.FinishWrites()

	//// Test case 1: key not found
	//toTxNum := uint64(0)
	//toRestore, needDeleting, err := hc.ifUnwindKey(key, toTxNum, rwTx)
	//require.NoError(t, err)
	//require.Nil(t, toRestore)
	//require.True(t, needDeleting)
	//
	//// Test case 2: key found, but no value at toTxNum
	//toTxNum = 6
	//toRestore, needDeleting, err = hc.ifUnwindKey(key, toTxNum, rwTx)
	//require.NoError(t, err)
	//require.Nil(t, toRestore)
	//require.True(t, needDeleting)

	var toTxNum uint64
	// Test case 3: key found, value at toTxNum, no value after toTxNum
	toTxNum = 3
	toRestore, needDeleting, err := hc.ifUnwindKey(key, toTxNum, rwTx)
	require.NoError(t, err)
	require.NotNil(t, toRestore)
	require.True(t, needDeleting)
	require.Equal(t, uint64(2), toRestore.TxNum)
	require.Equal(t, []byte("value_2"), toRestore.Value)

	// Test case 4: key found, value at toTxNum, value after toTxNum
	toTxNum = 2
	toRestore, needDeleting, err = hc.ifUnwindKey(key, toTxNum, rwTx)
	require.NoError(t, err)
	require.NotNil(t, toRestore)
	require.True(t, needDeleting)
	require.Equal(t, uint64(1), toRestore.TxNum)
	require.Equal(t, []byte("value_1"), toRestore.Value)
}

func TestHisory_Unwind(t *testing.T) {
	logger := log.New()
	logEvery := time.NewTicker(30 * time.Second)
	defer logEvery.Stop()
	ctx := context.Background()

	test := func(t *testing.T, h *History, db kv.RwDB, txs uint64) {
		t.Helper()
		require := require.New(t)

		tx, err := db.BeginRw(ctx)
		require.NoError(err)
		hctx := h.MakeContext()
		defer hctx.Close()

		hctx.StartWrites()
		defer hctx.FinishWrites()

		unwindKeys := make([][]byte, 8)
		for i := 0; i < len(unwindKeys); i++ {
			unwindKeys[i] = []byte(fmt.Sprintf("unwind_key%d", i))
		}

		v := make([]byte, 8)
		for i := uint64(0); i < txs; i += 6 {
			hctx.SetTxNum(i)

			binary.BigEndian.PutUint64(v, i)

			for _, uk1 := range unwindKeys {
				err := hctx.AddPrevValue(uk1, nil, v)
				require.NoError(err)
			}
		}
		err = hctx.Rotate().Flush(ctx, tx)
		require.NoError(err)
		hctx.FinishWrites()
		require.NoError(tx.Commit())

		collateAndMergeHistory(t, db, h, txs)

		tx, err = db.BeginRw(ctx)
		require.NoError(err)
		defer tx.Rollback()
		var keys, vals []string
		_, _ = keys, vals

		ic := h.MakeContext()
		defer ic.Close()

		for i := 0; i < len(unwindKeys); i++ {
			// it, err := ic.IdxRange(unwindKeys[i], 30, int(txs), order.Asc, -1, tx)
			val, found, err := ic.GetNoStateWithRecent(unwindKeys[i], 30, tx)
			require.NoError(err)
			require.True(found)
			fmt.Printf("unwind key %x, val=%x (txn %d)\n", unwindKeys[i], val, binary.BigEndian.Uint64(val))

			// for it.HasNext() {
			// 	txN, err := it.Next()
			// 	require.NoError(err)
			// 	fmt.Printf("txN=%d\n", txN)
			// }
			rec, needDel, err := ic.ifUnwindKey(unwindKeys[i], 32, tx)
			require.NoError(err)
			require.True(needDel)
			if rec != nil {
				fmt.Printf("txn %d v=%x|prev %x\n", rec.TxNum, rec.Value, rec.PValue)
			}
		}

		// it, err := ic.HistoryRange(2, 200, order.Asc, -1, tx)
		// require.NoError(err)
		// uniq := make(map[string]int)
		// for it.HasNext() {

		// 	k, v, err := it.Next()
		// 	require.NoError(err)
		// 	keys = append(keys, fmt.Sprintf("%x", k))
		// 	vals = append(vals, fmt.Sprintf("%x", v))
		// 	uniq[fmt.Sprintf("%x", k)]++
		// 	fmt.Printf("k=%x, v=%x\n", k, v)
		// }
		// for k, v := range uniq {
		// 	if v > 1 {
		// 		fmt.Printf("count k=%s, v=%d\n", k, v)
		// 	}
		// }
	}
	t.Run("small_values", func(t *testing.T) {
		db, h := testDbAndHistory(t, false, logger)
		defer db.Close()
		defer h.Close()

		test(t, h, db, 1000)
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
		db, h, txs := filledHistory(t, true, logger)
		test(t, h, db, txs)
	})
	t.Run("small_values", func(t *testing.T) {
		db, h, txs := filledHistory(t, false, logger)
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
		db, h, txs := filledHistory(t, true, logger)
		test(t, h, db, txs)
	})
	t.Run("small_values", func(t *testing.T) {
		db, h, txs := filledHistory(t, false, logger)
		test(t, h, db, txs)
	})
}

func TestScanStaticFilesH(t *testing.T) {
	h := &History{InvertedIndex: emptyTestInvertedIndex(1),
		files: btree2.NewBTreeG[*filesItem](filesItemLess),
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
	h.integrityCheck = func(fromStep, toStep uint64) bool { return false }
	h.scanStateFiles(files)
	require.Equal(t, 0, h.files.Len())

}
