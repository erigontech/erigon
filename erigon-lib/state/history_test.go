// Copyright 2022 The Erigon Authors
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
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"os"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/erigontech/erigon-lib/common/length"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon-lib/seg"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/datadir"

	"github.com/stretchr/testify/require"
	btree2 "github.com/tidwall/btree"

	"github.com/erigontech/erigon-lib/common/background"
	"github.com/erigontech/erigon-lib/common/hexutility"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/kv/mdbx"
	"github.com/erigontech/erigon-lib/kv/order"
	"github.com/erigontech/erigon-lib/kv/stream"
	"github.com/erigontech/erigon-lib/recsplit"
	"github.com/erigontech/erigon-lib/recsplit/eliasfano32"
)

func testDbAndHistory(tb testing.TB, largeValues bool, logger log.Logger) (kv.RwDB, *History) {
	tb.Helper()
	dirs := datadir.New(tb.TempDir())
	keysTable := "AccountKeys"
	indexTable := "AccountIndex"
	valsTable := "AccountVals"
	settingsTable := "Settings"
	db := mdbx.NewMDBX(logger).InMem(dirs.SnapDomain).WithTableCfg(func(defaultBuckets kv.TableCfg) kv.TableCfg {
		if largeValues {
			return kv.TableCfg{
				keysTable:             kv.TableCfgItem{Flags: kv.DupSort},
				indexTable:            kv.TableCfgItem{Flags: kv.DupSort},
				valsTable:             kv.TableCfgItem{Flags: kv.DupSort},
				settingsTable:         kv.TableCfgItem{},
				kv.TblPruningProgress: kv.TableCfgItem{},
			}
		}
		return kv.TableCfg{
			keysTable:             kv.TableCfgItem{Flags: kv.DupSort},
			indexTable:            kv.TableCfgItem{Flags: kv.DupSort},
			valsTable:             kv.TableCfgItem{Flags: kv.DupSort},
			settingsTable:         kv.TableCfgItem{},
			kv.TblPruningProgress: kv.TableCfgItem{},
		}
	}).MustOpen()
	//TODO: tests will fail if set histCfg.compression = CompressKeys | CompressValues
	salt := uint32(1)
	cfg := histCfg{
		iiCfg:             iiCfg{salt: &salt, dirs: dirs, db: db},
		withLocalityIndex: false, withExistenceIndex: false, compression: seg.CompressNone, historyLargeValues: largeValues,
	}
	h, err := NewHistory(cfg, 16, "hist", keysTable, indexTable, valsTable, nil, logger)
	require.NoError(tb, err)
	h.DisableFsync()
	tb.Cleanup(db.Close)
	tb.Cleanup(h.Close)
	return db, h
}

func TestHistoryCollationsAndBuilds(t *testing.T) {
	runTest := func(t *testing.T, largeValues bool) {
		t.Helper()

		totalTx := uint64(1000)
		values := generateTestData(t, length.Addr, length.Addr+length.Hash, totalTx, 100, 10)
		db, h := filledHistoryValues(t, largeValues, values, log.New())
		defer db.Close()

		ctx := context.Background()
		rwtx, err := db.BeginRw(ctx)
		require.NoError(t, err)
		defer rwtx.Rollback()

		var lastAggergatedTx uint64
		for i := uint64(0); i+h.aggregationStep < totalTx; i += h.aggregationStep {
			collation, err := h.collate(ctx, i/h.aggregationStep, i, i+h.aggregationStep, rwtx)
			require.NoError(t, err)
			defer collation.Close()

			require.NotEmptyf(t, collation.historyPath, "collation.historyPath is empty")
			require.NotNil(t, collation.historyComp)
			require.NotEmptyf(t, collation.efHistoryPath, "collation.efHistoryPath is empty")
			require.NotNil(t, collation.efHistoryComp)

			sf, err := h.buildFiles(ctx, i/h.aggregationStep, collation, background.NewProgressSet())
			require.NoError(t, err)
			require.NotNil(t, sf)
			defer sf.CleanupOnError()

			efReader := seg.NewReader(sf.efHistoryDecomp.MakeGetter(), h.compression)
			hReader := seg.NewReader(sf.historyDecomp.MakeGetter(), h.compression)

			// ef contains all sorted keys
			// for each key it has a list of txNums
			// h contains all values for all keys ordered by key + txNum

			var keyBuf, valBuf, hValBuf []byte
			seenKeys := make([]string, 0)

			for efReader.HasNext() {
				keyBuf, _ = efReader.Next(nil)
				valBuf, _ = efReader.Next(nil)

				ef, _ := eliasfano32.ReadEliasFano(valBuf)
				efIt := ef.Iterator()

				require.Contains(t, values, string(keyBuf), "key not found in values")
				seenKeys = append(seenKeys, string(keyBuf))

				vi := 0
				updates, ok := values[string(keyBuf)]
				require.Truef(t, ok, "key not found in values")
				//require.Len(t, updates, int(ef.Count()), "updates count mismatch")

				for efIt.HasNext() {
					txNum, err := efIt.Next()
					require.NoError(t, err)
					require.EqualValuesf(t, updates[vi].txNum, txNum, "txNum mismatch")

					require.Truef(t, hReader.HasNext(), "hReader has no more values")
					hValBuf, _ = hReader.Next(nil)
					if updates[vi].value == nil {
						require.Emptyf(t, hValBuf, "value at %d is not empty (not nil)", vi)
					} else {
						require.EqualValuesf(t, updates[vi].value, hValBuf, "value at %d mismatch", vi)
					}
					vi++
				}
				values[string(keyBuf)] = updates[vi:]
				require.True(t, sort.StringsAreSorted(seenKeys))
			}
			h.integrateDirtyFiles(sf, i, i+h.aggregationStep)
			h.reCalcVisibleFiles(h.dirtyFilesEndTxNumMinimax())
			lastAggergatedTx = i + h.aggregationStep
		}

		for _, updates := range values {
			for _, upd := range updates {
				require.GreaterOrEqual(t, upd.txNum, lastAggergatedTx, "txNum %d is less than lastAggregatedTx %d", upd.txNum, lastAggergatedTx)
			}
		}
	}

	t.Run("largeValues=true", func(t *testing.T) {
		runTest(t, true)
	})
	t.Run("largeValues=false", func(t *testing.T) {
		runTest(t, false)
	})
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
		hc := h.BeginFilesRo()
		defer hc.Close()
		writer := hc.NewWriter()
		defer writer.close()

		writer.SetTxNum(2)
		err = writer.AddPrevValue([]byte("key1"), nil, nil, 0)
		require.NoError(err)

		writer.SetTxNum(3)
		err = writer.AddPrevValue([]byte("key2"), nil, nil, 0)
		require.NoError(err)

		writer.SetTxNum(6)
		err = writer.AddPrevValue([]byte("key1"), nil, []byte("value1.1"), 0)
		require.NoError(err)
		err = writer.AddPrevValue([]byte("key2"), nil, []byte("value2.1"), 0)
		require.NoError(err)

		flusher := writer
		writer = hc.NewWriter()

		writer.SetTxNum(7)
		err = writer.AddPrevValue([]byte("key2"), nil, []byte("value2.2"), 0)
		require.NoError(err)
		err = writer.AddPrevValue([]byte("key3"), nil, nil, 0)
		require.NoError(err)

		err = flusher.Flush(ctx, tx)
		require.NoError(err)

		err = writer.Flush(ctx, tx)
		require.NoError(err)

		c, err := h.collate(ctx, 0, 0, 8, tx)
		require.NoError(err)
		require.True(strings.HasSuffix(c.historyPath, "v1-hist.0-1.v"))
		require.Equal(6, c.historyCount)
		require.Equal(3, c.efHistoryComp.Count()/2)

		sf, err := h.buildFiles(ctx, 0, c, background.NewProgressSet())
		require.NoError(err)
		defer sf.CleanupOnError()
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
			ints, err := stream.ToArrayU64(ef.Iterator())
			require.NoError(err)
			intArrs = append(intArrs, ints)
		}
		require.Equal([]string{"key1", "key2", "key3"}, keyWords)
		require.Equal([][]uint64{{2, 6}, {3, 6, 7}, {7}}, intArrs)
		r := recsplit.NewIndexReader(sf.efHistoryIdx)
		for i := 0; i < len(keyWords); i++ {
			offset, ok := r.TwoLayerLookup([]byte(keyWords[i]))
			if !ok {
				continue
			}
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
				offset, ok := r.Lookup2(txKey[:], []byte(keyWords[i]))
				if !ok {
					continue
				}
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
		hc := h.BeginFilesRo()
		defer hc.Close()
		writer := hc.NewWriter()
		defer writer.close()

		writer.SetTxNum(2)
		err = writer.AddPrevValue([]byte("key1"), nil, nil, 0)
		require.NoError(err)

		writer.SetTxNum(3)
		err = writer.AddPrevValue([]byte("key2"), nil, nil, 0)
		require.NoError(err)

		writer.SetTxNum(6)
		err = writer.AddPrevValue([]byte("key1"), nil, []byte("value1.1"), 0)
		require.NoError(err)
		err = writer.AddPrevValue([]byte("key2"), nil, []byte("value2.1"), 0)
		require.NoError(err)

		writer.SetTxNum(7)
		err = writer.AddPrevValue([]byte("key2"), nil, []byte("value2.2"), 0)
		require.NoError(err)
		err = writer.AddPrevValue([]byte("key3"), nil, nil, 0)
		require.NoError(err)

		err = writer.Flush(ctx, tx)
		require.NoError(err)

		c, err := h.collate(ctx, 0, 0, 16, tx)
		require.NoError(err)

		sf, err := h.buildFiles(ctx, 0, c, background.NewProgressSet())
		require.NoError(err)

		h.integrateDirtyFiles(sf, 0, 16)
		h.reCalcVisibleFiles(h.dirtyFilesEndTxNumMinimax())
		hc.Close()

		hc = h.BeginFilesRo()
		_, err = hc.Prune(ctx, tx, 0, 16, math.MaxUint64, false, logEvery)
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
			require.Nilf(k, "table=%s", table)
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

func TestHistoryCanPrune(t *testing.T) {
	logger := log.New()
	logEvery := time.NewTicker(30 * time.Second)
	defer logEvery.Stop()
	ctx := context.Background()

	stepsTotal := uint64(4)
	stepKeepInDB := uint64(1)

	writeKey := func(t *testing.T, h *History, db kv.RwDB) (addr []byte) {
		t.Helper()

		require := require.New(t)
		tx, err := db.BeginRw(ctx)
		require.NoError(err)
		defer tx.Rollback()

		hc := h.BeginFilesRo()
		defer hc.Close()
		writer := hc.NewWriter()
		defer writer.close()

		addr = common.FromHex("ed7229d50cde8de174cc64a882a0833ca5f11669")
		prev := make([]byte, 0)
		prevStep := uint64(0)
		val := make([]byte, 8)

		for i := uint64(0); i < stepsTotal*h.aggregationStep; i++ {
			writer.SetTxNum(i)
			if cap(val) == 0 {
				val = make([]byte, 8)
			}
			if i%5 == 0 && i > 0 {
				val = nil
			} else {
				binary.BigEndian.PutUint64(val, i)
			}

			err = writer.AddPrevValue(addr[:], val, prev, prevStep)
			require.NoError(err)

			prevStep = i / h.aggregationStep
			prev = common.Copy(val)
		}

		require.NoError(writer.Flush(ctx, tx))
		require.NoError(tx.Commit())

		collateAndMergeHistory(t, db, h, stepsTotal*h.aggregationStep, false)
		return addr
	}
	t.Run("withFiles", func(t *testing.T) {
		db, h := testDbAndHistory(t, true, logger)
		h.snapshotsDisabled = false

		defer db.Close()
		writeKey(t, h, db)

		rwTx, err := db.BeginRw(context.Background())
		defer rwTx.Rollback()
		require.NoError(t, err)

		hc := h.BeginFilesRo()
		defer hc.Close()

		maxTxInSnaps := hc.files.EndTxNum()
		require.Equal(t, (stepsTotal-stepKeepInDB)*16, maxTxInSnaps)

		for i := uint64(0); i < stepsTotal; i++ {
			cp, untilTx := hc.canPruneUntil(rwTx, h.aggregationStep*(i+1))
			require.GreaterOrEqual(t, h.aggregationStep*(stepsTotal-stepKeepInDB), untilTx)
			if i >= stepsTotal-stepKeepInDB {
				require.Falsef(t, cp, "step %d should be NOT prunable", i)
			} else {
				require.Truef(t, cp, "step %d should be prunable", i)
			}
			stat, err := hc.Prune(context.Background(), rwTx, i*h.aggregationStep, (i+1)*h.aggregationStep, math.MaxUint64, false, logEvery)
			require.NoError(t, err)
			if i >= stepsTotal-stepKeepInDB {
				require.Falsef(t, cp, "step %d should be NOT prunable", i)
			} else {
				require.NotNilf(t, stat, "step %d should be pruned and prune stat available", i)
				require.Truef(t, cp, "step %d should be pruned", i)
			}
		}
	})
	t.Run("withoutFiles", func(t *testing.T) {
		db, h := testDbAndHistory(t, false, logger)
		h.snapshotsDisabled = true
		h.keepRecentTxnInDB = stepKeepInDB * h.aggregationStep

		defer db.Close()

		writeKey(t, h, db)

		rwTx, err := db.BeginRw(context.Background())
		defer rwTx.Rollback()
		require.NoError(t, err)

		hc := h.BeginFilesRo()
		defer hc.Close()

		for i := uint64(0); i < stepsTotal; i++ {
			t.Logf("step %d, until %d", i, (i+1)*h.aggregationStep)

			cp, untilTx := hc.canPruneUntil(rwTx, (i+1)*h.aggregationStep)
			require.GreaterOrEqual(t, h.aggregationStep*(stepsTotal-stepKeepInDB), untilTx) // we can prune until the last step
			if i >= stepsTotal-stepKeepInDB {
				require.Falsef(t, cp, "step %d should be NOT prunable", i)
			} else {
				require.Truef(t, cp, "step %d should be prunable", i)
			}
			stat, err := hc.Prune(context.Background(), rwTx, i*h.aggregationStep, (i+1)*h.aggregationStep, math.MaxUint64, false, logEvery)
			require.NoError(t, err)
			if i >= stepsTotal-stepKeepInDB {
				require.Falsef(t, cp, "step %d should be NOT prunable", i)
			} else {
				require.NotNilf(t, stat, "step %d should be pruned and prune stat available", i)
				require.Truef(t, cp, "step %d should be pruned", i)
			}
		}
	})
}

func TestHistoryPruneCorrectnessWithFiles(t *testing.T) {
	values := generateTestData(t, length.Addr, length.Addr, 1000, 1000, 1)
	db, h := filledHistoryValues(t, true, values, log.New())
	defer db.Close()
	defer h.Close()
	h.keepRecentTxnInDB = 900 // should be ignored since files are built
	t.Logf("step=%d\n", h.aggregationStep)

	collateAndMergeHistory(t, db, h, 500, false)

	logEvery := time.NewTicker(30 * time.Second)
	defer logEvery.Stop()

	pruneLimit := uint64(10)
	pruneIters := 8

	rwTx, err := db.BeginRw(context.Background())
	require.NoError(t, err)
	defer rwTx.Rollback()

	var from, to [8]byte
	binary.BigEndian.PutUint64(from[:], uint64(0))
	binary.BigEndian.PutUint64(to[:], uint64(pruneIters)*pruneLimit)

	hc := h.BeginFilesRo()
	defer hc.Close()

	itable, err := rwTx.CursorDupSort(hc.iit.ii.indexTable)
	require.NoError(t, err)
	defer itable.Close()
	limits := 10
	for k, v, err := itable.First(); k != nil; k, v, err = itable.Next() {
		if err != nil {
			t.Fatalf("err: %v", err)
		}
		limits--
		if limits == 0 {
			break
		}
		fmt.Printf("k=%x [%d] v=%x\n", k, binary.BigEndian.Uint64(k), v)
	}
	canHist, txTo := hc.canPruneUntil(rwTx, math.MaxUint64)
	t.Logf("canPrune=%t [%s] to=%d", canHist, hc.h.indexKeysTable, txTo)

	stat, err := hc.Prune(context.Background(), rwTx, 0, txTo, 50, false, logEvery)
	require.NoError(t, err)
	require.NotNil(t, stat)
	t.Logf("stat=%v", stat)

	stat, err = hc.Prune(context.Background(), rwTx, 0, 600, 500, false, logEvery)
	require.NoError(t, err)
	require.NotNil(t, stat)
	t.Logf("stat=%v", stat)
	stat, err = hc.Prune(context.Background(), rwTx, 0, 600, 10, true, logEvery)
	require.NoError(t, err)
	// require.NotNil(t, stat)
	t.Logf("stat=%v", stat)

	stat, err = hc.Prune(context.Background(), rwTx, 0, 600, 10, false, logEvery)
	require.NoError(t, err)
	t.Logf("stat=%v", stat)

	fmt.Printf("start hist table:\n")
	icc, err := rwTx.CursorDupSort(h.historyValsTable)
	require.NoError(t, err)
	defer icc.Close()

	nonPruned := 490

	k, _, err := icc.First()
	require.NoError(t, err)
	require.EqualValues(t, nonPruned, binary.BigEndian.Uint64(k[len(k)-8:]))

	// limits = 10

	// for k, v, err := icc.First(); k != nil; k, v, err = icc.Next() {
	// 	if err != nil {
	// 		t.Fatalf("err: %v", err)
	// 	}
	// 	limits--
	// 	if limits == 0 {
	// 		break
	// 	}
	// 	fmt.Printf("k=%x [%d], v=%x\n", k, binary.BigEndian.Uint64(k[len(k)-8:]), v)
	// }

	// fmt.Printf("start index table:\n")
	itable, err = rwTx.CursorDupSort(hc.iit.ii.indexTable)
	require.NoError(t, err)
	defer itable.Close()

	_, v, err := itable.First()
	if v != nil {
		require.NoError(t, err)
		require.EqualValues(t, nonPruned, binary.BigEndian.Uint64(v))
	}

	// limits = 10
	// for k, v, err := itable.First(); k != nil; k, v, err = itable.Next() {
	// 	if err != nil {
	// 		t.Fatalf("err: %v", err)
	// 	}
	// 	limits--
	// 	if limits == 0 {
	// 		break
	// 	}
	// 	fmt.Printf("k=%x [%d] v=%x\n", k, binary.BigEndian.Uint64(v), v)
	// }

	// fmt.Printf("start index keys table:\n")
	itable, err = rwTx.CursorDupSort(hc.iit.ii.indexKeysTable)
	require.NoError(t, err)
	defer itable.Close()

	k, _, err = itable.First()
	require.NoError(t, err)
	require.EqualValues(t, nonPruned, binary.BigEndian.Uint64(k))

	// limits = 10
	// for k, v, err := itable.First(); k != nil; k, v, err = itable.Next() {
	// 	if err != nil {
	// 		t.Fatalf("err: %v", err)
	// 	}
	// 	if limits == 0 {
	// 		break
	// 	}
	// 	limits--
	// 	fmt.Printf("k=%x [%d] v=%x\n", k, binary.BigEndian.Uint64(k), v)
	// }
}

func TestHistoryPruneCorrectness(t *testing.T) {
	values := generateTestData(t, length.Addr, length.Addr, 1000, 1000, 1)
	db, h := filledHistoryValues(t, true, values, log.New())
	defer db.Close()
	defer h.Close()

	logEvery := time.NewTicker(30 * time.Second)
	defer logEvery.Stop()

	pruneLimit := uint64(10)
	pruneIters := 8

	rwTx, err := db.BeginRw(context.Background())
	require.NoError(t, err)
	defer rwTx.Rollback()

	var from, to [8]byte
	binary.BigEndian.PutUint64(from[:], uint64(0))
	binary.BigEndian.PutUint64(to[:], uint64(pruneIters)*pruneLimit)

	icc, err := rwTx.CursorDupSort(h.historyValsTable)
	require.NoError(t, err)

	count := 0
	for key, _, err := icc.Seek(from[:]); key != nil; key, _, err = icc.Next() {
		require.NoError(t, err)
		//t.Logf("key %x\n", key)
		if bytes.Compare(key[len(key)-8:], to[:]) >= 0 {
			break
		}
		count++
	}
	require.EqualValues(t, pruneIters*int(pruneLimit), count)
	icc.Close()

	hc := h.BeginFilesRo()
	defer hc.Close()

	// this one should not prune anything due to forced=false but no files built
	stat, err := hc.Prune(context.Background(), rwTx, 0, 10, pruneLimit, false, logEvery)
	require.NoError(t, err)
	require.Nil(t, stat)

	// this one should prune value of tx=0 due to given range [0,1) (we have first value at tx=0) even it is forced
	stat, err = hc.Prune(context.Background(), rwTx, 0, 1, pruneLimit, true, logEvery)
	require.NoError(t, err)
	require.EqualValues(t, 1, stat.PruneCountValues)
	require.EqualValues(t, 1, stat.PruneCountTx)

	// this should prune exactly pruneLimit*pruneIter transactions
	for i := 0; i < pruneIters; i++ {
		stat, err = hc.Prune(context.Background(), rwTx, 0, 1000, pruneLimit, true, logEvery)
		require.NoError(t, err)
		t.Logf("[%d] stats: %v", i, stat)
	}

	icc, err = rwTx.CursorDupSort(h.historyValsTable)
	require.NoError(t, err)
	defer icc.Close()

	key, _, err := icc.First()
	require.NoError(t, err)
	require.NotNil(t, key)
	require.EqualValues(t, pruneIters*int(pruneLimit), binary.BigEndian.Uint64(key[len(key)-8:])-1)

	icc, err = rwTx.CursorDupSort(h.indexTable)
	require.NoError(t, err)
	defer icc.Close()
}

func filledHistoryValues(tb testing.TB, largeValues bool, values map[string][]upd, logger log.Logger) (kv.RwDB, *History) {
	tb.Helper()

	for key, upds := range values {
		upds[0].value = nil // history starts from nil
		values[key] = upds
	}

	// history closed inside tb.Cleanup
	db, h := testDbAndHistory(tb, largeValues, logger)
	tb.Cleanup(db.Close)
	tb.Cleanup(h.Close)

	ctx := context.Background()
	//tx, err := db.BeginRw(ctx)
	//require.NoError(tb, err)
	//defer tx.Rollback()

	err := db.Update(ctx, func(tx kv.RwTx) error {
		hc := h.BeginFilesRo()
		defer hc.Close()
		writer := hc.NewWriter()
		defer writer.close()
		// keys are encodings of numbers 1..31
		// each key changes value on every txNum which is multiple of the key
		var flusher flusher
		var keyFlushCount, ps = 0, uint64(0)
		for key, upds := range values {
			for i := 0; i < len(upds); i++ {
				writer.SetTxNum(upds[i].txNum)
				if i > 0 {
					ps = upds[i].txNum / hc.h.aggregationStep
				}
				err := writer.AddPrevValue([]byte(key), nil, upds[i].value, ps)
				require.NoError(tb, err)
			}
			keyFlushCount++
			if keyFlushCount%10 == 0 {
				if flusher != nil {
					err := flusher.Flush(ctx, tx)
					require.NoError(tb, err)
					flusher = nil //nolint
				}
				flusher = writer
				writer = hc.NewWriter()
			}
		}
		if flusher != nil {
			err := flusher.Flush(ctx, tx)
			require.NoError(tb, err)
		}
		return writer.Flush(ctx, tx)
	})
	require.NoError(tb, err)

	return db, h
}

func filledHistory(tb testing.TB, largeValues bool, logger log.Logger) (kv.RwDB, *History, uint64) {
	tb.Helper()
	db, h := testDbAndHistory(tb, largeValues, logger)
	ctx := context.Background()
	tx, err := db.BeginRw(ctx)
	require.NoError(tb, err)
	defer tx.Rollback()
	hc := h.BeginFilesRo()
	defer hc.Close()
	writer := hc.NewWriter()
	defer writer.close()

	txs := uint64(1000)
	// keys are encodings of numbers 1..31
	// each key changes value on every txNum which is multiple of the key
	var prevVal [32][]byte
	var flusher flusher
	for txNum := uint64(1); txNum <= txs; txNum++ {
		writer.SetTxNum(txNum)
		for keyNum := uint64(1); keyNum <= uint64(31); keyNum++ {
			if txNum%keyNum == 0 {
				valNum := txNum / keyNum
				var k [8]byte
				var v [8]byte
				binary.BigEndian.PutUint64(k[:], keyNum)
				binary.BigEndian.PutUint64(v[:], valNum)
				k[0] = 1   //mark key to simplify debug
				v[0] = 255 //mark value to simplify debug
				err = writer.AddPrevValue(k[:], nil, prevVal[keyNum], 0)
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
			flusher = writer
			writer = hc.NewWriter()
		}
	}
	if flusher != nil {
		err = flusher.Flush(ctx, tx)
		require.NoError(tb, err)
	}
	err = writer.Flush(ctx, tx)
	require.NoError(tb, err)
	err = tx.Commit()
	require.NoError(tb, err)

	return db, h, txs
}

func checkHistoryHistory(t *testing.T, h *History, txs uint64) {
	t.Helper()
	// Check the history
	hc := h.BeginFilesRo()
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
			val, ok, err := hc.historySeekInFiles(k[:], txNum+1)
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
				c, err := h.collate(ctx, step, step*h.aggregationStep, (step+1)*h.aggregationStep, tx)
				require.NoError(err)
				sf, err := h.buildFiles(ctx, step, c, background.NewProgressSet())
				require.NoError(err)
				h.integrateDirtyFiles(sf, step*h.aggregationStep, (step+1)*h.aggregationStep)
				h.reCalcVisibleFiles(h.dirtyFilesEndTxNumMinimax())

				hc := h.BeginFilesRo()
				_, err = hc.Prune(ctx, tx, step*h.aggregationStep, (step+1)*h.aggregationStep, math.MaxUint64, false, logEvery)
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

func collateAndMergeHistory(tb testing.TB, db kv.RwDB, h *History, txs uint64, doPrune bool) {
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
		c, err := h.collate(ctx, step, step*h.aggregationStep, (step+1)*h.aggregationStep, tx)
		require.NoError(err)
		sf, err := h.buildFiles(ctx, step, c, background.NewProgressSet())
		require.NoError(err)
		h.integrateDirtyFiles(sf, step*h.aggregationStep, (step+1)*h.aggregationStep)
		h.reCalcVisibleFiles(h.dirtyFilesEndTxNumMinimax())

		if doPrune {
			hc := h.BeginFilesRo()
			_, err = hc.Prune(ctx, tx, step*h.aggregationStep, (step+1)*h.aggregationStep, math.MaxUint64, false, logEvery)
			hc.Close()
			require.NoError(err)
		}
	}

	var r HistoryRanges
	maxSpan := h.aggregationStep * StepsInColdFile

	for {
		if stop := func() bool {
			hc := h.BeginFilesRo()
			defer hc.Close()
			r = hc.findMergeRange(hc.files.EndTxNum(), maxSpan)
			if !r.any() {
				return true
			}
			indexOuts, historyOuts, err := hc.staticFilesInRange(r)
			require.NoError(err)
			indexIn, historyIn, err := hc.mergeFiles(ctx, indexOuts, historyOuts, r, background.NewProgressSet())
			require.NoError(err)
			h.integrateMergedDirtyFiles(indexOuts, historyOuts, indexIn, historyIn)
			h.reCalcVisibleFiles(h.dirtyFilesEndTxNumMinimax())
			return false
		}(); stop {
			break
		}
	}

	hc := h.BeginFilesRo()
	defer hc.Close()
	err = hc.iit.BuildOptionalMissedIndices(ctx, background.NewProgressSet())
	require.NoError(err)

	err = tx.Commit()
	require.NoError(err)
}

func TestHistoryMergeFiles(t *testing.T) {
	logger := log.New()
	test := func(t *testing.T, h *History, db kv.RwDB, txs uint64) {
		t.Helper()
		collateAndMergeHistory(t, db, h, txs, true)
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

		collateAndMergeHistory(t, db, h, txs, true)
		hc := h.BeginFilesRo()
		defer hc.Close()
		// Recreate domain and re-scan the files
		require.NoError(h.openFolder())
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

func TestIterateChanged(t *testing.T) {
	logger := log.New()
	logEvery := time.NewTicker(30 * time.Second)
	defer logEvery.Stop()
	ctx := context.Background()

	test := func(t *testing.T, h *History, db kv.RwDB, txs uint64) {
		t.Helper()
		require := require.New(t)

		collateAndMergeHistory(t, db, h, txs, true)

		tx, err := db.BeginRo(ctx)
		require.NoError(err)
		defer tx.Rollback()
		var keys, vals []string
		var steps []uint64
		ic := h.BeginFilesRo()
		defer ic.Close()

		it, err := ic.HistoryRange(2, 20, order.Asc, -1, tx)
		require.NoError(err)
		for it.HasNext() {
			k, v, step, err := it.Next()
			require.NoError(err)
			keys = append(keys, fmt.Sprintf("%x", k))
			vals = append(vals, fmt.Sprintf("%x", v))
			steps = append(steps, step)
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
		require.Equal(make([]uint64, 19), steps)
		it, err = ic.HistoryRange(995, 1000, order.Asc, -1, tx)
		require.NoError(err)
		keys, vals, steps = keys[:0], vals[:0], steps[:0]
		for it.HasNext() {
			k, v, step, err := it.Next()
			require.NoError(err)
			keys = append(keys, fmt.Sprintf("%x", k))
			vals = append(vals, fmt.Sprintf("%x", v))
			steps = append(steps, step)
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

		require.Equal(make([]uint64, 9), steps)

		// no upper bound
		it, err = ic.HistoryRange(995, -1, order.Asc, -1, tx)
		require.NoError(err)
		keys, vals, steps = keys[:0], vals[:0], steps[:0]
		for it.HasNext() {
			k, v, step, err := it.Next()
			require.NoError(err)
			keys = append(keys, fmt.Sprintf("%x", k))
			vals = append(vals, fmt.Sprintf("%x", v))
			steps = append(steps, step)
		}
		require.Equal([]string{"0100000000000001", "0100000000000002", "0100000000000003", "0100000000000004", "0100000000000005", "0100000000000006", "0100000000000008", "0100000000000009", "010000000000000a", "010000000000000c", "0100000000000014", "0100000000000019", "010000000000001b"}, keys)
		require.Equal([]string{"ff000000000003e2", "ff000000000001f1", "ff0000000000014b", "ff000000000000f8", "ff000000000000c6", "ff000000000000a5", "ff0000000000007c", "ff0000000000006e", "ff00000000000063", "ff00000000000052", "ff00000000000031", "ff00000000000027", "ff00000000000024"}, vals)
		require.Equal(make([]uint64, 13), steps)

		// no upper bound, limit=2
		it, err = ic.HistoryRange(995, -1, order.Asc, 2, tx)
		require.NoError(err)
		keys, vals, steps = keys[:0], vals[:0], steps[:0]
		for it.HasNext() {
			k, v, step, err := it.Next()
			require.NoError(err)
			keys = append(keys, fmt.Sprintf("%x", k))
			vals = append(vals, fmt.Sprintf("%x", v))
			steps = append(steps, step)
		}
		require.Equal([]string{"0100000000000001", "0100000000000002"}, keys)
		require.Equal([]string{"ff000000000003e2", "ff000000000001f1"}, vals)
		require.Equal(make([]uint64, 2), steps)

		// no lower bound, limit=2
		it, err = ic.HistoryRange(-1, 1000, order.Asc, 2, tx)
		require.NoError(err)
		keys, vals, steps = keys[:0], vals[:0], steps[:0]
		for it.HasNext() {
			k, v, step, err := it.Next()
			require.NoError(err)
			keys = append(keys, fmt.Sprintf("%x", k))
			vals = append(vals, fmt.Sprintf("%x", v))
			steps = append(steps, step)
		}
		require.Equal([]string{"0100000000000001", "0100000000000002"}, keys)
		require.Equal([]string{"ff000000000003cf", "ff000000000001e7"}, vals)
		require.Equal(make([]uint64, 2), steps)
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
			{txNum: 99, k: "00000000000063", v: ""},
			{txNum: 199, k: "00000000000063", v: "d1ce000000000383"},
			{txNum: 900, k: "0100000000000001", v: "ff00000000000383"},
			{txNum: 1000, k: "0100000000000001", v: "ff000000000003e7"},
		}
		var firstKey [8]byte
		binary.BigEndian.PutUint64(firstKey[:], 1)
		firstKey[0] = 1 //mark key to simplify debug

		var keys, vals []string
		var steps []uint64
		t.Run("before merge", func(t *testing.T) {
			hc, require := h.BeginFilesRo(), require.New(t)
			defer hc.Close()

			{ //check IdxRange
				idxIt, err := hc.IdxRange(firstKey[:], -1, -1, order.Asc, -1, roTx)
				require.NoError(err)
				cnt, err := stream.CountU64(idxIt)
				require.NoError(err)
				require.Equal(1000, cnt)

				idxIt, err = hc.IdxRange(firstKey[:], 2, 20, order.Asc, -1, roTx)
				require.NoError(err)
				idxItDesc, err := hc.IdxRange(firstKey[:], 19, 1, order.Desc, -1, roTx)
				require.NoError(err)
				descArr, err := stream.ToArrayU64(idxItDesc)
				require.NoError(err)
				stream.ExpectEqualU64(t, idxIt, stream.ReverseArray(descArr))
			}

			it, err := hc.HistoryRange(2, 20, order.Asc, -1, roTx)
			require.NoError(err)
			for it.HasNext() {
				k, v, step, err := it.Next()
				require.NoError(err)
				keys = append(keys, fmt.Sprintf("%x", k))
				vals = append(vals, fmt.Sprintf("%x", v))
				steps = append(steps, step)
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
			require.Equal(make([]uint64, 19), steps)
			keys, vals, steps = keys[:0], vals[:0], steps[:0]

			it, err = hc.HistoryRange(995, 1000, order.Asc, -1, roTx)
			require.NoError(err)
			for it.HasNext() {
				k, v, step, err := it.Next()
				require.NoError(err)
				keys = append(keys, fmt.Sprintf("%x", k))
				vals = append(vals, fmt.Sprintf("%x", v))
				steps = append(steps, step)
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

			require.Equal(make([]uint64, 9), steps)

			// single Get test-cases
			tx, err := db.BeginRo(ctx)
			require.NoError(err)
			defer tx.Rollback()

			v, ok, err := hc.HistorySeek(hexutility.MustDecodeHex("0100000000000001"), 900, tx)
			require.NoError(err)
			require.True(ok)
			require.Equal(hexutility.MustDecodeHex("ff00000000000383"), v)
			v, ok, err = hc.HistorySeek(hexutility.MustDecodeHex("0100000000000001"), 0, tx)
			require.NoError(err)
			require.True(ok)
			require.Equal([]byte{}, v)
			v, ok, err = hc.HistorySeek(hexutility.MustDecodeHex("0100000000000001"), 1000, tx)
			require.NoError(err)
			require.True(ok)
			require.Equal(hexutility.MustDecodeHex("ff000000000003e7"), v)
			_ = testCases
		})
		t.Run("after merge", func(t *testing.T) {
			collateAndMergeHistory(t, db, h, txs, true)
			hc, require := h.BeginFilesRo(), require.New(t)
			defer hc.Close()

			keys = keys[:0]
			it, err := hc.HistoryRange(2, 20, order.Asc, -1, roTx)
			require.NoError(err)
			for it.HasNext() {
				k, _, _, err := it.Next()
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

			v, ok, err := hc.HistorySeek(hexutility.MustDecodeHex("0100000000000001"), 900, tx)
			require.NoError(err)
			require.True(ok)
			require.Equal(hexutility.MustDecodeHex("ff00000000000383"), v)
			v, ok, err = hc.HistorySeek(hexutility.MustDecodeHex("0100000000000001"), 0, tx)
			require.NoError(err)
			require.True(ok)
			require.Equal([]byte{}, v)
			v, ok, err = hc.HistorySeek(hexutility.MustDecodeHex("0100000000000001"), 1000, tx)
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
		dirtyFiles: btree2.NewBTreeG[*filesItem](filesItemLess),
	}
	files := []string{
		"v1-test.0-1.v",
		"v1-test.1-2.v",
		"v1-test.0-4.v",
		"v1-test.2-3.v",
		"v1-test.3-4.v",
		"v1-test.4-5.v",
	}
	h.scanDirtyFiles(files)
	require.Equal(t, 6, h.dirtyFiles.Len())

	h.dirtyFiles.Clear()
	h.integrityCheck = func(fromStep, toStep uint64) bool { return false }
	h.scanDirtyFiles(files)
	require.Equal(t, 0, h.dirtyFiles.Len())

}

func writeSomeHistory(tb testing.TB, largeValues bool, logger log.Logger) (kv.RwDB, *History, [][]byte, uint64) {
	tb.Helper()
	db, h := testDbAndHistory(tb, largeValues, logger)
	ctx := context.Background()
	tx, err := db.BeginRw(ctx)
	require.NoError(tb, err)
	defer tx.Rollback()
	hc := h.BeginFilesRo()
	defer hc.Close()
	writer := hc.NewWriter()
	defer writer.close()

	keys := [][]byte{
		common.FromHex(""),
		common.FromHex("a4dba136b5541817a78b160dd140190d9676d0f0"),
		common.FromHex("01"),
		common.FromHex("00"),
		keyCommitmentState,
		common.FromHex("8240a92799b51e7d99d3ef53c67bca7d068bd8d64e895dd56442c4ac01c9a27d"),
		common.FromHex("cedce3c4eb5e0eedd505c33fd0f8c06d1ead96e63d6b3a27b5186e4901dce59e"),
	}

	txs := uint64(1000)
	var prevVal [7][]byte
	var flusher flusher
	for txNum := uint64(1); txNum <= txs; txNum++ {
		writer.SetTxNum(txNum)

		for ik, k := range keys {
			var v [8]byte
			binary.BigEndian.PutUint64(v[:], txNum)
			if ik == 0 && txNum%33 == 0 {
				continue
			}
			err = writer.AddPrevValue(k, nil, prevVal[ik], 0)
			require.NoError(tb, err)

			prevVal[ik] = v[:]
		}

		if txNum%33 == 0 {
			err = writer.AddPrevValue(keys[0], nil, nil, 0)
			require.NoError(tb, err)
		}

		if flusher != nil {
			err = flusher.Flush(ctx, tx)
			require.NoError(tb, err)
			flusher = nil
		}
		if txNum%10 == 0 {
			flusher = writer
			writer = hc.NewWriter()
		}
	}
	if flusher != nil {
		err = flusher.Flush(ctx, tx)
		require.NoError(tb, err)
	}
	err = writer.Flush(ctx, tx)
	require.NoError(tb, err)
	err = tx.Commit()
	require.NoError(tb, err)

	return db, h, keys, txs
}

func Test_HistoryIterate_VariousKeysLen(t *testing.T) {
	logger := log.New()
	logEvery := time.NewTicker(30 * time.Second)
	defer logEvery.Stop()
	ctx := context.Background()

	test := func(t *testing.T, h *History, db kv.RwDB, writtenKeys [][]byte, txs uint64) {
		t.Helper()
		require := require.New(t)

		collateAndMergeHistory(t, db, h, txs, true)

		tx, err := db.BeginRo(ctx)
		require.NoError(err)
		defer tx.Rollback()
		ic := h.BeginFilesRo()
		defer ic.Close()

		iter, err := ic.HistoryRange(1, -1, order.Asc, -1, tx)
		require.NoError(err)

		keys := make([][]byte, 0)
		for iter.HasNext() {
			k, _, _, err := iter.Next()
			require.NoError(err)
			keys = append(keys, k)
			//vals = append(vals, fmt.Sprintf("%x", v))
		}

		sort.Slice(writtenKeys, func(i, j int) bool {
			return bytes.Compare(writtenKeys[i], writtenKeys[j]) < 0
		})

		require.Equal(fmt.Sprintf("%#x", writtenKeys[0]), fmt.Sprintf("%#x", keys[0]))
		require.Equal(len(writtenKeys), len(keys))
		require.Equal(fmt.Sprintf("%#x", writtenKeys), fmt.Sprintf("%#x", keys))
	}

	//LargeHistoryValues: don't support various keys len
	//TODO: write hist test for non-various keys len
	//t.Run("large_values", func(t *testing.T) {
	//	db, h, keys, txs := writeSomeHistory(t, true, logger)
	//	test(t, h, db, keys, txs)
	//})
	t.Run("small_values", func(t *testing.T) {
		db, h, keys, txs := writeSomeHistory(t, false, logger)
		test(t, h, db, keys, txs)
	})

}

func TestHistory_OpenFolder(t *testing.T) {
	logger := log.New()
	db, h, txs := filledHistory(t, true, logger)
	collateAndMergeHistory(t, db, h, txs, true)

	list := h._visibleFiles
	require.NotEmpty(t, list)
	ff := list[len(list)-1]
	fn := ff.src.decompressor.FilePath()
	h.Close()

	err := os.Remove(fn)
	require.NoError(t, err)
	err = os.WriteFile(fn, make([]byte, 33), 0644)
	require.NoError(t, err)

	err = h.openFolder()
	require.NoError(t, err)
	h.Close()
}
