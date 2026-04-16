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
	"slices"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/background"
	"github.com/erigontech/erigon/common/dir"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/config3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/mdbx"
	"github.com/erigontech/erigon/db/kv/order"
	"github.com/erigontech/erigon/db/kv/stream"
	"github.com/erigontech/erigon/db/recsplit"
	"github.com/erigontech/erigon/db/recsplit/multiencseq"
	"github.com/erigontech/erigon/db/seg"
	"github.com/erigontech/erigon/db/state/statecfg"
	"github.com/erigontech/erigon/execution/commitment/commitmentdb"
)

func testDbAndHistory(tb testing.TB, largeValues bool, logger log.Logger) (kv.RwDB, *History) {
	tb.Helper()
	dirs := datadir.New(tb.TempDir())
	db := mdbx.New(dbcfg.ChainDB, logger).InMem(tb, dirs.Chaindata).MustOpen()
	tb.Cleanup(db.Close)

	//TODO: tests will fail if set histCfg.Compression = CompressKeys | CompressValues
	salt := uint32(1)
	cfg := statecfg.Schema.AccountsDomain

	cfg.Hist.IiCfg.Accessors = statecfg.AccessorHashMap
	cfg.Hist.HistoryLargeValues = largeValues

	//perf of tests
	cfg.Hist.IiCfg.Compression = seg.CompressNone
	cfg.Hist.Compression = seg.CompressNone
	//cfg.hist.historyValuesOnCompressedPage = 16
	aggregationStep := uint64(16)
	h, err := NewHistory(cfg.Hist, aggregationStep, config3.DefaultStepsInFrozenFile, dirs, logger)
	require.NoError(tb, err)
	tb.Cleanup(h.Close)
	h.salt.Store(&salt)
	h.DisableFsync()
	return db, h
}

func TestHistoryCollationsAndBuilds(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	t.Parallel()

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
		for i := uint64(0); i+h.stepSize < totalTx; i += h.stepSize {
			collation, err := h.collate(ctx, kv.Step(i/h.stepSize), i, i+h.stepSize, rwtx)
			require.NoError(t, err)

			require.NotEmptyf(t, collation.historyPath, "collation.historyPath is empty")
			require.NotNil(t, collation.historyComp)
			require.NotEmptyf(t, collation.efHistoryPath, "collation.efHistoryPath is empty")
			require.NotNil(t, collation.efHistoryComp)

			sf, err := h.buildFiles(ctx, kv.Step(i/h.stepSize), collation, background.NewProgressSet())
			collation.Close()
			require.NoError(t, err)
			require.NotNil(t, sf)
			defer sf.CleanupOnError()

			compressedPageValuesCount := sf.historyDecomp.CompressedPageValuesCount()

			if sf.historyDecomp.CompressionFormatVersion() == seg.FileCompressionFormatV0 {
				compressedPageValuesCount = h.HistoryValuesOnCompressedPage
			}

			efReader := h.InvertedIndex.dataReader(sf.efHistoryDecomp)
			hReader := seg.NewPagedReader(h.dataReader(sf.historyDecomp), compressedPageValuesCount, true)

			// ef contains all sorted keys
			// for each key it has a list of txNums
			// h contains all values for all keys ordered by key + txNum

			var keyBuf, valBuf, hValBuf []byte
			seenKeys := make([]string, 0)
			for efReader.HasNext() {
				keyBuf, _ = efReader.Next(nil)
				valBuf, _ = efReader.Next(nil)

				ef := multiencseq.ReadMultiEncSeq(i, valBuf)
				efIt := ef.Iterator(0)

				require.Contains(t, values, string(keyBuf), "key not found in values")
				seenKeys = append(seenKeys, string(keyBuf))

				vi := 0
				updates, ok := values[string(keyBuf)]
				require.Truef(t, ok, "key not found in values")
				//require.Len(t, updates, int(ef.Count()), "updates count mismatch")

				for efIt.HasNext() {
					txNum, err := efIt.Next()
					require.NoError(t, err)
					require.Equalf(t, updates[vi].txNum, txNum, "txNum mismatch")

					require.Truef(t, hReader.HasNext(), "hReader has no more values")
					hValBuf, _ = hReader.Next(nil)
					if updates[vi].value == nil {
						require.Emptyf(t, hValBuf, "value at %d is not empty (not nil)", vi)
					} else {
						require.Equalf(t, updates[vi].value, hValBuf, "value at %d mismatch", vi)
					}
					vi++
				}
				values[string(keyBuf)] = updates[vi:]
				require.True(t, slices.IsSorted(seenKeys))
			}
			h.integrateDirtyFiles(sf, i, i+h.stepSize)
			lastAggergatedTx = i + h.stepSize
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
	if testing.Short() {
		t.Skip()
	}

	t.Parallel()

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
		hc := h.beginForTests()
		defer hc.Close()
		writer := hc.NewWriter()
		defer writer.close()

		err = writer.AddPrevValue([]byte("key1"), 2, nil)
		require.NoError(err)

		err = writer.AddPrevValue([]byte("key2"), 3, nil)
		require.NoError(err)

		err = writer.AddPrevValue([]byte("key1"), 6, []byte("value1.1"))
		require.NoError(err)
		err = writer.AddPrevValue([]byte("key2"), 6, []byte("value2.1"))
		require.NoError(err)

		flusher := writer
		writer = hc.NewWriter()

		err = writer.AddPrevValue([]byte("key2"), 7, []byte("value2.2"))
		require.NoError(err)
		err = writer.AddPrevValue([]byte("key3"), 7, nil)
		require.NoError(err)

		err = flusher.Flush(ctx, tx)
		require.NoError(err)

		err = writer.Flush(ctx, tx)
		require.NoError(err)

		c, err := h.collate(ctx, 0, 0, 8, tx)
		require.NoError(err)
		defer c.Close()

		require.True(strings.HasSuffix(c.historyPath, h.vFileName(0, 1)))
		require.Equal(3, c.efHistoryComp.Count()/2)
		require.Equal(seg.WordsAmount2PagesAmount(6, h.CompressorCfg.ValuesOnCompressedPage), 1) // because page size is 1
		require.Equal(6, c.historyComp.Count())                                                  // comp disabled on collate

		sf, err := h.buildFiles(ctx, 0, c, background.NewProgressSet())
		require.NoError(err)
		defer sf.CleanupOnError()
		var valWords []string

		compressedPageValuesCount := sf.historyDecomp.CompressedPageValuesCount()

		if sf.historyDecomp.CompressionFormatVersion() == seg.FileCompressionFormatV0 {
			compressedPageValuesCount = h.HistoryValuesOnCompressedPage
		}

		gh := seg.NewPagedReader(h.dataReader(sf.historyDecomp), compressedPageValuesCount, true)
		gh.Reset(0)
		for gh.HasNext() {
			w, _ := gh.Next(nil)
			valWords = append(valWords, string(w))
		}
		require.Equal([]string{"", "value1.1", "", "value2.1", "value2.2", ""}, valWords)
		require.Equal(6, int(sf.historyIdx.KeyCount()))
		ge := sf.efHistoryDecomp.MakeGetter()
		ge.Reset(0)
		var keyWords []string
		var intArrs [][]uint64
		for ge.HasNext() {
			w, _ := ge.Next(nil)
			keyWords = append(keyWords, string(w))
			w, _ = ge.Next(w[:0])
			ef := multiencseq.ReadMultiEncSeq(0, w)
			ints, err := stream.ToArrayU64(ef.Iterator(0))
			require.NoError(err)
			intArrs = append(intArrs, ints)
		}
		require.Equal([]string{"key1", "key2", "key3"}, keyWords)
		require.Equal([][]uint64{{2, 6}, {3, 6, 7}, {7}}, intArrs)
		r := recsplit.NewIndexReader(sf.efHistoryIdx)
		defer r.Close()
		for i := 0; i < len(keyWords); i++ {
			var offset uint64
			var ok bool
			if h.InvertedIndex.Accessors.Has(statecfg.AccessorExistence) {
				offset, ok = r.Lookup([]byte(keyWords[i]))
				if !ok {
					continue
				}
			} else {
				offset, ok = r.TwoLayerLookup([]byte(keyWords[i]))
				if !ok {
					continue
				}
			}
			ge.Reset(offset)
			w, _ := ge.Next(nil)
			require.Equal(keyWords[i], string(w))
		}
		r = recsplit.NewIndexReader(sf.historyIdx)
		defer r.Close()

		gh = seg.NewPagedReader(h.dataReader(sf.historyDecomp), compressedPageValuesCount, true)
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
				gh.Reset(offset)
				w, _ := gh.Next(nil)
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

// TestHistoryBuildVI_PageCounterResetOnCollisionRetry verifies that the page
// counter 'i' used for paged history files is reset when buildVI retries due
// to a recsplit collision. Without the reset, the .vi index would contain
// incorrect offsets on the retry pass.
//
// The bug only manifests when compressedPageValuesCount > 0 (paged history),
// which happens during merge (not initial collation). This test forces a
// collision retry during merge and verifies history lookups remain correct.
func TestHistoryBuildVI_PageCounterResetOnCollisionRetry(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	t.Parallel()

	logger := log.New()

	test := func(t *testing.T, largeValues bool) {
		t.Helper()
		db, h, txs := filledHistory(t, largeValues, logger)

		// Force collision retries during buildVI calls.
		// Set the hook AFTER collation but BEFORE merge, so only merge's
		// buildVI calls get the forced collision.
		collateAndMergeHistoryWithCollisionRetry(t, db, h, txs)
		checkHistoryHistory(t, h, txs)
	}
	t.Run("large_values", func(t *testing.T) {
		test(t, true)
	})
	t.Run("small_values", func(t *testing.T) {
		test(t, false)
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
		hc := h.beginForTests()
		defer hc.Close()
		writer := hc.NewWriter()
		defer writer.close()

		err = writer.AddPrevValue([]byte("key1"), 2, nil)
		require.NoError(err)

		err = writer.AddPrevValue([]byte("key2"), 3, nil)
		require.NoError(err)

		err = writer.AddPrevValue([]byte("key1"), 6, []byte("value1.1"))
		require.NoError(err)
		err = writer.AddPrevValue([]byte("key2"), 6, []byte("value2.1"))
		require.NoError(err)

		err = writer.AddPrevValue([]byte("key2"), 7, []byte("value2.2"))
		require.NoError(err)
		err = writer.AddPrevValue([]byte("key3"), 7, nil)
		require.NoError(err)

		err = writer.Flush(ctx, tx)
		require.NoError(err)

		require.NoError(h.collateBuildIntegrate(ctx, 0, tx, background.NewProgressSet()))
		hc.Close()

		hc = h.beginForTests()
		_, err = hc.Prune(ctx, tx, 0, 16, math.MaxUint64, false, logEvery)
		hc.Close()

		require.NoError(err)

		for _, table := range []string{h.KeysTable, h.ValuesTable, h.ValuesTable} {
			func() {
				cur, err := tx.Cursor(table)
				require.NoError(err)
				defer cur.Close()
				k, _, err := cur.First()
				require.NoError(err)
				require.Nilf(k, "table=%s", table)
			}()
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

func TestHistoryRangeWithPrune(t *testing.T) {
	logger := log.New()
	logEvery := time.NewTicker(30 * time.Second)
	defer logEvery.Stop()
	ctx := context.Background()

	db, h, _ := filledHistory(t, true, logger)
	collateAndMergeHistory(t, db, h, 32, true)

	roTx, err := db.BeginRo(ctx)
	require.NoError(t, err)
	defer roTx.Rollback()

	hc := h.beginForTests()
	defer hc.Close()

	var keys, vals []string
	it, err := hc.HistoryRange(14, 31, order.Asc, -1, roTx)
	require.NoError(t, err)

	for it.HasNext() {
		k, v, err := it.Next()
		require.NoError(t, err)
		keys = append(keys, fmt.Sprintf("%x", k))
		vals = append(vals, fmt.Sprintf("%x", v))
	}

	db2, h2, _ := filledHistory(t, true, logger)
	collateAndMergeHistory(t, db2, h2, 32, false)

	roTx2, err := db2.BeginRo(ctx)
	require.NoError(t, err)
	defer roTx2.Rollback()
	hc2 := h2.beginForTests()
	defer hc2.Close()

	var keys2, vals2 []string
	it2, err := hc2.HistoryRange(14, 31, order.Asc, -1, roTx2)
	require.NoError(t, err)

	for it2.HasNext() {
		k, v, err := it2.Next()
		require.NoError(t, err)
		keys2 = append(keys2, fmt.Sprintf("%x", k))
		vals2 = append(vals2, fmt.Sprintf("%x", v))
	}

	require.Equal(t, keys, keys2)
	require.Equal(t, vals, vals2)
}

func TestHistoryAsOfWithPrune(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	logger := log.New()
	logEvery := time.NewTicker(30 * time.Second)
	defer logEvery.Stop()
	ctx := context.Background()

	db, h, _ := filledHistory(t, true, logger)
	collateAndMergeHistory(t, db, h, 200, false)

	roTx, err := db.BeginRo(ctx)
	require.NoError(t, err)
	defer roTx.Rollback()

	hc := h.beginForTests()
	defer hc.Close()

	var keys, vals []string

	from, to := hexutil.MustDecode("0x0100000000000009"), hexutil.MustDecode("0x0100000000000014") // 9, 20
	it, err := hc.RangeAsOf(ctx, 14, from, to, order.Asc, -1, roTx)
	require.NoError(t, err)

	for it.HasNext() {
		k, v, err := it.Next()
		require.NoError(t, err)
		keys = append(keys, fmt.Sprintf("%x", k))
		vals = append(vals, fmt.Sprintf("%x", v))
	}

	db2, h2, _ := filledHistory(t, true, logger)
	collateAndMergeHistory(t, db2, h2, 200, true)

	roTx2, err := db2.BeginRo(ctx)
	require.NoError(t, err)
	defer roTx2.Rollback()
	hc2 := h2.beginForTests()
	defer hc2.Close()

	var keys2, vals2 []string
	it2, err := hc2.RangeAsOf(ctx, 14, from, to, order.Asc, -1, roTx2)
	require.NoError(t, err)

	for it2.HasNext() {
		k, v, err := it2.Next()
		require.NoError(t, err)
		keys2 = append(keys2, fmt.Sprintf("%x", k))
		vals2 = append(vals2, fmt.Sprintf("%x", v))
	}

	require.Equal(t, keys, keys2)
	require.Equal(t, vals, vals2)
}

func TestHistoryCanPrune(t *testing.T) {
	t.Parallel()

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

		hc := h.beginForTests()
		defer hc.Close()
		writer := hc.NewWriter()
		defer writer.close()

		addr = common.FromHex("ed7229d50cde8de174cc64a882a0833ca5f11669")
		prev := make([]byte, 0)
		val := make([]byte, 8)

		for i := uint64(0); i < stepsTotal*h.stepSize; i++ {
			if cap(val) == 0 {
				val = make([]byte, 8)
			}
			if i%5 == 0 && i > 0 {
				val = nil
			} else {
				binary.BigEndian.PutUint64(val, i)
			}

			err = writer.AddPrevValue(append(addr, val...), i, prev)
			require.NoError(err)

			prev = common.Copy(val)
		}

		require.NoError(writer.Flush(ctx, tx))
		require.NoError(tx.Commit())

		collateAndMergeHistory(t, db, h, stepsTotal*h.stepSize, false)
		return addr
	}

	if !testing.Short() {
		t.Run("withFiles", func(t *testing.T) {
			db, h := testDbAndHistory(t, true, logger)
			h.SnapshotsDisabled = false

			defer db.Close()
			writeKey(t, h, db)

			rwTx, err := db.BeginRw(context.Background())
			require.NoError(t, err)
			defer rwTx.Rollback()

			hc := h.beginForTests()
			defer hc.Close()

			maxTxInSnaps := hc.files.EndTxNum()
			require.Equal(t, (stepsTotal-stepKeepInDB)*16, maxTxInSnaps)

			for i := uint64(0); i < stepsTotal; i++ {
				cp, untilTx := hc.canPruneUntil(rwTx, h.stepSize*(i+1))
				require.GreaterOrEqual(t, h.stepSize*(stepsTotal-stepKeepInDB), untilTx)
				if i >= stepsTotal-stepKeepInDB {
					require.Falsef(t, cp, "step %d should be NOT prunable", i)
				} else {
					require.Truef(t, cp, "step %d should be prunable", i)
				}
				stat, err := hc.Prune(context.Background(), rwTx, i*h.stepSize, (i+1)*h.stepSize, math.MaxUint64, false, logEvery)
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

	t.Run("withoutFiles", func(t *testing.T) {
		db, h := testDbAndHistory(t, false, logger)
		h.SnapshotsDisabled = true
		h.KeepRecentTxnInDB = stepKeepInDB * h.stepSize

		defer db.Close()

		writeKey(t, h, db)

		rwTx, err := db.BeginRw(context.Background())
		require.NoError(t, err)
		defer rwTx.Rollback()

		hc := h.beginForTests()
		defer hc.Close()

		for i := uint64(0); i < stepsTotal; i++ {
			t.Logf("step %d, until %d", i, (i+1)*h.stepSize)

			cp, untilTx := hc.canPruneUntil(rwTx, (i+1)*h.stepSize)
			require.GreaterOrEqual(t, h.stepSize*(stepsTotal-stepKeepInDB), untilTx) // we can prune until the last step
			if i >= stepsTotal-stepKeepInDB {
				require.Falsef(t, cp, "step %d should be NOT prunable", i)
			} else {
				require.Truef(t, cp, "step %d should be prunable", i)
			}
			stat, err := hc.Prune(context.Background(), rwTx, i*h.stepSize, (i+1)*h.stepSize, math.MaxUint64, false, logEvery)
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
	const nonPruned = 490

	// setup builds snapshot files then returns the open History, a write tx, and a log ticker.
	// DB/History cleanup is registered by filledHistoryValues.
	setup := func(t *testing.T) (*History, kv.RwTx, *time.Ticker) {
		t.Helper()
		values := generateTestData(t, length.Addr, length.Addr, 1000, 1000, 1)
		db, h := filledHistoryValues(t, true, values, log.New()) // registers Cleanup for db and h
		h.KeepRecentTxnInDB = 900                                // should be ignored since files are built
		t.Logf("step=%d\n", h.stepSize)

		collateAndMergeHistory(t, db, h, 500, false)

		logEvery := time.NewTicker(30 * time.Second)
		t.Cleanup(logEvery.Stop)

		rwTx, err := db.BeginRw(context.Background())
		require.NoError(t, err)
		t.Cleanup(rwTx.Rollback)

		return h, rwTx, logEvery
	}

	assertResults := func(t *testing.T, h *History, rwTx kv.RwTx, hc *HistoryRoTx) {
		t.Helper()
		icc, err := rwTx.CursorDupSort(h.ValuesTable)
		require.NoError(t, err)
		defer icc.Close()
		k, _, err := icc.First()
		require.NoError(t, err)
		require.EqualValues(t, nonPruned, binary.BigEndian.Uint64(k[len(k)-8:]))

		itable, err := rwTx.CursorDupSort(hc.iit.ii.ValuesTable)
		require.NoError(t, err)
		defer itable.Close()
		_, v, err := itable.First()
		if v != nil {
			require.NoError(t, err)
			require.EqualValues(t, nonPruned, binary.BigEndian.Uint64(v))
		}

		itable2, err := rwTx.CursorDupSort(hc.iit.ii.KeysTable)
		require.NoError(t, err)
		defer itable2.Close()
		k, _, err = itable2.First()
		require.NoError(t, err)
		require.EqualValues(t, nonPruned, binary.BigEndian.Uint64(k))
	}

	t.Run("scan_prune", func(t *testing.T) {
		t.Skip("TODO: figure out pretty way to do this check")
		h, rwTx, logEvery := setup(t)
		hc := h.beginForTests()
		defer hc.Close()

		canHist, txTo := hc.canPruneUntil(rwTx, math.MaxUint64)
		t.Logf("canPrune=%t [%s] to=%d", canHist, hc.h.KeysTable, txTo)

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
		t.Logf("stat=%v", stat)

		stat, err = hc.Prune(context.Background(), rwTx, 0, 600, 10, false, logEvery)
		require.NoError(t, err)
		t.Logf("stat=%v", stat)

		assertResults(t, h, rwTx, hc)
		checkHistoryDBCleanliness(t, h, hc, rwTx, nonPruned)
	})
}

func TestHistoryPruneCorrectness(t *testing.T) {
	t.Parallel()

	pruneLimit := uint64(10)
	pruneIters := 8

	// setup creates an independent DB+History filled with test data, verifies
	// the initial count in [0, pruneIters*pruneLimit), and returns the open
	// History, an open write transaction, and a log ticker. All resources are
	// cleaned up via t.Cleanup when the (sub-)test ends.
	setup := func(t *testing.T) (*History, kv.RwTx, *time.Ticker) {
		t.Helper()
		values := generateTestData(t, length.Addr, length.Addr, 1000, 1000, 1)
		db, h := filledHistoryValues(t, true, values, log.New()) // registers Cleanup for db and h

		logEvery := time.NewTicker(30 * time.Second)
		t.Cleanup(logEvery.Stop)

		rwTx, err := db.BeginRw(context.Background())
		require.NoError(t, err)
		t.Cleanup(rwTx.Rollback)

		var from, to [8]byte
		binary.BigEndian.PutUint64(from[:], 0)
		binary.BigEndian.PutUint64(to[:], uint64(pruneIters)*pruneLimit)

		icc, err := rwTx.CursorDupSort(h.ValuesTable)
		require.NoError(t, err)
		defer icc.Close()

		count := 0
		for key, _, err := icc.Seek(from[:]); key != nil; key, _, err = icc.Next() {
			require.NoError(t, err)
			if bytes.Compare(key[len(key)-8:], to[:]) >= 0 {
				break
			}
			count++
		}
		require.Equal(t, pruneIters*int(pruneLimit), count)

		return h, rwTx, logEvery
	}

	t.Run("scan_prune", func(t *testing.T) {
		t.Parallel()
		h, rwTx, logEvery := setup(t)

		hc := h.beginForTests()
		defer hc.Close()

		// should not prune anything: forced=false but no files built
		stat, err := hc.Prune(context.Background(), rwTx, 0, 10, pruneLimit, false, logEvery)
		require.NoError(t, err)
		require.Nil(t, stat)

		// should prune tx=0: range [0,1) forced=true
		stat, err = hc.Prune(context.Background(), rwTx, 0, 1, MaxUint64, true, logEvery)
		require.NoError(t, err)
		require.EqualValues(t, 1, stat.PruneCountValues)
		require.EqualValues(t, 1, stat.PruneCountTx)

		//TODO: figure out pretty way to deal with it.
		//// prune exactly pruneLimit*pruneIters transactions
		//for i := 0; i < pruneIters; i++ {
		//	stat, err = hc.Prune(context.Background(), rwTx, 0, 1000, pruneLimit, true, logEvery)
		//	require.NoError(t, err)
		//	t.Logf("[%d] stats: %v", i, stat)
		//}
		//icc, err := rwTx.CursorDupSort(h.ValuesTable)
		//require.NoError(t, err)
		//defer icc.Close()
		//key, _, err := icc.First()
		//require.NoError(t, err)
		//require.NotNil(t, key)
		//require.EqualValues(t, pruneIters*int(pruneLimit), binary.BigEndian.Uint64(key[len(key)-8:])-1)
	})
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
		hc := h.beginForTests()
		defer hc.Close()
		writer := hc.NewWriter()
		defer writer.close()
		// keys are encodings of numbers 1..31
		// each key changes value on every txNum which is multiple of the key
		var flusher flusher
		var keyFlushCount = 0
		for key, upds := range values {
			for i := 0; i < len(upds); i++ {
				err := writer.AddPrevValue([]byte(key), upds[i].txNum, upds[i].value)
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
	hc := h.beginForTests()
	defer hc.Close()
	writer := hc.NewWriter()
	defer writer.close()

	txs := uint64(1000)
	// keys are encodings of numbers 1..31
	// each key changes value on every txNum which is multiple of the key
	var prevVal [32][]byte
	var flusher flusher
	for txNum := uint64(1); txNum <= txs; txNum++ {
		for keyNum := uint64(1); keyNum <= uint64(31); keyNum++ {
			if txNum%keyNum == 0 {
				valNum := txNum / keyNum
				var k [8]byte
				var v [8]byte
				binary.BigEndian.PutUint64(k[:], keyNum)
				binary.BigEndian.PutUint64(v[:], valNum)
				k[0] = 1   //mark key to simplify debug
				v[0] = 255 //mark value to simplify debug
				err = writer.AddPrevValue(k[:], txNum, prevVal[keyNum])
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
	hc := h.beginForTests()
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

	checkHistoryProperties(t, hc)
}

// checkHistoryProperties verifies structural invariants of frozen history files:
// checkHistoryProperties runs all structural invariant checks on frozen history files.
func checkHistoryProperties(t *testing.T, hc *HistoryRoTx) {
	t.Helper()
	checkHistoryFileMetadata(t, hc)
	checkHistoryNoDuplicates(t, hc)
	checkHistoryIndexConsistency(t, hc)
}

// checkHistoryFileMetadata verifies structural invariants on file metadata:
//   - No empty files: startTxNum < endTxNum for every file.
//   - Step alignment: startTxNum and endTxNum are multiples of stepSize.
//   - No gaps or overlaps: adjacent files satisfy file[i-1].endTxNum == file[i].startTxNum.
//   - Symmetric coverage: history and index files cover the exact same set of ranges.
func checkHistoryFileMetadata(t *testing.T, hc *HistoryRoTx) {
	t.Helper()

	for _, label := range []struct {
		name  string
		files visibleFiles
	}{
		{"history", hc.files},
		{"index", hc.iit.files},
	} {
		files := label.files
		for i, f := range files {
			require.Less(t, f.startTxNum, f.endTxNum,
				"%s file %d: empty range [%d, %d)", label.name, i, f.startTxNum, f.endTxNum)

			require.Zero(t, f.startTxNum%hc.stepSize,
				"%s file %d: startTxNum=%d not aligned to stepSize=%d",
				label.name, i, f.startTxNum, hc.stepSize)

			require.Zero(t, f.endTxNum%hc.stepSize,
				"%s file %d: endTxNum=%d not aligned to stepSize=%d",
				label.name, i, f.endTxNum, hc.stepSize)

			if i > 0 {
				require.Equal(t, files[i-1].endTxNum, f.startTxNum,
					"%s files: gap or overlap between file %d [%d-%d) and file %d [%d-%d)",
					label.name, i-1, files[i-1].startTxNum, files[i-1].endTxNum,
					i, f.startTxNum, f.endTxNum)
			}
		}
	}

	// History and index must cover the identical set of ranges.
	require.Equal(t, len(hc.files), len(hc.iit.files),
		"history has %d files but index has %d files", len(hc.files), len(hc.iit.files))
	for i := range hc.files {
		hf, idx := hc.files[i], hc.iit.files[i]
		require.Equal(t, hf.startTxNum, idx.startTxNum,
			"file %d: history range [%d-%d) != index range [%d-%d)",
			i, hf.startTxNum, hf.endTxNum, idx.startTxNum, idx.endTxNum)
		require.Equal(t, hf.endTxNum, idx.endTxNum,
			"file %d: history range [%d-%d) != index range [%d-%d)",
			i, hf.startTxNum, hf.endTxNum, idx.startTxNum, idx.endTxNum)
	}
}

// checkHistoryDBCleanliness verifies that after pruning [0, prunedUpTo), the history
// ValuesTable, II KeysTable, and II ValuesTable contain no entries for txNum < prunedUpTo.
// This complements prune-correctness tests that only check the first remaining entry.
func checkHistoryDBCleanliness(t *testing.T, h *History, hc *HistoryRoTx, tx kv.Tx, prunedUpTo uint64) {
	t.Helper()

	// History ValuesTable: keys encode txNum in the last 8 bytes.
	{
		c, err := tx.CursorDupSort(h.ValuesTable)
		require.NoError(t, err)
		defer c.Close()
		for k, _, err := c.First(); k != nil; k, _, err = c.Next() {
			require.NoError(t, err)
			txNum := binary.BigEndian.Uint64(k[len(k)-8:])
			require.GreaterOrEqual(t, txNum, prunedUpTo,
				"history ValuesTable has entry with txNum=%d below prunedUpTo=%d (key=%x)",
				txNum, prunedUpTo, k)
		}
	}

	// II KeysTable: keys start with txNum (8 bytes).
	{
		c, err := tx.CursorDupSort(hc.iit.ii.KeysTable)
		require.NoError(t, err)
		defer c.Close()
		for k, _, err := c.First(); k != nil; k, _, err = c.Next() {
			require.NoError(t, err)
			if len(k) < 8 {
				continue
			}
			txNum := binary.BigEndian.Uint64(k[:8])
			require.GreaterOrEqual(t, txNum, prunedUpTo,
				"II KeysTable has entry with txNum=%d below prunedUpTo=%d (key=%x)",
				txNum, prunedUpTo, k)
		}
	}

	// II ValuesTable: values encode txNum as 8 bytes.
	{
		c, err := tx.CursorDupSort(hc.iit.ii.ValuesTable)
		require.NoError(t, err)
		defer c.Close()
		for _, v, err := c.First(); v != nil; _, v, err = c.Next() {
			require.NoError(t, err)
			if len(v) < 8 {
				continue
			}
			txNum := binary.BigEndian.Uint64(v[:8])
			require.GreaterOrEqual(t, txNum, prunedUpTo,
				"II ValuesTable has entry with txNum=%d below prunedUpTo=%d (val=%x)",
				txNum, prunedUpTo, v)
		}
	}
}

// checkHistoryNoDuplicates iterates all (key, txNum) entries from frozen inverted index files
// and verifies two properties per key:
//   - Monotonic ordering: txNums must be strictly increasing.
//   - No consecutive duplicates: adjacent entries must have different values.
func checkHistoryNoDuplicates(t *testing.T, hc *HistoryRoTx) {
	t.Helper()

	it, err := hc.iterateKeyTxNumFrozen(0, -1, order.Asc, -1)
	require.NoError(t, err)
	defer it.Close()

	type prevEntry struct {
		val   []byte
		txNum uint64
	}
	prevByKey := map[string]prevEntry{}

	for it.HasNext() {
		key, txNum, err := it.Next()
		require.NoError(t, err)

		val, ok, err := hc.historySeekInFiles(key, txNum+1)
		require.NoError(t, err)
		if !ok {
			continue
		}

		ks := string(key)
		if prev, exists := prevByKey[ks]; exists {
			require.Greater(t, txNum, prev.txNum,
				"non-monotonic txNum for key %x: txNum=%d after txNum=%d",
				key, txNum, prev.txNum)

			require.False(t, bytes.Equal(prev.val, val),
				"duplicate history entry for key %x: same value %x at txNum=%d and txNum=%d",
				key, val, prev.txNum, txNum)
		}
		prevByKey[ks] = prevEntry{val: common.Copy(val), txNum: txNum}
	}
}

// checkHistoryIndexConsistency verifies that every (key, txNum) from the inverted index
// that falls within the history files' range is covered by a history file.
func checkHistoryIndexConsistency(t *testing.T, hc *HistoryRoTx) {
	t.Helper()

	it, err := hc.iterateKeyTxNumFrozen(0, -1, order.Asc, -1)
	require.NoError(t, err)
	defer it.Close()

	histEndTxNum := hc.files.EndTxNum()
	for it.HasNext() {
		key, txNum, err := it.Next()
		require.NoError(t, err)
		if txNum >= histEndTxNum {
			continue // index may cover more txNums than history files
		}
		_, ok := hc.getFile(txNum)
		require.True(t, ok,
			"index has entry for key %x txNum=%d but no history file covers it (histEndTxNum=%d)",
			key, txNum, histEndTxNum)
	}
}

func TestHistoryHistory(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

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

// collateBuildIntegrate collates, builds files and integrates them for the given step.
// It is a test helper that combines the common collate→buildFiles→integrateDirtyFiles pattern.
func (h *History) collateBuildIntegrate(ctx context.Context, step kv.Step, tx kv.Tx, ps *background.ProgressSet) error {
	txFrom, txTo := step.ToTxNum(h.stepSize), (step + 1).ToTxNum(h.stepSize)
	c, err := h.collate(ctx, step, txFrom, txTo, tx)
	if err != nil {
		return err
	}
	defer c.Close()
	sf, err := h.buildFiles(ctx, step, c, ps)
	if err != nil {
		return err
	}
	h.integrateDirtyFiles(sf, txFrom, txTo)
	return nil
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
	for step := kv.Step(0); step < kv.Step(txs/h.stepSize)-1; step++ {
		require.NoError(h.collateBuildIntegrate(ctx, step, tx, background.NewProgressSet()))

		if doPrune {
			hc := h.beginForTests()
			_, err = hc.Prune(ctx, tx, step.ToTxNum(h.stepSize), (step + 1).ToTxNum(h.stepSize), math.MaxUint64, false, logEvery)
			hc.Close()
			require.NoError(err)
		}
	}

	var r HistoryRanges
	maxSpan := h.stepSize * config3.DefaultStepsInFrozenFile

	for {
		if stop := func() bool {
			hc := h.beginForTests()
			defer hc.Close()
			r = hc.findMergeRange(hc.files.EndTxNum(), maxSpan)
			if !r.any() {
				return true
			}
			indexOuts, historyOuts, err := hc.staticFilesInRange(r)
			require.NoError(err)
			indexIn, historyIn, err := hc.mergeFiles(ctx, indexOuts, historyOuts, r, background.NewProgressSet())
			require.NoError(err)
			h.integrateMergedDirtyFiles(indexIn, historyIn)
			return false
		}(); stop {
			break
		}
	}

	err = tx.Commit()
	require.NoError(err)
}

// collateAndMergeHistoryWithCollisionRetry is like collateAndMergeHistory
// but forces a recsplit collision retry on every buildVI call during merge.
func collateAndMergeHistoryWithCollisionRetry(tb testing.TB, db kv.RwDB, h *History, txs uint64) {
	tb.Helper()
	require := require.New(tb)

	logEvery := time.NewTicker(30 * time.Second)
	defer logEvery.Stop()
	ctx := context.Background()
	tx, err := db.BeginRwNosync(ctx)
	require.NoError(err)
	defer tx.Rollback()

	// Collate without collision forcing
	for step := kv.Step(0); step < kv.Step(txs/h.stepSize)-1; step++ {
		require.NoError(h.collateBuildIntegrate(ctx, step, tx, background.NewProgressSet()))

		hc := h.beginForTests()
		_, err = hc.Prune(ctx, tx, step.ToTxNum(h.stepSize), (step + 1).ToTxNum(h.stepSize), math.MaxUint64, false, logEvery)
		hc.Close()
		require.NoError(err)
	}

	// Enable collision forcing for merge phase only
	collisionRetries := 0
	h._testBuildVIHook = func(rs *recsplit.RecSplit) {
		rs.ForceCollisionOnce()
		collisionRetries++
	}

	var r HistoryRanges
	maxSpan := h.stepSize * config3.DefaultStepsInFrozenFile

	for {
		if stop := func() bool {
			hc := h.beginForTests()
			defer hc.Close()
			r = hc.findMergeRange(hc.files.EndTxNum(), maxSpan)
			if !r.any() {
				return true
			}
			indexOuts, historyOuts, err := hc.staticFilesInRange(r)
			require.NoError(err)
			indexIn, historyIn, err := hc.mergeFiles(ctx, indexOuts, historyOuts, r, background.NewProgressSet())
			require.NoError(err)
			h.integrateMergedDirtyFiles(indexIn, historyIn)
			return false
		}(); stop {
			break
		}
	}

	require.Greater(collisionRetries, 0, "expected at least one buildVI collision retry during merge")

	err = tx.Commit()
	require.NoError(err)
}

func TestHistoryMergeFiles(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	t.Parallel()

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
	if testing.Short() {
		t.Skip()
	}

	t.Parallel()

	logger := log.New()
	logEvery := time.NewTicker(30 * time.Second)
	defer logEvery.Stop()
	test := func(t *testing.T, h *History, db kv.RwDB, txs uint64) {
		t.Helper()
		require := require.New(t)

		collateAndMergeHistory(t, db, h, txs, true)
		hc := h.beginForTests()
		defer hc.Close()
		// Recreate domain and re-scan the files
		scanDirsRes, err := scanDirs(h.dirs)
		require.NoError(err)
		require.NoError(h.openFolder(scanDirsRes))
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

func TestHistoryRange1(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	t.Parallel()

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
		ic := h.beginForTests()
		defer ic.Close()

		it, err := ic.HistoryRange(2, 20, order.Asc, -1, tx)
		require.NoError(err)
		defer it.Close()
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
		it.Close()
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
		it.Close()
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
		it.Close()
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
		it.Close()
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

func TestHistoryRange2(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	t.Parallel()

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
		t.Run("before merge", func(t *testing.T) {
			hc, require := h.beginForTests(), require.New(t)
			defer hc.Close()

			{ //check IdxRange
				idxIt, err := hc.IdxRange(firstKey[:], -1, -1, order.Asc, -1, roTx)
				require.NoError(err)
				defer idxIt.Close()
				cnt, err := stream.CountU64(idxIt)
				require.NoError(err)
				require.Equal(1000, cnt)

				idxIt, err = hc.IdxRange(firstKey[:], 2, 20, order.Asc, -1, roTx)
				require.NoError(err)
				defer idxIt.Close()
				idxItDesc, err := hc.IdxRange(firstKey[:], 19, 1, order.Desc, -1, roTx)
				require.NoError(err)
				defer idxItDesc.Close()
				descArr, err := stream.ToArrayU64(idxItDesc)
				require.NoError(err)
				stream.ExpectEqualU64(t, idxIt, stream.ReverseArray(descArr))
			}

			it, err := hc.HistoryRange(2, 20, order.Asc, -1, roTx)
			require.NoError(err)
			defer it.Close()
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
			it.Close()
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

			v, ok, err := hc.HistorySeek(hexutil.MustDecodeHex("0100000000000001"), 900, tx)
			require.NoError(err)
			require.True(ok)
			require.Equal(hexutil.MustDecodeHex("ff00000000000383"), v)
			v, ok, err = hc.HistorySeek(hexutil.MustDecodeHex("0100000000000001"), 0, tx)
			require.NoError(err)
			require.True(ok)
			require.Equal([]byte{}, v)
			v, ok, err = hc.HistorySeek(hexutil.MustDecodeHex("0100000000000001"), 1000, tx)
			require.NoError(err)
			require.True(ok)
			require.Equal(hexutil.MustDecodeHex("ff000000000003e7"), v)
			_ = testCases
		})
		t.Run("after merge", func(t *testing.T) {
			collateAndMergeHistory(t, db, h, txs, true)
			hc, require := h.beginForTests(), require.New(t)
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

			v, ok, err := hc.HistorySeek(hexutil.MustDecodeHex("0100000000000001"), 900, tx)
			require.NoError(err)
			require.True(ok)
			require.Equal(hexutil.MustDecodeHex("ff00000000000383"), v)
			v, ok, err = hc.HistorySeek(hexutil.MustDecodeHex("0100000000000001"), 0, tx)
			require.NoError(err)
			require.True(ok)
			require.Equal([]byte{}, v)
			v, ok, err = hc.HistorySeek(hexutil.MustDecodeHex("0100000000000001"), 1000, tx)
			require.NoError(err)
			require.True(ok)
			require.Equal(hexutil.MustDecodeHex("ff000000000003e7"), v)
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
	t.Parallel()

	newTestDomain := func() (*InvertedIndex, *History) {
		d := emptyTestDomain(t, 1)
		d.History.InvertedIndex.Accessors = 0
		d.History.Accessors = 0
		return d.History.InvertedIndex, d.History
	}

	_, h := newTestDomain()

	files := []string{
		"v1.0-accounts.0-1.v",
		"v1.0-accounts.1-2.v",
		"v1.0-accounts.0-4.v",
		"v1.0-accounts.2-3.v",
		"v1.0-accounts.3-4.v",
		"v1.0-accounts.4-5.v",
	}
	h.scanDirtyFiles(files)
	require.Equal(t, 6, h.dirtyFiles.Len())
	hc := h.beginForTests()
	require.Equal(t, 0, len(hc.files))
	hc.Close()
}

func writeSomeHistory(tb testing.TB, largeValues bool, logger log.Logger) (kv.RwDB, *History, [][]byte, uint64) {
	tb.Helper()
	db, h := testDbAndHistory(tb, largeValues, logger)
	ctx := context.Background()
	tx, err := db.BeginRw(ctx)
	require.NoError(tb, err)
	defer tx.Rollback()
	hc := h.beginForTests()
	defer hc.Close()
	writer := hc.NewWriter()
	defer writer.close()

	keys := [][]byte{
		common.FromHex(""),
		common.FromHex("a4dba136b5541817a78b160dd140190d9676d0f0"),
		common.FromHex("01"),
		common.FromHex("00"),
		commitmentdb.KeyCommitmentState,
		common.FromHex("8240a92799b51e7d99d3ef53c67bca7d068bd8d64e895dd56442c4ac01c9a27d"),
		common.FromHex("cedce3c4eb5e0eedd505c33fd0f8c06d1ead96e63d6b3a27b5186e4901dce59e"),
	}

	txs := uint64(1000)
	var prevVal [7][]byte
	var flusher flusher
	for txNum := uint64(1); txNum <= txs; txNum++ {
		for ik, k := range keys {
			var v [8]byte
			binary.BigEndian.PutUint64(v[:], txNum)
			if ik == 0 && txNum%33 == 0 {
				continue
			}
			err = writer.AddPrevValue(k, txNum, prevVal[ik])
			require.NoError(tb, err)

			prevVal[ik] = v[:]
		}

		if txNum%33 == 0 {
			err = writer.AddPrevValue(keys[0], txNum, nil)
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
	if testing.Short() {
		t.Skip()
	}

	t.Parallel()

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
		ic := h.beginForTests()
		defer ic.Close()

		iter, err := ic.HistoryRange(1, -1, order.Asc, -1, tx)
		require.NoError(err)
		defer iter.Close()

		keys := make([][]byte, 0)
		for iter.HasNext() {
			k, _, err := iter.Next()
			require.NoError(err)
			keys = append(keys, k)
			//vals = append(vals, fmt.Sprintf("%x", v))
		}

		sort.Slice(writtenKeys, func(i, j int) bool {
			return bytes.Compare(writtenKeys[i], writtenKeys[j]) < 0
		})

		require.Equal(fmt.Sprintf("%#x", writtenKeys[0]), fmt.Sprintf("%#x", keys[0]))
		require.Len(keys, len(writtenKeys))
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
	if testing.Short() {
		t.Skip()
	}

	t.Parallel()

	logger := log.New()
	db, h, txs := filledHistory(t, true, logger)
	collateAndMergeHistory(t, db, h, txs, true)

	hc := h.beginForTests()
	list := hc.files
	require.NotEmpty(t, list)
	ff := list[len(list)-1]
	fn := ff.src.decompressor.FilePath()
	hc.Close()
	h.Close()

	err := dir.RemoveFile(fn)
	require.NoError(t, err)
	err = os.WriteFile(fn, make([]byte, 33), 0644)
	require.NoError(t, err)

	scanDirsRes, err := scanDirs(h.dirs)
	require.NoError(t, err)
	err = h.openFolder(scanDirsRes)
	require.NoError(t, err)
	h.Close()
}

// TestHistoryRange_DBOnly verifies HistoryRange when data resides only in DB
// (no segments collated). This exclusively exercises HistoryChangesIterDB.
// Results must match those of TestHistoryRange1 which uses segment files.
func TestHistoryRange_DBOnly(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	t.Parallel()

	logger := log.New()
	ctx := context.Background()

	test := func(t *testing.T, h *History, db kv.RwDB) {
		t.Helper()
		require := require.New(t)

		// Deliberately skip collateAndMergeHistory — all data stays in the DB.
		tx, err := db.BeginRo(ctx)
		require.NoError(err)
		defer tx.Rollback()

		ic := h.beginForTests()
		defer ic.Close()

		collect := func(it stream.KV) (keys, vals []string) {
			t.Helper()
			for it.HasNext() {
				k, v, err := it.Next()
				require.NoError(err)
				keys = append(keys, fmt.Sprintf("%x", k))
				vals = append(vals, fmt.Sprintf("%x", v))
			}
			it.Close()
			return keys, vals
		}

		// [2, 20): same 19 keys / values as in the file-backed TestHistoryRange1.
		it, err := ic.HistoryRange(2, 20, order.Asc, -1, tx)
		require.NoError(err)
		keys, vals := collect(it)
		require.Equal([]string{
			"0100000000000001", "0100000000000002", "0100000000000003",
			"0100000000000004", "0100000000000005", "0100000000000006",
			"0100000000000007", "0100000000000008", "0100000000000009",
			"010000000000000a", "010000000000000b", "010000000000000c",
			"010000000000000d", "010000000000000e", "010000000000000f",
			"0100000000000010", "0100000000000011", "0100000000000012",
			"0100000000000013",
		}, keys)
		require.Equal([]string{
			"ff00000000000001", "", "", "", "", "", "", "", "",
			"", "", "", "", "", "", "", "", "", "",
		}, vals)

		// [995, 1000): same 9 keys / values as in TestHistoryRange1.
		it, err = ic.HistoryRange(995, 1000, order.Asc, -1, tx)
		require.NoError(err)
		keys, vals = collect(it)
		require.Equal([]string{
			"0100000000000001", "0100000000000002", "0100000000000003",
			"0100000000000004", "0100000000000005", "0100000000000006",
			"0100000000000009", "010000000000000c", "010000000000001b",
		}, keys)
		require.Equal([]string{
			"ff000000000003e2", "ff000000000001f1", "ff0000000000014b",
			"ff000000000000f8", "ff000000000000c6", "ff000000000000a5",
			"ff0000000000006e", "ff00000000000052", "ff00000000000024",
		}, vals)

		// [5, 6): exactly one txNum — only keys 1 and 5 change at txNum=5.
		// key=1: prevVal = 0xff...0004 (the value set at txNum=4).
		// key=5: prevVal = nil (txNum=5 is key=5's first-ever change).
		it, err = ic.HistoryRange(5, 6, order.Asc, -1, tx)
		require.NoError(err)
		keys, vals = collect(it)
		require.Equal([]string{"0100000000000001", "0100000000000005"}, keys)
		require.Equal([]string{"ff00000000000004", ""}, vals)
	}

	t.Run("large_values", func(t *testing.T) {
		db, h, _ := filledHistory(t, true, logger)
		test(t, h, db)
	})
	t.Run("small_values", func(t *testing.T) {
		db, h, _ := filledHistory(t, false, logger)
		test(t, h, db)
	})
}

// TestRangeAsOf_ValuesMatchHistorySeek cross-validates RangeAsOf against
// HistorySeek: for each key returned by RangeAsOf(txNum), the value must equal
// what HistorySeek(key, txNum) returns for the same key and txNum.
func TestRangeAsOf_ValuesMatchHistorySeek(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	t.Parallel()

	logger := log.New()
	ctx := context.Background()

	test := func(t *testing.T, h *History, db kv.RwDB, txs uint64) {
		t.Helper()
		require := require.New(t)
		collateAndMergeHistory(t, db, h, txs, true)

		roTx, err := db.BeginRo(ctx)
		require.NoError(err)
		defer roTx.Rollback()

		hc := h.beginForTests()
		defer hc.Close()

		// Encode keys 1..5 with the same layout used by filledHistory.
		makeKey := func(keyNum uint64) []byte {
			var k [8]byte
			binary.BigEndian.PutUint64(k[:], keyNum)
			k[0] = 1
			return k[:]
		}
		fromKey := makeKey(1)
		toKey := makeKey(6) // exclusive upper bound

		checkTxNum := uint64(10)

		// RangeAsOf returns one entry per key; collect them.
		it, err := hc.RangeAsOf(ctx, checkTxNum, fromKey, toKey, order.Asc, -1, roTx)
		require.NoError(err)
		defer it.Close()

		count := 0
		for it.HasNext() {
			k, v, err := it.Next()
			require.NoError(err)

			// Oracle: HistorySeek for the same key and txNum.
			want, ok, err := hc.HistorySeek(k, checkTxNum, roTx)
			require.NoError(err)
			require.True(ok, "HistorySeek returned not-found for key %x at txNum %d", k, checkTxNum)
			require.Equal(
				fmt.Sprintf("%x", want),
				fmt.Sprintf("%x", v),
				"mismatch for key %x at txNum %d", k, checkTxNum,
			)
			count++
		}
		require.Equal(5, count, "expected 5 keys (1..5) from RangeAsOf")

		// Spot-check exact values derived from the filledHistory data pattern
		// (key=N changes at txNums that are multiples of N; prevVal encodes
		// the change-count with marker byte 0xff).
		//
		// key=1 changes every tx; prevVal at tx=10 = 0xff...0009 (count before tx10 = 9).
		v1, ok, err := hc.HistorySeek(makeKey(1), checkTxNum, roTx)
		require.NoError(err)
		require.True(ok)
		require.Equal("ff00000000000009", fmt.Sprintf("%x", v1))

		// key=2 changes at even txs; prevVal at tx=10 = 0xff...0004 (count before tx10 = 4).
		v2, ok, err := hc.HistorySeek(makeKey(2), checkTxNum, roTx)
		require.NoError(err)
		require.True(ok)
		require.Equal("ff00000000000004", fmt.Sprintf("%x", v2))

		// key=5 changes at multiples of 5; prevVal at tx=10 = 0xff...0001 (only tx5 before tx10).
		v5, ok, err := hc.HistorySeek(makeKey(5), checkTxNum, roTx)
		require.NoError(err)
		require.True(ok)
		require.Equal("ff00000000000001", fmt.Sprintf("%x", v5))
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

// TestRangeAsOf_DBIteratorSkipsFileRange verifies that the DB iterator in
// RangeAsOf does not read entries within the file range. After collating files,
// we insert a fake DB-only key with a txNum inside the file range. Without the
// fix (DB iterator starting from startTxNum), this phantom key would appear in
// results. With the fix (DB iterator clamped to files.EndTxNum()), it is skipped.
func TestRangeAsOf_DBIteratorSkipsFileRange(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	t.Parallel()

	logger := log.New()
	ctx := context.Background()

	test := func(t *testing.T, largeValues bool) {
		t.Helper()
		require := require.New(t)

		db, h, txs := filledHistory(t, largeValues, logger)
		collateAndMergeHistory(t, db, h, txs, false)

		hc := h.beginForTests()
		defer hc.Close()

		endTxNum := hc.iit.files.EndTxNum()
		require.Greater(endTxNum, uint64(0), "files should cover a non-empty range")

		startTxNum := endTxNum / 2

		// Insert a phantom key that exists ONLY in DB, at a txNum inside the
		// file range. This key (0x02...) is outside the normal test data range
		// (keys use 0x01 prefix) so it has no file entry.
		phantomKey := []byte{0x02, 0, 0, 0, 0, 0, 0, 0x01}
		phantomVal := []byte("PHANTOM")
		rwTx, err := db.BeginRw(ctx)
		require.NoError(err)
		defer rwTx.Rollback()
		if largeValues {
			dbKey := make([]byte, len(phantomKey)+8)
			copy(dbKey, phantomKey)
			binary.BigEndian.PutUint64(dbKey[len(phantomKey):], startTxNum)
			require.NoError(rwTx.Put(h.ValuesTable, dbKey, phantomVal))
		} else {
			var txBuf [8]byte
			binary.BigEndian.PutUint64(txBuf[:], startTxNum)
			require.NoError(rwTx.Put(h.ValuesTable, phantomKey, append(txBuf[:], phantomVal...)))
		}
		require.NoError(rwTx.Commit())

		// Query the range that includes phantomKey.
		roTx, err := db.BeginRo(ctx)
		require.NoError(err)
		defer roTx.Rollback()

		it, err := hc.RangeAsOf(ctx, startTxNum, phantomKey, nil, order.Asc, -1, roTx)
		require.NoError(err)
		defer it.Close()

		// The phantom key must NOT appear — the DB iterator should start from
		// files.EndTxNum(), skipping the phantom entry whose txNum < endTxNum.
		for it.HasNext() {
			k, _, err := it.Next()
			require.NoError(err)
			require.False(
				bytes.Equal(phantomKey, k),
				"DB iterator leaked phantom key from within the file range",
			)
		}
	}

	t.Run("large_values", func(t *testing.T) { test(t, true) })
	t.Run("small_values", func(t *testing.T) { test(t, false) })
}

// TestHistoryRange_EmptyRange verifies that degenerate ranges (empty, inverted,
// or beyond the data horizon) produce no results without error.
func TestHistoryRange_EmptyRange(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	t.Parallel()

	logger := log.New()
	ctx := context.Background()

	test := func(t *testing.T, h *History, db kv.RwDB, txs uint64) {
		t.Helper()
		require := require.New(t)
		collateAndMergeHistory(t, db, h, txs, true)

		tx, err := db.BeginRo(ctx)
		require.NoError(err)
		defer tx.Rollback()

		ic := h.beginForTests()
		defer ic.Close()

		drain := func(label string, it stream.KV, err error) {
			t.Helper()
			require.NoError(err, label)
			defer it.Close()
			n, err := stream.CountKV(it)
			require.NoError(err, label)
			require.Zero(n, "expected empty result for %s", label)
		}

		// Equal bounds: [n, n) is empty.
		it, err := ic.HistoryRange(500, 500, order.Asc, -1, tx)
		drain("[500,500)", it, err)

		// Inverted bounds: from > to is empty.
		it, err = ic.HistoryRange(500, 499, order.Asc, -1, tx)
		drain("[500,499)", it, err)

		// Beyond the data horizon.
		it, err = ic.HistoryRange(int(txs)+1, int(txs)+1000, order.Asc, -1, tx)
		drain("beyond horizon", it, err)
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

// TestHistory_IterateChangedRecent_SkipsFileRange verifies that HistoryRange
// returns correct results when the queried range spans both files and DB,
// even after DB entries within the file range have been pruned.
// This tests the fix where iterateChangedRecent adjusts fromTxNum to
// max(fromTxNum, files.EndTxNum()) so the DB iterator never reads data
// that belongs to the file range.
func TestHistory_IterateChangedRecent_SkipsFileRange(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	t.Parallel()

	logger := log.New()
	ctx := context.Background()
	logEvery := time.NewTicker(30 * time.Second)
	defer logEvery.Stop()

	test := func(t *testing.T, largeValues bool) {
		t.Helper()
		require := require.New(t)

		// Instance 1: build files, no prune (reference).
		db1, h1, txs := filledHistory(t, largeValues, logger)
		collateAndMergeHistory(t, db1, h1, txs, false)

		// Instance 2: build files, then prune DB entries in the file range.
		db2, h2, _ := filledHistory(t, largeValues, logger)
		collateAndMergeHistory(t, db2, h2, txs, false)

		var filesEnd int
		func() {
			hc2 := h2.beginForTests()
			defer hc2.Close()
			filesEnd = int(hc2.iit.files.EndTxNum())
			require.Greater(filesEnd, 0, "expected files to cover some range")
		}()

		// Prune DB entries [0, filesEnd) — these are covered by files.
		rwTx, err := db2.BeginRw(ctx)
		require.NoError(err)
		defer rwTx.Rollback()
		func() {
			hc2 := h2.beginForTests()
			defer hc2.Close()
			_, err = hc2.Prune(ctx, rwTx, 0, uint64(filesEnd), math.MaxUint64, true, logEvery)
			require.NoError(err)
		}()
		require.NoError(rwTx.Commit())

		// Query a range that spans both files and DB.
		// fromTxNum is well within file range, toTxNum extends past it.
		fromTxNum := filesEnd / 2
		toTxNum := -1 // unlimited

		collect := func(db kv.RwDB, h *History) (keys, vals []string) {
			tx, err := db.BeginRo(ctx)
			require.NoError(err)
			defer tx.Rollback()

			hc := h.beginForTests()
			defer hc.Close()

			it, err := hc.HistoryRange(fromTxNum, toTxNum, order.Asc, -1, tx)
			require.NoError(err)
			defer it.Close()
			for it.HasNext() {
				k, v, err := it.Next()
				require.NoError(err)
				keys = append(keys, fmt.Sprintf("%x", k))
				vals = append(vals, fmt.Sprintf("%x", v))
			}
			return
		}

		keys1, vals1 := collect(db1, h1)
		keys2, vals2 := collect(db2, h2)

		require.NotEmpty(keys1, "expected non-empty results")
		require.Equal(keys1, keys2, "keys mismatch after pruning DB entries in file range")
		require.Equal(vals1, vals2, "values mismatch after pruning DB entries in file range")

		// Also verify with a bounded toTxNum that still spans the boundary.
		toTxNum = filesEnd + (int(txs)-filesEnd)/2
		keys1b, vals1b := collect(db1, h1)
		keys2b, vals2b := collect(db2, h2)
		require.Equal(keys1b, keys2b, "bounded range: keys mismatch after prune")
		require.Equal(vals1b, vals2b, "bounded range: values mismatch after prune")
	}

	t.Run("large_values", func(t *testing.T) { test(t, true) })
	t.Run("small_values", func(t *testing.T) { test(t, false) })
}

// TestHistory_IterateChangedRecent_PhantomDBKey inserts a "phantom" key into
// the DB at a txNum within the file range — a key that does not exist in
// segment files. Because iterateChangedRecent now starts the DB iterator at
// files.EndTxNum(), the phantom is never reached and must not appear in the
// HistoryRange output. This directly proves the DB iterator skips the file
// range rather than relying on the union to mask duplicates.
func TestHistory_IterateChangedRecent_PhantomDBKey(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	t.Parallel()

	logger := log.New()
	ctx := context.Background()

	test := func(t *testing.T, largeValues bool) {
		t.Helper()
		require := require.New(t)

		db, h, txs := filledHistory(t, largeValues, logger)
		collateAndMergeHistory(t, db, h, txs, false)

		var filesEnd int
		var valsTable string
		func() {
			hc := h.beginForTests()
			defer hc.Close()
			filesEnd = int(hc.iit.files.EndTxNum())
			valsTable = hc.h.ValuesTable
			require.Greater(filesEnd, 0)
		}()

		// Pick a txNum well inside the file range.
		phantomTxNum := uint64(filesEnd / 2)
		// A key that filledHistory never generates (marker byte 0x02 vs 0x01).
		var phantomKey [8]byte
		binary.BigEndian.PutUint64(phantomKey[:], 0x0200000000000099)
		phantomVal := []byte("phantom")

		// Insert the phantom entry into the DB values table.
		rwTx, err := db.BeginRw(ctx)
		require.NoError(err)
		defer rwTx.Rollback()
		if largeValues {
			// Large: key layout is actualKey+txNum -> value.
			dbKey := make([]byte, len(phantomKey)+8)
			copy(dbKey, phantomKey[:])
			binary.BigEndian.PutUint64(dbKey[len(phantomKey):], phantomTxNum)
			require.NoError(rwTx.Put(valsTable, dbKey, phantomVal))
		} else {
			// Small (DupSort): key layout is actualKey, dup is txNum+value.
			dup := make([]byte, 8+len(phantomVal))
			binary.BigEndian.PutUint64(dup[:8], phantomTxNum)
			copy(dup[8:], phantomVal)
			require.NoError(rwTx.Put(valsTable, phantomKey[:], dup))
		}
		require.NoError(rwTx.Commit())

		// Query HistoryRange starting from within the file range.
		fromTxNum := filesEnd / 4 // well before the phantom
		roTx, err := db.BeginRo(ctx)
		require.NoError(err)
		defer roTx.Rollback()

		hc := h.beginForTests()
		defer hc.Close()

		it, err := hc.HistoryRange(fromTxNum, -1, order.Asc, -1, roTx)
		require.NoError(err)
		defer it.Close()

		phantomKeyHex := fmt.Sprintf("%x", phantomKey[:])
		for it.HasNext() {
			k, _, err := it.Next()
			require.NoError(err)
			require.NotEqual(phantomKeyHex, fmt.Sprintf("%x", k),
				"phantom DB key within file range must not appear in HistoryRange output")
		}
	}

	t.Run("large_values", func(t *testing.T) { test(t, true) })
	t.Run("small_values", func(t *testing.T) { test(t, false) })
}

// BenchmarkHistoryRange benchmarks the hot path: iterating all changed keys
// across a wide txNum range from segment files (exercises HistoryChangesIterFiles.advance).
func BenchmarkHistoryRange(b *testing.B) {
	logger := log.New()
	ctx := context.Background()

	db, h, txs := filledHistory(b, true, logger)
	collateAndMergeHistory(b, db, h, txs, true)

	tx, err := db.BeginRo(ctx)
	require.NoError(b, err)
	defer tx.Rollback()

	ic := h.beginForTests()
	defer ic.Close()

	b.ResetTimer()
	b.ReportAllocs()
	for b.Loop() {
		it, err := ic.HistoryRange(0, int(txs), order.Asc, -1, tx)
		require.NoError(b, err)
		for it.HasNext() {
			_, _, err := it.Next()
			require.NoError(b, err)
		}
		it.Close()
	}
}

// BenchmarkRangeAsOf benchmarks iterating the full key-space at a given txNum
// from segment files (exercises HistoryRangeAsOfFiles.advanceInFiles).
func BenchmarkRangeAsOf(b *testing.B) {
	logger := log.New()
	ctx := context.Background()

	db, h, txs := filledHistory(b, true, logger)
	collateAndMergeHistory(b, db, h, txs, true)

	tx, err := db.BeginRo(ctx)
	require.NoError(b, err)
	defer tx.Rollback()

	ic := h.beginForTests()
	defer ic.Close()

	checkTxNum := txs / 2

	b.ResetTimer()
	b.ReportAllocs()
	for b.Loop() {
		it, err := ic.RangeAsOf(ctx, checkTxNum, nil, nil, order.Asc, -1, tx)
		require.NoError(b, err)
		for it.HasNext() {
			_, _, err := it.Next()
			require.NoError(b, err)
		}
		it.Close()
	}
}

// collateHistory collates all steps into separate per-step files without merging them.
// This leaves many small files in the heap, exercising heap operations during iteration.
func collateHistory(b *testing.B, db kv.RwDB, h *History, txs uint64) {
	b.Helper()
	ctx := context.Background()
	tx, err := db.BeginRwNosync(ctx)
	require.NoError(b, err)
	defer tx.Rollback()
	for step := kv.Step(0); step < kv.Step(txs/h.stepSize)-1; step++ {
		require.NoError(b, h.collateBuildIntegrate(ctx, step, tx, background.NewProgressSet()))
	}
	require.NoError(b, tx.Commit())
}

// BenchmarkHistoryRange_MultiFile is like BenchmarkHistoryRange but keeps all
// step-files unmerged so the heap has ~60 elements, actually exercising heap ops.
func BenchmarkHistoryRange_MultiFile(b *testing.B) {
	logger := log.New()
	ctx := context.Background()

	db, h, txs := filledHistory(b, true, logger)
	collateHistory(b, db, h, txs)

	tx, err := db.BeginRo(ctx)
	require.NoError(b, err)
	defer tx.Rollback()

	ic := h.beginForTests()
	defer ic.Close()

	b.ResetTimer()
	b.ReportAllocs()
	for b.Loop() {
		it, err := ic.HistoryRange(0, int(txs), order.Asc, -1, tx)
		require.NoError(b, err)
		for it.HasNext() {
			_, _, err := it.Next()
			require.NoError(b, err)
		}
		it.Close()
	}
}

// BenchmarkRangeAsOf_MultiFile is like BenchmarkRangeAsOf but keeps all
// step-files unmerged so the heap has ~60 elements, actually exercising heap ops.
func BenchmarkRangeAsOf_MultiFile(b *testing.B) {
	logger := log.New()
	ctx := context.Background()

	db, h, txs := filledHistory(b, true, logger)
	collateHistory(b, db, h, txs)

	tx, err := db.BeginRo(ctx)
	require.NoError(b, err)
	defer tx.Rollback()

	ic := h.beginForTests()
	defer ic.Close()

	checkTxNum := txs / 2

	b.ResetTimer()
	b.ReportAllocs()
	for b.Loop() {
		it, err := ic.RangeAsOf(ctx, checkTxNum, nil, nil, order.Asc, -1, tx)
		require.NoError(b, err)
		for it.HasNext() {
			_, _, err := it.Next()
			require.NoError(b, err)
		}
		it.Close()
	}
}
