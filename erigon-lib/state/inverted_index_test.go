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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon-lib/common/background"
	"github.com/erigontech/erigon-lib/common/datadir"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/kv/mdbx"
	"github.com/erigontech/erigon-lib/kv/order"
	"github.com/erigontech/erigon-lib/kv/stream"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon-lib/recsplit"
	"github.com/erigontech/erigon-lib/recsplit/eliasfano32"
	"github.com/erigontech/erigon-lib/seg"
)

func testDbAndInvertedIndex(tb testing.TB, aggStep uint64, logger log.Logger) (kv.RwDB, *InvertedIndex) {
	tb.Helper()
	dirs := datadir.New(tb.TempDir())
	keysTable := "Keys"
	indexTable := "Index"
	db := mdbx.NewMDBX(logger).InMem(dirs.Chaindata).WithTableCfg(func(defaultBuckets kv.TableCfg) kv.TableCfg {
		return kv.TableCfg{
			keysTable:             kv.TableCfgItem{Flags: kv.DupSort},
			indexTable:            kv.TableCfgItem{Flags: kv.DupSort},
			kv.TblPruningProgress: kv.TableCfgItem{},
		}
	}).MustOpen()
	tb.Cleanup(db.Close)
	salt := uint32(1)
	cfg := iiCfg{salt: &salt, dirs: dirs, db: db}
	ii, err := NewInvertedIndex(cfg, aggStep, "inv", keysTable, indexTable, nil, logger)
	require.NoError(tb, err)
	ii.DisableFsync()
	tb.Cleanup(ii.Close)
	return db, ii
}

func TestInvIndexPruningCorrectness(t *testing.T) {
	t.Parallel()

	db, ii, _ := filledInvIndexOfSize(t, 1000, 16, 1, log.New())
	defer ii.Close()

	ic := ii.BeginFilesRo()
	defer ic.Close()

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

	icc, err := rwTx.CursorDupSort(ii.indexKeysTable)
	require.NoError(t, err)

	count := 0
	for txn, _, err := icc.Seek(from[:]); txn != nil; txn, _, err = icc.Next() {
		require.NoError(t, err)
		if bytes.Compare(txn, to[:]) > 0 {
			break
		}
		count++
	}
	icc.Close()
	require.EqualValues(t, count, pruneIters*int(pruneLimit))

	// this one should not prune anything due to forced=false but no files built
	stat, err := ic.Prune(context.Background(), rwTx, 0, 10, pruneLimit, logEvery, false, nil)
	require.NoError(t, err)
	require.Zero(t, stat.PruneCountTx)
	require.Zero(t, stat.PruneCountValues)

	// this one should not prune anything as well due to given range [0,1) even it is forced
	stat, err = ic.Prune(context.Background(), rwTx, 0, 1, pruneLimit, logEvery, true, nil)
	require.NoError(t, err)
	require.Zero(t, stat.PruneCountTx)
	require.Zero(t, stat.PruneCountValues)

	// this should prune exactly pruneLimit*pruneIter transactions
	for i := 0; i < pruneIters; i++ {
		stat, err = ic.Prune(context.Background(), rwTx, 0, 1000, pruneLimit, logEvery, true, nil)
		require.NoError(t, err)
		t.Logf("[%d] stats: %v", i, stat)
	}

	// ascending - empty
	it, err := ic.IdxRange(nil, 0, pruneIters*int(pruneLimit), order.Asc, -1, rwTx)
	require.NoError(t, err)
	require.False(t, it.HasNext())
	it.Close()

	// descending - empty
	it, err = ic.IdxRange(nil, pruneIters*int(pruneLimit), 0, order.Desc, -1, rwTx)
	require.NoError(t, err)
	require.False(t, it.HasNext())
	it.Close()

	// straight from pruned - not empty
	icc, err = rwTx.CursorDupSort(ii.indexKeysTable)
	require.NoError(t, err)
	txn, _, err := icc.Seek(from[:])
	require.NoError(t, err)
	// we pruned by limit so next transaction after prune should be equal to `pruneIters*pruneLimit+1`
	// If we would prune by txnum then txTo prune should be available after prune is finished
	require.EqualValues(t, pruneIters*int(pruneLimit), binary.BigEndian.Uint64(txn)-1)
	icc.Close()

	// check second table
	icc, err = rwTx.CursorDupSort(ii.indexTable)
	require.NoError(t, err)
	key, txn, err := icc.First()
	t.Logf("key: %x, txn: %x", key, txn)
	require.NoError(t, err)
	// we pruned by limit so next transaction after prune should be equal to `pruneIters*pruneLimit+1`
	// If we would prune by txnum then txTo prune should be available after prune is finished
	require.EqualValues(t, pruneIters*int(pruneLimit), binary.BigEndian.Uint64(txn)-1)
	icc.Close()
}

func TestInvIndexCollationBuild(t *testing.T) {
	t.Parallel()

	logger := log.New()
	logEvery := time.NewTicker(30 * time.Second)
	defer logEvery.Stop()
	db, ii := testDbAndInvertedIndex(t, 16, logger)
	ctx := context.Background()
	tx, err := db.BeginRw(ctx)
	require.NoError(t, err)
	defer tx.Rollback()
	ic := ii.BeginFilesRo()
	defer ic.Close()
	writer := ic.NewWriter()
	defer writer.close()

	writer.SetTxNum(2)
	err = writer.Add([]byte("key1"))
	require.NoError(t, err)

	writer.SetTxNum(3)
	err = writer.Add([]byte("key2"))
	require.NoError(t, err)

	writer.SetTxNum(6)
	err = writer.Add([]byte("key1"))
	require.NoError(t, err)
	err = writer.Add([]byte("key3"))
	require.NoError(t, err)

	writer.SetTxNum(17)
	err = writer.Add([]byte("key10"))
	require.NoError(t, err)

	err = writer.Flush(ctx, tx)
	require.NoError(t, err)
	err = tx.Commit()
	require.NoError(t, err)

	roTx, err := db.BeginRo(ctx)
	require.NoError(t, err)
	defer roTx.Rollback()

	bs, err := ii.collate(ctx, 0, roTx)
	require.NoError(t, err)

	sf, err := ii.buildFiles(ctx, 0, bs, background.NewProgressSet())
	require.NoError(t, err)
	defer sf.CleanupOnError()

	g := sf.decomp.MakeGetter()
	g.Reset(0)
	var words []string
	var intArrs [][]uint64
	for g.HasNext() {
		w, _ := g.Next(nil)
		words = append(words, string(w))
		w, _ = g.Next(w[:0])
		ef, _ := eliasfano32.ReadEliasFano(w)
		var ints []uint64
		it := ef.Iterator()
		for it.HasNext() {
			v, _ := it.Next()
			ints = append(ints, v)
		}
		intArrs = append(intArrs, ints)
	}
	require.Equal(t, []string{"key1", "key2", "key3"}, words)
	require.Equal(t, [][]uint64{{2, 6}, {3}, {6}}, intArrs)
	r := recsplit.NewIndexReader(sf.index)
	for i := 0; i < len(words); i++ {
		offset, _ := r.TwoLayerLookup([]byte(words[i]))
		g.Reset(offset)
		w, _ := g.Next(nil)
		require.Equal(t, words[i], string(w))
	}
}

func TestInvIndexAfterPrune(t *testing.T) {
	t.Parallel()

	logger := log.New()
	logEvery := time.NewTicker(30 * time.Second)
	defer logEvery.Stop()
	db, ii := testDbAndInvertedIndex(t, 16, logger)
	ctx := context.Background()
	tx, err := db.BeginRw(ctx)
	require.NoError(t, err)
	defer func() {
		if tx != nil {
			tx.Rollback()
		}
	}()
	ic := ii.BeginFilesRo()
	defer ic.Close()
	writer := ic.NewWriter()
	defer writer.close()

	writer.SetTxNum(2)
	err = writer.Add([]byte("key1"))
	require.NoError(t, err)

	writer.SetTxNum(3)
	err = writer.Add([]byte("key2"))
	require.NoError(t, err)

	writer.SetTxNum(6)
	err = writer.Add([]byte("key1"))
	require.NoError(t, err)
	err = writer.Add([]byte("key3"))
	require.NoError(t, err)

	err = writer.Flush(ctx, tx)
	require.NoError(t, err)
	err = tx.Commit()
	require.NoError(t, err)

	roTx, err := db.BeginRo(ctx)
	require.NoError(t, err)
	defer roTx.Rollback()

	bs, err := ii.collate(ctx, 0, roTx)
	require.NoError(t, err)

	sf, err := ii.buildFiles(ctx, 0, bs, background.NewProgressSet())
	require.NoError(t, err)

	ii.integrateDirtyFiles(sf, 0, 16)
	ii.reCalcVisibleFiles(ii.dirtyFilesEndTxNumMinimax())

	ic.Close()
	err = db.Update(ctx, func(tx kv.RwTx) error {
		from, to := ii.stepsRangeInDB(tx)
		require.Equal(t, "0.1", fmt.Sprintf("%.1f", from))
		require.Equal(t, "0.4", fmt.Sprintf("%.1f", to))

		ic = ii.BeginFilesRo()
		defer ic.Close()

		_, err = ic.Prune(ctx, tx, 0, 16, math.MaxUint64, logEvery, false, nil)
		require.NoError(t, err)
		return nil
	})
	require.NoError(t, err)

	require.NoError(t, err)
	tx, err = db.BeginRw(ctx)
	require.NoError(t, err)
	defer tx.Rollback()

	for _, table := range []string{ii.indexKeysTable, ii.indexTable} {
		var cur kv.Cursor
		cur, err = tx.Cursor(table)
		require.NoError(t, err)
		defer cur.Close()
		var k []byte
		k, _, err = cur.First()
		require.NoError(t, err)
		require.Nil(t, k, table)
	}

	from, to := ii.stepsRangeInDB(tx)
	require.Equal(t, float64(0), from)
	require.Equal(t, float64(0), to)
}

func filledInvIndex(tb testing.TB, logger log.Logger) (kv.RwDB, *InvertedIndex, uint64) {
	tb.Helper()
	return filledInvIndexOfSize(tb, uint64(1000), 16, 31, logger)
}

// Creates InvertedIndex instance and fills it with generated data.
// Txns - amount of transactions to generate
// AggStep - aggregation step for InvertedIndex
// Module - amount of keys to generate
func filledInvIndexOfSize(tb testing.TB, txs, aggStep, module uint64, logger log.Logger) (kv.RwDB, *InvertedIndex, uint64) {
	tb.Helper()
	db, ii := testDbAndInvertedIndex(tb, aggStep, logger)
	ctx, require := context.Background(), require.New(tb)
	tb.Cleanup(db.Close)

	err := db.Update(ctx, func(tx kv.RwTx) error {
		ic := ii.BeginFilesRo()
		defer ic.Close()
		writer := ic.NewWriter()
		defer writer.close()

		var flusher flusher

		// keys are encodings of numbers 1..31
		// each key changes value on every txNum which is multiple of the key
		for txNum := uint64(1); txNum <= txs; txNum++ {
			writer.SetTxNum(txNum)
			for keyNum := uint64(1); keyNum <= module; keyNum++ {
				if txNum%keyNum == 0 {
					var k [8]byte
					binary.BigEndian.PutUint64(k[:], keyNum)
					err := writer.Add(k[:])
					require.NoError(err)
				}
			}
			if flusher != nil {
				require.NoError(flusher.Flush(ctx, tx))
			}
			if txNum%10 == 0 {
				flusher = writer
				writer = ic.NewWriter()
			}
		}
		if flusher != nil {
			require.NoError(flusher.Flush(ctx, tx))
		}
		return writer.Flush(ctx, tx)
	})
	require.NoError(err)
	return db, ii, txs
}

func checkRanges(t *testing.T, db kv.RwDB, ii *InvertedIndex, txs uint64) {
	t.Helper()
	ctx := context.Background()
	ic := ii.BeginFilesRo()
	defer ic.Close()

	// Check the iterator invertedIndex first without roTx
	for keyNum := uint64(1); keyNum <= uint64(31); keyNum++ {
		var k [8]byte
		binary.BigEndian.PutUint64(k[:], keyNum)
		var values []uint64
		t.Run("asc", func(t *testing.T) {
			it, err := ic.IdxRange(k[:], 0, 976, order.Asc, -1, nil)
			require.NoError(t, err)
			for i := keyNum; i < 976; i += keyNum {
				label := fmt.Sprintf("keyNum=%d, txNum=%d", keyNum, i)
				require.True(t, it.HasNext(), label)
				n, err := it.Next()
				require.NoError(t, err)
				require.Equal(t, i, n, label)
				values = append(values, n)
			}
			require.False(t, it.HasNext())
		})

		t.Run("desc", func(t *testing.T) {
			reverseStream, err := ic.IdxRange(k[:], 976-1, 0, order.Desc, -1, nil)
			require.NoError(t, err)
			stream.ExpectEqualU64(t, stream.ReverseArray(values), reverseStream)
		})
		t.Run("unbounded asc", func(t *testing.T) {
			forwardLimited, err := ic.IdxRange(k[:], -1, 976, order.Asc, 2, nil)
			require.NoError(t, err)
			stream.ExpectEqualU64(t, stream.Array(values[:2]), forwardLimited)
		})
		t.Run("unbounded desc", func(t *testing.T) {
			reverseLimited, err := ic.IdxRange(k[:], 976-1, -1, order.Desc, 2, nil)
			require.NoError(t, err)
			stream.ExpectEqualU64(t, stream.ReverseArray(values[len(values)-2:]), reverseLimited)
		})
		t.Run("tiny bound asc", func(t *testing.T) {
			it, err := ic.IdxRange(k[:], 100, 102, order.Asc, -1, nil)
			require.NoError(t, err)
			expect := stream.FilterU64(stream.Array(values), func(k uint64) bool { return k >= 100 && k < 102 })
			stream.ExpectEqualU64(t, expect, it)
		})
		t.Run("tiny bound desc", func(t *testing.T) {
			it, err := ic.IdxRange(k[:], 102, 100, order.Desc, -1, nil)
			require.NoError(t, err)
			expect := stream.FilterU64(stream.ReverseArray(values), func(k uint64) bool { return k <= 102 && k > 100 })
			stream.ExpectEqualU64(t, expect, it)
		})
	}
	// Now check invertedIndex that require access to DB
	roTx, err := db.BeginRo(ctx)
	require.NoError(t, err)
	defer roTx.Rollback()
	for keyNum := uint64(1); keyNum <= uint64(31); keyNum++ {
		var k [8]byte
		binary.BigEndian.PutUint64(k[:], keyNum)
		it, err := ic.IdxRange(k[:], 400, 1000, true, -1, roTx)
		require.NoError(t, err)
		var values []uint64
		for i := keyNum * ((400 + keyNum - 1) / keyNum); i < txs; i += keyNum {
			label := fmt.Sprintf("keyNum=%d, txNum=%d", keyNum, i)
			require.True(t, it.HasNext(), label)
			n, err := it.Next()
			require.NoError(t, err)
			require.Equal(t, i, n, label)
			values = append(values, n)
		}
		require.False(t, it.HasNext())

		reverseStream, err := ic.IdxRange(k[:], 1000-1, 400-1, false, -1, roTx)
		require.NoError(t, err)
		arr := stream.ToArrU64Must(reverseStream)
		expect := stream.ToArrU64Must(stream.ReverseArray(values))
		require.Equal(t, expect, arr)
	}
}

func mergeInverted(tb testing.TB, db kv.RwDB, ii *InvertedIndex, txs uint64) {
	tb.Helper()
	logEvery := time.NewTicker(30 * time.Second)
	defer logEvery.Stop()
	ctx := context.Background()
	// Leave the last 2 aggregation steps un-collated
	tx, err := db.BeginRw(ctx)
	require.NoError(tb, err)
	defer tx.Rollback()

	// Leave the last 2 aggregation steps un-collated
	for step := uint64(0); step < txs/ii.aggregationStep-1; step++ {
		func() {
			bs, err := ii.collate(ctx, step, tx)
			require.NoError(tb, err)
			sf, err := ii.buildFiles(ctx, step, bs, background.NewProgressSet())
			require.NoError(tb, err)
			ii.integrateDirtyFiles(sf, step*ii.aggregationStep, (step+1)*ii.aggregationStep)
			ii.reCalcVisibleFiles(ii.dirtyFilesEndTxNumMinimax())
			ic := ii.BeginFilesRo()
			defer ic.Close()
			_, err = ic.Prune(ctx, tx, step*ii.aggregationStep, (step+1)*ii.aggregationStep, math.MaxUint64, logEvery, false, nil)
			require.NoError(tb, err)
			var found bool
			var startTxNum, endTxNum uint64
			maxEndTxNum := ii.dirtyFilesEndTxNumMinimax()
			maxSpan := ii.aggregationStep * StepsInColdFile

			for {
				if stop := func() bool {
					ic := ii.BeginFilesRo()
					defer ic.Close()
					mr := ic.findMergeRange(maxEndTxNum, maxSpan)
					found, startTxNum, endTxNum = mr.needMerge, mr.from, mr.to
					if !found {
						return true
					}
					outs := ic.staticFilesInRange(startTxNum, endTxNum)
					in, err := ic.mergeFiles(ctx, outs, startTxNum, endTxNum, background.NewProgressSet())
					require.NoError(tb, err)
					ii.integrateMergedDirtyFiles(outs, in)
					ii.reCalcVisibleFiles(ii.dirtyFilesEndTxNumMinimax())
					return false
				}(); stop {
					break
				}
			}
		}()
	}
	err = tx.Commit()
	require.NoError(tb, err)
}

func TestInvIndexRanges(t *testing.T) {
	t.Parallel()

	logger := log.New()
	logEvery := time.NewTicker(30 * time.Second)
	defer logEvery.Stop()
	db, ii, txs := filledInvIndex(t, logger)
	ctx := context.Background()
	tx, err := db.BeginRw(ctx)
	require.NoError(t, err)
	defer tx.Rollback()

	// Leave the last 2 aggregation steps un-collated
	for step := uint64(0); step < txs/ii.aggregationStep-1; step++ {
		func() {
			bs, err := ii.collate(ctx, step, tx)
			require.NoError(t, err)
			sf, err := ii.buildFiles(ctx, step, bs, background.NewProgressSet())
			require.NoError(t, err)
			ii.integrateDirtyFiles(sf, step*ii.aggregationStep, (step+1)*ii.aggregationStep)
			ii.reCalcVisibleFiles(ii.dirtyFilesEndTxNumMinimax())
			ic := ii.BeginFilesRo()
			defer ic.Close()
			_, err = ic.Prune(ctx, tx, step*ii.aggregationStep, (step+1)*ii.aggregationStep, math.MaxUint64, logEvery, false, nil)
			require.NoError(t, err)
		}()
	}
	err = tx.Commit()
	require.NoError(t, err)

	checkRanges(t, db, ii, txs)
}

func TestInvIndexMerge(t *testing.T) {
	logger := log.New()
	db, ii, txs := filledInvIndex(t, logger)

	mergeInverted(t, db, ii, txs)
	checkRanges(t, db, ii, txs)
}

func TestInvIndexScanFiles(t *testing.T) {
	logger, require := log.New(), require.New(t)
	db, ii, txs := filledInvIndex(t, logger)

	// Recreate InvertedIndex to scan the files
	var err error
	salt := uint32(1)
	cfg := iiCfg{salt: &salt, dirs: ii.dirs, db: db}
	ii, err = NewInvertedIndex(cfg, ii.aggregationStep, ii.filenameBase, ii.indexKeysTable, ii.indexTable, nil, logger)
	require.NoError(err)
	defer ii.Close()
	err = ii.openFolder()
	require.NoError(err)

	mergeInverted(t, db, ii, txs)
	checkRanges(t, db, ii, txs)
}

func TestChangedKeysIterator(t *testing.T) {
	t.Parallel()

	logger := log.New()
	db, ii, txs := filledInvIndex(t, logger)
	ctx := context.Background()
	mergeInverted(t, db, ii, txs)
	roTx, err := db.BeginRo(ctx)
	require.NoError(t, err)
	defer func() {
		roTx.Rollback()
	}()
	ic := ii.BeginFilesRo()
	defer ic.Close()
	it := ic.IterateChangedKeys(0, 20, roTx)
	defer func() {
		it.Close()
	}()
	var keys []string
	for it.HasNext() {
		k := it.Next(nil)
		keys = append(keys, fmt.Sprintf("%x", k))
	}
	it.Close()
	require.Equal(t, []string{
		"0000000000000001",
		"0000000000000002",
		"0000000000000003",
		"0000000000000004",
		"0000000000000005",
		"0000000000000006",
		"0000000000000007",
		"0000000000000008",
		"0000000000000009",
		"000000000000000a",
		"000000000000000b",
		"000000000000000c",
		"000000000000000d",
		"000000000000000e",
		"000000000000000f",
		"0000000000000010",
		"0000000000000011",
		"0000000000000012",
		"0000000000000013"}, keys)
	it = ic.IterateChangedKeys(995, 1000, roTx)
	keys = keys[:0]
	for it.HasNext() {
		k := it.Next(nil)
		keys = append(keys, fmt.Sprintf("%x", k))
	}
	it.Close()
	require.Equal(t, []string{
		"0000000000000001",
		"0000000000000002",
		"0000000000000003",
		"0000000000000004",
		"0000000000000005",
		"0000000000000006",
		"0000000000000009",
		"000000000000000c",
		"000000000000001b",
	}, keys)
}

func TestScanStaticFiles(t *testing.T) {
	t.Parallel()

	ii := emptyTestInvertedIndex(1)
	files := []string{
		"v1-test.0-1.ef",
		"v1-test.1-2.ef",
		"v1-test.0-4.ef",
		"v1-test.2-3.ef",
		"v1-test.3-4.ef",
		"v1-test.4-5.ef",
	}
	ii.scanDirtyFiles(files)
	require.Equal(t, 6, ii.dirtyFiles.Len())

	//integrity extension case
	ii.dirtyFiles.Clear()
	ii.integrityCheck = func(fromStep, toStep uint64) bool { return false }
	ii.scanDirtyFiles(files)
	require.Equal(t, 0, ii.dirtyFiles.Len())
}

func TestCtxFiles(t *testing.T) {
	ii := emptyTestInvertedIndex(1)
	files := []string{
		"v1-test.0-1.ef", // overlap with same `endTxNum=4`
		"v1-test.1-2.ef",
		"v1-test.0-4.ef",
		"v1-test.2-3.ef",
		"v1-test.3-4.ef",
		"v1-test.4-5.ef",     // no overlap
		"v1-test.480-484.ef", // overlap with same `startTxNum=480`
		"v1-test.480-488.ef",
		"v1-test.480-496.ef",
		"v1-test.480-512.ef",
	}
	ii.scanDirtyFiles(files)
	require.Equal(t, 10, ii.dirtyFiles.Len())
	ii.dirtyFiles.Scan(func(item *filesItem) bool {
		fName := ii.efFilePath(item.startTxNum/ii.aggregationStep, item.endTxNum/ii.aggregationStep)
		item.decompressor = &seg.Decompressor{FileName1: fName}
		return true
	})

	visibleFiles := calcVisibleFiles(ii.dirtyFiles, 0, false, ii.dirtyFilesEndTxNumMinimax())
	for i, item := range visibleFiles {
		if item.src.canDelete.Load() {
			require.Failf(t, "deleted file", "%d-%d", item.startTxNum, item.endTxNum)
		}
		if i == 0 {
			continue
		}
		if item.src.isSubsetOf(visibleFiles[i-1].src) || visibleFiles[i-1].src.isSubsetOf(item.src) {
			require.Failf(t, "overlaping files", "%d-%d, %d-%d", item.startTxNum, item.endTxNum, visibleFiles[i-1].startTxNum, visibleFiles[i-1].endTxNum)
		}
	}
	require.Equal(t, 3, len(visibleFiles))

	require.Equal(t, 0, int(visibleFiles[0].startTxNum))
	require.Equal(t, 4, int(visibleFiles[0].endTxNum))

	require.Equal(t, 4, int(visibleFiles[1].startTxNum))
	require.Equal(t, 5, int(visibleFiles[1].endTxNum))

	require.Equal(t, 480, int(visibleFiles[2].startTxNum))
	require.Equal(t, 512, int(visibleFiles[2].endTxNum))
}

func TestIsSubset(t *testing.T) {
	t.Parallel()

	assert := assert.New(t)
	assert.True((&filesItem{startTxNum: 0, endTxNum: 1}).isSubsetOf(&filesItem{startTxNum: 0, endTxNum: 2}))
	assert.True((&filesItem{startTxNum: 1, endTxNum: 2}).isSubsetOf(&filesItem{startTxNum: 0, endTxNum: 2}))
	assert.False((&filesItem{startTxNum: 0, endTxNum: 2}).isSubsetOf(&filesItem{startTxNum: 0, endTxNum: 2}))
	assert.False((&filesItem{startTxNum: 0, endTxNum: 3}).isSubsetOf(&filesItem{startTxNum: 0, endTxNum: 2}))
	assert.False((&filesItem{startTxNum: 2, endTxNum: 3}).isSubsetOf(&filesItem{startTxNum: 0, endTxNum: 2}))
	assert.False((&filesItem{startTxNum: 0, endTxNum: 1}).isSubsetOf(&filesItem{startTxNum: 1, endTxNum: 2}))
	assert.False((&filesItem{startTxNum: 0, endTxNum: 2}).isSubsetOf(&filesItem{startTxNum: 1, endTxNum: 2}))
}

func TestIsBefore(t *testing.T) {
	t.Parallel()

	assert := assert.New(t)
	assert.False((&filesItem{startTxNum: 0, endTxNum: 1}).isBefore(&filesItem{startTxNum: 0, endTxNum: 2}))
	assert.False((&filesItem{startTxNum: 1, endTxNum: 2}).isBefore(&filesItem{startTxNum: 0, endTxNum: 2}))
	assert.False((&filesItem{startTxNum: 0, endTxNum: 2}).isBefore(&filesItem{startTxNum: 0, endTxNum: 2}))
	assert.False((&filesItem{startTxNum: 0, endTxNum: 3}).isBefore(&filesItem{startTxNum: 0, endTxNum: 2}))
	assert.False((&filesItem{startTxNum: 2, endTxNum: 3}).isBefore(&filesItem{startTxNum: 0, endTxNum: 2}))
	assert.True((&filesItem{startTxNum: 0, endTxNum: 1}).isBefore(&filesItem{startTxNum: 1, endTxNum: 2}))
	assert.False((&filesItem{startTxNum: 0, endTxNum: 2}).isBefore(&filesItem{startTxNum: 1, endTxNum: 2}))
	assert.True((&filesItem{startTxNum: 0, endTxNum: 1}).isBefore(&filesItem{startTxNum: 2, endTxNum: 4}))
	assert.True((&filesItem{startTxNum: 0, endTxNum: 2}).isBefore(&filesItem{startTxNum: 2, endTxNum: 4}))
}

func TestInvIndex_OpenFolder(t *testing.T) {
	t.Parallel()

	db, ii, txs := filledInvIndex(t, log.New())

	mergeInverted(t, db, ii, txs)

	list := ii._visible.files
	require.NotEmpty(t, list)
	ff := list[len(list)-1]
	fn := ff.src.decompressor.FilePath()
	ii.Close()

	err := os.Remove(fn)
	require.NoError(t, err)
	err = os.WriteFile(fn, make([]byte, 33), 0644)
	require.NoError(t, err)

	err = ii.openFolder()
	require.NoError(t, err)
	ii.Close()
}
