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
	"os"
	"testing"
	"time"

	"github.com/ledgerwatch/erigon-lib/common/background"
	"github.com/ledgerwatch/erigon-lib/kv/iter"
	"github.com/ledgerwatch/erigon-lib/kv/order"
	"github.com/ledgerwatch/log/v3"
	"github.com/stretchr/testify/require"
	btree2 "github.com/tidwall/btree"

	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/mdbx"
	"github.com/ledgerwatch/erigon-lib/recsplit"
	"github.com/ledgerwatch/erigon-lib/recsplit/eliasfano32"
)

func testDbAndInvertedIndex(tb testing.TB, aggStep uint64, logger log.Logger) (string, kv.RwDB, *InvertedIndex) {
	tb.Helper()
	path := tb.TempDir()
	tb.Cleanup(func() { os.RemoveAll(path) })
	keysTable := "Keys"
	indexTable := "Index"
	db := mdbx.NewMDBX(logger).InMem(path).WithTableCfg(func(defaultBuckets kv.TableCfg) kv.TableCfg {
		return kv.TableCfg{
			keysTable:  kv.TableCfgItem{Flags: kv.DupSort},
			indexTable: kv.TableCfgItem{Flags: kv.DupSort},
		}
	}).MustOpen()
	tb.Cleanup(db.Close)
	ii, err := NewInvertedIndex(path, path, aggStep, "inv" /* filenameBase */, keysTable, indexTable, false, nil, logger)
	require.NoError(tb, err)
	ii.DisableFsync()
	tb.Cleanup(ii.Close)
	return path, db, ii
}

func TestInvIndexCollationBuild(t *testing.T) {
	logger := log.New()
	logEvery := time.NewTicker(30 * time.Second)
	defer logEvery.Stop()
	_, db, ii := testDbAndInvertedIndex(t, 16, logger)
	ctx := context.Background()
	tx, err := db.BeginRw(ctx)
	require.NoError(t, err)
	defer tx.Rollback()
	ii.SetTx(tx)
	ii.StartWrites()
	defer ii.FinishWrites()

	ii.SetTxNum(2)
	err = ii.Add([]byte("key1"))
	require.NoError(t, err)

	ii.SetTxNum(3)
	err = ii.Add([]byte("key2"))
	require.NoError(t, err)

	ii.SetTxNum(6)
	err = ii.Add([]byte("key1"))
	require.NoError(t, err)
	err = ii.Add([]byte("key3"))
	require.NoError(t, err)

	err = ii.Rotate().Flush(ctx, tx)
	require.NoError(t, err)
	err = tx.Commit()
	require.NoError(t, err)

	roTx, err := db.BeginRo(ctx)
	require.NoError(t, err)
	defer roTx.Rollback()

	bs, err := ii.collate(ctx, 0, 7, roTx)
	require.NoError(t, err)
	require.Equal(t, 3, len(bs))
	require.Equal(t, []uint64{3}, bs["key2"].ToArray())
	require.Equal(t, []uint64{2, 6}, bs["key1"].ToArray())
	require.Equal(t, []uint64{6}, bs["key3"].ToArray())

	sf, err := ii.buildFiles(ctx, 0, bs, background.NewProgressSet())
	require.NoError(t, err)
	defer sf.Close()

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
		offset := r.Lookup([]byte(words[i]))
		g.Reset(offset)
		w, _ := g.Next(nil)
		require.Equal(t, words[i], string(w))
	}
}

func TestInvIndexAfterPrune(t *testing.T) {
	logger := log.New()
	logEvery := time.NewTicker(30 * time.Second)
	defer logEvery.Stop()
	_, db, ii := testDbAndInvertedIndex(t, 16, logger)
	ctx := context.Background()
	tx, err := db.BeginRw(ctx)
	require.NoError(t, err)
	defer func() {
		if tx != nil {
			tx.Rollback()
		}
	}()
	ii.SetTx(tx)
	ii.StartWrites()
	defer ii.FinishWrites()

	ii.SetTxNum(2)
	err = ii.Add([]byte("key1"))
	require.NoError(t, err)

	ii.SetTxNum(3)
	err = ii.Add([]byte("key2"))
	require.NoError(t, err)

	ii.SetTxNum(6)
	err = ii.Add([]byte("key1"))
	require.NoError(t, err)
	err = ii.Add([]byte("key3"))
	require.NoError(t, err)

	err = ii.Rotate().Flush(ctx, tx)
	require.NoError(t, err)
	err = tx.Commit()
	require.NoError(t, err)

	roTx, err := db.BeginRo(ctx)
	require.NoError(t, err)
	defer roTx.Rollback()

	bs, err := ii.collate(ctx, 0, 16, roTx)
	require.NoError(t, err)

	sf, err := ii.buildFiles(ctx, 0, bs, background.NewProgressSet())
	require.NoError(t, err)

	tx, err = db.BeginRw(ctx)
	require.NoError(t, err)
	ii.SetTx(tx)

	ii.integrateFiles(sf, 0, 16)

	err = ii.prune(ctx, 0, 16, math.MaxUint64, logEvery)
	require.NoError(t, err)
	err = tx.Commit()
	require.NoError(t, err)
	tx, err = db.BeginRw(ctx)
	require.NoError(t, err)
	ii.SetTx(tx)

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
}

func filledInvIndex(tb testing.TB, logger log.Logger) (string, kv.RwDB, *InvertedIndex, uint64) {
	tb.Helper()
	return filledInvIndexOfSize(tb, uint64(1000), 16, 31, logger)
}

func filledInvIndexOfSize(tb testing.TB, txs, aggStep, module uint64, logger log.Logger) (string, kv.RwDB, *InvertedIndex, uint64) {
	tb.Helper()
	path, db, ii := testDbAndInvertedIndex(tb, aggStep, logger)
	ctx, require := context.Background(), require.New(tb)
	tx, err := db.BeginRw(ctx)
	require.NoError(err)
	defer tx.Rollback()
	ii.SetTx(tx)
	ii.StartWrites()
	defer ii.FinishWrites()

	var flusher flusher

	// keys are encodings of numbers 1..31
	// each key changes value on every txNum which is multiple of the key
	for txNum := uint64(1); txNum <= txs; txNum++ {
		ii.SetTxNum(txNum)
		for keyNum := uint64(1); keyNum <= module; keyNum++ {
			if txNum%keyNum == 0 {
				var k [8]byte
				binary.BigEndian.PutUint64(k[:], keyNum)
				err = ii.Add(k[:])
				require.NoError(err)
			}
		}
		if flusher != nil {
			require.NoError(flusher.Flush(ctx, tx))
		}
		if txNum%10 == 0 {
			flusher = ii.Rotate()
		}
	}
	if flusher != nil {
		require.NoError(flusher.Flush(ctx, tx))
	}
	err = ii.Rotate().Flush(ctx, tx)
	require.NoError(err)
	err = tx.Commit()
	require.NoError(err)
	return path, db, ii, txs
}

func checkRanges(t *testing.T, db kv.RwDB, ii *InvertedIndex, txs uint64) {
	t.Helper()
	ctx := context.Background()
	ic := ii.MakeContext()
	defer ic.Close()

	// Check the iterator ranges first without roTx
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
			iter.ExpectEqualU64(t, iter.ReverseArray(values), reverseStream)
		})
		t.Run("unbounded asc", func(t *testing.T) {
			forwardLimited, err := ic.IdxRange(k[:], -1, 976, order.Asc, 2, nil)
			require.NoError(t, err)
			iter.ExpectEqualU64(t, iter.Array(values[:2]), forwardLimited)
		})
		t.Run("unbounded desc", func(t *testing.T) {
			reverseLimited, err := ic.IdxRange(k[:], 976-1, -1, order.Desc, 2, nil)
			require.NoError(t, err)
			iter.ExpectEqualU64(t, iter.ReverseArray(values[len(values)-2:]), reverseLimited)
		})
		t.Run("tiny bound asc", func(t *testing.T) {
			it, err := ic.IdxRange(k[:], 100, 102, order.Asc, -1, nil)
			require.NoError(t, err)
			expect := iter.FilterU64(iter.Array(values), func(k uint64) bool { return k >= 100 && k < 102 })
			iter.ExpectEqualU64(t, expect, it)
		})
		t.Run("tiny bound desc", func(t *testing.T) {
			it, err := ic.IdxRange(k[:], 102, 100, order.Desc, -1, nil)
			require.NoError(t, err)
			expect := iter.FilterU64(iter.ReverseArray(values), func(k uint64) bool { return k <= 102 && k > 100 })
			iter.ExpectEqualU64(t, expect, it)
		})
	}
	// Now check ranges that require access to DB
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
		arr := iter.ToArrU64Must(reverseStream)
		expect := iter.ToArrU64Must(iter.ReverseArray(values))
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
	ii.SetTx(tx)

	// Leave the last 2 aggregation steps un-collated
	for step := uint64(0); step < txs/ii.aggregationStep-1; step++ {
		func() {
			bs, err := ii.collate(ctx, step*ii.aggregationStep, (step+1)*ii.aggregationStep, tx)
			require.NoError(tb, err)
			sf, err := ii.buildFiles(ctx, step, bs, background.NewProgressSet())
			require.NoError(tb, err)
			ii.integrateFiles(sf, step*ii.aggregationStep, (step+1)*ii.aggregationStep)
			err = ii.prune(ctx, step*ii.aggregationStep, (step+1)*ii.aggregationStep, math.MaxUint64, logEvery)
			require.NoError(tb, err)
			var found bool
			var startTxNum, endTxNum uint64
			maxEndTxNum := ii.endTxNumMinimax()
			maxSpan := ii.aggregationStep * StepsInBiggestFile

			for {
				if stop := func() bool {
					ic := ii.MakeContext()
					defer ic.Close()
					found, startTxNum, endTxNum = ii.findMergeRange(maxEndTxNum, maxSpan)
					if !found {
						return true
					}
					outs, _ := ic.staticFilesInRange(startTxNum, endTxNum)
					in, err := ii.mergeFiles(ctx, outs, startTxNum, endTxNum, 1, background.NewProgressSet())
					require.NoError(tb, err)
					ii.integrateMergedFiles(outs, in)
					require.NoError(tb, err)
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
	logger := log.New()
	logEvery := time.NewTicker(30 * time.Second)
	defer logEvery.Stop()
	_, db, ii, txs := filledInvIndex(t, logger)
	ctx := context.Background()
	tx, err := db.BeginRw(ctx)
	require.NoError(t, err)
	defer tx.Rollback()
	ii.SetTx(tx)

	// Leave the last 2 aggregation steps un-collated
	for step := uint64(0); step < txs/ii.aggregationStep-1; step++ {
		func() {
			bs, err := ii.collate(ctx, step*ii.aggregationStep, (step+1)*ii.aggregationStep, tx)
			require.NoError(t, err)
			sf, err := ii.buildFiles(ctx, step, bs, background.NewProgressSet())
			require.NoError(t, err)
			ii.integrateFiles(sf, step*ii.aggregationStep, (step+1)*ii.aggregationStep)
			err = ii.prune(ctx, step*ii.aggregationStep, (step+1)*ii.aggregationStep, math.MaxUint64, logEvery)
			require.NoError(t, err)
		}()
	}
	err = tx.Commit()
	require.NoError(t, err)

	checkRanges(t, db, ii, txs)
}

func TestInvIndexMerge(t *testing.T) {
	logger := log.New()
	_, db, ii, txs := filledInvIndex(t, logger)

	mergeInverted(t, db, ii, txs)
	checkRanges(t, db, ii, txs)
}

func TestInvIndexScanFiles(t *testing.T) {
	logger := log.New()
	path, db, ii, txs := filledInvIndex(t, logger)

	// Recreate InvertedIndex to scan the files
	var err error
	ii, err = NewInvertedIndex(path, path, ii.aggregationStep, ii.filenameBase, ii.indexKeysTable, ii.indexTable, false, nil, logger)
	require.NoError(t, err)
	defer ii.Close()

	mergeInverted(t, db, ii, txs)
	checkRanges(t, db, ii, txs)
}

func TestChangedKeysIterator(t *testing.T) {
	logger := log.New()
	_, db, ii, txs := filledInvIndex(t, logger)
	ctx := context.Background()
	mergeInverted(t, db, ii, txs)
	roTx, err := db.BeginRo(ctx)
	require.NoError(t, err)
	defer func() {
		roTx.Rollback()
	}()
	ic := ii.MakeContext()
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
	logger := log.New()
	ii := &InvertedIndex{filenameBase: "test", aggregationStep: 1,
		files:  btree2.NewBTreeG[*filesItem](filesItemLess),
		logger: logger,
	}
	files := []string{
		"test.0-1.ef",
		"test.1-2.ef",
		"test.0-4.ef",
		"test.2-3.ef",
		"test.3-4.ef",
		"test.4-5.ef",
	}
	ii.scanStateFiles(files)
	require.Equal(t, 6, ii.files.Len())

	//integrity extension case
	ii.files.Clear()
	ii.integrityFileExtensions = []string{"v"}
	ii.scanStateFiles(files)
	require.Equal(t, 0, ii.files.Len())
}

func TestCtxFiles(t *testing.T) {
	logger := log.New()
	ii := &InvertedIndex{filenameBase: "test", aggregationStep: 1,
		files:  btree2.NewBTreeG[*filesItem](filesItemLess),
		logger: logger,
	}
	files := []string{
		"test.0-1.ef", // overlap with same `endTxNum=4`
		"test.1-2.ef",
		"test.0-4.ef",
		"test.2-3.ef",
		"test.3-4.ef",
		"test.4-5.ef",     // no overlap
		"test.480-484.ef", // overlap with same `startTxNum=480`
		"test.480-488.ef",
		"test.480-496.ef",
		"test.480-512.ef",
	}
	ii.scanStateFiles(files)
	require.Equal(t, 10, ii.files.Len())

	roFiles := ctxFiles(ii.files)
	for i, item := range roFiles {
		if item.src.canDelete.Load() {
			require.Failf(t, "deleted file", "%d-%d", item.src.startTxNum, item.src.endTxNum)
		}
		if i == 0 {
			continue
		}
		if item.src.isSubsetOf(roFiles[i-1].src) || roFiles[i-1].src.isSubsetOf(item.src) {
			require.Failf(t, "overlaping files", "%d-%d, %d-%d", item.src.startTxNum, item.src.endTxNum, roFiles[i-1].src.startTxNum, roFiles[i-1].src.endTxNum)
		}
	}
	require.Equal(t, 3, len(roFiles))

	require.Equal(t, 0, int(roFiles[0].startTxNum))
	require.Equal(t, 4, int(roFiles[0].endTxNum))

	require.Equal(t, 4, int(roFiles[1].startTxNum))
	require.Equal(t, 5, int(roFiles[1].endTxNum))

	require.Equal(t, 480, int(roFiles[2].startTxNum))
	require.Equal(t, 512, int(roFiles[2].endTxNum))
}
