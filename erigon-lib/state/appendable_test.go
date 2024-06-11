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
	"github.com/ledgerwatch/erigon-lib/common/hexutility"
	"github.com/ledgerwatch/erigon-lib/kv/iter"
	"github.com/ledgerwatch/erigon-lib/kv/order"
	"go.uber.org/mock/gomock"
	"math"
	"os"
	"testing"
	"time"

	"github.com/ledgerwatch/erigon-lib/common/background"
	"github.com/ledgerwatch/erigon-lib/common/datadir"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/mdbx"
	"github.com/ledgerwatch/erigon-lib/seg"
	"github.com/ledgerwatch/log/v3"
	"github.com/stretchr/testify/require"
	btree2 "github.com/tidwall/btree"
)

func testDbAndAppendable(tb testing.TB, aggStep uint64, logger log.Logger) (kv.RwDB, *Appendable) {
	tb.Helper()
	dirs := datadir.New(tb.TempDir())
	table := "Appendable"
	db := mdbx.NewMDBX(logger).InMem(dirs.Chaindata).WithTableCfg(func(defaultBuckets kv.TableCfg) kv.TableCfg {
		return kv.TableCfg{
			table:                 kv.TableCfgItem{Flags: kv.DupSort},
			kv.TblPruningProgress: kv.TableCfgItem{},
			kv.HeaderCanonical:    kv.TableCfgItem{},
		}
	}).MustOpen()
	tb.Cleanup(db.Close)
	salt := uint32(1)
	cfg := AppendableCfg{Salt: &salt, Dirs: dirs, DB: db, CanonicalMarkersTable: kv.HeaderCanonical}
	ii, err := NewAppendable(cfg, aggStep, "receipt", table, nil, logger)
	require.NoError(tb, err)
	ii.DisableFsync()
	tb.Cleanup(ii.Close)
	return db, ii
}

func TestAppendableCollationBuild(t *testing.T) {
	logEvery := time.NewTicker(30 * time.Second)
	defer logEvery.Stop()
	db, ii, txs := filledAppendable(t, log.New())
	ctx := context.Background()
	aggStep := uint64(16)
	steps := txs / aggStep

	t.Run("can see own writes", func(t *testing.T) {
		//nonbuf api can see own writes
		require := require.New(t)

		tx, err := db.BeginRo(ctx)
		require.NoError(err)
		defer tx.Rollback()
		ic := ii.BeginFilesRo()
		defer ic.Close()

		//can see own writes
		v, ok, err := ic.Get(1, tx)
		require.NoError(err)
		require.True(ok)
		require.Equal(1, int(binary.BigEndian.Uint64(v)))

		//never existed key
		v, ok, err = ic.Get(txs+1, tx)
		require.NoError(err)
		require.False(ok)

		//non-canonical key: must exist before collate+prune
		v, ok, err = ic.Get(steps+1, tx)
		require.NoError(err)
		require.True(ok)

		err = tx.Commit()
		require.NoError(err)
	})
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	//see only canonical records in files
	iters := NewMockIterFactory(ctrl)
	iters.EXPECT().TxnIdsOfCanonicalBlocks(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(tx kv.Tx, txFrom, txTo int, by order.By, i3 int) (iter.U64, error) {
			currentStep := uint64(txFrom) / aggStep
			canonicalBlockTxNum := aggStep*currentStep + 1
			it := iter.Array[uint64]([]uint64{canonicalBlockTxNum})
			return it, nil
		}).
		AnyTimes()
	ii.cfg.iters = iters

	mergeAppendable(t, db, ii, txs)

	t.Run("read after collate and prune", func(t *testing.T) {
		require := require.New(t)

		ic := ii.BeginFilesRo()
		defer ic.Close()

		tx, err := db.BeginRo(ctx)
		require.NoError(err)
		defer tx.Rollback()

		//canonical keys
		w, ok, err := ic.Get(0, tx)
		require.NoError(err)
		require.True(ok)
		require.Equal(1, int(binary.BigEndian.Uint64(w)))

		w, ok, err = ic.Get(1, tx)
		require.True(ok)
		require.Equal(int(aggStep+1), int(binary.BigEndian.Uint64(w)))

		//non-canonical key: must exist before collate+prune
		w, ok = ic.getFromFiles(steps + 1)
		require.False(ok)

		from, to := ii.stepsRangeInDB(tx)
		require.Equal(float64(0), from)
		require.Equal(62.4375, to)

		//non-canonical key: must exist before collate+prune
		w, ok, err = ic.Get(steps+1, tx)
		require.False(ok)

		//non-canonical keys of last step: must exist after collate+prune
		w, ok, err = ic.Get(aggStep*steps+2, tx)
		require.NoError(err)
		require.True(ok)
	})

	t.Run("scan files", func(t *testing.T) {
		checkRangesAppendable(t, db, ii, txs)

		// Recreate to scan the files
		var err error
		salt := uint32(1)
		cfg := AppendableCfg{Salt: &salt, Dirs: ii.cfg.Dirs, DB: db, CanonicalMarkersTable: kv.HeaderCanonical}
		ii, err = NewAppendable(cfg, ii.aggregationStep, ii.filenameBase, ii.table, nil, log.New())
		require.NoError(t, err)
		defer ii.Close()

		mergeAppendable(t, db, ii, txs)
		checkRangesAppendable(t, db, ii, txs)
	})
}

func filledAppendable(tb testing.TB, logger log.Logger) (kv.RwDB, *Appendable, uint64) {
	tb.Helper()
	return filledAppendableOfSize(tb, uint64(1000), 16, logger)
}

func filledAppendableOfSize(tb testing.TB, txs, aggStep uint64, logger log.Logger) (kv.RwDB, *Appendable, uint64) {
	tb.Helper()
	db, ii := testDbAndAppendable(tb, aggStep, logger)
	ctx, require := context.Background(), require.New(tb)
	tx, err := db.BeginRw(ctx)
	require.NoError(err)
	defer tx.Rollback()
	ic := ii.BeginFilesRo()
	defer ic.Close()

	for i := uint64(0); i < txs; i++ {
		err = ic.Put(i, hexutility.EncodeTs(i), tx)
		require.NoError(err)
	}
	err = tx.Commit()
	require.NoError(err)
	return db, ii, txs
}

func checkRangesAppendable(t *testing.T, db kv.RwDB, ii *Appendable, txs uint64) {
	//t.Helper()
	//ctx := context.Background()
	//ic := ii.BeginFilesRo()
	//defer ic.Close()
	//
	//// Check the iterator ranges first without roTx
	//for keyNum := uint64(1); keyNum <= uint64(31); keyNum++ {
	//	var k [8]byte
	//	binary.BigEndian.PutUint64(k[:], keyNum)
	//	var values []uint64
	//	t.Run("asc", func(t *testing.T) {
	//		it, err := ic.IdxRange(k[:], 0, 976, order.Asc, -1, nil)
	//		require.NoError(t, err)
	//		for i := keyNum; i < 976; i += keyNum {
	//			label := fmt.Sprintf("keyNum=%d, txNum=%d", keyNum, i)
	//			require.True(t, it.HasNext(), label)
	//			n, err := it.Next()
	//			require.NoError(t, err)
	//			require.Equal(t, i, n, label)
	//			values = append(values, n)
	//		}
	//		require.False(t, it.HasNext())
	//	})
	//
	//	t.Run("desc", func(t *testing.T) {
	//		reverseStream, err := ic.IdxRange(k[:], 976-1, 0, order.Desc, -1, nil)
	//		require.NoError(t, err)
	//		iter.ExpectEqualU64(t, iter.ReverseArray(values), reverseStream)
	//	})
	//	t.Run("unbounded asc", func(t *testing.T) {
	//		forwardLimited, err := ic.IdxRange(k[:], -1, 976, order.Asc, 2, nil)
	//		require.NoError(t, err)
	//		iter.ExpectEqualU64(t, iter.Array(values[:2]), forwardLimited)
	//	})
	//	t.Run("unbounded desc", func(t *testing.T) {
	//		reverseLimited, err := ic.IdxRange(k[:], 976-1, -1, order.Desc, 2, nil)
	//		require.NoError(t, err)
	//		iter.ExpectEqualU64(t, iter.ReverseArray(values[len(values)-2:]), reverseLimited)
	//	})
	//	t.Run("tiny bound asc", func(t *testing.T) {
	//		it, err := ic.IdxRange(k[:], 100, 102, order.Asc, -1, nil)
	//		require.NoError(t, err)
	//		expect := iter.FilterU64(iter.Array(values), func(k uint64) bool { return k >= 100 && k < 102 })
	//		iter.ExpectEqualU64(t, expect, it)
	//	})
	//	t.Run("tiny bound desc", func(t *testing.T) {
	//		it, err := ic.IdxRange(k[:], 102, 100, order.Desc, -1, nil)
	//		require.NoError(t, err)
	//		expect := iter.FilterU64(iter.ReverseArray(values), func(k uint64) bool { return k <= 102 && k > 100 })
	//		iter.ExpectEqualU64(t, expect, it)
	//	})
	//}
	//// Now check ranges that require access to DB
	//roTx, err := db.BeginRo(ctx)
	//require.NoError(t, err)
	//defer roTx.Rollback()
	//for keyNum := uint64(1); keyNum <= uint64(31); keyNum++ {
	//	var k [8]byte
	//	binary.BigEndian.PutUint64(k[:], keyNum)
	//	it, err := ic.IdxRange(k[:], 400, 1000, true, -1, roTx)
	//	require.NoError(t, err)
	//	var values []uint64
	//	for i := keyNum * ((400 + keyNum - 1) / keyNum); i < txs; i += keyNum {
	//		label := fmt.Sprintf("keyNum=%d, txNum=%d", keyNum, i)
	//		require.True(t, it.HasNext(), label)
	//		n, err := it.Next()
	//		require.NoError(t, err)
	//		require.Equal(t, i, n, label)
	//		values = append(values, n)
	//	}
	//	require.False(t, it.HasNext())
	//
	//	reverseStream, err := ic.IdxRange(k[:], 1000-1, 400-1, false, -1, roTx)
	//	require.NoError(t, err)
	//	arr := iter.ToArrU64Must(reverseStream)
	//	expect := iter.ToArrU64Must(iter.ReverseArray(values))
	//	require.Equal(t, expect, arr)
	//}
}

func mergeAppendable(tb testing.TB, db kv.RwDB, ii *Appendable, txs uint64) {
	tb.Helper()
	logEvery := time.NewTicker(30 * time.Second)
	defer logEvery.Stop()
	ctx := context.Background()
	// Leave the last 2 aggregation steps un-collated
	tx, err := db.BeginRw(ctx)
	require.NoError(tb, err)
	defer tx.Rollback()
	//panic("implement me")

	// Leave the last 2 aggregation steps un-collated
	for step := uint64(0); step < txs/ii.aggregationStep-1; step++ {
		func() {
			bs, err := ii.collate(ctx, step, tx)
			require.NoError(tb, err)
			sf, err := ii.buildFiles(ctx, step, bs, background.NewProgressSet())
			require.NoError(tb, err)

			ii.integrateDirtyFiles(sf, step*ii.aggregationStep, (step+1)*ii.aggregationStep)
			ii.reCalcVisibleFiles()
			ic := ii.BeginFilesRo()
			defer ic.Close()
			_, err = ic.Prune(ctx, tx, step*ii.aggregationStep, (step+1)*ii.aggregationStep, math.MaxUint64, logEvery, false, false, nil)
			require.NoError(tb, err)
			var found bool
			var startTxNum, endTxNum uint64
			maxEndTxNum := ii.endTxNumMinimax()
			maxSpan := ii.aggregationStep * StepsInColdFile

			for {
				if stop := func() bool {
					ic := ii.BeginFilesRo()
					defer ic.Close()
					found, startTxNum, endTxNum = ic.findMergeRange(maxEndTxNum, maxSpan)
					if !found {
						return true
					}
					outs, _ := ic.staticFilesInRange(startTxNum, endTxNum)
					in, err := ic.mergeFiles(ctx, outs, startTxNum, endTxNum, background.NewProgressSet())
					require.NoError(tb, err)
					ii.integrateMergedDirtyFiles(outs, in)
					ii.reCalcVisibleFiles()
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

func TestAppendableKeysIterator(t *testing.T) {
	logger := log.New()
	db, ii, txs := filledAppendable(t, logger)
	ctx := context.Background()
	mergeAppendable(t, db, ii, txs)
	roTx, err := db.BeginRo(ctx)
	require.NoError(t, err)
	defer func() {
		roTx.Rollback()
	}()
	ic := ii.BeginFilesRo()
	defer ic.Close()
	panic("implement me")
	//it := ic.IterateChangedKeys(0, 20, roTx)
	//defer func() {
	//	it.Close()
	//}()
	//var keys []string
	//for it.HasNext() {
	//	k := it.Next(nil)
	//	keys = append(keys, fmt.Sprintf("%x", k))
	//}
	//it.Close()
	//require.Equal(t, []string{
	//	"0000000000000001",
	//	"0000000000000002",
	//	"0000000000000003",
	//	"0000000000000004",
	//	"0000000000000005",
	//	"0000000000000006",
	//	"0000000000000007",
	//	"0000000000000008",
	//	"0000000000000009",
	//	"000000000000000a",
	//	"000000000000000b",
	//	"000000000000000c",
	//	"000000000000000d",
	//	"000000000000000e",
	//	"000000000000000f",
	//	"0000000000000010",
	//	"0000000000000011",
	//	"0000000000000012",
	//	"0000000000000013"}, keys)
	//it = ic.IterateChangedKeys(995, 1000, roTx)
	//keys = keys[:0]
	//for it.HasNext() {
	//	k := it.Next(nil)
	//	keys = append(keys, fmt.Sprintf("%x", k))
	//}
	//it.Close()
	//require.Equal(t, []string{
	//	"0000000000000001",
	//	"0000000000000002",
	//	"0000000000000003",
	//	"0000000000000004",
	//	"0000000000000005",
	//	"0000000000000006",
	//	"0000000000000009",
	//	"000000000000000c",
	//	"000000000000001b",
	//}, keys)
}

func emptyTestAppendable(aggStep uint64) *Appendable {
	salt := uint32(1)
	logger := log.New()
	return &Appendable{cfg: AppendableCfg{Salt: &salt, DB: nil, CanonicalMarkersTable: kv.HeaderCanonical},
		logger:       logger,
		filenameBase: "test", aggregationStep: aggStep, dirtyFiles: btree2.NewBTreeG[*filesItem](filesItemLess)}
}

func TestAppendableScanStaticFiles(t *testing.T) {
	ii := emptyTestAppendable(1)
	files := []string{
		"v1-test.0-1.ef",
		"v1-test.1-2.ef",
		"v1-test.0-4.ef",
		"v1-test.2-3.ef",
		"v1-test.3-4.ef",
		"v1-test.4-5.ef",
	}
	ii.scanStateFiles(files)
	require.Equal(t, 6, ii.dirtyFiles.Len())

	//integrity extension case
	ii.dirtyFiles.Clear()
	ii.integrityCheck = func(fromStep, toStep uint64) bool { return false }
	ii.scanStateFiles(files)
	require.Equal(t, 0, ii.dirtyFiles.Len())
}

func TestAppendableCtxFiles(t *testing.T) {
	ii := emptyTestAppendable(1)
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
	ii.scanStateFiles(files)
	require.Equal(t, 10, ii.dirtyFiles.Len())
	ii.dirtyFiles.Scan(func(item *filesItem) bool {
		fName := ii.fkFilePath(item.startTxNum/ii.aggregationStep, item.endTxNum/ii.aggregationStep)
		item.decompressor = &seg.Decompressor{FileName1: fName}
		return true
	})

	visibleFiles := calcVisibleFiles(ii.dirtyFiles, 0, false)
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

func TestAppendable_OpenFolder(t *testing.T) {
	db, ii, txs := filledAppendable(t, log.New())

	mergeAppendable(t, db, ii, txs)

	list := ii._visibleFiles
	require.NotEmpty(t, list)
	ff := list[len(list)-1]
	fn := ff.src.decompressor.FilePath()
	ii.Close()

	err := os.Remove(fn)
	require.NoError(t, err)
	err = os.WriteFile(fn, make([]byte, 33), 0644)
	require.NoError(t, err)

	err = ii.OpenFolder(true)
	require.NoError(t, err)
	ii.Close()
}
