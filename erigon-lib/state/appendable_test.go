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
	"context"
	"encoding/binary"
	"math"
	"os"
	"testing"
	"time"

	"go.uber.org/mock/gomock"

	"github.com/erigontech/erigon-lib/common/hexutility"
	"github.com/erigontech/erigon-lib/kv/order"
	"github.com/erigontech/erigon-lib/kv/stream"
	"github.com/erigontech/erigon-lib/log/v3"

	"github.com/stretchr/testify/require"
	btree2 "github.com/tidwall/btree"

	"github.com/erigontech/erigon-lib/common/background"
	"github.com/erigontech/erigon-lib/common/datadir"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/kv/mdbx"
	"github.com/erigontech/erigon-lib/seg"
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
	cfg := AppendableCfg{Salt: &salt, Dirs: dirs, DB: db}
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
		_, ok, err = ic.Get(kv.TxnId(txs+1), tx)
		require.NoError(err)
		require.False(ok)

		//non-canonical key: must exist before collate+prune
		_, ok, err = ic.Get(kv.TxnId(steps+1), tx)
		require.NoError(err)
		require.True(ok)

		err = tx.Commit()
		require.NoError(err)
	})
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	//see only canonical records in files
	iters := NewMockCanonicalsReader(ctrl)
	iters.EXPECT().TxnIdsOfCanonicalBlocks(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(tx kv.Tx, txFrom, txTo int, by order.By, i3 int) (stream.U64, error) {
			currentStep := uint64(txFrom) / aggStep
			canonicalBlockTxNum := aggStep*currentStep + 1
			it := stream.Array[uint64]([]uint64{canonicalBlockTxNum})
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

		checkAppendableGet(t, tx, ic, txs)
	})

	t.Run("scan files", func(t *testing.T) {
		require := require.New(t)

		require.Equal(5, ii.dirtyFiles.Len())
		require.Equal(5, len(ii._visibleFiles))

		// Recreate to scan the files
		ii, err := NewAppendable(ii.cfg, ii.aggregationStep, ii.filenameBase, ii.table, nil, log.New())
		require.NoError(err)
		defer ii.Close()
		err = ii.openFolder(true)
		require.NoError(err)
		require.Equal(5, ii.dirtyFiles.Len())
		require.Equal(0, len(ii._visibleFiles))
		ii.reCalcVisibleFiles()
		require.Equal(5, len(ii._visibleFiles))

		ic := ii.BeginFilesRo()
		defer ic.Close()

		require.Equal(5, len(ic.files))

		tx, err := db.BeginRo(ctx)
		require.NoError(err)
		defer tx.Rollback()

		checkAppendableGet(t, tx, ic, txs)
	})

	t.Run("open_folder_can_handle_broken_files", func(t *testing.T) {
		require := require.New(t)

		list := ii._visibleFiles
		require.NotEmpty(list)
		ff := list[len(list)-1]
		fn := ff.src.decompressor.FilePath()
		ii.Close()

		err := os.Remove(fn)
		require.NoError(err)
		err = os.WriteFile(fn, make([]byte, 33), 0644)
		require.NoError(err)

		err = ii.openFolder(true)
		require.NoError(err)
		ii.Close()
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
		err = ic.Append(kv.TxnId(i), hexutility.EncodeTs(i), tx)
		require.NoError(err)
	}
	err = tx.Commit()
	require.NoError(err)
	return db, ii, txs
}

func checkAppendableGet(t *testing.T, dbtx kv.Tx, tx *AppendableRoTx, txs uint64) {
	t.Helper()
	aggStep := tx.ap.aggregationStep
	steps := txs / aggStep

	require := require.New(t)
	//canonical keys
	w, ok, err := tx.Get(0, dbtx)
	require.NoError(err)
	require.True(ok)
	require.Equal(1, int(binary.BigEndian.Uint64(w)))

	w, ok, err = tx.Get(1, dbtx)
	require.NoError(err)
	require.True(ok)
	require.Equal(int(aggStep+1), int(binary.BigEndian.Uint64(w)))

	//non-canonical key: must exist before collate+prune
	_, ok = tx.getFromFiles(steps + 1)
	require.False(ok)

	from, to := tx.ap.stepsRangeInDB(dbtx)
	require.Equal(float64(0), from)
	require.Equal(62.4375, to)

	//non-canonical key: must exist before collate+prune
	_, ok, err = tx.Get(kv.TxnId(steps+1), dbtx)
	require.NoError(err)
	require.False(ok)

	//non-canonical keys of last step: must exist after collate+prune
	_, ok, err = tx.Get(kv.TxnId(aggStep*steps+2), dbtx)
	require.NoError(err)
	require.True(ok)
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
			_, err = ic.Prune(ctx, tx, step*ii.aggregationStep, (step+1)*ii.aggregationStep, math.MaxUint64, logEvery, false, nil)
			require.NoError(tb, err)
			maxSpan := ii.aggregationStep * StepsInColdFile

			for {
				if stop := func() bool {
					ic := ii.BeginFilesRo()
					defer ic.Close()
					r := ic.findMergeRange(ic.files.EndTxNum(), maxSpan)
					if !r.needMerge {
						return true
					}
					outs := ic.staticFilesInRange(r.from, r.to)
					in, err := ic.mergeFiles(ctx, outs, r.from, r.to, background.NewProgressSet())
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

func emptyTestAppendable(aggStep uint64) *Appendable {
	salt := uint32(1)
	logger := log.New()
	return &Appendable{cfg: AppendableCfg{Salt: &salt, DB: nil},
		logger:       logger,
		filenameBase: "test", aggregationStep: aggStep, dirtyFiles: btree2.NewBTreeG[*filesItem](filesItemLess)}
}

func TestAppendableScanStaticFiles(t *testing.T) {
	ii := emptyTestAppendable(1)
	files := []string{
		"v1-test.0-1.ap",
		"v1-test.1-2.ap",
		"v1-test.0-4.ap",
		"v1-test.2-3.ap",
		"v1-test.3-4.ap",
		"v1-test.4-5.ap",
	}
	ii.scanDirtyFiles(files)
	require.Equal(t, 6, ii.dirtyFiles.Len())

	//integrity extension case
	ii.dirtyFiles.Clear()
	ii.integrityCheck = func(fromStep, toStep uint64) bool { return false }
	ii.scanDirtyFiles(files)
	require.Equal(t, 0, ii.dirtyFiles.Len())
}

func TestAppendableCtxFiles(t *testing.T) {
	ii := emptyTestAppendable(1)
	files := []string{
		"v1-test.0-1.ap", // overlap with same `endTxNum=4`
		"v1-test.1-2.ap",
		"v1-test.0-4.ap",
		"v1-test.2-3.ap",
		"v1-test.3-4.ap",
		"v1-test.4-5.ap",     // no overlap
		"v1-test.480-484.ap", // overlap with same `startTxNum=480`
		"v1-test.480-488.ap",
		"v1-test.480-496.ap",
		"v1-test.480-512.ap",
	}
	ii.scanDirtyFiles(files)
	require.Equal(t, 10, ii.dirtyFiles.Len())
	ii.dirtyFiles.Scan(func(item *filesItem) bool {
		fName := ii.apFilePath(item.startTxNum/ii.aggregationStep, item.endTxNum/ii.aggregationStep)
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
