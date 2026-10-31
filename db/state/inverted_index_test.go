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
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"math/rand/v2"
	"os"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/semaphore"

	"github.com/erigontech/erigon/db/kv/mdbx/mdbxtest"
	"github.com/erigontech/erigon/db/kv/prune"

	"github.com/erigontech/erigon/common/background"
	"github.com/erigontech/erigon/common/dir"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/config3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/mdbx"
	"github.com/erigontech/erigon/db/kv/order"
	"github.com/erigontech/erigon/db/kv/stream"
	"github.com/erigontech/erigon/db/kv/stream/streamtest"
	"github.com/erigontech/erigon/db/recsplit"
	"github.com/erigontech/erigon/db/recsplit/multiencseq"
	"github.com/erigontech/erigon/db/seg"
	"github.com/erigontech/erigon/db/state/statecfg"
	"github.com/erigontech/erigon/db/version"
)

func testDbAndInvertedIndex(tb testing.TB, aggStep uint64, logger log.Logger) (kv.RwDB, *InvertedIndex) {
	tb.Helper()
	dirs := datadir.New(tb.TempDir())
	keysTable := "Keys"
	indexTable := "Index"
	db := mdbxtest.InMem(tb, mdbx.New(dbcfg.ChainDB, logger), dirs.Chaindata).WithTableCfg(func(defaultBuckets kv.TableCfg) kv.TableCfg {
		return kv.TableCfg{
			keysTable:             kv.TableCfgItem{Flags: kv.DupSort},
			indexTable:            kv.TableCfgItem{Flags: kv.DupSort},
			kv.TblPruningProgress: kv.TableCfgItem{},
			kv.TblPruningValsProg: kv.TableCfgItem{},
		}
	}).MustOpen()
	salt := uint32(1)
	cfg := statecfg.InvIdxCfg{Enabled: true, FilenameBase: "inv", KeysTable: keysTable, ValuesTable: indexTable, FileVersion: statecfg.IIVersionTypes{DataEF: version.V1_0_standart, AccessorEFI: version.V1_0_standart}}
	cfg.Accessors = statecfg.AccessorHashMap
	ii, err := NewInvertedIndex(cfg, aggStep, config3.DefaultStepsInFrozenFile, dirs, logger)
	require.NoError(tb, err)
	tb.Cleanup(func() {
		ii.Close()
		db.Close()

		//if runtime.GOOS == "windows" {
		//	runtime.GC()
		//	debug.FreeOSMemory()
		//}
		//
		//accFile := filepath.Join(dirs.SnapAccessors, "v1.0-inv.0-1.efi")
		//err := os.Remove(accFile)
		//tb.Log("remove accessor file after Close:", err, accFile)
		//idxFile := filepath.Join(dirs.SnapIdx, "v1.0-inv.0-1.ef")
		//err = os.Remove(idxFile)
		//tb.Log("remove idx file after Close:", err, idxFile)
	})
	ii.salt.Store(&salt)
	ii.DisableFsync()
	return db, ii
}

func TestInvertedIndexVisibleEnd(t *testing.T) {
	db, ii := testDbAndInvertedIndex(t, 16, log.New())
	tx, err := db.BeginRw(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()

	iit := ii.beginForTests()
	defer iit.Close()
	require.Zero(t, iit.Progress(tx))
	require.Zero(t, iit.visibleEnd(tx))

	var txNum [8]byte
	require.NoError(t, tx.Put(ii.KeysTable, txNum[:], []byte{1}))
	require.Zero(t, iit.Progress(tx))
	require.Equal(t, uint64(1), iit.visibleEnd(tx))

	binary.BigEndian.PutUint64(txNum[:], 100)
	require.NoError(t, tx.Put(ii.KeysTable, txNum[:], []byte{1}))
	require.Equal(t, uint64(100), iit.Progress(tx))
	require.Equal(t, uint64(101), iit.visibleEnd(tx))

	iit.files = visibleFiles{{endTxNum: 200}}
	require.Equal(t, uint64(200), iit.Progress(tx))
	require.Equal(t, uint64(200), iit.visibleEnd(tx))
}

func TestInvIndexPruningCorrectness(t *testing.T) {
	t.Parallel()

	pruneLimit := uint64(10)
	pruneIters := 8

	setup := func(t *testing.T) (*InvertedIndex, kv.RwTx, *time.Ticker, [8]byte, [8]byte) {
		t.Helper()
		db, ii, _ := filledInvIndexOfSize(t, 1000, 16, 1, log.New())
		t.Cleanup(func() { ii.Close(); db.Close() })

		tx, err := db.BeginRw(t.Context())
		require.NoError(t, err)
		t.Cleanup(tx.Rollback)

		logEvery := time.NewTicker(30 * time.Second)
		t.Cleanup(logEvery.Stop)

		var from, to [8]byte
		binary.BigEndian.PutUint64(from[:], uint64(0))
		binary.BigEndian.PutUint64(to[:], uint64(pruneIters)*pruneLimit)
		return ii, tx, logEvery, from, to
	}

	t.Run("hash_prune", func(t *testing.T) {
		t.Parallel()
		ii, tx, logEvery, from, to := setup(t)

		t.Run("no_files_no_force", func(t *testing.T) {
			ic := ii.beginForTests()
			defer ic.Close()

			icc, err := tx.CursorDupSort(ii.KeysTable)
			require.NoError(t, err)
			defer icc.Close()

			count := 0
			for txn, _, err := icc.Seek(from[:]); txn != nil; txn, _, err = icc.Next() {
				require.NoError(t, err)
				if bytes.Compare(txn, to[:]) > 0 {
					break
				}
				count++
			}
			icc.Close()
			require.Equal(t, count, pruneIters*int(pruneLimit))

			// this one should not prune anything due to forced=false but no files built
			stat, err := ic.HashSeekingPrune(t.Context(), tx, 0, 10, pruneLimit, logEvery, false, nil, nil, prune.DefaultStorageMode)
			require.NoError(t, err)
			require.Zero(t, stat.PruneCountTx)
			require.Zero(t, stat.PruneCountValues)

			// this one should not prune anything as well due to given range [0,1) even it is forced
			stat, err = ic.HashSeekingPrune(t.Context(), tx, 0, 1, pruneLimit, logEvery, true, nil, nil, prune.DefaultStorageMode)
			require.NoError(t, err)
			require.Zero(t, stat.PruneCountTx)
			require.Zero(t, stat.PruneCountValues)

			ic.Close()
		})

		t.Run("retire_one_step_no_force", func(t *testing.T) {
			collation, err := ii.collate(t.Context(), 0, tx)
			require.NoError(t, err)
			sf, _ := ii.buildFiles(t.Context(), 0, collation, background.NewProgressSet())
			collation.Close()
			txFrom, txTo := firstTxNumOfStep(0, ii.stepSize), firstTxNumOfStep(1, ii.stepSize)

			// without `reCalcVisibleFiles` must be nothing to prune - because files are not visible yet.
			ic := ii.beginForTests()
			defer ic.Close()

			ii.integrateDirtyFiles(sf, txFrom, txTo)
			stat, err := ic.HashSeekingPrune(t.Context(), tx, 0, 10, pruneLimit, logEvery, false, nil, nil, prune.DefaultStorageMode)
			require.NoError(t, err)
			require.Zero(t, stat.PruneCountTx)
			require.Zero(t, stat.PruneCountValues)

			ic = ii.beginForTests()
			defer ic.Close()
			stat, err = ic.HashSeekingPrune(t.Context(), tx, 0, 10, pruneLimit, logEvery, false, nil, nil, prune.DefaultStorageMode)
			require.NoError(t, err)
			require.Equal(t, 9, int(stat.PruneCountTx))
			require.Equal(t, 9, int(stat.PruneCountValues))

			// prune only what left in step 0. Even if requested more. don't allow print more than what we have in visible files
			stat, err = ic.HashSeekingPrune(t.Context(), tx, 0, 20, pruneLimit, logEvery, false, nil, nil, prune.DefaultStorageMode)
			require.NoError(t, err)
			require.Equal(t, 6, int(stat.PruneCountTx))
			require.Equal(t, 6, int(stat.PruneCountValues))
		})

		t.Run("force", func(t *testing.T) {
			ic := ii.beginForTests()
			defer ic.Close()

			// this should prune exactly pruneLimit*pruneIter transactions
			for i := range pruneIters {
				stat, err := ic.HashSeekingPrune(t.Context(), tx, 0, 1000, pruneLimit, logEvery, true, nil, nil, prune.DefaultStorageMode)
				require.NoError(t, err)
				t.Logf("[%d] stats: %v", i, stat)
			}

			// ascending - empty
			it, err := ic.IdxRange(nil, 0, pruneIters*int(pruneLimit), order.Asc, -1, tx)
			require.NoError(t, err)
			require.False(t, it.HasNext())
			it.Close()

			// descending - empty
			it, err = ic.IdxRange(nil, pruneIters*int(pruneLimit), 0, order.Desc, -1, tx)
			require.NoError(t, err)
			require.False(t, it.HasNext())
			it.Close()

			// straight from pruned - not empty
			icc, err := tx.CursorDupSort(ii.KeysTable)
			require.NoError(t, err)
			defer icc.Close()
			txn, _, err := icc.Seek(from[:])
			require.NoError(t, err)
			println("from", binary.BigEndian.Uint64(from[:]), "txn", binary.BigEndian.Uint64(txn))

			prunedInStep0 := 16 - 1
			// we pruned by limit so next transaction after prune should be equal to `pruneIters*pruneLimit+1`
			// If we would prune by txnum then txTo prune should be available after prune is finished
			require.EqualValues(t, pruneIters*int(pruneLimit)+prunedInStep0, int(binary.BigEndian.Uint64(txn)-1))
			icc.Close()

			// check second table
			icc2, err := tx.CursorDupSort(ii.ValuesTable)
			require.NoError(t, err)
			defer icc2.Close()
			key, chunk, err := icc2.First()
			t.Logf("key: %x, chunk: %x", key, chunk)
			require.NoError(t, err)
			txNums, err := decodeIIChunk(nil, chunk)
			require.NoError(t, err)
			// we pruned by limit so next transaction after prune should be equal to `pruneIters*pruneLimit+1`
			// If we would prune by txnum then txTo prune should be available after prune is finished
			require.EqualValues(t, pruneIters*int(pruneLimit)+prunedInStep0, int(txNums[0]-1))
		})
	}) // hash_prune

	t.Run("scan_prune", func(t *testing.T) {
		t.Parallel()
		ii, tx, logEvery, from, to := setup(t)
		thr := time.Millisecond * 20
		ctx := context.WithValue(t.Context(), throttle, &thr)

		t.Run("no_files_no_force", func(t *testing.T) {
			ic := ii.beginForTests()
			defer ic.Close()

			icc, err := tx.CursorDupSort(ii.KeysTable)
			require.NoError(t, err)
			defer icc.Close()
			count := 0
			for txn, _, err := icc.Seek(from[:]); txn != nil; txn, _, err = icc.Next() {
				require.NoError(t, err)
				if bytes.Compare(txn, to[:]) > 0 {
					break
				}
				count++
			}

			require.Equal(t, count, pruneIters*int(pruneLimit))
			st := &prune.Stat{
				MinTxNum:         0,
				MaxTxNum:         0,
				PruneCountTx:     0,
				PruneCountValues: 0,
				DupsDeleted:      0,
				LastPrunedValue:  nil,
				LastPrunedKey:    nil,
				KeyProgress:      prune.Done,
				ValueProgress:    prune.Done,
				TxFrom:           0,
				TxTo:             10,
			}
			err = SavePruneValProgress(tx, ic.ii.ValuesTable, st)
			require.NoError(t, err)
			// this one should not prune anything due to forced=false but no files built
			stat, err := ic.TableScanningPrune(ctx, tx, 0, 10, pruneLimit, logEvery, false, nil, nil, mxPruneSizeIndex, prune.DefaultStorageMode)
			require.NoError(t, err)
			require.Zero(t, stat.PruneCountTx)
			require.Zero(t, stat.PruneCountValues)

			// this one should not prune anything as well due to given range [0,1) even it is forced
			stat, err = ic.TableScanningPrune(ctx, tx, 0, 1, pruneLimit, logEvery, true, nil, nil, mxPruneSizeIndex, prune.DefaultStorageMode)
			require.NoError(t, err)
			require.Zero(t, stat.PruneCountTx)
			require.Zero(t, stat.PruneCountValues)
		})

		t.Run("prune was in progress", func(t *testing.T) {
			collation, err := ii.collate(t.Context(), 0, tx)
			require.NoError(t, err)
			defer collation.Close()
			sf, _ := ii.buildFiles(t.Context(), 0, collation, background.NewProgressSet())
			defer sf.CleanupOnError()
			txFrom, txTo := firstTxNumOfStep(0, ii.stepSize), firstTxNumOfStep(1, ii.stepSize)

			// without `reCalcVisibleFiles` must be nothing to prune - because files are not visible yet.
			ic := ii.beginForTests()
			defer ic.Close()

			ii.integrateDirtyFiles(sf, txFrom, txTo)
			st := &prune.Stat{
				MinTxNum:         0,
				MaxTxNum:         0,
				PruneCountTx:     0,
				PruneCountValues: 0,
				DupsDeleted:      0,
				LastPrunedValue:  nil,
				LastPrunedKey:    nil,
				KeyProgress:      prune.Done,
				ValueProgress:    prune.InProgress,
				TxFrom:           0,
				TxTo:             10,
			}
			err = SavePruneValProgress(tx, ic.ii.ValuesTable, st)
			require.NoError(t, err)
			can := ic.CanPrune(tx, 10)
			require.True(t, can)
		})

		t.Run("prune was done", func(t *testing.T) {
			ii, tx, logEvery, _, _ := setup(t)

			collation, err := ii.collate(t.Context(), 0, tx)
			require.NoError(t, err)
			defer collation.Close()
			sf, _ := ii.buildFiles(t.Context(), 0, collation, background.NewProgressSet())
			defer sf.CleanupOnError()
			txFrom, txTo := firstTxNumOfStep(0, ii.stepSize), firstTxNumOfStep(1, ii.stepSize)

			// without `reCalcVisibleFiles` must be nothing to prune - because files are not visible yet.
			ic := ii.beginForTests()
			defer ic.Close()
			ii.integrateDirtyFiles(sf, txFrom, txTo)

			// Drain step 0 from the db so minTxNumInDB >= files.EndTxNum; otherwise
			// CanPrune's files-based clause would return true regardless of progress state.
			_, err = ic.TableScanningPrune(ctx, tx, txFrom, txTo, math.MaxUint64, logEvery, true, nil, nil, mxPruneSizeIndex, prune.DefaultStorageMode)
			require.NoError(t, err)

			st := &prune.Stat{
				KeyProgress:   prune.Done,
				ValueProgress: prune.Done,
				TxFrom:        0,
				TxTo:          10,
			}
			err = SavePruneValProgress(tx, ic.ii.ValuesTable, st)
			require.NoError(t, err)
			require.False(t, ic.CanPrune(tx, 10))
		})

		t.Run("retire_one_step_no_force", func(t *testing.T) {
			ii, tx, logEvery, _, _ := setup(t)
			collation, err := ii.collate(t.Context(), 0, tx)
			require.NoError(t, err)
			sf, _ := ii.buildFiles(t.Context(), 0, collation, background.NewProgressSet())
			collation.Close()
			txFrom, txTo := firstTxNumOfStep(0, ii.stepSize), firstTxNumOfStep(1, ii.stepSize)
			ii.integrateDirtyFiles(sf, txFrom, txTo)

			ic := ii.beginForTests()
			defer ic.Close()
			stat, err := ic.TableScanningPrune(t.Context(), tx, 0, 10, pruneLimit, logEvery, false, nil, nil, mxPruneSizeIndex, prune.DefaultStorageMode)
			require.NoError(t, err)
			require.Equal(t, 9, int(stat.PruneCountTx))
			require.Equal(t, 9, int(stat.PruneCountValues))

			// even if requested more, don't prune beyond what visible files cover (step 0 ends at stepSize).
			stat, err = ic.TableScanningPrune(t.Context(), tx, 0, 20, pruneLimit, logEvery, false, nil, nil, mxPruneSizeIndex, prune.DefaultStorageMode)
			require.NoError(t, err)
			require.Equal(t, 6, int(stat.PruneCountTx))
			require.Equal(t, 6, int(stat.PruneCountValues))
		})

		t.Run("force", func(t *testing.T) {
			ii, tx, logEvery, from, _ := setup(t)
			ic := ii.beginForTests()
			defer ic.Close()

			// Unlike HashSeekingPrune, TableScanningPrune does not honor `limit` for the
			// key range: a single forced call prunes the whole [txFrom, txTo) range.
			pruneTo := uint64(pruneIters) * pruneLimit
			stat, err := ic.TableScanningPrune(t.Context(), tx, 0, pruneTo, pruneLimit, logEvery, true, nil, nil, mxPruneSizeIndex, prune.DefaultStorageMode)
			require.NoError(t, err)
			t.Logf("pruned tx=%d vals=%d", stat.PruneCountTx, stat.PruneCountValues)

			// [0, pruneTo) fully pruned
			it, err := ic.IdxRange(nil, 0, int(pruneTo), order.Asc, -1, tx)
			require.NoError(t, err)
			require.False(t, it.HasNext())
			it.Close()

			it, err = ic.IdxRange(nil, int(pruneTo), 0, order.Desc, -1, tx)
			require.NoError(t, err)
			require.False(t, it.HasNext())
			it.Close()

			// first surviving txn in the keys table == pruneTo
			icc, err := tx.CursorDupSort(ii.KeysTable)
			require.NoError(t, err)
			defer icc.Close()
			txn, _, err := icc.Seek(from[:])
			require.NoError(t, err)
			require.EqualValues(t, pruneTo, binary.BigEndian.Uint64(txn))
		})
	}) // scan_prune
}

type throttleType string

const throttle throttleType = "throttle"

func TestInvIndexCollationBuild(t *testing.T) {
	t.Parallel()

	logger := log.New()
	logEvery := time.NewTicker(30 * time.Second)
	defer logEvery.Stop()
	db, ii := testDbAndInvertedIndex(t, 16, logger)
	ctx := t.Context()
	tx, err := db.BeginRw(ctx)
	require.NoError(t, err)
	defer tx.Rollback()
	ic := ii.beginForTests()
	defer ic.Close()
	writer := ic.NewWriter(db)
	defer writer.close()

	err = writer.Add([]byte("key1"), 2)
	require.NoError(t, err)

	err = writer.Add([]byte("key2"), 3)
	require.NoError(t, err)

	err = writer.Add([]byte("key1"), 6)
	require.NoError(t, err)
	err = writer.Add([]byte("key3"), 6)
	require.NoError(t, err)

	err = writer.Add([]byte("key10"), 17)
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
	bs.Close()
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
		ef := multiencseq.ReadMultiEncSeq(0, w)
		var ints []uint64
		it := ef.Iterator(0)
		for it.HasNext() {
			v, _ := it.Next()
			ints = append(ints, v)
		}
		intArrs = append(intArrs, ints)
	}
	require.Equal(t, []string{"key1", "key2", "key3"}, words)
	require.Equal(t, [][]uint64{{2, 6}, {3}, {6}}, intArrs)
	r := recsplit.NewIndexReader(sf.index)
	defer r.Close()
	for i := 0; i < len(words); i++ {
		offset, _ := r.TwoLayerLookup([]byte(words[i]))
		g.Reset(offset)
		w, _ := g.Next(nil)
		require.Equal(t, words[i], string(w))
	}
}

func TestInvIndexAfterPrune(t *testing.T) {
	t.Parallel()
	logEvery := time.NewTicker(30 * time.Second)
	defer logEvery.Stop()
	db, ii := testDbAndInvertedIndex(t, 16, log.New())
	ctx := t.Context()
	tx, err := db.BeginRw(ctx) //nolint:gocritic
	require.NoError(t, err)
	defer func() {
		if tx != nil {
			tx.Rollback()
		}
	}()
	ic := ii.beginForTests()
	defer ic.Close()
	writer := ic.NewWriter(db)
	defer writer.close()

	err = writer.Add([]byte("key1"), 2)
	require.NoError(t, err)

	err = writer.Add([]byte("key2"), 3)
	require.NoError(t, err)

	err = writer.Add([]byte("key1"), 6)
	require.NoError(t, err)
	err = writer.Add([]byte("key3"), 6)
	require.NoError(t, err)

	err = writer.Flush(ctx, tx)
	require.NoError(t, err)
	err = tx.Commit()
	require.NoError(t, err)

	roTx, err := db.BeginRo(ctx)
	require.NoError(t, err)
	defer roTx.Rollback()

	require.NoError(t, ii.collateBuildIntegrate(ctx, 0, roTx, background.NewProgressSet()))

	err = db.Update(ctx, func(tx kv.RwTx) error {
		from, to := ic.stepsRangeInDB(tx)
		require.Equal(t, "0.1", fmt.Sprintf("%.1f", from))
		require.Equal(t, "0.4", fmt.Sprintf("%.1f", to))

		ic = ii.beginForTests()
		defer ic.Close()

		_, err = ic.TableScanningPrune(ctx, tx, 0, 16, math.MaxUint64, logEvery, false, nil, nil, mxPruneSizeIndex, prune.DefaultStorageMode)
		require.NoError(t, err)
		return nil
	})
	require.NoError(t, err)

	require.NoError(t, err)
	tx, err = db.BeginRw(ctx)
	require.NoError(t, err)
	defer tx.Rollback()

	for _, table := range []string{ii.KeysTable, ii.ValuesTable} {
		func() {
			cur, err := tx.Cursor(table)
			require.NoError(t, err)
			defer cur.Close()
			k, _, err := cur.First()
			require.NoError(t, err)
			require.Nil(t, k, table)
		}()
	}

	from, to := ic.stepsRangeInDB(tx)
	require.Equal(t, float64(0), from) //nolint:testifylint
	require.Equal(t, float64(0), to)   //nolint:testifylint
}

// TestInvIndex_PruneRollingCursorProgress verifies the three properties of the
// rolling-cursor optimisation added to inverted_index.tableScanningPrune:
//
//  1. Early-exit: prg.TxTo >= txTo && KeyProgress==Done && ValueProgress==Done
//     → return immediately, no deletions.
//  2. Rotation reset: after Done a call with larger txTo resets cursor to First
//     and picks up newly pruneable entries.
//  3. Cursor preservation: an InProgress cursor survives a txTo advance —
//     the subsequent call resumes from LastPrunedValue rather than First.
func TestInvIndex_PruneRollingCursorProgress(t *testing.T) {
	t.Parallel()

	const (
		aggStep  = uint64(16)
		keyCount = 40 // unique keys written to the index
	)
	ctx := t.Context()
	db, ii := testDbAndInvertedIndex(t, aggStep, log.New())

	rwTx, err := db.BeginRw(ctx)
	require.NoError(t, err)
	defer rwTx.Rollback()

	// writeKeys adds keyCount entries to the index at the given txNum.
	// forced=true in TableScanningPrune bypasses the CanPrune file check.
	writeKeys := func(txNum uint64) {
		t.Helper()
		ic := ii.beginForTests()
		defer ic.Close()
		w := ic.NewWriter(db)
		defer w.close()
		for k := range uint64(keyCount) {
			var key [8]byte
			binary.BigEndian.PutUint64(key[:], k)
			require.NoError(t, w.Add(key[:], txNum))
		}
		require.NoError(t, w.Flush(ctx, rwTx))
	}

	logEvery := time.NewTicker(30 * time.Second)
	defer logEvery.Stop()

	// callPrune calls TableScanningPrune in table-scanning mode (txTo != MaxUint64,
	// forced=true so no file-presence check is required).
	callPrune := func(txFrom, txTo uint64) *InvertedIndexPruneStat {
		t.Helper()
		ic := ii.beginForTests()
		defer ic.Close()
		stat, err := ic.TableScanningPrune(ctx, rwTx, txFrom, txTo, math.MaxUint64, logEvery,
			true /*forced*/, nil, nil, nil, prune.DefaultStorageMode)
		require.NoError(t, err)
		return stat
	}

	// ── Write step-0 and step-1 data (txNums 1 and aggStep+1). ────────────
	writeKeys(1)           // step 0
	writeKeys(aggStep + 1) // step 1

	// ── Property 1: early-exit ─────────────────────────────────────────────
	// Full prune of step 0 (txTo = aggStep).
	stat0 := callPrune(0, aggStep)
	require.Equal(t, prune.Done, stat0.Progress)
	require.NotZero(t, stat0.PruneCountValues+stat0.PruneCountTx, "step-0 entries must be deleted")

	prg, err := GetPruneValProgress(rwTx, []byte(ii.ValuesTable))
	require.NoError(t, err)
	require.Equal(t, prune.Done, prg.ValueProgress)
	require.Equal(t, prune.Done, prg.KeyProgress)
	require.EqualValues(t, aggStep, prg.TxTo)

	// Repeat with same txTo → early-exit, no work.
	statEarly := callPrune(0, aggStep)
	require.Equal(t, prune.Done, statEarly.Progress)
	require.Zero(t, statEarly.PruneCountValues+statEarly.PruneCountTx,
		"early-exit must delete nothing")

	// ── Property 2: rotation reset when txTo advances ──────────────────────
	stat1 := callPrune(aggStep, aggStep*2)
	require.Equal(t, prune.Done, stat1.Progress)
	require.NotZero(t, stat1.PruneCountValues+stat1.PruneCountTx,
		"step-1 entries must be deleted in the new rotation")

	prg1, err := GetPruneValProgress(rwTx, []byte(ii.ValuesTable))
	require.NoError(t, err)
	require.Equal(t, prune.Done, prg1.ValueProgress)
	require.Nil(t, prg1.LastPrunedValue, "cursor must be nil after rotation completion")

	// ── Property 3: cursor preservation when txTo advances mid-rotation ────
	writeKeys(aggStep*2 + 1) // step 2

	// Inject InProgress at the midpoint key so the prune resumes from there.
	midKey := make([]byte, 8)
	binary.BigEndian.PutUint64(midKey, keyCount/2)
	txFrom2, txTo2 := aggStep*2, aggStep*3
	injected := &prune.Stat{
		ValueProgress:   prune.InProgress,
		KeyProgress:     prune.Done, // keys pass already done
		LastPrunedValue: midKey,
		TxFrom:          txFrom2,
		TxTo:            txTo2,
	}
	require.NoError(t, SavePruneValProgress(rwTx, ii.ValuesTable, injected))

	stat2 := callPrune(txFrom2, txTo2)
	require.Equal(t, prune.Done, stat2.Progress)
	// Cursor started at midKey so only the second half of entries could be pruned.
	require.LessOrEqual(t, stat2.PruneCountValues, uint64(keyCount/2),
		"cursor preserved: only entries at/after midKey should be deleted")
}

func filledInvIndex(tb testing.TB, logger log.Logger) (kv.RwDB, *InvertedIndex, uint64) {
	tb.Helper()
	return filledInvIndexOfSize(tb, uint64(1000), 16, 31, logger)
}

// Creates InvertedIndex instance and fills it with generated data.
// Txs - amount of transactions to generate
// AggStep - aggregation step for InvertedIndex
// Module - amount of keys to generate
func filledInvIndexOfSize(tb testing.TB, txs, aggStep, module uint64, logger log.Logger) (kv.RwDB, *InvertedIndex, uint64) {
	tb.Helper()
	db, ii := testDbAndInvertedIndex(tb, aggStep, logger)
	ctx, require := tb.Context(), require.New(tb)

	err := db.Update(ctx, func(tx kv.RwTx) error {
		ic := ii.beginForTests()
		defer ic.Close()
		writer := ic.NewWriter(db)
		defer writer.close()

		var flusher flusher

		// keys are encodings of numbers 1..31
		// each key changes value on every txNum which is multiple of the key
		for txNum := uint64(1); txNum <= txs; txNum++ {
			for keyNum := uint64(1); keyNum <= module; keyNum++ {
				if txNum%keyNum == 0 {
					var k [8]byte
					binary.BigEndian.PutUint64(k[:], keyNum)
					err := writer.Add(k[:], txNum)
					require.NoError(err)
				}
			}
			if flusher != nil {
				require.NoError(flusher.Flush(ctx, tx))
			}
			if txNum%10 == 0 {
				flusher = writer
				writer = ic.NewWriter(db)
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
	ctx := t.Context()
	ic := ii.beginForTests()
	defer ic.Close()

	// Check the iterator invertedIndex first without roTx
	for keyNum := uint64(1); keyNum <= uint64(31); keyNum++ {
		var k [8]byte
		binary.BigEndian.PutUint64(k[:], keyNum)
		var values []uint64
		t.Run("asc", func(t *testing.T) {
			it, err := ic.IdxRange(k[:], 0, 976, order.Asc, -1, nil)
			require.NoError(t, err)
			defer it.Close()
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
			defer reverseStream.Close()
			streamtest.ExpectEqualU64(t, stream.ReverseArray(values), reverseStream)
		})
		t.Run("unbounded asc", func(t *testing.T) {
			forwardLimited, err := ic.IdxRange(k[:], -1, 976, order.Asc, 2, nil)
			require.NoError(t, err)
			defer forwardLimited.Close()
			streamtest.ExpectEqualU64(t, stream.Array(values[:2]), forwardLimited)
		})
		t.Run("unbounded desc", func(t *testing.T) {
			reverseLimited, err := ic.IdxRange(k[:], 976-1, -1, order.Desc, 2, nil)
			require.NoError(t, err)
			defer reverseLimited.Close()
			streamtest.ExpectEqualU64(t, stream.ReverseArray(values[len(values)-2:]), reverseLimited)
		})
		t.Run("tiny bound asc", func(t *testing.T) {
			it, err := ic.IdxRange(k[:], 100, 102, order.Asc, -1, nil)
			require.NoError(t, err)
			defer it.Close()
			expect := stream.FilterU64(stream.Array(values), func(k uint64) bool { return k >= 100 && k < 102 })
			streamtest.ExpectEqualU64(t, expect, it)
		})
		t.Run("tiny bound desc", func(t *testing.T) {
			it, err := ic.IdxRange(k[:], 102, 100, order.Desc, -1, nil)
			require.NoError(t, err)
			defer it.Close()
			expect := stream.FilterU64(stream.ReverseArray(values), func(k uint64) bool { return k <= 102 && k > 100 })
			streamtest.ExpectEqualU64(t, expect, it)
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
		it.Close()

		reverseStream, err := ic.IdxRange(k[:], 1000-1, 400-1, false, -1, roTx)
		require.NoError(t, err)
		arr := stream.ToArrU64Must(reverseStream)
		expect := stream.ToArrU64Must(stream.ReverseArray(values))
		require.Equal(t, expect, arr)
		reverseStream.Close()
	}
}

// collateBuildIntegrate collates, builds files and integrates them for the given step.
// It is a test helper that combines the common collate→buildFiles→integrateDirtyFiles pattern.
func (ii *InvertedIndex) collateBuildIntegrate(ctx context.Context, step kv.Step, tx kv.Tx, ps *background.ProgressSet) error {
	bs, err := ii.collate(ctx, step, tx)
	if err != nil {
		return err
	}
	defer bs.Close()
	sf, err := ii.buildFiles(ctx, step, bs, ps)
	if err != nil {
		return err
	}
	ii.integrateDirtyFiles(sf, step.ToTxNum(ii.stepSize), (step + 1).ToTxNum(ii.stepSize))
	return nil
}

func mergeInverted(tb testing.TB, db kv.RwDB, ii *InvertedIndex, txs uint64) {
	tb.Helper()
	logEvery := time.NewTicker(30 * time.Second)
	defer logEvery.Stop()
	ctx := tb.Context()
	// Leave the last 2 aggregation steps un-collated
	tx, err := db.BeginRw(ctx)
	require.NoError(tb, err)
	defer tx.Rollback()

	// Leave the last 2 aggregation steps un-collated
	for step := kv.Step(0); step < kv.Step(txs/ii.stepSize)-1; step++ {
		func() {
			require.NoError(tb, ii.collateBuildIntegrate(ctx, step, tx, background.NewProgressSet()))
			ic := ii.beginForTests()
			defer ic.Close()
			_, err = ic.TableScanningPrune(ctx, tx, step.ToTxNum(ii.stepSize), (step + 1).ToTxNum(ii.stepSize), math.MaxUint64, logEvery, false, nil, nil, mxPruneSizeIndex, prune.DefaultStorageMode)
			require.NoError(tb, err)
			var found bool
			var startTxNum, endTxNum uint64
			maxEndTxNum := ii.dirtyFilesEndTxNumMinimax()
			maxSpan := ii.stepSize * config3.DefaultStepsInFrozenFile

			for {
				if stop := func() bool {
					ic := ii.beginForTests()
					defer ic.Close()
					mr := ic.findMergeRange(maxEndTxNum, maxSpan)
					found, startTxNum, endTxNum = mr.needMerge, mr.from, mr.to
					if !found {
						return true
					}
					outs := ic.staticFilesInRange(startTxNum, endTxNum)
					in, err := ic.mergeFiles(ctx, outs, startTxNum, endTxNum, background.NewProgressSet())
					require.NoError(tb, err)
					ii.integrateMergedDirtyFiles(in)
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
	if testing.Short() {
		t.Skip()
	}

	t.Parallel()

	logger := log.New()
	logEvery := time.NewTicker(30 * time.Second)
	defer logEvery.Stop()
	db, ii, txs := filledInvIndex(t, logger)
	ctx := t.Context()
	tx, err := db.BeginRw(ctx)
	require.NoError(t, err)
	defer tx.Rollback()

	// Leave the last 2 aggregation steps un-collated
	for step := kv.Step(0); step < kv.Step(txs/ii.stepSize)-1; step++ {
		func() {
			require.NoError(t, ii.collateBuildIntegrate(ctx, step, tx, background.NewProgressSet()))
			ic := ii.beginForTests()
			defer ic.Close()
			_, err = ic.TableScanningPrune(ctx, tx, step.ToTxNum(ii.stepSize), (step + 1).ToTxNum(ii.stepSize), math.MaxUint64, logEvery, false, nil, nil, mxPruneSizeIndex, prune.DefaultStorageMode)
			require.NoError(t, err)
		}()
	}
	err = tx.Commit()
	require.NoError(t, err)

	checkRanges(t, db, ii, txs)
}

func TestInvIndexMerge(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	logger := log.New()
	db, ii, txs := filledInvIndex(t, logger)

	mergeInverted(t, db, ii, txs)
	checkRanges(t, db, ii, txs)
}

func TestInvIndexScanFiles(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	logger, require := log.New(), require.New(t)
	db, ii, txs := filledInvIndex(t, logger)

	// Recreate InvertedIndex to scan the files
	salt := uint32(1)
	cfg := ii.InvIdxCfg

	var err error
	ii, err = NewInvertedIndex(cfg, 16, config3.DefaultStepsInFrozenFile, ii.dirs, logger)
	require.NoError(err)
	defer ii.Close()
	ii.salt.Store(&salt)

	scanDirsRes, err := scanDirs(ii.dirs)
	require.NoError(err)
	_, err = ii.openFolder(t.Context(), scanDirsRes)
	require.NoError(err)

	mergeInverted(t, db, ii, txs)
	checkRanges(t, db, ii, txs)
}

func TestScanStaticFiles(t *testing.T) {
	t.Parallel()

	ii := emptyTestInvertedIndex(t, 1)
	files := []string{
		"v1.0-accounts.0-1.ef",
		"v1.0-accounts.1-2.ef",
		"v1.0-accounts.0-4.ef",
		"v1.0-accounts.2-3.ef",
		"v1.0-accounts.3-4.ef",
		"v1.0-accounts.4-5.ef",
	}
	ii.scanDirtyFiles(files)
	require.Equal(t, 6, ii.dirtyFiles.Len())
	ic := ii.beginForTests()
	require.Equal(t, 0, len(ic.files))
	ic.Close()
}

func TestCtxFiles(t *testing.T) {
	ii := emptyTestInvertedIndex(t, 1)
	files := []string{
		"v1.0-accounts.0-1.ef", // overlap with same `endTxNum=4`
		"v1.0-accounts.1-2.ef",
		"v1.0-accounts.0-4.ef",
		"v1.0-accounts.2-3.ef",
		"v1.0-accounts.3-4.ef",
		"v1.0-accounts.4-5.ef",     // no overlap
		"v1.0-accounts.480-484.ef", // overlap with same `startTxNum=480`
		"v1.0-accounts.480-488.ef",
		"v1.0-accounts.480-496.ef",
		"v1.0-accounts.480-512.ef",
	}
	ii.scanDirtyFiles(files)
	require.Equal(t, 10, ii.dirtyFiles.Len())
	ii.dirtyFiles.Scan(func(item *FilesItem) bool {
		item.decompressor = &seg.Decompressor{}
		return true
	})

	visibleFiles := calcVisibleFiles(ii.dirtyFiles, 0, nil, false, ii.dirtyFilesEndTxNumMinimax())
	for i, item := range visibleFiles {
		if item.src.canDelete.Load() {
			require.Failf(t, "deleted file", "%d-%d", item.startTxNum, item.endTxNum)
		}
		if i == 0 {
			continue
		}
		if item.src.isProperSubsetOf(visibleFiles[i-1].src) || visibleFiles[i-1].src.isProperSubsetOf(item.src) {
			require.Failf(t, "overlaping files", "%d-%d, %d-%d", item.startTxNum, item.endTxNum, visibleFiles[i-1].startTxNum, visibleFiles[i-1].endTxNum)
		}
	}
	require.Len(t, visibleFiles, 3)

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
	assert.True((&FilesItem{startTxNum: 0, endTxNum: 1}).isProperSubsetOf(&FilesItem{startTxNum: 0, endTxNum: 2}))
	assert.True((&FilesItem{startTxNum: 1, endTxNum: 2}).isProperSubsetOf(&FilesItem{startTxNum: 0, endTxNum: 2}))
	assert.False((&FilesItem{startTxNum: 0, endTxNum: 2}).isProperSubsetOf(&FilesItem{startTxNum: 0, endTxNum: 2}))
	assert.False((&FilesItem{startTxNum: 0, endTxNum: 3}).isProperSubsetOf(&FilesItem{startTxNum: 0, endTxNum: 2}))
	assert.False((&FilesItem{startTxNum: 2, endTxNum: 3}).isProperSubsetOf(&FilesItem{startTxNum: 0, endTxNum: 2}))
	assert.False((&FilesItem{startTxNum: 0, endTxNum: 1}).isProperSubsetOf(&FilesItem{startTxNum: 1, endTxNum: 2}))
	assert.False((&FilesItem{startTxNum: 0, endTxNum: 2}).isProperSubsetOf(&FilesItem{startTxNum: 1, endTxNum: 2}))
}

func TestIsBefore(t *testing.T) {
	t.Parallel()

	assert := assert.New(t)
	assert.False((&FilesItem{startTxNum: 0, endTxNum: 1}).isBefore(&FilesItem{startTxNum: 0, endTxNum: 2}))
	assert.False((&FilesItem{startTxNum: 1, endTxNum: 2}).isBefore(&FilesItem{startTxNum: 0, endTxNum: 2}))
	assert.False((&FilesItem{startTxNum: 0, endTxNum: 2}).isBefore(&FilesItem{startTxNum: 0, endTxNum: 2}))
	assert.False((&FilesItem{startTxNum: 0, endTxNum: 3}).isBefore(&FilesItem{startTxNum: 0, endTxNum: 2}))
	assert.False((&FilesItem{startTxNum: 2, endTxNum: 3}).isBefore(&FilesItem{startTxNum: 0, endTxNum: 2}))
	assert.True((&FilesItem{startTxNum: 0, endTxNum: 1}).isBefore(&FilesItem{startTxNum: 1, endTxNum: 2}))
	assert.False((&FilesItem{startTxNum: 0, endTxNum: 2}).isBefore(&FilesItem{startTxNum: 1, endTxNum: 2}))
	assert.True((&FilesItem{startTxNum: 0, endTxNum: 1}).isBefore(&FilesItem{startTxNum: 2, endTxNum: 4}))
	assert.True((&FilesItem{startTxNum: 0, endTxNum: 2}).isBefore(&FilesItem{startTxNum: 2, endTxNum: 4}))
}

func TestInvIndex_OpenFolder(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	t.Parallel()

	db, ii, txs := filledInvIndex(t, log.New())

	mergeInverted(t, db, ii, txs)

	ic := ii.beginForTests()
	list := ic.files
	require.NotEmpty(t, list)
	ff := list[len(list)-1]
	fn := ff.src.decompressor.FilePath()
	ic.Close()
	ii.Close()

	err := dir.RemoveFile(fn)
	require.NoError(t, err)
	err = os.WriteFile(fn, make([]byte, 33), 0o644)
	require.NoError(t, err)

	scanDirsRes, err := scanDirs(ii.dirs)
	require.NoError(t, err)
	_, err = ii.openFolder(t.Context(), scanDirsRes)
	require.NoError(t, err)
	ii.Close()
}

// TestInvertedIndex_IdxRange_SkipsFileRange verifies that recentIterateRange narrows
// its DB query to exclude txNums already covered by frozen files.
// Without the fix the DB query would read prunable data in the file range,
// and after that data is pruned the results would be incomplete.
func TestInvertedIndex_IdxRange_SkipsFileRange(t *testing.T) {
	t.Parallel()
	logger := log.New()
	logEvery := time.NewTicker(30 * time.Second)
	defer logEvery.Stop()

	aggStep := uint64(16)
	db, ii := testDbAndInvertedIndex(t, aggStep, logger)
	ctx := t.Context()
	require := require.New(t)

	// Write entries for a single key across 3 steps (txNums 1..47).
	// Step 0: txNums 0-15, Step 1: 16-31, Step 2: 32-47
	var key [8]byte
	binary.BigEndian.PutUint64(key[:], 1)

	txNums := []uint64{2, 5, 10, 18, 25, 30, 35, 42}

	err := db.Update(ctx, func(tx kv.RwTx) error {
		ic := ii.beginForTests()
		defer ic.Close()
		writer := ic.NewWriter(db)
		defer writer.close()
		for _, txNum := range txNums {
			if err := writer.Add(key[:], txNum); err != nil {
				return err
			}
		}
		return writer.Flush(ctx, tx)
	})
	require.NoError(err)

	// Build files for steps 0 and 1 (covers txNums 0..31).
	tx, err := db.BeginRw(ctx)
	require.NoError(err)
	defer tx.Rollback()

	for step := kv.Step(0); step <= 1; step++ {
		require.NoError(ii.collateBuildIntegrate(ctx, step, tx, background.NewProgressSet()))
	}

	// Prune DB entries that are now covered by files (txNums 0..31).
	ic := ii.beginForTests()
	_, err = ic.TableScanningPrune(ctx, tx, 0, aggStep*2, math.MaxUint64, logEvery, false, nil, nil, mxPruneSizeIndex, prune.DefaultStorageMode)
	require.NoError(err)
	ic.Close()

	require.NoError(tx.Commit())

	// Now query IdxRange spanning both file and DB ranges.
	roTx, err := db.BeginRo(ctx)
	require.NoError(err)
	defer roTx.Rollback()

	ic = ii.beginForTests()
	defer ic.Close()

	// Ascending: full range [0, 48) should return all txNums.
	it, err := ic.IdxRange(key[:], 0, 48, order.Asc, -1, roTx)
	require.NoError(err)
	var got []uint64
	for it.HasNext() {
		v, err := it.Next()
		require.NoError(err)
		got = append(got, v)
	}
	it.Close()
	require.Equal(txNums, got, "ascending IdxRange after prune")

	// Descending: full range (48, 0] should return all txNums reversed.
	it, err = ic.IdxRange(key[:], 48, 0, order.Desc, -1, roTx)
	require.NoError(err)
	var gotDesc []uint64
	for it.HasNext() {
		v, err := it.Next()
		require.NoError(err)
		gotDesc = append(gotDesc, v)
	}
	it.Close()
	wantDesc := make([]uint64, len(txNums))
	copy(wantDesc, txNums)
	for i, j := 0, len(wantDesc)-1; i < j; i, j = i+1, j-1 {
		wantDesc[i], wantDesc[j] = wantDesc[j], wantDesc[i]
	}
	require.Equal(wantDesc, gotDesc, "descending IdxRange after prune")
}

// TestInvertedIndex_IdxRange_IgnoresDBInFileRange proves the DB is not consulted
// for txNums within the file range. It inserts a rogue txNum directly into the DB's
// ValuesTable inside the frozen range; IdxRange must NOT return it.
func TestInvertedIndex_IdxRange_IgnoresDBInFileRange(t *testing.T) {
	t.Parallel()
	logger := log.New()

	aggStep := uint64(16)
	db, ii := testDbAndInvertedIndex(t, aggStep, logger)
	ctx := t.Context()
	require := require.New(t)

	var key [8]byte
	binary.BigEndian.PutUint64(key[:], 1)

	txNums := []uint64{2, 5, 10, 18, 25, 30, 35, 42}

	err := db.Update(ctx, func(tx kv.RwTx) error {
		ic := ii.beginForTests()
		defer ic.Close()
		writer := ic.NewWriter(db)
		defer writer.close()
		for _, txNum := range txNums {
			if err := writer.Add(key[:], txNum); err != nil {
				return err
			}
		}
		return writer.Flush(ctx, tx)
	})
	require.NoError(err)

	// Build files for steps 0 and 1 (covers txNums 0..31).
	tx, err := db.BeginRw(ctx)
	require.NoError(err)
	defer tx.Rollback()

	for step := kv.Step(0); step <= 1; step++ {
		require.NoError(ii.collateBuildIntegrate(ctx, step, tx, background.NewProgressSet()))
	}

	// Inject a rogue txNum (7) into the DB's ValuesTable within the frozen range.
	// If the DB is queried for the file range, this would appear in results.
	var rogueTxNum [8]byte
	binary.BigEndian.PutUint64(rogueTxNum[:], 7)
	require.NoError(tx.Put(ii.ValuesTable, key[:], rogueTxNum[:]))
	require.NoError(tx.Commit())

	roTx, err := db.BeginRo(ctx)
	require.NoError(err)
	defer roTx.Rollback()

	ic := ii.beginForTests()
	defer ic.Close()

	// Ascending: rogue txNum 7 must not appear.
	it, err := ic.IdxRange(key[:], 0, 48, order.Asc, -1, roTx)
	require.NoError(err)
	var got []uint64
	for it.HasNext() {
		v, err := it.Next()
		require.NoError(err)
		got = append(got, v)
	}
	it.Close()
	require.Equal(txNums, got, "ascending: rogue DB entry in file range must be invisible")

	// Descending: rogue txNum 7 must not appear.
	it, err = ic.IdxRange(key[:], 48, 0, order.Desc, -1, roTx)
	require.NoError(err)
	var gotDesc []uint64
	for it.HasNext() {
		v, err := it.Next()
		require.NoError(err)
		gotDesc = append(gotDesc, v)
	}
	it.Close()
	wantDesc := make([]uint64, len(txNums))
	copy(wantDesc, txNums)
	for i, j := 0, len(wantDesc)-1; i < j; i, j = i+1, j-1 {
		wantDesc[i], wantDesc[j] = wantDesc[j], wantDesc[i]
	}
	require.Equal(wantDesc, gotDesc, "descending: rogue DB entry in file range must be invisible")
}

// TestInvertedIndexDisabledDiscardsWrites pins that Enabled=false is a real
// master switch: a writer taken from a disabled index must drop everything
// instead of quietly collecting and flushing into the DB tables.
func TestInvertedIndexDisabledDiscardsWrites(t *testing.T) {
	logger := log.New()
	db, ii := testDbAndInvertedIndex(t, 16, logger)
	ii.Enabled = false
	ctx := t.Context()

	tx, err := db.BeginRw(ctx)
	require.NoError(t, err)
	defer tx.Rollback()

	ic := ii.beginForTests()
	defer ic.Close()
	w := ic.NewWriter(db)
	defer w.close()

	require.NoError(t, w.Add([]byte("key1"), 2))
	require.NoError(t, w.Flush(ctx, tx))

	for _, table := range []string{ii.KeysTable, ii.ValuesTable} {
		n, err := tx.Count(table)
		require.NoError(t, err)
		require.Zerof(t, n, "table %s must stay empty", table)
	}
}

// A cached miss is stored as found=0. txNum=0 also satisfies txNum<=found, so
// testing the hit first hands the caller an all-zero seek result - which a
// positional value index resolves to some other key's value.
func TestSeekInFilesCacheMissAtTxNumZero(t *testing.T) {
	if testing.Short() {
		t.Skip("long-running test")
	}
	t.Parallel()

	db, h, txs := filledHistory(t, false, log.New())
	collateAndMergeHistory(t, db, h, txs, true)

	tx, err := db.BeginRo(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()
	hc := h.beginForTests()
	defer hc.Close()

	absent := []byte("no-such-key-at-all")
	v1, ok1, err := hc.HistorySeek(absent, 0, tx)
	require.NoError(t, err)
	v2, ok2, err := hc.HistorySeek(absent, 0, tx) // served from the seek cache
	require.NoError(t, err)
	require.Equal(t, ok1, ok2)
	require.Equal(t, v1, v2)
}

type prefetchIndexTx struct {
	kv.RwTx
	table  string
	cursor *prefetchIndexCursor
}

func (tx *prefetchIndexTx) RwCursorDupSort(table string) (kv.RwCursorDupSort, error) {
	if table != tx.table {
		return tx.RwTx.RwCursorDupSort(table)
	}
	return tx.cursor, nil
}

type prefetchIndexCursor struct {
	kv.RwCursorDupSort
	t             *testing.T
	prefetchErr   error
	prefetched    sync.Map // keys whose pages a prefetch reader touched
	prefetchSeeks atomic.Int32
	prefetchCalls atomic.Int32
	rollbacks     atomic.Int32
}

func (c *prefetchIndexCursor) Put(k, v []byte) error {
	c.requirePrefetched(k)
	return c.RwCursorDupSort.Put(k, v)
}

func (c *prefetchIndexCursor) PutCurrent(k, v []byte) error {
	c.requirePrefetched(k)
	return c.RwCursorDupSort.PutCurrent(k, v)
}

func (c *prefetchIndexCursor) requirePrefetched(k []byte) {
	_, ok := c.prefetched.Load(string(k))
	require.True(c.t, ok, "prefetch must precede every write of key %x", k)
}

func (c *prefetchIndexCursor) Close() {}

// iiValuePairs lists the values table of an inverted index as key:txNum pairs.
func iiValuePairs(tb testing.TB, tx kv.Tx, table string) []string {
	tb.Helper()
	var pairs []string
	require.NoError(tb, tx.ForEach(table, nil, func(k, v []byte) error {
		txNums, err := decodeIIChunk(nil, v)
		for _, txNum := range txNums {
			pairs = append(pairs, fmt.Sprintf("%x:%016x", k, txNum))
		}
		return err
	}))
	return pairs
}

type prefetchIndexDB struct {
	kv.RoDB
	cursor *prefetchIndexCursor
}

func (db *prefetchIndexDB) BeginRo(ctx context.Context) (kv.Tx, error) {
	db.cursor.prefetchCalls.Add(1)
	if db.cursor.prefetchErr != nil {
		return nil, db.cursor.prefetchErr
	}
	tx, err := db.RoDB.BeginRo(ctx) //nolint:gocritic // The caller owns the returned transaction.
	if err != nil {
		return nil, err
	}
	return &prefetchIndexReadTx{Tx: tx, cursor: db.cursor}, nil
}

type prefetchIndexReadTx struct {
	kv.Tx
	cursor *prefetchIndexCursor
}

func (tx *prefetchIndexReadTx) Rollback() {
	tx.Tx.Rollback()
	tx.cursor.rollbacks.Add(1)
}

func (tx *prefetchIndexReadTx) CursorDupSort(table string) (kv.CursorDupSort, error) {
	c, err := tx.Tx.CursorDupSort(table) //nolint:gocritic // The caller owns the returned cursor.
	if err != nil {
		return nil, err
	}
	return &prefetchIndexReadCursor{CursorDupSort: c, cursor: tx.cursor}, nil
}

type prefetchIndexReadCursor struct {
	kv.CursorDupSort
	cursor *prefetchIndexCursor
}

func (c *prefetchIndexReadCursor) SeekBothRange(k, v []byte) ([]byte, error) {
	value, err := c.CursorDupSort.SeekBothRange(k, v)
	if err == nil {
		c.cursor.prefetched.Store(string(k), struct{}{})
		c.cursor.prefetchSeeks.Add(1)
	}
	return value, err
}

func TestInvertedIndexPrefetch(t *testing.T) {
	const workers, batchSize = uint64(16), uint64(1024)
	type entry struct {
		txNum uint64
		key   string
	}
	entries := []entry{
		{6, "c"},
		{5, "b"},
		{6, "a"},
		{5, "b"},
		{6, "c"},
		{8, "z"},
		{6, "d"},
		{5, "a"},
		{6, "a\x00"},
		{8, "c"},
		{7, "c"},
		{9, "c"},
		{9, "c"},
		{0, "m"},
		{0, ""},
		{3, ""},
		{2, ""},
		{3, ""},
	}
	for i := range 3073 {
		key := "boundary"
		if i >= 1025 {
			key = "boundary-next"
		}
		entries = append(entries, entry{uint64(16 + i%31), key})
	}
	for _, seed := range []struct {
		name    string
		entries []entry
	}{
		{"empty", nil},
		{"append", []entry{{1, "z"}}},
		{"overlap", []entry{{5, "b"}, {6, "b"}, {9, "z"}}},
		{"same_last_key", []entry{{5, "b"}}},
		{"empty_key", []entry{{2, ""}, {5, ""}}},
	} {
		for _, spill := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/spill=%t", seed.name, spill), func(t *testing.T) {
				db, ii := testDbAndInvertedIndex(t, 16, log.New())
				require.NoError(t, db.Update(t.Context(), func(tx kv.RwTx) error {
					for _, e := range seed.entries {
						var txNum [8]byte
						binary.BigEndian.PutUint64(txNum[:], e.txNum)
						if err := tx.Put(ii.KeysTable, txNum[:], []byte(e.key)); err != nil {
							return err
						}
						if err := tx.Put(ii.ValuesTable, []byte(e.key), txNum[:]); err != nil {
							return err
						}
					}
					return nil
				}))
				tx, err := db.BeginRw(t.Context())
				require.NoError(t, err)
				defer tx.Rollback()
				expected := map[string]map[string]struct{}{ii.KeysTable: {}, ii.ValuesTable: {}}
				for _, group := range [][]entry{seed.entries, entries} {
					for _, e := range group {
						var txNum [8]byte
						binary.BigEndian.PutUint64(txNum[:], e.txNum)
						expected[ii.KeysTable][fmt.Sprintf("%x:%x", txNum, e.key)] = struct{}{}
						expected[ii.ValuesTable][fmt.Sprintf("%x:%x", e.key, txNum)] = struct{}{}
					}
				}
				iit := ii.beginForTests()
				defer iit.Close()
				c, err := tx.RwCursorDupSort(ii.ValuesTable)
				require.NoError(t, err)
				defer c.Close()
				cursor := &prefetchIndexCursor{RwCursorDupSort: c, t: t}
				prefetchDB := &prefetchIndexDB{RoDB: db, cursor: cursor}
				w := iit.NewWriter(prefetchDB)
				defer w.close()
				w.prefetcher = newInvertedIndexPrefetcher(prefetchDB, ii.ValuesTable, workers)
				w.prefetchBatchSize = batchSize
				for i, e := range entries {
					require.NoError(t, w.Add([]byte(e.key), e.txNum))
					if spill && i%2 == 1 {
						require.NoError(t, w.indexKeys.Flush())
						require.NoError(t, w.index.Flush())
					}
				}
				require.NoError(t, w.Flush(t.Context(), &prefetchIndexTx{RwTx: tx, table: ii.ValuesTable, cursor: cursor}))
				require.EqualValues(t, workers, cursor.prefetchCalls.Load(), "open readers once per flush")
				require.EqualValues(t, workers, cursor.rollbacks.Load(), "release readers before returning from flush")
				for table, values := range expected {
					want := make([]string, 0, len(values))
					for value := range values {
						want = append(want, value)
					}
					var got []string
					if table == ii.ValuesTable {
						got = iiValuePairs(t, tx, table)
					} else {
						require.NoError(t, tx.ForEach(table, nil, func(k, v []byte) error {
							got = append(got, fmt.Sprintf("%x:%x", k, v))
							return nil
						}))
					}
					require.ElementsMatch(t, want, got, table)
				}
			})
		}
	}
}

func TestInvertedIndexPrefetchError(t *testing.T) {
	db, ii := testDbAndInvertedIndex(t, 16, log.New())
	tx, err := db.BeginRw(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()
	iit := ii.beginForTests()
	defer iit.Close()
	wantErr := errors.New("read failed")
	cursor := &prefetchIndexCursor{prefetchErr: wantErr}
	prefetchDB := &prefetchIndexDB{RoDB: db, cursor: cursor}
	w := iit.NewWriter(prefetchDB)
	defer w.close()
	w.prefetcher = newInvertedIndexPrefetcher(prefetchDB, ii.ValuesTable, 3)
	w.prefetchBatchSize = 1024
	require.NoError(t, w.Add([]byte("key"), 1))
	require.NoError(t, w.Flush(t.Context(), tx))
	require.EqualValues(t, 1, cursor.prefetchCalls.Load())
	var txNum [8]byte
	binary.BigEndian.PutUint64(txNum[:], 1)
	value, err := tx.GetOne(ii.ValuesTable, []byte("key"))
	require.NoError(t, err)
	require.Equal(t, txNum[:], value)
	key, err := tx.GetOne(ii.KeysTable, txNum[:])
	require.NoError(t, err)
	require.Equal(t, []byte("key"), key)
}

func TestInvertedIndexPrefetchReuse(t *testing.T) {
	for _, tc := range []struct{ workers, batchSize uint64 }{{1, 1024}, {3, 1024}, {3, 0}, {3, 1}} {
		t.Run(fmt.Sprintf("workers=%d/batch=%d", tc.workers, tc.batchSize), func(t *testing.T) {
			db, ii := testDbAndInvertedIndex(t, 16, log.New())
			iit := ii.beginForTests()
			defer iit.Close()
			cursor := &prefetchIndexCursor{}
			prefetchDB := &prefetchIndexDB{RoDB: db, cursor: cursor}
			w := iit.NewWriter(prefetchDB)
			defer w.close()
			w.prefetcher = newInvertedIndexPrefetcher(prefetchDB, ii.ValuesTable, tc.workers)
			w.prefetchBatchSize = tc.batchSize
			p := w.prefetcher
			for txNum := uint64(1); txNum <= 2; txNum++ {
				ctx, cancel := context.WithCancel(t.Context())
				require.NoError(t, w.Add([]byte("key"), txNum))
				err := db.Update(ctx, func(tx kv.RwTx) error { return w.Flush(ctx, tx) })
				cancel()
				require.NoError(t, err)
				require.Same(t, p, w.prefetcher)
				require.EqualValues(t, txNum*tc.workers, cursor.prefetchCalls.Load())
				require.EqualValues(t, txNum*tc.workers, cursor.rollbacks.Load())
			}
			require.NoError(t, db.View(t.Context(), func(tx kv.Tx) error {
				n, err := tx.Count(ii.KeysTable)
				require.NoError(t, err)
				require.EqualValues(t, 2, n)
				require.Len(t, iiValuePairs(t, tx, ii.ValuesTable), 2)
				return nil
			}))
		})
	}
}

func TestInvertedIndexPrefetchCancellation(t *testing.T) {
	for _, batchSize := range []uint64{1, 1024} {
		t.Run(fmt.Sprintf("batch=%d", batchSize), func(t *testing.T) {
			db, ii := testDbAndInvertedIndex(t, 16, log.New())
			tx, err := db.BeginRw(t.Context())
			require.NoError(t, err)
			defer tx.Rollback()
			iit := ii.beginForTests()
			defer iit.Close()
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			var rollbacks int
			prefetchDB := &prefetchTestDB{newCursor: func(context.Context) kv.CursorDupSort {
				return &prefetchTestCursor{seek: func(_, _ []byte) error {
					cancel()
					return ctx.Err()
				}}
			}, rollback: func() { rollbacks++ }}
			w := iit.NewWriter(db)
			defer w.close()
			w.prefetcher = newInvertedIndexPrefetcher(prefetchDB, ii.ValuesTable, 1)
			w.prefetchBatchSize = batchSize
			require.NoError(t, w.Add([]byte("key"), 1))
			require.ErrorIs(t, w.Flush(ctx, tx), context.Canceled)
			require.Equal(t, 1, rollbacks)
			for _, table := range []string{ii.KeysTable, ii.ValuesTable} {
				n, err := tx.Count(table)
				require.NoError(t, err)
				require.Zero(t, n)
			}
		})
	}
}

func TestInvertedIndexPrefetchReadLimit(t *testing.T) {
	for _, slots := range []int64{0, 1} {
		t.Run(fmt.Sprintf("slots=%d", slots), func(t *testing.T) {
			limiter := semaphore.NewWeighted(slots)
			db := mdbxtest.InMem(t, mdbx.New(dbcfg.ChainDB, log.New()), t.TempDir()).
				RoTxsLimiter(limiter).MustOpen()
			t.Cleanup(db.Close)
			tx, err := db.BeginRw(t.Context())
			require.NoError(t, err)
			defer tx.Rollback()
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			cursor := &prefetchIndexCursor{}
			prefetchDB := &prefetchIndexDB{RoDB: db, cursor: cursor}
			p := newInvertedIndexPrefetcher(prefetchDB, kv.TblTracesToIdx, 16)
			pairs := [][2][]byte{{[]byte("key"), make([]byte, 8)}}
			w := &InvertedIndexBufferedWriter{
				prefetcher:        p,
				prefetchBatchSize: 1024,
				indexTable:        kv.TblTracesToIdx,
				indexKeysTable:    kv.TblTracesToKeys,
				tmpdir:            t.TempDir(),
				logger:            log.New(),
			}
			defer w.close()
			require.NoError(t, w.Add(pairs[0][0], 0))
			require.NoError(t, w.Flush(ctx, tx))
			value, err := tx.GetOne(kv.TblTracesToIdx, pairs[0][0])
			require.NoError(t, err)
			require.Equal(t, pairs[0][1], value)
			key, err := tx.GetOne(kv.TblTracesToKeys, pairs[0][1])
			require.NoError(t, err)
			require.Equal(t, pairs[0][0], key)
			require.EqualValues(t, slots+1, cursor.prefetchCalls.Load())
			require.EqualValues(t, slots, cursor.rollbacks.Load())
			require.True(t, limiter.TryAcquire(slots), "release all partially opened readers")
			limiter.Release(slots)
		})
	}
}

func TestInvertedIndex_IdxRange_DescFromFilesEnd(t *testing.T) {
	t.Parallel()
	logger := log.New()

	aggStep := uint64(16)
	db, ii := testDbAndInvertedIndex(t, aggStep, logger)
	ctx := t.Context()
	require := require.New(t)

	var key [8]byte
	binary.BigEndian.PutUint64(key[:], 1)

	txNums := []uint64{2, 18, 31, 32, 40}

	err := db.Update(ctx, func(tx kv.RwTx) error {
		ic := ii.beginForTests()
		defer ic.Close()
		writer := ic.NewWriter(db)
		defer writer.close()
		for _, txNum := range txNums {
			if err := writer.Add(key[:], txNum); err != nil {
				return err
			}
		}
		return writer.Flush(ctx, tx)
	})
	require.NoError(err)

	tx, err := db.BeginRw(ctx)
	require.NoError(err)
	defer tx.Rollback()
	for step := kv.Step(0); step <= 1; step++ {
		require.NoError(ii.collateBuildIntegrate(ctx, step, tx, background.NewProgressSet()))
	}
	require.NoError(tx.Commit())

	roTx, err := db.BeginRo(ctx)
	require.NoError(err)
	defer roTx.Rollback()

	ic := ii.beginForTests()
	defer ic.Close()
	require.Equal(uint64(32), ic.files.EndTxNum())

	it, err := ic.IdxRange(key[:], 32, -1, order.Desc, -1, roTx)
	require.NoError(err)
	got, err := stream.ToArrayU64(it)
	require.NoError(err)
	require.Equal([]uint64{32, 31, 18, 2}, got)

	it, err = ic.IdxRange(key[:], 32, 17, order.Desc, -1, roTx)
	require.NoError(err)
	got, err = stream.ToArrayU64(it)
	require.NoError(err)
	require.Equal([]uint64{32, 31, 18}, got)
}

// TestInvIndexHasTxNumsInDB pins the txNum-indexed existence lookup: it answers
// from the keys table alone, so it must see exactly the txNums that hold records.
func TestInvIndexHasTxNumsInDB(t *testing.T) {
	t.Parallel()
	logger := log.New()
	db, ii := testDbAndInvertedIndex(t, 16, logger)
	ctx := t.Context()
	tx, err := db.BeginRw(ctx)
	require.NoError(t, err)
	defer tx.Rollback()
	ic := ii.beginForTests()
	defer ic.Close()
	writer := ic.NewWriter(db)
	defer writer.close()
	require.NoError(t, writer.Add([]byte("key1"), 2))
	require.NoError(t, writer.Add([]byte("key2"), 3))
	require.NoError(t, writer.Add([]byte("key1"), 17))
	require.NoError(t, writer.Flush(ctx, tx))

	for _, tc := range []struct {
		from, to uint64
		want     bool
	}{
		{0, 2, false},
		{2, 3, true},
		{3, 4, true},
		{4, 17, false},
		{17, 18, true},
		{18, 100, false},
	} {
		got, err := ic.HasTxNumsInDB(tc.from, tc.to, tx)
		require.NoError(t, err)
		require.Equal(t, tc.want, got, "txNums [%d, %d)", tc.from, tc.to)
	}
}

// iiChunkModel is the expected content of a values table: key -> ascending txNums.
type iiChunkModel map[string][]uint64

func (m iiChunkModel) add(key string, txNums ...uint64) {
	m[key] = append(m[key], txNums...)
	slices.Sort(m[key])
	m[key] = slices.Compact(m[key])
}

func (m iiChunkModel) delete(key string, from, to uint64) {
	m[key] = slices.DeleteFunc(m[key], func(n uint64) bool { return n >= from && n < to })
	if len(m[key]) == 0 {
		delete(m, key)
	}
}

func iiChunkAdd(t *testing.T, tx kv.RwTx, table string, m iiChunkModel, key string, txNums ...uint64) {
	t.Helper()
	c, err := tx.RwCursorDupSort(table)
	require.NoError(t, err)
	defer c.Close()
	w := &iiChunkWriter{c: c}
	for _, n := range txNums {
		require.NoError(t, w.add([]byte(key), binary.BigEndian.AppendUint64(nil, n)))
	}
	require.NoError(t, w.flush())
	m.add(key, txNums...)
}

func iiChunkDelete(t *testing.T, tx kv.RwTx, table string, m iiChunkModel, key string, from, to uint64) {
	t.Helper()
	c, err := tx.RwCursorDupSort(table)
	require.NoError(t, err)
	defer c.Close()
	d := newIIChunkDeleter(c)
	want := len(m[key])
	require.NoError(t, d.deleteRange([]byte(key), from, to))
	m.delete(key, from, to)
	require.EqualValues(t, want-len(m[key]), d.count)
}

func requireIIChunks(t *testing.T, tx kv.Tx, table string, m iiChunkModel) {
	t.Helper()
	var want []string
	for k, txNums := range m {
		for _, n := range txNums {
			want = append(want, fmt.Sprintf("%x:%016x", k, n))
		}
	}
	require.ElementsMatch(t, want, iiValuePairs(t, tx, table))

	var prevKey []byte
	var prevMax uint64
	require.NoError(t, tx.ForEach(table, nil, func(k, v []byte) error {
		require.LessOrEqual(t, len(v), iiChunkMaxBytes)
		txNums, err := decodeIIChunk(nil, v)
		require.NoError(t, err)
		require.Equal(t, binary.BigEndian.Uint64(v), txNums[len(txNums)-1], "chunk prefix is its max")
		if bytes.Equal(k, prevKey) {
			require.Less(t, prevMax, txNums[0], "chunks of a key are disjoint and ordered")
		}
		prevKey, prevMax = bytes.Clone(k), txNums[len(txNums)-1]
		return nil
	}))

	for k, txNums := range m {
		lo, hi := int(txNums[0]), int(txNums[len(txNums)-1])
		bounds := []int{-1, lo, lo + 1, (lo + hi) / 2, hi, hi + 1}
		for _, from := range bounds {
			for _, to := range bounds {
				for _, asc := range []order.By{order.Asc, order.Desc} {
					for _, limit := range []int{-1, 3} {
						var exp []uint64
						for _, n := range txNums {
							if asc && (from < 0 || n >= uint64(from)) && (to < 0 || n < uint64(to)) ||
								!asc && (from < 0 || n <= uint64(from)) && (to < 0 || n > uint64(to)) {
								exp = append(exp, n)
							}
						}
						if !asc {
							slices.Reverse(exp)
						}
						if limit >= 0 && len(exp) > limit {
							exp = exp[:limit]
						}
						s, err := newIIChunkStream(tx, table, []byte(k), from, to, asc, limit)
						require.NoError(t, err)
						require.Equal(t, limit < 0, s.CanSeek(), "a limited stream must not seek past elements it does not count")
						got, err := stream.ToArrayU64(s)
						s.Close()
						require.NoError(t, err)
						require.Equal(t, exp, got, "key=%x from=%d to=%d asc=%t limit=%d", k, from, to, asc, limit)
					}
				}
			}
		}
		for _, target := range append(bounds[1:], int(txNums[min(5, len(txNums)-1)])+1) {
			for _, asc := range []order.By{order.Asc, order.Desc} {
				s, err := newIIChunkStream(tx, table, []byte(k), -1, -1, asc, -1)
				require.NoError(t, err)
				head, err := s.Next()
				require.NoError(t, err)
				require.NoError(t, s.Seek(uint64(target)))
				got, err := stream.ToArrayU64(s)
				require.NoError(t, err)
				var exp []uint64
				for _, n := range txNums {
					if asc && n > head && n >= uint64(target) || !asc && n < head && n <= uint64(target) {
						exp = append(exp, n)
					}
				}
				if !asc {
					slices.Reverse(exp)
				}
				require.Equal(t, exp, got, "key=%x seek=%d asc=%t", k, target, asc)
				s.Close()
			}
		}
	}
}

func TestInvertedIndexChunks(t *testing.T) {
	sparse := func(seed, n int, from uint64) []uint64 {
		txNums := make([]uint64, n)
		for i := range txNums {
			from += uint64(1 + (seed*31+i*17)%300)
			txNums[i] = from
		}
		return txNums
	}
	setup := func(t *testing.T) (kv.RwTx, string, iiChunkModel) {
		db, ii := testDbAndInvertedIndex(t, 16, log.New())
		tx, err := db.BeginRw(t.Context())
		require.NoError(t, err)
		t.Cleanup(tx.Rollback)
		m := iiChunkModel{}
		txNums := sparse(1, 3000, 1000)
		for i := 0; i < len(txNums); i += 50 {
			iiChunkAdd(t, tx, ii.ValuesTable, m, "k", txNums[i:i+50]...)
		}
		iiChunkAdd(t, tx, ii.ValuesTable, m, "k\x00", sparse(2, 700, 0)...)
		iiChunkAdd(t, tx, ii.ValuesTable, m, "", 5)
		return tx, ii.ValuesTable, m
	}

	t.Run("append_splits_chunks", func(t *testing.T) {
		tx, table, m := setup(t)
		n, err := tx.Count(table)
		require.NoError(t, err)
		require.Greater(t, n, uint64(4))
		requireIIChunks(t, tx, table, m)
	})
	t.Run("merge_below_last_chunk", func(t *testing.T) {
		tx, table, m := setup(t)
		txNums := m["k"]
		iiChunkAdd(t, tx, table, m, "k", txNums[100]+1, txNums[2000]+1, txNums[2999]+7, txNums[0]-1)
		iiChunkAdd(t, tx, table, m, "k", txNums[10], txNums[11]) // already present
		requireIIChunks(t, tx, table, m)
	})
	t.Run("delete_range", func(t *testing.T) {
		tx, table, m := setup(t)
		txNums := m["k"]
		iiChunkDelete(t, tx, table, m, "k", txNums[500], txNums[1500])
		iiChunkDelete(t, tx, table, m, "k", txNums[2500]+1, math.MaxUint64) // tail: max of a chunk shrinks
		iiChunkDelete(t, tx, table, m, "k", 0, txNums[300])
		iiChunkDelete(t, tx, table, m, "k", txNums[1500]+1, txNums[1500]+1)
		iiChunkDelete(t, tx, table, m, "k\x00", 0, math.MaxUint64)
		iiChunkDelete(t, tx, table, m, "absent", 0, math.MaxUint64)
		requireIIChunks(t, tx, table, m)
		iiChunkAdd(t, tx, table, m, "k", txNums[2600], txNums[2601])
		requireIIChunks(t, tx, table, m)
	})
	t.Run("legacy_values", func(t *testing.T) {
		db, ii := testDbAndInvertedIndex(t, 16, log.New())
		tx, err := db.BeginRw(t.Context())
		require.NoError(t, err)
		defer tx.Rollback()
		m := iiChunkModel{}
		for _, n := range []uint64{3, 9, 12} { // one 8-byte txNum per dup: the layout before chunking
			require.NoError(t, tx.Put(ii.ValuesTable, []byte("k"), binary.BigEndian.AppendUint64(nil, n)))
			m.add("k", n)
		}
		requireIIChunks(t, tx, ii.ValuesTable, m)
		iiChunkAdd(t, tx, ii.ValuesTable, m, "k", 15, 20)
		iiChunkDelete(t, tx, ii.ValuesTable, m, "k", 9, 16)
		requireIIChunks(t, tx, ii.ValuesTable, m)
	})

	t.Run("random_flush_prune_unwind", func(t *testing.T) {
		db, ii := testDbAndInvertedIndex(t, 16, log.New())
		tx, err := db.BeginRw(t.Context())
		require.NoError(t, err)
		defer tx.Rollback()
		r := rand.New(rand.NewPCG(25304, 1))
		keys := []string{"a", "ab", "b", "hot"}
		m := iiChunkModel{}
		tip := uint64(0)
		for ; tip < 400; tip += 7 {
			require.NoError(t, tx.Put(ii.ValuesTable, []byte("hot"), binary.BigEndian.AppendUint64(nil, tip)))
			m.add("hot", tip)
		}
		for range 300 {
			key := keys[r.IntN(len(keys))]
			switch r.IntN(5) {
			case 0, 1:
				var txNums []uint64
				for range r.IntN(300) {
					tip += 1 + r.Uint64N(3)
					if r.IntN(50) == 0 {
						tip += math.MaxUint16
					}
					txNums = append(txNums, tip)
				}
				iiChunkAdd(t, tx, ii.ValuesTable, m, key, txNums...)
			case 2:
				txNums := make([]uint64, 1+r.IntN(50))
				for i := range txNums {
					txNums[i] = r.Uint64N(tip + 1)
				}
				slices.Sort(txNums)
				iiChunkAdd(t, tx, ii.ValuesTable, m, key, slices.Compact(txNums)...)
			case 3:
				iiChunkDelete(t, tx, ii.ValuesTable, m, key, 0, r.Uint64N(tip+1))
			case 4:
				tip -= min(tip, r.Uint64N(500))
				for _, k := range keys {
					iiChunkDelete(t, tx, ii.ValuesTable, m, k, tip+1, math.MaxUint64)
				}
			}
			requireIIChunks(t, tx, ii.ValuesTable, m)
		}
	})
}

func TestInvertedIndexPruneChunksResume(t *testing.T) {
	db, ii := testDbAndInvertedIndex(t, 16, log.New())
	tx, err := db.BeginRw(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()
	m := iiChunkModel{}
	for _, key := range []string{"a", "b", "c"} {
		iiChunkAdd(t, tx, ii.ValuesTable, m, key, 1, 5, 9, 13)
	}
	c, err := tx.RwCursorDupSort(ii.ValuesTable)
	require.NoError(t, err)
	defer c.Close()
	iit := ii.beginForTests()
	defer iit.Close()
	logEvery := time.NewTicker(time.Hour)
	defer logEvery.Stop()

	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	stat := &prune.Stat{MinTxNum: math.MaxUint64, ValueProgress: prune.First}
	require.NoError(t, iit.pruneChunks(canceled, c, 0, 10, logEvery, stat))
	require.Equal(t, prune.InProgress, stat.ValueProgress)
	require.Equal(t, []byte("a"), stat.LastPrunedValue)

	require.NoError(t, iit.pruneChunks(t.Context(), c, 0, 10, logEvery, stat))
	require.Equal(t, prune.Done, stat.ValueProgress)
	require.EqualValues(t, 9, stat.PruneCountValues)
	for _, key := range []string{"a", "b", "c"} {
		m.delete(key, 0, 10)
	}
	requireIIChunks(t, tx, ii.ValuesTable, m)
}

func TestInvertedIndexPrefetchOncePerKey(t *testing.T) {
	db, ii := testDbAndInvertedIndex(t, 16, log.New())
	tx, err := db.BeginRw(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()
	iit := ii.beginForTests()
	defer iit.Close()
	cursor := &prefetchIndexCursor{t: t}
	prefetchDB := &prefetchIndexDB{RoDB: db, cursor: cursor}
	w := iit.NewWriter(prefetchDB)
	defer w.close()
	w.prefetcher = newInvertedIndexPrefetcher(prefetchDB, ii.ValuesTable, 1)
	w.prefetchBatchSize = 1024
	for txNum := uint64(1); txNum <= 100; txNum++ {
		require.NoError(t, w.Add([]byte("hot"), txNum))
	}
	require.NoError(t, w.Add([]byte("cold"), 7))
	require.NoError(t, w.Flush(t.Context(), tx))
	require.EqualValues(t, 2, cursor.prefetchSeeks.Load(), "the writer seeks each key once per batch")
	require.Len(t, iiValuePairs(t, tx, ii.ValuesTable), 101)
}

func TestInvertedIndexChunkLayout(t *testing.T) {
	v := appendIIChunk(nil, []uint64{1000, 1003, 1010, 1500})
	want := binary.BigEndian.AppendUint64(nil, 1500)
	for _, d := range []uint16{3, 10, 500} {
		want = binary.NativeEndian.AppendUint16(want, d)
	}
	require.Equal(t, want, v, "BE8(max) || native-endian uint16 deltas from min, min implicit")
	txNums, err := decodeIIChunk(nil, v)
	require.NoError(t, err)
	require.Equal(t, []uint64{1000, 1003, 1010, 1500}, txNums)

	require.Equal(t, "0000000000000007", hex.EncodeToString(appendIIChunk(nil, []uint64{7})), "a one-txNum chunk is the 8-byte value of the old format")

	require.Equal(t, 2, iiChunkFit([]uint64{10, 10 + math.MaxUint16, 10 + math.MaxUint16 + 1}), "max-min must fit into uint16")
	dense := make([]uint64, 300)
	for i := range dense {
		dense[i] = uint64(i)
	}
	require.Equal(t, 257, iiChunkFit(dense))

	_, err = decodeIIChunk(nil, []byte{0, 0, 0, 0, 0, 0, 0, 1, 2, 0})
	require.Error(t, err, "delta above max")
	_, err = decodeIIChunk(nil, []byte{0, 0, 0, 0, 0, 0, 0, 9, 2})
	require.Error(t, err, "odd deltas length")
}
