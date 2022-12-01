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
	"strings"
	"testing"
	"testing/fstest"
	"time"

	"github.com/google/btree"
	"github.com/ledgerwatch/log/v3"
	"github.com/stretchr/testify/require"

	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/mdbx"
	"github.com/ledgerwatch/erigon-lib/recsplit"
)

func testDbAndDomain(t *testing.T, prefixLen int) (string, kv.RwDB, *Domain) {
	t.Helper()
	path := t.TempDir()
	t.Cleanup(func() { os.RemoveAll(path) })
	logger := log.New()
	keysTable := "Keys"
	valsTable := "Vals"
	historyKeysTable := "HistoryKeys"
	historyValsTable := "HistoryVals"
	settingsTable := "Settings"
	indexTable := "Index"
	db := mdbx.NewMDBX(logger).Path(path).WithTableCfg(func(defaultBuckets kv.TableCfg) kv.TableCfg {
		return kv.TableCfg{
			keysTable:        kv.TableCfgItem{Flags: kv.DupSort},
			valsTable:        kv.TableCfgItem{},
			historyKeysTable: kv.TableCfgItem{Flags: kv.DupSort},
			historyValsTable: kv.TableCfgItem{},
			settingsTable:    kv.TableCfgItem{},
			indexTable:       kv.TableCfgItem{Flags: kv.DupSort},
		}
	}).MustOpen()
	t.Cleanup(db.Close)
	d, err := NewDomain(path, path, 16 /* aggregationStep */, "base" /* filenameBase */, keysTable, valsTable, historyKeysTable, historyValsTable, settingsTable, indexTable, prefixLen, true /* compressVals */)
	require.NoError(t, err)
	t.Cleanup(d.Close)
	return path, db, d
}

func TestCollationBuild(t *testing.T) {
	logEvery := time.NewTicker(30 * time.Second)
	defer logEvery.Stop()
	_, db, d := testDbAndDomain(t, 0 /* prefixLen */)
	ctx := context.Background()

	tx, err := db.BeginRw(ctx)
	require.NoError(t, err)
	defer tx.Rollback()
	d.SetTx(tx)
	d.StartWrites("")
	defer d.FinishWrites()

	d.SetTxNum(2)
	err = d.Put([]byte("key1"), nil, []byte("value1.1"))
	require.NoError(t, err)

	d.SetTxNum(3)
	err = d.Put([]byte("key2"), nil, []byte("value2.1"))
	require.NoError(t, err)

	d.SetTxNum(6)
	err = d.Put([]byte("key1"), nil, []byte("value1.2"))
	require.NoError(t, err)

	err = d.Rotate().Flush(tx)
	require.NoError(t, err)

	c, err := d.collate(ctx, 0, 0, 7, tx, logEvery)
	require.NoError(t, err)
	require.True(t, strings.HasSuffix(c.valuesPath, "base.0-1.kv"))
	require.Equal(t, 2, c.valuesCount)
	require.True(t, strings.HasSuffix(c.historyPath, "base.0-1.v"))
	require.Equal(t, 3, c.historyCount)
	require.Equal(t, 2, len(c.indexBitmaps))
	require.Equal(t, []uint64{3}, c.indexBitmaps["key2"].ToArray())
	require.Equal(t, []uint64{2, 6}, c.indexBitmaps["key1"].ToArray())

	sf, err := d.buildFiles(ctx, 0, c)
	require.NoError(t, err)
	defer sf.Close()
	g := sf.valuesDecomp.MakeGetter()
	g.Reset(0)
	var words []string
	for g.HasNext() {
		w, _ := g.Next(nil)
		words = append(words, string(w))
	}
	require.Equal(t, []string{"key1", "value1.2", "key2", "value2.1"}, words)
	// Check index
	require.Equal(t, 2, int(sf.valuesIdx.KeyCount()))
	r := recsplit.NewIndexReader(sf.valuesIdx)
	for i := 0; i < len(words); i += 2 {
		offset := r.Lookup([]byte(words[i]))
		g.Reset(offset)
		w, _ := g.Next(nil)
		require.Equal(t, words[i], string(w))
		w, _ = g.Next(nil)
		require.Equal(t, words[i+1], string(w))
	}
}

func TestIterationBasic(t *testing.T) {
	_, db, d := testDbAndDomain(t, 5 /* prefixLen */)
	ctx := context.Background()
	tx, err := db.BeginRw(ctx)
	require.NoError(t, err)
	defer tx.Rollback()
	d.SetTx(tx)
	d.StartWrites("")
	defer d.FinishWrites()

	d.SetTxNum(2)
	err = d.Put([]byte("addr1"), []byte("loc1"), []byte("value1"))
	require.NoError(t, err)
	err = d.Put([]byte("addr1"), []byte("loc2"), []byte("value1"))
	require.NoError(t, err)
	err = d.Put([]byte("addr1"), []byte("loc3"), []byte("value1"))
	require.NoError(t, err)
	err = d.Put([]byte("addr2"), []byte("loc1"), []byte("value1"))
	require.NoError(t, err)
	err = d.Put([]byte("addr2"), []byte("loc2"), []byte("value1"))
	require.NoError(t, err)
	err = d.Put([]byte("addr3"), []byte("loc1"), []byte("value1"))
	require.NoError(t, err)
	err = d.Put([]byte("addr3"), []byte("loc2"), []byte("value1"))
	require.NoError(t, err)

	var keys, vals []string
	err = d.MakeContext().IteratePrefix([]byte("addr2"), func(k, v []byte) {
		keys = append(keys, string(k))
		vals = append(vals, string(v))
	})
	require.NoError(t, err)
	require.Equal(t, []string{"addr2loc1", "addr2loc2"}, keys)
	require.Equal(t, []string{"value1", "value1"}, vals)
}

func TestAfterPrune(t *testing.T) {
	logEvery := time.NewTicker(30 * time.Second)
	defer logEvery.Stop()
	_, db, d := testDbAndDomain(t, 0 /* prefixLen */)
	ctx := context.Background()

	tx, err := db.BeginRw(ctx)
	require.NoError(t, err)
	defer tx.Rollback()
	d.SetTx(tx)
	d.StartWrites("")
	defer d.FinishWrites()

	d.SetTxNum(2)
	err = d.Put([]byte("key1"), nil, []byte("value1.1"))
	require.NoError(t, err)

	d.SetTxNum(3)
	err = d.Put([]byte("key2"), nil, []byte("value2.1"))
	require.NoError(t, err)

	d.SetTxNum(6)
	err = d.Put([]byte("key1"), nil, []byte("value1.2"))
	require.NoError(t, err)

	d.SetTxNum(17)
	err = d.Put([]byte("key1"), nil, []byte("value1.3"))
	require.NoError(t, err)

	d.SetTxNum(18)
	err = d.Put([]byte("key2"), nil, []byte("value2.2"))
	require.NoError(t, err)

	err = d.Rotate().Flush(tx)
	require.NoError(t, err)

	c, err := d.collate(ctx, 0, 0, 16, tx, logEvery)
	require.NoError(t, err)

	sf, err := d.buildFiles(ctx, 0, c)
	require.NoError(t, err)
	defer sf.Close()

	d.integrateFiles(sf, 0, 16)
	var v []byte
	dc := d.MakeContext()
	v, err = dc.Get([]byte("key1"), nil, tx)
	require.NoError(t, err)
	require.Equal(t, []byte("value1.3"), v)
	v, err = dc.Get([]byte("key2"), nil, tx)
	require.NoError(t, err)
	require.Equal(t, []byte("value2.2"), v)

	err = d.prune(ctx, 0, 0, 16, math.MaxUint64, logEvery)
	require.NoError(t, err)

	for _, table := range []string{d.keysTable, d.valsTable, d.indexKeysTable, d.historyValsTable, d.indexTable} {
		var cur kv.Cursor
		cur, err = tx.Cursor(table)
		require.NoError(t, err)
		defer cur.Close()
		var k []byte
		k, _, err = cur.First()
		require.NoError(t, err)
		require.NotNilf(t, k, table, string(k))
	}

	v, err = dc.Get([]byte("key1"), nil, tx)
	require.NoError(t, err)
	require.Equal(t, []byte("value1.3"), v)
	v, err = dc.Get([]byte("key2"), nil, tx)
	require.NoError(t, err)
	require.Equal(t, []byte("value2.2"), v)
}

func filledDomain(t *testing.T) (string, kv.RwDB, *Domain, uint64) {
	t.Helper()
	path, db, d := testDbAndDomain(t, 0 /* prefixLen */)
	ctx := context.Background()
	tx, err := db.BeginRw(ctx)
	require.NoError(t, err)
	defer tx.Rollback()
	d.SetTx(tx)
	d.StartWrites("")
	defer d.FinishWrites()

	txs := uint64(1000)
	// keys are encodings of numbers 1..31
	// each key changes value on every txNum which is multiple of the key
	for txNum := uint64(1); txNum <= txs; txNum++ {
		d.SetTxNum(txNum)
		for keyNum := uint64(1); keyNum <= uint64(31); keyNum++ {
			if txNum%keyNum == 0 {
				valNum := txNum / keyNum
				var k [8]byte
				var v [8]byte
				binary.BigEndian.PutUint64(k[:], keyNum)
				binary.BigEndian.PutUint64(v[:], valNum)
				err = d.Put(k[:], nil, v[:])
				require.NoError(t, err)
			}
		}
		if txNum%10 == 0 {
			err = d.Rotate().Flush(tx)
			require.NoError(t, err)
		}
	}
	err = d.Rotate().Flush(tx)
	require.NoError(t, err)
	err = tx.Commit()
	require.NoError(t, err)
	return path, db, d, txs
}

func checkHistory(t *testing.T, db kv.RwDB, d *Domain, txs uint64) {
	t.Helper()
	ctx := context.Background()
	var err error
	// Check the history
	var roTx kv.Tx
	dc := d.MakeContext()
	for txNum := uint64(0); txNum <= txs; txNum++ {
		if txNum == 976 {
			// Create roTx obnly for the last several txNum, because all history before that
			// we should be able to read without any DB access
			roTx, err = db.BeginRo(ctx)
			require.NoError(t, err)
			defer roTx.Rollback()
		}
		for keyNum := uint64(1); keyNum <= uint64(31); keyNum++ {
			valNum := txNum / keyNum
			var k [8]byte
			var v [8]byte
			label := fmt.Sprintf("txNum=%d, keyNum=%d", txNum, keyNum)
			binary.BigEndian.PutUint64(k[:], keyNum)
			binary.BigEndian.PutUint64(v[:], valNum)
			val, err := dc.GetBeforeTxNum(k[:], txNum+1, roTx)
			require.NoError(t, err, label)
			if txNum >= keyNum {
				require.Equal(t, v[:], val, label)
			} else {
				require.Nil(t, val, label)
			}
			if txNum == txs {
				val, err := dc.Get(k[:], nil, roTx)
				require.NoError(t, err)
				require.EqualValues(t, v[:], val)
			}
		}
	}
}

func TestHistory(t *testing.T) {
	logEvery := time.NewTicker(30 * time.Second)
	defer logEvery.Stop()
	_, db, d, txs := filledDomain(t)
	ctx := context.Background()
	tx, err := db.BeginRw(ctx)
	require.NoError(t, err)
	d.SetTx(tx)
	defer tx.Rollback()

	// Leave the last 2 aggregation steps un-collated
	for step := uint64(0); step < txs/d.aggregationStep-1; step++ {
		func() {
			c, err := d.collate(ctx, step, step*d.aggregationStep, (step+1)*d.aggregationStep, tx, logEvery)
			require.NoError(t, err)
			sf, err := d.buildFiles(ctx, step, c)
			require.NoError(t, err)
			d.integrateFiles(sf, step*d.aggregationStep, (step+1)*d.aggregationStep)

			err = d.prune(ctx, step, step*d.aggregationStep, (step+1)*d.aggregationStep, math.MaxUint64, logEvery)
			require.NoError(t, err)
		}()
	}
	err = tx.Commit()
	require.NoError(t, err)
	checkHistory(t, db, d, txs)
}

func TestIterationMultistep(t *testing.T) {
	logEvery := time.NewTicker(30 * time.Second)
	defer logEvery.Stop()
	_, db, d := testDbAndDomain(t, 5 /* prefixLen */)
	ctx := context.Background()
	tx, err := db.BeginRw(ctx)
	require.NoError(t, err)
	defer tx.Rollback()
	d.SetTx(tx)
	d.StartWrites("")
	defer d.FinishWrites()

	d.SetTxNum(2)
	err = d.Put([]byte("addr1"), []byte("loc1"), []byte("value1"))
	require.NoError(t, err)
	err = d.Put([]byte("addr1"), []byte("loc2"), []byte("value1"))
	require.NoError(t, err)
	err = d.Put([]byte("addr1"), []byte("loc3"), []byte("value1"))
	require.NoError(t, err)
	err = d.Put([]byte("addr2"), []byte("loc1"), []byte("value1"))
	require.NoError(t, err)
	err = d.Put([]byte("addr2"), []byte("loc2"), []byte("value1"))
	require.NoError(t, err)
	err = d.Put([]byte("addr3"), []byte("loc1"), []byte("value1"))
	require.NoError(t, err)
	err = d.Put([]byte("addr3"), []byte("loc2"), []byte("value1"))
	require.NoError(t, err)

	d.SetTxNum(2 + 16)
	err = d.Put([]byte("addr2"), []byte("loc1"), []byte("value1"))
	require.NoError(t, err)
	err = d.Put([]byte("addr2"), []byte("loc2"), []byte("value1"))
	require.NoError(t, err)
	err = d.Put([]byte("addr2"), []byte("loc3"), []byte("value1"))
	require.NoError(t, err)
	err = d.Put([]byte("addr2"), []byte("loc4"), []byte("value1"))
	require.NoError(t, err)

	d.SetTxNum(2 + 16 + 16)
	err = d.Delete([]byte("addr2"), []byte("loc1"))
	require.NoError(t, err)

	err = d.Rotate().Flush(tx)
	require.NoError(t, err)

	for step := uint64(0); step <= 2; step++ {
		func() {
			c, err := d.collate(ctx, step, step*d.aggregationStep, (step+1)*d.aggregationStep, tx, logEvery)
			require.NoError(t, err)
			sf, err := d.buildFiles(ctx, step, c)
			require.NoError(t, err)
			d.integrateFiles(sf, step*d.aggregationStep, (step+1)*d.aggregationStep)
			err = d.prune(ctx, step, step*d.aggregationStep, (step+1)*d.aggregationStep, math.MaxUint64, logEvery)
			require.NoError(t, err)
		}()
	}

	var keys []string
	var vals []string
	err = d.MakeContext().IteratePrefix([]byte("addr2"), func(k, v []byte) {
		keys = append(keys, string(k))
		vals = append(vals, string(v))
	})
	require.NoError(t, err)
	require.Equal(t, []string{"addr2loc2", "addr2loc3", "addr2loc4"}, keys)
	require.Equal(t, []string{"value1", "value1", "value1"}, vals)
}

func collateAndMerge(t *testing.T, db kv.RwDB, tx kv.RwTx, d *Domain, txs uint64) {
	t.Helper()
	logEvery := time.NewTicker(30 * time.Second)
	defer logEvery.Stop()
	ctx := context.Background()
	var err error
	useExternalTx := tx != nil
	if !useExternalTx {
		tx, err = db.BeginRw(ctx)
		require.NoError(t, err)
		defer tx.Rollback()
	}
	d.SetTx(tx)
	// Leave the last 2 aggregation steps un-collated
	for step := uint64(0); step < txs/d.aggregationStep-1; step++ {
		func() {
			c, err := d.collate(ctx, step, step*d.aggregationStep, (step+1)*d.aggregationStep, tx, logEvery)
			require.NoError(t, err)
			sf, err := d.buildFiles(ctx, step, c)
			require.NoError(t, err)
			d.integrateFiles(sf, step*d.aggregationStep, (step+1)*d.aggregationStep)
			err = d.prune(ctx, step, step*d.aggregationStep, (step+1)*d.aggregationStep, math.MaxUint64, logEvery)
			require.NoError(t, err)
			var r DomainRanges
			maxEndTxNum := d.endTxNumMinimax()
			maxSpan := uint64(16 * 16)
			for r = d.findMergeRange(maxEndTxNum, maxSpan); r.any(); r = d.findMergeRange(maxEndTxNum, maxSpan) {
				valuesOuts, indexOuts, historyOuts, _ := d.staticFilesInRange(r)
				valuesIn, indexIn, historyIn, err := d.mergeFiles(ctx, valuesOuts, indexOuts, historyOuts, r, 1)
				require.NoError(t, err)
				d.integrateMergedFiles(valuesOuts, indexOuts, historyOuts, valuesIn, indexIn, historyIn)
				err = d.deleteFiles(valuesOuts, indexOuts, historyOuts)
				require.NoError(t, err)
			}
		}()
	}
	if !useExternalTx {
		err := tx.Commit()
		require.NoError(t, err)
	}
}

func collateAndMergeOnce(t *testing.T, d *Domain, step uint64) {
	t.Helper()
	logEvery := time.NewTicker(30 * time.Second)
	defer logEvery.Stop()
	ctx := context.Background()
	txFrom, txTo := (step)*d.aggregationStep, (step+1)*d.aggregationStep

	c, err := d.collate(ctx, step, txFrom, txTo, d.tx, logEvery)
	require.NoError(t, err)

	sf, err := d.buildFiles(ctx, step, c)
	require.NoError(t, err)
	d.integrateFiles(sf, txFrom, txTo)

	err = d.prune(ctx, step, txFrom, txTo, math.MaxUint64, logEvery)
	require.NoError(t, err)

	var r DomainRanges
	maxEndTxNum := d.endTxNumMinimax()
	maxSpan := d.aggregationStep * d.aggregationStep
	for r = d.findMergeRange(maxEndTxNum, maxSpan); r.any(); r = d.findMergeRange(maxEndTxNum, maxSpan) {
		valuesOuts, indexOuts, historyOuts, _ := d.staticFilesInRange(r)
		valuesIn, indexIn, historyIn, err := d.mergeFiles(ctx, valuesOuts, indexOuts, historyOuts, r, 1)
		require.NoError(t, err)

		d.integrateMergedFiles(valuesOuts, indexOuts, historyOuts, valuesIn, indexIn, historyIn)
		err = d.deleteFiles(valuesOuts, indexOuts, historyOuts)
		require.NoError(t, err)
	}
}

func TestMergeFiles(t *testing.T) {
	_, db, d, txs := filledDomain(t)
	defer db.Close()
	defer d.Close()

	collateAndMerge(t, db, nil, d, txs)
	checkHistory(t, db, d, txs)
}

func TestScanFiles(t *testing.T) {
	path, db, d, txs := filledDomain(t)
	defer db.Close()
	defer func() {
		d.Close()
	}()
	var err error
	var tx kv.RwTx
	defer func() {
		if tx != nil {
			tx.Rollback()
		}
	}()

	collateAndMerge(t, db, nil, d, txs)
	// Recreate domain and re-scan the files
	txNum := d.txNum
	d.Close()
	d, err = NewDomain(path, path, d.aggregationStep, d.filenameBase, d.keysTable, d.valsTable, d.indexKeysTable, d.historyValsTable, d.settingsTable, d.indexTable, d.prefixLen, d.compressVals)
	require.NoError(t, err)
	d.SetTxNum(txNum)
	// Check the history
	checkHistory(t, db, d, txs)
}

func TestDelete(t *testing.T) {
	_, db, d := testDbAndDomain(t, 0 /* prefixLen */)
	ctx := context.Background()
	tx, err := db.BeginRw(ctx)
	require.NoError(t, err)
	defer tx.Rollback()
	d.SetTx(tx)
	d.StartWrites("")
	defer d.FinishWrites()

	// Put on even txNum, delete on odd txNum
	for txNum := uint64(0); txNum < uint64(1000); txNum++ {
		d.SetTxNum(txNum)
		if txNum%2 == 0 {
			err = d.Put([]byte("key1"), nil, []byte("value1"))
		} else {
			err = d.Delete([]byte("key1"), nil)
		}
		require.NoError(t, err)
	}
	err = d.Rotate().Flush(tx)
	require.NoError(t, err)
	collateAndMerge(t, db, tx, d, 1000)
	// Check the history
	dc := d.MakeContext()
	for txNum := uint64(0); txNum < 1000; txNum++ {
		val, err := dc.GetBeforeTxNum([]byte("key1"), txNum+1, tx)
		require.NoError(t, err)
		label := fmt.Sprintf("txNum=%d", txNum)
		if txNum%2 == 0 {
			require.Equal(t, []byte("value1"), val, label)
		} else {
			require.Nil(t, val, label)
		}
		val, err = dc.GetBeforeTxNum([]byte("key2"), txNum+1, tx)
		require.NoError(t, err)
		require.Nil(t, val, label)
	}
}

func filledDomainFixedSize(t *testing.T, keysCount, txCount uint64) (string, kv.RwDB, *Domain, map[string][]bool) {
	t.Helper()
	path, db, d := testDbAndDomain(t, 0 /* prefixLen */)
	ctx := context.Background()
	tx, err := db.BeginRw(ctx)
	require.NoError(t, err)
	defer tx.Rollback()
	d.SetTx(tx)
	d.StartWrites("")
	defer d.FinishWrites()

	// keys are encodings of numbers 1..31
	// each key changes value on every txNum which is multiple of the key
	dat := make(map[string][]bool) // K:V is key -> list of bools. If list[i] == true, i'th txNum should persists

	for txNum := uint64(1); txNum <= txCount; txNum++ {
		d.SetTxNum(txNum)
		for keyNum := uint64(1); keyNum <= keysCount; keyNum++ {
			if keyNum == txNum%d.aggregationStep {
				continue
			}
			var k [8]byte
			var v [8]byte
			binary.BigEndian.PutUint64(k[:], keyNum)
			binary.BigEndian.PutUint64(v[:], txNum)
			err = d.Put(k[:], nil, v[:])
			require.NoError(t, err)

			if _, ok := dat[fmt.Sprintf("%d", keyNum)]; !ok {
				dat[fmt.Sprintf("%d", keyNum)] = make([]bool, txCount+1)
			}
			dat[fmt.Sprintf("%d", keyNum)][txNum] = true
		}
		if txNum%d.aggregationStep == 0 {
			err = d.Rotate().Flush(tx)
			require.NoError(t, err)
		}
	}
	err = tx.Commit()
	require.NoError(t, err)
	return path, db, d, dat
}

// firstly we write all the data to domain
// then we collate-merge-prune
// then check.
// in real life we periodically do collate-merge-prune without stopping adding data
func TestDomain_Prune_AfterAllWrites(t *testing.T) {
	keyCount, txCount := uint64(4), uint64(64)
	_, db, dom, data := filledDomainFixedSize(t, keyCount, txCount)
	defer db.Close()
	defer dom.Close()

	collateAndMerge(t, db, nil, dom, txCount)

	ctx := context.Background()
	roTx, err := db.BeginRo(ctx)
	require.NoError(t, err)
	defer roTx.Rollback()

	// Check the history
	dc := dom.MakeContext()
	for txNum := uint64(1); txNum <= txCount; txNum++ {
		for keyNum := uint64(1); keyNum <= keyCount; keyNum++ {
			var k [8]byte
			var v [8]byte
			label := fmt.Sprintf("txNum=%d, keyNum=%d\n", txNum, keyNum)
			binary.BigEndian.PutUint64(k[:], keyNum)
			binary.BigEndian.PutUint64(v[:], txNum)

			val, err := dc.GetBeforeTxNum(k[:], txNum+1, roTx)
			// during generation such keys are skipped so value should be nil for this call
			require.NoError(t, err, label)
			if !data[fmt.Sprintf("%d", keyNum)][txNum] {
				if txNum > 1 {
					binary.BigEndian.PutUint64(v[:], txNum-1)
				} else {
					require.Nil(t, val, label)
					continue
				}
			}
			require.EqualValues(t, v[:], val)
		}
	}

	var v [8]byte
	binary.BigEndian.PutUint64(v[:], txCount)

	for keyNum := uint64(1); keyNum <= keyCount; keyNum++ {
		var k [8]byte
		label := fmt.Sprintf("txNum=%d, keyNum=%d\n", txCount, keyNum)
		binary.BigEndian.PutUint64(k[:], keyNum)

		storedV, err := dc.Get(k[:], nil, roTx)
		require.NoError(t, err, label)
		require.EqualValues(t, v[:], storedV, label)
	}
}

func TestDomain_PruneOnWrite(t *testing.T) {
	keysCount, txCount := uint64(16), uint64(64)

	path, db, d := testDbAndDomain(t, 0 /* prefixLen */)
	ctx := context.Background()
	defer os.Remove(path)

	tx, err := db.BeginRw(ctx)
	require.NoError(t, err)
	defer tx.Rollback()
	d.SetTx(tx)
	d.StartWrites("")
	defer d.FinishWrites()

	// keys are encodings of numbers 1..31
	// each key changes value on every txNum which is multiple of the key
	data := make(map[string][]uint64)

	for txNum := uint64(1); txNum <= txCount; txNum++ {
		d.SetTxNum(txNum)
		for keyNum := uint64(1); keyNum <= keysCount; keyNum++ {
			if keyNum == txNum%d.aggregationStep {
				continue
			}
			var k [8]byte
			var v [8]byte
			binary.BigEndian.PutUint64(k[:], keyNum)
			binary.BigEndian.PutUint64(v[:], txNum)
			err = d.Put(k[:], nil, v[:])
			require.NoError(t, err)

			list, ok := data[fmt.Sprintf("%d", keyNum)]
			if !ok {
				data[fmt.Sprintf("%d", keyNum)] = make([]uint64, 0)
			}
			data[fmt.Sprintf("%d", keyNum)] = append(list, txNum)
		}
		if txNum%d.aggregationStep == 0 {
			step := txNum/d.aggregationStep - 1
			if step == 0 {
				continue
			}
			step--
			err = d.Rotate().Flush(tx)
			require.NoError(t, err)

			collateAndMergeOnce(t, d, step)
		}
	}
	err = d.Rotate().Flush(tx)
	require.NoError(t, err)

	// Check the history
	dc := d.MakeContext()
	for txNum := uint64(1); txNum <= txCount; txNum++ {
		for keyNum := uint64(1); keyNum <= keysCount; keyNum++ {
			valNum := txNum
			var k [8]byte
			var v [8]byte
			label := fmt.Sprintf("txNum=%d, keyNum=%d\n", txNum, keyNum)
			binary.BigEndian.PutUint64(k[:], keyNum)
			binary.BigEndian.PutUint64(v[:], valNum)

			val, err := dc.GetBeforeTxNum(k[:], txNum+1, tx)
			if keyNum == txNum%d.aggregationStep {
				if txNum > 1 {
					binary.BigEndian.PutUint64(v[:], txNum-1)
					require.EqualValues(t, v[:], val)
					continue
				} else {
					require.Nil(t, val, label)
					continue
				}
			}
			require.NoError(t, err, label)
			require.EqualValues(t, v[:], val, label)
		}
	}

	var v [8]byte
	binary.BigEndian.PutUint64(v[:], txCount)

	for keyNum := uint64(1); keyNum <= keysCount; keyNum++ {
		var k [8]byte
		label := fmt.Sprintf("txNum=%d, keyNum=%d\n", txCount, keyNum)
		binary.BigEndian.PutUint64(k[:], keyNum)

		storedV, err := dc.Get(k[:], nil, tx)
		require.NoError(t, err, label)
		require.EqualValues(t, v[:], storedV, label)
	}
}

func TestScanStaticFilesD(t *testing.T) {
	ii := &Domain{History: &History{InvertedIndex: &InvertedIndex{filenameBase: "test", aggregationStep: 1}},
		files: btree.NewG[*filesItem](32, filesItemLess),
	}
	ffs := fstest.MapFS{
		"test.0-1.kv": {},
		"test.1-2.kv": {},
		"test.0-4.kv": {},
		"test.2-3.kv": {},
		"test.3-4.kv": {},
		"test.4-5.kv": {},
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
