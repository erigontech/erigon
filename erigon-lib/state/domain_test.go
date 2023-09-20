/*
   Copyright 2022 Erigon contributors

   Licensed under the Apache License, VerSsion 2.0 (the "License");
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
	"time"

	"github.com/ledgerwatch/erigon-lib/common/background"
	"github.com/ledgerwatch/log/v3"
	"github.com/stretchr/testify/require"
	btree2 "github.com/tidwall/btree"

	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/mdbx"
	"github.com/ledgerwatch/erigon-lib/recsplit"
)

func testDbAndDomain(t *testing.T, logger log.Logger) (string, kv.RwDB, *Domain) {
	t.Helper()
	path := t.TempDir()
	keysTable := "Keys"
	valsTable := "Vals"
	historyKeysTable := "HistoryKeys"
	historyValsTable := "HistoryVals"
	settingsTable := "Settings"
	indexTable := "Index"
	db := mdbx.NewMDBX(logger).InMem(path).WithTableCfg(func(defaultBuckets kv.TableCfg) kv.TableCfg {
		return kv.TableCfg{
			keysTable:        kv.TableCfgItem{Flags: kv.DupSort},
			valsTable:        kv.TableCfgItem{},
			historyKeysTable: kv.TableCfgItem{Flags: kv.DupSort},
			historyValsTable: kv.TableCfgItem{Flags: kv.DupSort},
			settingsTable:    kv.TableCfgItem{},
			indexTable:       kv.TableCfgItem{Flags: kv.DupSort},
		}
	}).MustOpen()
	t.Cleanup(db.Close)
	d, err := NewDomain(path, path, 16, "base", keysTable, valsTable, historyKeysTable, historyValsTable, indexTable, true, false, logger)
	require.NoError(t, err)
	t.Cleanup(d.Close)
	d.DisableFsync()
	return path, db, d
}

// btree index should work correctly if K < m
func TestCollationBuild(t *testing.T) {
	logger := log.New()
	logEvery := time.NewTicker(30 * time.Second)
	defer logEvery.Stop()
	_, db, d := testDbAndDomain(t, logger)
	ctx := context.Background()
	defer d.Close()

	tx, err := db.BeginRw(ctx)
	require.NoError(t, err)
	defer tx.Rollback()
	d.SetTx(tx)
	d.StartWrites()
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

	err = d.Rotate().Flush(ctx, tx)
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

	sf, err := d.buildFiles(ctx, 0, c, background.NewProgressSet())
	require.NoError(t, err)
	defer sf.Close()
	c.Close()

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
	defer r.Close()
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
	logger := log.New()
	_, db, d := testDbAndDomain(t, logger)
	ctx := context.Background()
	tx, err := db.BeginRw(ctx)
	require.NoError(t, err)
	defer tx.Rollback()
	d.SetTx(tx)
	d.StartWrites()
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
	dc := d.MakeContext()
	defer dc.Close()
	err = dc.IteratePrefix([]byte("addr2"), func(k, v []byte) {
		keys = append(keys, string(k))
		vals = append(vals, string(v))
	})
	require.NoError(t, err)
	require.Equal(t, []string{"addr2loc1", "addr2loc2"}, keys)
	require.Equal(t, []string{"value1", "value1"}, vals)
}

func TestAfterPrune(t *testing.T) {
	logger := log.New()
	logEvery := time.NewTicker(30 * time.Second)
	defer logEvery.Stop()
	_, db, d := testDbAndDomain(t, logger)
	ctx := context.Background()

	tx, err := db.BeginRw(ctx)
	require.NoError(t, err)
	defer tx.Rollback()
	d.SetTx(tx)
	d.StartWrites()
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

	err = d.Rotate().Flush(ctx, tx)
	require.NoError(t, err)

	c, err := d.collate(ctx, 0, 0, 16, tx, logEvery)
	require.NoError(t, err)

	sf, err := d.buildFiles(ctx, 0, c, background.NewProgressSet())
	require.NoError(t, err)

	d.integrateFiles(sf, 0, 16)
	var v []byte
	dc := d.MakeContext()
	defer dc.Close()
	v, err = dc.Get([]byte("key1"), nil, tx)
	require.NoError(t, err)
	require.Equal(t, []byte("value1.3"), v)
	v, err = dc.Get([]byte("key2"), nil, tx)
	require.NoError(t, err)
	require.Equal(t, []byte("value2.2"), v)

	err = d.prune(ctx, 0, 0, 16, math.MaxUint64, logEvery)
	require.NoError(t, err)

	isEmpty, err := d.isEmpty(tx)
	require.NoError(t, err)
	require.False(t, isEmpty)

	v, err = dc.Get([]byte("key1"), nil, tx)
	require.NoError(t, err)
	require.Equal(t, []byte("value1.3"), v)
	v, err = dc.Get([]byte("key2"), nil, tx)
	require.NoError(t, err)
	require.Equal(t, []byte("value2.2"), v)
}

func filledDomain(t *testing.T, logger log.Logger) (string, kv.RwDB, *Domain, uint64) {
	t.Helper()
	path, db, d := testDbAndDomain(t, logger)
	ctx := context.Background()
	tx, err := db.BeginRw(ctx)
	require.NoError(t, err)
	defer tx.Rollback()
	d.SetTx(tx)
	d.StartWrites()
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
			err = d.Rotate().Flush(ctx, tx)
			require.NoError(t, err)
		}
	}
	err = d.Rotate().Flush(ctx, tx)
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
	defer dc.Close()
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
	logger := log.New()
	logEvery := time.NewTicker(30 * time.Second)
	defer logEvery.Stop()
	_, db, d, txs := filledDomain(t, logger)
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
			sf, err := d.buildFiles(ctx, step, c, background.NewProgressSet())
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
	logger := log.New()
	logEvery := time.NewTicker(30 * time.Second)
	defer logEvery.Stop()
	_, db, d := testDbAndDomain(t, logger)
	ctx := context.Background()
	tx, err := db.BeginRw(ctx)
	require.NoError(t, err)
	defer tx.Rollback()
	d.SetTx(tx)
	d.StartWrites()
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

	err = d.Rotate().Flush(ctx, tx)
	require.NoError(t, err)

	for step := uint64(0); step <= 2; step++ {
		func() {
			c, err := d.collate(ctx, step, step*d.aggregationStep, (step+1)*d.aggregationStep, tx, logEvery)
			require.NoError(t, err)
			sf, err := d.buildFiles(ctx, step, c, background.NewProgressSet())
			require.NoError(t, err)
			d.integrateFiles(sf, step*d.aggregationStep, (step+1)*d.aggregationStep)
			err = d.prune(ctx, step, step*d.aggregationStep, (step+1)*d.aggregationStep, math.MaxUint64, logEvery)
			require.NoError(t, err)
		}()
	}

	var keys []string
	var vals []string
	dc := d.MakeContext()
	defer dc.Close()
	err = dc.IteratePrefix([]byte("addr2"), func(k, v []byte) {
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
		c, err := d.collate(ctx, step, step*d.aggregationStep, (step+1)*d.aggregationStep, tx, logEvery)
		require.NoError(t, err)
		sf, err := d.buildFiles(ctx, step, c, background.NewProgressSet())
		require.NoError(t, err)
		d.integrateFiles(sf, step*d.aggregationStep, (step+1)*d.aggregationStep)
		err = d.prune(ctx, step, step*d.aggregationStep, (step+1)*d.aggregationStep, math.MaxUint64, logEvery)
		require.NoError(t, err)
	}
	var r DomainRanges
	maxEndTxNum := d.endTxNumMinimax()
	maxSpan := d.aggregationStep * StepsInBiggestFile

	for {
		if stop := func() bool {
			dc := d.MakeContext()
			defer dc.Close()
			r = d.findMergeRange(maxEndTxNum, maxSpan)
			if !r.any() {
				return true
			}
			valuesOuts, indexOuts, historyOuts, _ := dc.staticFilesInRange(r)
			valuesIn, indexIn, historyIn, err := d.mergeFiles(ctx, valuesOuts, indexOuts, historyOuts, r, 1, background.NewProgressSet())
			require.NoError(t, err)
			d.integrateMergedFiles(valuesOuts, indexOuts, historyOuts, valuesIn, indexIn, historyIn)
			return false
		}(); stop {
			break
		}
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

	sf, err := d.buildFiles(ctx, step, c, background.NewProgressSet())
	require.NoError(t, err)
	d.integrateFiles(sf, txFrom, txTo)

	err = d.prune(ctx, step, txFrom, txTo, math.MaxUint64, logEvery)
	require.NoError(t, err)

	var r DomainRanges
	maxEndTxNum := d.endTxNumMinimax()
	maxSpan := d.aggregationStep * StepsInBiggestFile
	for r = d.findMergeRange(maxEndTxNum, maxSpan); r.any(); r = d.findMergeRange(maxEndTxNum, maxSpan) {
		dc := d.MakeContext()
		valuesOuts, indexOuts, historyOuts, _ := dc.staticFilesInRange(r)
		valuesIn, indexIn, historyIn, err := d.mergeFiles(ctx, valuesOuts, indexOuts, historyOuts, r, 1, background.NewProgressSet())
		require.NoError(t, err)

		d.integrateMergedFiles(valuesOuts, indexOuts, historyOuts, valuesIn, indexIn, historyIn)
		dc.Close()
	}
}

func TestDomain_MergeFiles(t *testing.T) {
	logger := log.New()
	_, db, d, txs := filledDomain(t, logger)

	collateAndMerge(t, db, nil, d, txs)
	checkHistory(t, db, d, txs)
}

func TestDomain_ScanFiles(t *testing.T) {
	logger := log.New()
	path, db, d, txs := filledDomain(t, logger)
	_ = path
	collateAndMerge(t, db, nil, d, txs)
	// Recreate domain and re-scan the files
	txNum := d.txNum
	d.closeWhatNotInList([]string{})
	d.OpenFolder()

	d.SetTxNum(txNum)
	// Check the history
	checkHistory(t, db, d, txs)
}

func TestDomain_Delete(t *testing.T) {
	logger := log.New()
	_, db, d := testDbAndDomain(t, logger)
	ctx, require := context.Background(), require.New(t)
	tx, err := db.BeginRw(ctx)
	require.NoError(err)
	defer tx.Rollback()
	d.SetTx(tx)
	d.StartWrites()
	defer d.FinishWrites()

	// Put on even txNum, delete on odd txNum
	for txNum := uint64(0); txNum < uint64(1000); txNum++ {
		d.SetTxNum(txNum)
		if txNum%2 == 0 {
			err = d.Put([]byte("key1"), nil, []byte("value1"))
		} else {
			err = d.Delete([]byte("key1"), nil)
		}
		require.NoError(err)
	}
	err = d.Rotate().Flush(ctx, tx)
	require.NoError(err)
	collateAndMerge(t, db, tx, d, 1000)
	// Check the history
	dc := d.MakeContext()
	defer dc.Close()
	for txNum := uint64(0); txNum < 1000; txNum++ {
		label := fmt.Sprintf("txNum=%d", txNum)
		//val, ok, err := dc.GetBeforeTxNum([]byte("key1"), txNum+1, tx)
		//require.NoError(err)
		//require.True(ok)
		//if txNum%2 == 0 {
		//	require.Equal([]byte("value1"), val, label)
		//} else {
		//	require.Nil(val, label)
		//}
		//if txNum == 976 {
		val, err := dc.GetBeforeTxNum([]byte("key2"), txNum+1, tx)
		require.NoError(err)
		//require.False(ok, label)
		require.Nil(val, label)
		//}
	}
}

func filledDomainFixedSize(t *testing.T, keysCount, txCount uint64, logger log.Logger) (string, kv.RwDB, *Domain, map[string][]bool) {
	t.Helper()
	path, db, d := testDbAndDomain(t, logger)
	ctx := context.Background()
	tx, err := db.BeginRw(ctx)
	require.NoError(t, err)
	defer tx.Rollback()
	d.SetTx(tx)
	d.StartWrites()
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
			err = d.Rotate().Flush(ctx, tx)
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
	logger := log.New()
	keyCount, txCount := uint64(4), uint64(64)
	_, db, dom, data := filledDomainFixedSize(t, keyCount, txCount, logger)

	collateAndMerge(t, db, nil, dom, txCount)

	ctx := context.Background()
	roTx, err := db.BeginRo(ctx)
	require.NoError(t, err)
	defer roTx.Rollback()

	// Check the history
	dc := dom.MakeContext()
	defer dc.Close()
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
	logger := log.New()
	keysCount, txCount := uint64(16), uint64(64)

	path, db, d := testDbAndDomain(t, logger)
	ctx := context.Background()
	defer os.Remove(path)

	tx, err := db.BeginRw(ctx)
	require.NoError(t, err)
	defer tx.Rollback()
	d.SetTx(tx)
	d.StartWrites()
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
			err = d.Rotate().Flush(ctx, tx)
			require.NoError(t, err)

			collateAndMergeOnce(t, d, step)
		}
	}
	err = d.Rotate().Flush(ctx, tx)
	require.NoError(t, err)

	// Check the history
	dc := d.MakeContext()
	defer dc.Close()
	for txNum := uint64(1); txNum <= txCount; txNum++ {
		for keyNum := uint64(1); keyNum <= keysCount; keyNum++ {
			valNum := txNum
			var k [8]byte
			var v [8]byte
			label := fmt.Sprintf("txNum=%d, keyNum=%d\n", txNum, keyNum)
			binary.BigEndian.PutUint64(k[:], keyNum)
			binary.BigEndian.PutUint64(v[:], valNum)

			val, err := dc.GetBeforeTxNum(k[:], txNum+1, tx)
			require.NoError(t, err)
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
	logger := log.New()
	ii := &Domain{History: &History{InvertedIndex: &InvertedIndex{filenameBase: "test", aggregationStep: 1, logger: logger}, logger: logger},
		files:  btree2.NewBTreeG[*filesItem](filesItemLess),
		logger: logger,
	}
	files := []string{
		"test.0-1.kv",
		"test.1-2.kv",
		"test.0-4.kv",
		"test.2-3.kv",
		"test.3-4.kv",
		"test.4-5.kv",
	}
	ii.scanStateFiles(files)
	var found []string
	ii.files.Walk(func(items []*filesItem) bool {
		for _, item := range items {
			found = append(found, fmt.Sprintf("%d-%d", item.startTxNum, item.endTxNum))
		}
		return true
	})
	require.Equal(t, 6, len(found))
}
