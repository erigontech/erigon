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
	"strings"
	"testing"

	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/mdbx"
	"github.com/ledgerwatch/erigon-lib/recsplit"
	"github.com/ledgerwatch/erigon-lib/recsplit/eliasfano32"
	"github.com/ledgerwatch/log/v3"
	"github.com/stretchr/testify/require"
)

func testDbAndDomain(t *testing.T, prefixLen int) (kv.RwDB, *Domain) {
	t.Helper()
	path := t.TempDir()
	logger := log.New()
	keysTable := "Keys"
	valsTable := "Vals"
	historyKeysTable := "HistoryKeys"
	historyValsTable := "HistoryVals"
	settingsTable := "Settings"
	indexTable := "Index"
	db := mdbx.NewMDBX(logger).Path(path).WithTablessCfg(func(defaultBuckets kv.TableCfg) kv.TableCfg {
		return kv.TableCfg{
			keysTable:        kv.TableCfgItem{Flags: kv.DupSort},
			valsTable:        kv.TableCfgItem{},
			historyKeysTable: kv.TableCfgItem{Flags: kv.DupSort},
			historyValsTable: kv.TableCfgItem{},
			settingsTable:    kv.TableCfgItem{},
			indexTable:       kv.TableCfgItem{Flags: kv.DupSort},
		}
	}).MustOpen()
	d, err := NewDomain(path, 16 /* aggregationStep */, "base" /* filenameBase */, keysTable, valsTable, historyKeysTable, historyValsTable, settingsTable, indexTable, prefixLen)
	require.NoError(t, err)
	return db, d
}

func TestCollationBuild(t *testing.T) {
	db, d := testDbAndDomain(t, 0 /* prefixLen */)
	defer db.Close()
	defer d.Close()
	tx, err := db.BeginRw(context.Background())
	require.NoError(t, err)
	defer tx.Rollback()
	d.SetTx(tx)

	d.SetTxNum(2)
	err = d.Put([]byte("key1"), []byte("value1.1"))
	require.NoError(t, err)

	d.SetTxNum(3)
	err = d.Put([]byte("key2"), []byte("value2.1"))
	require.NoError(t, err)

	d.SetTxNum(6)
	err = d.Put([]byte("key1"), []byte("value1.2"))
	require.NoError(t, err)

	err = tx.Commit()
	require.NoError(t, err)

	roTx, err := db.BeginRo(context.Background())
	require.NoError(t, err)
	defer roTx.Rollback()

	c, err := d.collate(0, 0, 7, roTx)
	require.NoError(t, err)
	require.True(t, strings.HasSuffix(c.valuesPath, "base-values.0-16.dat"))
	require.Equal(t, 2, c.valuesCount)
	require.True(t, strings.HasSuffix(c.historyPath, "base-history.0-16.dat"))
	require.Equal(t, 3, c.historyCount)
	require.Equal(t, 2, len(c.indexBitmaps))
	require.Equal(t, []uint64{3}, c.indexBitmaps["key2"].ToArray())
	require.Equal(t, []uint64{2, 6}, c.indexBitmaps["key1"].ToArray())

	sf, err := d.buildFiles(0, c)
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
	g = sf.historyDecomp.MakeGetter()
	g.Reset(0)
	words = words[:0]
	for g.HasNext() {
		w, _ := g.Next(nil)
		words = append(words, string(w))
	}
	require.Equal(t, []string{"\x00\x00\x00\x00\x00\x00\x00\x02key1", "", "\x00\x00\x00\x00\x00\x00\x00\x03key2", "", "\x00\x00\x00\x00\x00\x00\x00\x06key1", "value1.1"}, words)
	require.Equal(t, 3, int(sf.historyIdx.KeyCount()))
	r = recsplit.NewIndexReader(sf.historyIdx)
	for i := 0; i < len(words); i += 2 {
		offset := r.Lookup([]byte(words[i]))
		g.Reset(offset)
		w, _ := g.Next(nil)
		require.Equal(t, words[i+1], string(w))
	}
	g = sf.efHistoryDecomp.MakeGetter()
	g.Reset(0)
	words = words[:0]
	var intArrs [][]uint64
	for g.HasNext() {
		w, _ := g.Next(nil)
		words = append(words, string(w))
		w, _ = g.Next(w[:0])
		ef, _ := eliasfano32.ReadEliasFano(w)
		var ints []uint64
		it := ef.Iterator()
		for it.HasNext() {
			ints = append(ints, it.Next())
		}
		intArrs = append(intArrs, ints)
	}
	require.Equal(t, []string{"key1", "key2"}, words)
	require.Equal(t, [][]uint64{{2, 6}, {3}}, intArrs)
	r = recsplit.NewIndexReader(sf.efHistoryIdx)
	for i := 0; i < len(words); i++ {
		offset := r.Lookup([]byte(words[i]))
		g.Reset(offset)
		w, _ := g.Next(nil)
		require.Equal(t, words[i], string(w))
	}
}

func TestIterationBasic(t *testing.T) {
	db, d := testDbAndDomain(t, 5 /* prefixLen */)
	defer db.Close()
	defer d.Close()
	tx, err := db.BeginRw(context.Background())
	require.NoError(t, err)
	defer tx.Rollback()
	d.SetTx(tx)

	d.SetTxNum(2)
	err = d.Put([]byte("addr1loc1"), []byte("value1"))
	require.NoError(t, err)
	err = d.Put([]byte("addr1loc2"), []byte("value1"))
	require.NoError(t, err)
	err = d.Put([]byte("addr1loc3"), []byte("value1"))
	require.NoError(t, err)
	err = d.Put([]byte("addr2loc1"), []byte("value1"))
	require.NoError(t, err)
	err = d.Put([]byte("addr2loc2"), []byte("value1"))
	require.NoError(t, err)
	err = d.Put([]byte("addr3loc1"), []byte("value1"))
	require.NoError(t, err)
	err = d.Put([]byte("addr3loc2"), []byte("value1"))
	require.NoError(t, err)

	var keys, vals []string
	err = d.IteratePrefix([]byte("addr2"), func(k, v []byte) {
		keys = append(keys, string(k))
		vals = append(vals, string(v))
	})
	require.NoError(t, err)
	require.Equal(t, []string{"addr2loc1", "addr2loc2"}, keys)
	require.Equal(t, []string{"value1", "value1"}, vals)
}

func TestAfterPrune(t *testing.T) {
	db, d := testDbAndDomain(t, 0 /* prefixLen */)
	defer db.Close()
	defer d.Close()
	tx, err := db.BeginRw(context.Background())
	require.NoError(t, err)
	defer func() {
		if tx != nil {
			tx.Rollback()
		}
	}()
	d.SetTx(tx)

	d.SetTxNum(2)
	err = d.Put([]byte("key1"), []byte("value1.1"))
	require.NoError(t, err)

	d.SetTxNum(3)
	err = d.Put([]byte("key2"), []byte("value2.1"))
	require.NoError(t, err)

	d.SetTxNum(6)
	err = d.Put([]byte("key1"), []byte("value1.2"))
	require.NoError(t, err)

	err = tx.Commit()
	require.NoError(t, err)

	roTx, err := db.BeginRo(context.Background())
	require.NoError(t, err)
	defer roTx.Rollback()

	c, err := d.collate(0, 0, 16, roTx)
	require.NoError(t, err)

	sf, err := d.buildFiles(0, c)
	require.NoError(t, err)
	defer sf.Close()

	tx, err = db.BeginRw(context.Background())
	require.NoError(t, err)
	d.SetTx(tx)

	d.integrateFiles(sf, 0, 16)
	var v []byte
	v, err = d.Get([]byte("key1"))
	require.NoError(t, err)
	require.Equal(t, []byte("value1.2"), v)
	v, err = d.Get([]byte("key2"))
	require.NoError(t, err)
	require.Equal(t, []byte("value2.1"), v)

	err = d.prune(0, 0, 16)
	require.NoError(t, err)
	err = tx.Commit()
	require.NoError(t, err)
	tx, err = db.BeginRw(context.Background())
	require.NoError(t, err)
	d.SetTx(tx)

	for _, table := range []string{d.keysTable, d.valsTable, d.historyKeysTable, d.historyValsTable, d.indexTable} {
		var cur kv.Cursor
		cur, err = tx.Cursor(table)
		require.NoError(t, err)
		defer cur.Close()
		var k []byte
		k, _, err = cur.First()
		require.NoError(t, err)
		require.Nil(t, k, table)
	}

	v, err = d.Get([]byte("key1"))
	require.NoError(t, err)
	require.Equal(t, []byte("value1.2"), v)
	v, err = d.Get([]byte("key2"))
	require.NoError(t, err)
	require.Equal(t, []byte("value2.1"), v)
}

func TestHistory(t *testing.T) {
	db, d := testDbAndDomain(t, 0 /* prefixLen */)
	defer db.Close()
	defer d.Close()
	tx, err := db.BeginRw(context.Background())
	require.NoError(t, err)
	defer func() {
		if tx != nil {
			tx.Rollback()
		}
	}()
	d.SetTx(tx)
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
				err = d.Put(k[:], v[:])
				require.NoError(t, err)
			}
		}
		if txNum%10 == 0 {
			err = tx.Commit()
			require.NoError(t, err)
			tx, err = db.BeginRw(context.Background())
			require.NoError(t, err)
			d.SetTx(tx)
		}
	}
	err = tx.Commit()
	require.NoError(t, err)

	// Leave the last 2 aggregation steps un-collated
	for step := uint64(0); step < txs/d.aggregationStep-1; step++ {
		func() {
			require.NoError(t, err)
			roTx, err := db.BeginRo(context.Background())
			require.NoError(t, err)
			c, err := d.collate(step, step*d.aggregationStep, (step+1)*d.aggregationStep, roTx)
			roTx.Rollback()
			require.NoError(t, err)
			sf, err := d.buildFiles(step, c)
			require.NoError(t, err)
			d.integrateFiles(sf, step*d.aggregationStep, (step+1)*d.aggregationStep)
			tx, err = db.BeginRw(context.Background())
			require.NoError(t, err)
			d.SetTx(tx)
			d.prune(step, step*d.aggregationStep, (step+1)*d.aggregationStep)
			err = tx.Commit()
			require.NoError(t, err)
		}()
	}
	// Check the history
	tx, err = db.BeginRw(context.Background())
	require.NoError(t, err)
	d.SetTx(tx)
	for txNum := uint64(1); txNum <= txs; txNum++ {
		d.SetTxNum(txNum)
		for keyNum := uint64(1); keyNum <= uint64(31); keyNum++ {
			if txNum%keyNum == 0 {
				valNum := txNum / keyNum
				var k [8]byte
				var v [8]byte
				label := fmt.Sprintf("txNum=%d, keyNum=%d", txNum, keyNum)
				binary.BigEndian.PutUint64(k[:], keyNum)
				binary.BigEndian.PutUint64(v[:], valNum)
				val, err := d.getAfterTxNum(k[:], txNum)
				require.NoError(t, err, label)
				require.Equal(t, v[:], val, label)
			}
		}
	}
}

func TestIterationMultistep(t *testing.T) {
	db, d := testDbAndDomain(t, 5 /* prefixLen */)
	defer db.Close()
	defer d.Close()
	tx, err := db.BeginRw(context.Background())
	require.NoError(t, err)
	defer func() {
		if tx != nil {
			tx.Rollback()
		}
	}()
	d.SetTx(tx)

	d.SetTxNum(2)
	err = d.Put([]byte("addr1loc1"), []byte("value1"))
	require.NoError(t, err)
	err = d.Put([]byte("addr1loc2"), []byte("value1"))
	require.NoError(t, err)
	err = d.Put([]byte("addr1loc3"), []byte("value1"))
	require.NoError(t, err)
	err = d.Put([]byte("addr2loc1"), []byte("value1"))
	require.NoError(t, err)
	err = d.Put([]byte("addr2loc2"), []byte("value1"))
	require.NoError(t, err)
	err = d.Put([]byte("addr3loc1"), []byte("value1"))
	require.NoError(t, err)
	err = d.Put([]byte("addr3loc2"), []byte("value1"))
	require.NoError(t, err)

	d.SetTxNum(2 + 16)
	err = d.Put([]byte("addr2loc1"), []byte("value1"))
	require.NoError(t, err)
	err = d.Put([]byte("addr2loc2"), []byte("value1"))
	require.NoError(t, err)
	err = d.Put([]byte("addr2loc3"), []byte("value1"))
	require.NoError(t, err)
	err = d.Put([]byte("addr2loc4"), []byte("value1"))
	require.NoError(t, err)

	d.SetTxNum(2 + 16 + 16)
	err = d.Delete([]byte("addr2loc1"))
	require.NoError(t, err)

	err = tx.Commit()
	require.NoError(t, err)

	for step := uint64(0); step <= 2; step++ {
		func() {
			require.NoError(t, err)
			roTx, err := db.BeginRo(context.Background())
			require.NoError(t, err)
			c, err := d.collate(step, step*d.aggregationStep, (step+1)*d.aggregationStep, roTx)
			roTx.Rollback()
			require.NoError(t, err)
			sf, err := d.buildFiles(step, c)
			require.NoError(t, err)
			d.integrateFiles(sf, step*d.aggregationStep, (step+1)*d.aggregationStep)
			tx, err = db.BeginRw(context.Background())
			require.NoError(t, err)
			d.SetTx(tx)
			d.prune(step, step*d.aggregationStep, (step+1)*d.aggregationStep)
			err = tx.Commit()
			require.NoError(t, err)
		}()
	}

	tx, err = db.BeginRw(context.Background())
	require.NoError(t, err)
	d.SetTx(tx)

	var keys []string
	var vals []string
	err = d.IteratePrefix([]byte("addr2"), func(k, v []byte) {
		keys = append(keys, string(k))
		vals = append(vals, string(v))
	})
	require.NoError(t, err)
	require.Equal(t, []string{"addr2loc2", "addr2loc3", "addr2loc4"}, keys)
	require.Equal(t, []string{"value1", "value1", "value1"}, vals)
}

func TestMergeFiles(t *testing.T) {
	db, d := testDbAndDomain(t, 0 /* prefixLen */)
	defer db.Close()
	defer d.Close()
	tx, err := db.BeginRw(context.Background())
	require.NoError(t, err)
	defer func() {
		if tx != nil {
			tx.Rollback()
		}
	}()
	d.SetTx(tx)
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
				err = d.Put(k[:], v[:])
				require.NoError(t, err)
			}
		}
		if txNum%10 == 0 {
			err = tx.Commit()
			require.NoError(t, err)
			tx, err = db.BeginRw(context.Background())
			require.NoError(t, err)
			d.SetTx(tx)
		}
	}
	err = tx.Commit()
	require.NoError(t, err)

	// Leave the last 2 aggregation steps un-collated
	for step := uint64(0); step < txs/d.aggregationStep-1; step++ {
		func() {
			require.NoError(t, err)
			roTx, err := db.BeginRo(context.Background())
			require.NoError(t, err)
			c, err := d.collate(step, step*d.aggregationStep, (step+1)*d.aggregationStep, roTx)
			roTx.Rollback()
			require.NoError(t, err)
			sf, err := d.buildFiles(step, c)
			require.NoError(t, err)
			d.integrateFiles(sf, step*d.aggregationStep, (step+1)*d.aggregationStep)
			tx, err = db.BeginRw(context.Background())
			require.NoError(t, err)
			d.SetTx(tx)
			d.prune(step, step*d.aggregationStep, (step+1)*d.aggregationStep)
			err = tx.Commit()
			require.NoError(t, err)
			var found bool
			var startTxNum, endTxNum uint64
			maxEndTxNum := d.endTxNumMinimax()
			maxSpan := uint64(16 * 16)
			for found, startTxNum, endTxNum = d.findMergeRange(maxEndTxNum, maxSpan); found; found, startTxNum, endTxNum = d.findMergeRange(maxEndTxNum, maxSpan) {
				outs, _ := d.staticFilesInRange(startTxNum, endTxNum)
				in, err := d.mergeFiles(outs, startTxNum, endTxNum, maxSpan)
				require.NoError(t, err)
				d.integrateMergedFiles(outs, in)
			}
		}()
	}
	// Check the history
	tx, err = db.BeginRw(context.Background())
	require.NoError(t, err)
	d.SetTx(tx)
	for txNum := uint64(1); txNum <= txs; txNum++ {
		d.SetTxNum(txNum)
		for keyNum := uint64(1); keyNum <= uint64(31); keyNum++ {
			if txNum%keyNum == 0 {
				valNum := txNum / keyNum
				var k [8]byte
				var v [8]byte
				label := fmt.Sprintf("txNum=%d, keyNum=%d", txNum, keyNum)
				binary.BigEndian.PutUint64(k[:], keyNum)
				binary.BigEndian.PutUint64(v[:], valNum)
				val, err := d.getAfterTxNum(k[:], txNum)
				require.NoError(t, err, label)
				require.Equal(t, v[:], val, label)
			}
		}
	}
}
