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
	"fmt"
	"io/fs"
	"math"
	"math/rand/v2"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"
	btree2 "github.com/tidwall/btree"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/background"
	datadir2 "github.com/erigontech/erigon-lib/common/datadir"
	"github.com/erigontech/erigon-lib/common/hexutility"
	"github.com/erigontech/erigon-lib/common/length"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/kv/mdbx"
	"github.com/erigontech/erigon-lib/kv/order"
	"github.com/erigontech/erigon-lib/kv/stream"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon-lib/seg"
	"github.com/erigontech/erigon-lib/types"
)

type rnd struct {
	src *rand.ChaCha8
	gen *rand.Rand
}

func newRnd(seed uint64) *rnd {
	_ = seed
	src := rand.NewChaCha8([32]byte{})
	return &rnd{src: src, gen: rand.New(src)}
}
func (r *rnd) Uint64N(n uint64) uint64          { return r.gen.Uint64N(n) }
func (r *rnd) Read(p []byte) (n int, err error) { return r.src.Read(p) }

func testDbAndDomain(t *testing.T, logger log.Logger) (kv.RwDB, *Domain) {
	t.Helper()
	return testDbAndDomainOfStep(t, 16, logger)
}

func testDbAndDomainOfStep(t *testing.T, aggStep uint64, logger log.Logger) (kv.RwDB, *Domain) {
	t.Helper()
	dirs := datadir2.New(t.TempDir())
	keysTable := "Keys"
	valsTable := "Vals"
	historyKeysTable := "HistoryKeys"
	historyValsTable := "HistoryVals"
	settingsTable := "Settings" //nolint
	indexTable := "Index"
	db := mdbx.NewMDBX(logger).InMem(dirs.Chaindata).WithTableCfg(func(defaultBuckets kv.TableCfg) kv.TableCfg {
		tcfg := kv.TableCfg{
			keysTable:             kv.TableCfgItem{Flags: kv.DupSort},
			valsTable:             kv.TableCfgItem{Flags: kv.DupSort},
			historyKeysTable:      kv.TableCfgItem{Flags: kv.DupSort},
			historyValsTable:      kv.TableCfgItem{Flags: kv.DupSort},
			settingsTable:         kv.TableCfgItem{},
			indexTable:            kv.TableCfgItem{Flags: kv.DupSort},
			kv.TblPruningProgress: kv.TableCfgItem{},
		}
		return tcfg
	}).MustOpen()
	t.Cleanup(db.Close)
	salt := uint32(1)
	cfg := domainCfg{
		hist: histCfg{
			iiCfg:             iiCfg{salt: &salt, dirs: dirs, db: db},
			withLocalityIndex: false, withExistenceIndex: false, compression: seg.CompressNone, historyLargeValues: true,
		}}
	d, err := NewDomain(cfg, aggStep, kv.AccountsDomain, valsTable, historyKeysTable, historyValsTable, indexTable, nil, logger)
	require.NoError(t, err)
	d.DisableFsync()
	t.Cleanup(d.Close)
	d.DisableFsync()
	return db, d
}

func TestDomain_CollationBuild(t *testing.T) {
	t.Parallel()

	t.Run("compressDomainVals=true", func(t *testing.T) {
		testCollationBuild(t, true)
	})
	t.Run("compressDomainVals=false", func(t *testing.T) {
		testCollationBuild(t, false)
	})
}

func TestDomain_OpenFolder(t *testing.T) {
	t.Parallel()

	db, d, txs := filledDomain(t, log.New())

	collateAndMerge(t, db, nil, d, txs)

	list := d._visible.files
	require.NotEmpty(t, list)
	ff := list[len(list)-1]
	fn := ff.src.decompressor.FilePath()
	d.Close()

	err := os.Remove(fn)
	require.NoError(t, err)
	err = os.WriteFile(fn, make([]byte, 33), 0644)
	require.NoError(t, err)

	err = d.openFolder()
	require.NoError(t, err)
	d.Close()
}

func testCollationBuild(t *testing.T, compressDomainVals bool) {
	t.Helper()

	logger := log.New()
	logEvery := time.NewTicker(30 * time.Second)
	defer logEvery.Stop()
	db, d := testDbAndDomainOfStep(t, 16, logger)
	ctx := context.Background()

	if compressDomainVals {
		d.compression = seg.CompressKeys | seg.CompressVals
	}

	tx, err := db.BeginRw(ctx)
	require.NoError(t, err)
	defer tx.Rollback()
	dc := d.BeginFilesRo()
	defer dc.Close()
	writer := dc.NewWriter()
	defer writer.close()

	writer.SetTxNum(2)

	var (
		k1     = []byte("key1")
		k2     = []byte("key2")
		v1     = []byte("value1.1")
		v2     = []byte("value2.1")
		p1, p2 []byte
	)

	err = writer.PutWithPrev(k1, nil, v1, p1, 0)
	require.NoError(t, err)

	writer.SetTxNum(3)
	err = writer.PutWithPrev(k2, nil, v2, p2, 0)
	require.NoError(t, err)

	p1, p2 = v1, v2
	_ = p2

	v1, v2 = []byte("value1.2"), []byte("value2.2") //nolint

	writer.SetTxNum(6)
	err = writer.PutWithPrev(k1, nil, v1, p1, 0)
	require.NoError(t, err)

	p1, v1 = v1, []byte("value1.3")
	writer.SetTxNum(d.aggregationStep + 2)
	err = writer.PutWithPrev(k1, nil, v1, p1, 0)
	require.NoError(t, err)

	p1, v1 = v1, []byte("value1.4")
	writer.SetTxNum(d.aggregationStep + 3)
	err = writer.PutWithPrev(k1, nil, v1, p1, 0)
	require.NoError(t, err)

	p1, v1 = v1, []byte("value1.5")
	expectedStep2 := uint64(2)
	writer.SetTxNum(expectedStep2*d.aggregationStep + 2)
	err = writer.PutWithPrev(k1, nil, v1, p1, 0)
	require.NoError(t, err)

	err = writer.Flush(ctx, tx)
	require.NoError(t, err)
	dc.Close()
	{
		c, err := d.collate(ctx, 0, 0, 16, tx)

		require.NoError(t, err)
		require.True(t, strings.HasSuffix(c.valuesPath, "v1-accounts.0-1.kv"))
		require.Equal(t, 2, c.valuesCount)
		require.True(t, strings.HasSuffix(c.historyPath, "v1-accounts.0-1.v"))
		require.Equal(t, 3, c.historyComp.Count())
		require.Equal(t, 2*c.valuesCount, c.efHistoryComp.Count())

		sf, err := d.buildFiles(ctx, 0, c, background.NewProgressSet())
		require.NoError(t, err)
		defer sf.CleanupOnError()
		c.Close()

		g := seg.NewReader(sf.valuesDecomp.MakeGetter(), d.compression)
		g.Reset(0)
		var words []string
		for g.HasNext() {
			w, _ := g.Next(nil)
			words = append(words, string(w))
		}
		require.Equal(t, []string{"key1", "value1.2", "key2", "value2.1"}, words)
		// Check index
		//require.Equal(t, 2, int(sf.valuesIdx.KeyCount()))
		require.Equal(t, 2, int(sf.valuesBt.KeyCount()))

		//r := recsplit.NewIndexReader(sf.valuesIdx)
		//defer r.Close()
		//for i := 0; i < len(words); i += 2 {
		//	offset, _ := r.Lookup([]byte(words[i]))
		//	g.Reset(offset)
		//	w, _ := g.Next(nil)
		//	require.Equal(t, words[i], string(w))
		//	w, _ = g.Next(nil)
		//	require.Equal(t, words[i+1], string(w))
		//}

		for i := 0; i < len(words); i += 2 {
			c, _ := sf.valuesBt.Seek(g, []byte(words[i]))
			require.Equal(t, words[i], string(c.Key()))
			require.Equal(t, words[i+1], string(c.Value()))
		}
	}
	{
		c, err := d.collate(ctx, 1, 1*d.aggregationStep, 2*d.aggregationStep, tx)
		require.NoError(t, err)
		sf, err := d.buildFiles(ctx, 1, c, background.NewProgressSet())
		require.NoError(t, err)
		defer sf.CleanupOnError()
		c.Close()

		g := seg.NewReader(sf.valuesDecomp.MakeGetter(), seg.CompressNone)
		g.Reset(0)
		var words []string
		for g.HasNext() {
			w, _ := g.Next(nil)
			words = append(words, string(w))
		}
		require.Equal(t, []string{"key1", "value1.4"}, words)
		// Check index
		require.Equal(t, 1, int(sf.valuesBt.KeyCount()))
		for i := 0; i < len(words); i += 2 {
			c, _ := sf.valuesBt.Seek(g, []byte(words[i]))
			require.Equal(t, words[i], string(c.Key()))
			require.Equal(t, words[i+1], string(c.Value()))
		}

		//require.Equal(t, 1, int(sf.valuesIdx.KeyCount()))
		//r := recsplit.NewIndexReader(sf.valuesIdx)
		//defer r.Close()
		//for i := 0; i < len(words); i += 2 {
		//	offset := r.Lookup([]byte(words[i]))
		//	g.Reset(offset)
		//	w, _ := g.Next(nil)
		//	require.Equal(t, words[i], string(w))
		//	w, _ = g.Next(nil)
		//	require.Equal(t, words[i+1], string(w))
		//}
	}
}

func TestDomain_AfterPrune(t *testing.T) {
	logger := log.New()
	logEvery := time.NewTicker(30 * time.Second)
	defer logEvery.Stop()
	db, d := testDbAndDomain(t, logger)
	ctx := context.Background()

	tx, err := db.BeginRw(ctx)
	require.NoError(t, err)
	defer tx.Rollback()
	dc := d.BeginFilesRo()
	defer d.Close()
	writer := dc.NewWriter()
	defer writer.close()

	var (
		k1 = []byte("key1")
		k2 = []byte("key2")
		p1 []byte
		p2 []byte

		n1, n2 = []byte("value1.1"), []byte("value2.1")
	)

	writer.SetTxNum(2)
	err = writer.PutWithPrev(k1, nil, n1, p1, 0)
	require.NoError(t, err)

	writer.SetTxNum(3)
	err = writer.PutWithPrev(k2, nil, n2, p2, 0)
	require.NoError(t, err)

	p1, p2 = n1, n2
	n1, n2 = []byte("value1.2"), []byte("value2.2")

	writer.SetTxNum(6)
	err = writer.PutWithPrev(k1, nil, n1, p1, 0)
	require.NoError(t, err)

	p1, n1 = n1, []byte("value1.3")

	writer.SetTxNum(17)
	err = writer.PutWithPrev(k1, nil, n1, p1, 0)
	require.NoError(t, err)

	p1 = n1

	writer.SetTxNum(18)
	err = writer.PutWithPrev(k2, nil, n2, p2, 0)
	require.NoError(t, err)
	p2 = n2

	err = writer.Flush(ctx, tx)
	require.NoError(t, err)

	c, err := d.collate(ctx, 0, 0, 16, tx)
	require.NoError(t, err)

	sf, err := d.buildFiles(ctx, 0, c, background.NewProgressSet())
	require.NoError(t, err)

	d.integrateDirtyFiles(sf, 0, 16)
	d.reCalcVisibleFiles(d.dirtyFilesEndTxNumMinimax())
	var v []byte
	dc = d.BeginFilesRo()
	defer dc.Close()
	v, _, found, err := dc.GetLatest(k1, nil, tx)
	require.Truef(t, found, "key1 not found")
	require.NoError(t, err)
	require.Equal(t, p1, v)
	v, _, found, err = dc.GetLatest(k2, nil, tx)
	require.Truef(t, found, "key2 not found")
	require.NoError(t, err)
	require.Equal(t, p2, v)

	_, err = dc.Prune(ctx, tx, 0, 0, 16, math.MaxUint64, logEvery)
	require.NoError(t, err)

	isEmpty, err := d.isEmpty(tx)
	require.NoError(t, err)
	require.False(t, isEmpty)

	v, _, found, err = dc.GetLatest(k1, nil, tx)
	require.NoError(t, err)
	require.Truef(t, found, "key1 not found")
	require.Equal(t, p1, v)

	v, _, found, err = dc.GetLatest(k2, nil, tx)
	require.NoError(t, err)
	require.Truef(t, found, "key2 not found")
	require.Equal(t, p2, v)
}

func filledDomain(t *testing.T, logger log.Logger) (kv.RwDB, *Domain, uint64) {
	t.Helper()
	require := require.New(t)
	db, d := testDbAndDomain(t, logger)
	ctx := context.Background()
	tx, err := db.BeginRw(ctx)
	require.NoError(err)
	defer tx.Rollback()

	txs := uint64(1000)

	dc := d.BeginFilesRo()
	defer dc.Close()
	writer := dc.NewWriter()
	defer writer.close()

	var prev [32][]byte
	// keys are encodings of numbers 1..31
	// each key changes value on every txNum which is multiple of the key
	for txNum := uint64(1); txNum <= txs; txNum++ {
		writer.SetTxNum(txNum)
		for keyNum := uint64(1); keyNum <= uint64(31); keyNum++ {
			if txNum%keyNum == 0 {
				valNum := txNum / keyNum
				var k [8]byte
				var v [8]byte
				binary.BigEndian.PutUint64(k[:], keyNum)
				binary.BigEndian.PutUint64(v[:], valNum)
				err = writer.PutWithPrev(k[:], nil, v[:], prev[keyNum], 0)
				prev[keyNum] = v[:]

				require.NoError(err)
			}
		}
		if txNum%10 == 0 {
			err = writer.Flush(ctx, tx)
			require.NoError(err)
		}
	}
	err = writer.Flush(ctx, tx)
	require.NoError(err)
	err = tx.Commit()
	require.NoError(err)
	return db, d, txs
}

func checkHistory(t *testing.T, db kv.RwDB, d *Domain, txs uint64) {
	t.Helper()
	fmt.Printf("txs: %d\n", txs)
	t.Helper()
	require := require.New(t)
	ctx := context.Background()
	var err error

	// Check the history
	dc := d.BeginFilesRo()
	defer dc.Close()
	roTx, err := db.BeginRo(ctx)
	require.NoError(err)
	defer roTx.Rollback()

	for txNum := uint64(0); txNum <= txs; txNum++ {
		for keyNum := uint64(1); keyNum <= uint64(31); keyNum++ {
			valNum := txNum / keyNum
			var k [8]byte
			var v [8]byte
			binary.BigEndian.PutUint64(k[:], keyNum)
			binary.BigEndian.PutUint64(v[:], valNum)

			label := fmt.Sprintf("key %x txNum=%d, keyNum=%d", k, txNum, keyNum)

			val, _, err := dc.GetAsOf(k[:], txNum+1, roTx)
			require.NoError(err, label)
			if txNum >= keyNum {
				require.Equal(v[:], val, label)
			} else {
				require.Nil(val, label)
			}
			if txNum == txs {
				val, _, found, err := dc.GetLatest(k[:], nil, roTx)
				require.True(found, label)
				require.NoError(err)
				require.EqualValues(v[:], val, label)
			}
		}
	}
}

func TestHistory(t *testing.T) {
	t.Parallel()

	logger := log.New()
	logEvery := time.NewTicker(30 * time.Second)
	defer logEvery.Stop()
	db, d, txs := filledDomain(t, logger)
	//ctx := context.Background()
	//tx, err := db.BeginRw(ctx)
	//require.NoError(t, err)
	//defer tx.Rollback()

	collateAndMerge(t, db, nil, d, txs)
	checkHistory(t, db, d, txs)
}

func collateAndMerge(t *testing.T, db kv.RwDB, tx kv.RwTx, d *Domain, txs uint64) {
	t.Helper()

	logEvery := time.NewTicker(30 * time.Second)
	defer logEvery.Stop()
	ctx := context.Background()
	var err error
	useExternalTx := tx != nil
	if !useExternalTx {
		tx, err = db.BeginRwNosync(ctx)
		require.NoError(t, err)
		defer tx.Rollback()
	}
	// Leave the last 2 aggregation steps un-collated
	for step := uint64(0); step < txs/d.aggregationStep-1; step++ {
		c, err := d.collate(ctx, step, step*d.aggregationStep, (step+1)*d.aggregationStep, tx)
		require.NoError(t, err)
		sf, err := d.buildFiles(ctx, step, c, background.NewProgressSet())
		require.NoError(t, err)
		d.integrateDirtyFiles(sf, step*d.aggregationStep, (step+1)*d.aggregationStep)
		d.reCalcVisibleFiles(d.dirtyFilesEndTxNumMinimax())

		dc := d.BeginFilesRo()
		_, err = dc.Prune(ctx, tx, step, step*d.aggregationStep, (step+1)*d.aggregationStep, math.MaxUint64, logEvery)
		dc.Close()
		require.NoError(t, err)
	}
	var r DomainRanges
	maxSpan := d.aggregationStep * StepsInColdFile

	for {
		if stop := func() bool {
			dc := d.BeginFilesRo()
			defer dc.Close()
			r = dc.findMergeRange(dc.files.EndTxNum(), maxSpan)
			if !r.any() {
				return true
			}
			valuesOuts, indexOuts, historyOuts := dc.staticFilesInRange(r)
			valuesIn, indexIn, historyIn, err := dc.mergeFiles(ctx, valuesOuts, indexOuts, historyOuts, r, nil, background.NewProgressSet())
			require.NoError(t, err)
			if valuesIn != nil && valuesIn.decompressor != nil {
				fmt.Printf("merge: %s\n", valuesIn.decompressor.FileName())
			}
			d.integrateMergedDirtyFiles(valuesOuts, indexOuts, historyOuts, valuesIn, indexIn, historyIn)
			d.reCalcVisibleFiles(d.dirtyFilesEndTxNumMinimax())
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

func collateAndMergeOnce(t *testing.T, d *Domain, tx kv.RwTx, step uint64, prune bool) {
	t.Helper()
	logEvery := time.NewTicker(30 * time.Second)
	defer logEvery.Stop()
	ctx := context.Background()
	txFrom, txTo := (step)*d.aggregationStep, (step+1)*d.aggregationStep

	c, err := d.collate(ctx, step, txFrom, txTo, tx)
	require.NoError(t, err)

	sf, err := d.buildFiles(ctx, step, c, background.NewProgressSet())
	require.NoError(t, err)
	d.integrateDirtyFiles(sf, txFrom, txTo)
	d.reCalcVisibleFiles(d.dirtyFilesEndTxNumMinimax())

	if prune {
		dc := d.BeginFilesRo()
		stat, err := dc.Prune(ctx, tx, step, txFrom, txTo, math.MaxUint64, logEvery)
		t.Logf("prune stat: %s  (%d-%d)", stat, txFrom, txTo)
		require.NoError(t, err)
		dc.Close()
	}

	maxSpan := d.aggregationStep * StepsInColdFile
	for {
		dc := d.BeginFilesRo()
		r := dc.findMergeRange(dc.files.EndTxNum(), maxSpan)
		if !r.any() {
			dc.Close()
			break
		}
		valuesOuts, indexOuts, historyOuts := dc.staticFilesInRange(r)
		valuesIn, indexIn, historyIn, err := dc.mergeFiles(ctx, valuesOuts, indexOuts, historyOuts, r, nil, background.NewProgressSet())
		require.NoError(t, err)

		d.integrateMergedDirtyFiles(valuesOuts, indexOuts, historyOuts, valuesIn, indexIn, historyIn)
		d.reCalcVisibleFiles(d.dirtyFilesEndTxNumMinimax())
		dc.Close()
	}
}

func TestDomain_MergeFiles(t *testing.T) {
	t.Parallel()

	logger := log.New()
	db, d, txs := filledDomain(t, logger)
	rwTx, err := db.BeginRw(context.Background())
	require.NoError(t, err)

	collateAndMerge(t, db, rwTx, d, txs)
	err = rwTx.Commit()
	require.NoError(t, err)
	checkHistory(t, db, d, txs)
}

func TestDomain_ScanFiles(t *testing.T) {
	t.Parallel()

	logger := log.New()
	db, d, txs := filledDomain(t, logger)
	collateAndMerge(t, db, nil, d, txs)
	// Recreate domain and re-scan the files
	dc := d.BeginFilesRo()
	defer dc.Close()
	d.closeWhatNotInList([]string{})
	require.NoError(t, d.openFolder())

	// Check the history
	checkHistory(t, db, d, txs)
}

func TestDomain_Delete(t *testing.T) {
	t.Parallel()

	logger := log.New()
	db, d := testDbAndDomain(t, logger)
	ctx, require := context.Background(), require.New(t)
	tx, err := db.BeginRw(ctx)
	require.NoError(err)
	defer tx.Rollback()
	dc := d.BeginFilesRo()
	defer dc.Close()
	writer := dc.NewWriter()
	defer writer.close()

	// Put on even txNum, delete on odd txNum
	for txNum := uint64(0); txNum < uint64(1000); txNum++ {
		writer.SetTxNum(txNum)
		original, originalStep, _, err := dc.GetLatest([]byte("key1"), nil, tx)
		require.NoError(err)
		if txNum%2 == 0 {
			err = writer.PutWithPrev([]byte("key1"), nil, []byte("value1"), original, originalStep)
		} else {
			err = writer.DeleteWithPrev([]byte("key1"), nil, original, originalStep)
		}
		require.NoError(err)
	}
	err = writer.Flush(ctx, tx)
	require.NoError(err)
	collateAndMerge(t, db, tx, d, 1000)
	dc.Close()

	// Check the history
	dc = d.BeginFilesRo()
	defer dc.Close()
	for txNum := uint64(0); txNum < 1000; txNum++ {
		label := fmt.Sprintf("txNum=%d", txNum)
		//val, ok, err := dc.GetLatestBeforeTxNum([]byte("key1"), txNum+1, tx)
		//require.NoError(err)
		//require.True(ok)
		//if txNum%2 == 0 {
		//	require.Equal([]byte("value1"), val, label)
		//} else {
		//	require.Nil(val, label)
		//}
		//if txNum == 976 {
		val, _, err := dc.GetAsOf([]byte("key2"), txNum+1, tx)
		require.NoError(err)
		//require.False(ok, label)
		require.Nil(val, label)
		//}
	}
}

func TestNewSegStreamReader(t *testing.T) {
	logger := log.New()
	keyCount := 1000
	valSize := 4

	fpath := generateKV(t, t.TempDir(), length.Addr, valSize, keyCount, logger, seg.CompressNone)
	dec, err := seg.NewDecompressor(fpath)
	require.NoError(t, err)

	defer dec.Close()
	r := seg.NewReader(dec.MakeGetter(), seg.CompressNone)

	sr := NewSegStreamReader(r, -1)
	require.NotNil(t, sr)
	defer sr.Close()

	count := 0
	var prevK []byte
	for sr.HasNext() {
		k, v, err := sr.Next()
		if prevK != nil {
			require.True(t, bytes.Compare(prevK, k) < 0)
		}
		prevK = common.Copy(k)

		require.NoError(t, err)
		require.NotEmpty(t, v)

		count++
	}
	require.EqualValues(t, keyCount, count)
}

// firstly we write all the data to domain
// then we collate-merge-prune
// then check.
// in real life we periodically do collate-merge-prune without stopping adding data
func TestDomain_Prune_AfterAllWrites(t *testing.T) {
	t.Parallel()

	logger := log.New()
	keyCount, txCount := uint64(4), uint64(64)
	db, dom, data := filledDomainFixedSize(t, keyCount, txCount, 16, logger)
	collateAndMerge(t, db, nil, dom, txCount)
	maxFrozenFiles := (txCount / dom.aggregationStep) / StepsInColdFile

	ctx := context.Background()
	roTx, err := db.BeginRo(ctx)
	require.NoError(t, err)
	defer roTx.Rollback()

	// Check the history
	dc := dom.BeginFilesRo()
	defer dc.Close()
	var k, v [8]byte

	for txNum := uint64(1); txNum <= txCount; txNum++ {
		for keyNum := uint64(0); keyNum < keyCount; keyNum++ {
			step := txNum / dom.aggregationStep
			frozenFileNum := step / 32
			if frozenFileNum < maxFrozenFiles { // frozen data
				if keyNum != frozenFileNum {
					continue
				}
				continue
				//fmt.Printf("put frozen: %d, step=%d, %d\n", keyNum, step, frozenFileNum)
			} else { //warm data
				if keyNum == 0 || keyNum == 1 {
					continue
				}
				if keyNum == txNum%dom.aggregationStep {
					continue
				}
				//fmt.Printf("put: %d, step=%d\n", keyNum, step)
			}

			label := fmt.Sprintf("txNum=%d, keyNum=%d\n", txNum, keyNum)
			binary.BigEndian.PutUint64(k[:], keyNum)
			binary.BigEndian.PutUint64(v[:], txNum)

			val, _, err := dc.GetAsOf(k[:], txNum+1, roTx)
			// during generation such keys are skipped so value should be nil for this call
			require.NoError(t, err, label)
			if !data[keyNum][txNum] {
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

	//warm keys
	binary.BigEndian.PutUint64(v[:], txCount)
	for keyNum := uint64(2); keyNum < keyCount; keyNum++ {
		label := fmt.Sprintf("txNum=%d, keyNum=%d\n", txCount-1, keyNum)
		binary.BigEndian.PutUint64(k[:], keyNum)

		storedV, _, found, err := dc.GetLatest(k[:], nil, roTx)
		require.Truef(t, found, label)
		require.NoError(t, err, label)
		require.EqualValues(t, v[:], storedV, label)
	}
}

func TestDomain_PruneOnWrite(t *testing.T) {
	t.Parallel()

	logger := log.New()
	keysCount, txCount := uint64(16), uint64(64)

	db, d := testDbAndDomain(t, logger)
	ctx := context.Background()
	d.aggregationStep = 16

	tx, err := db.BeginRw(ctx)
	require.NoError(t, err)
	defer tx.Rollback()
	dc := d.BeginFilesRo()
	defer dc.Close()
	writer := dc.NewWriter()
	defer writer.close()

	// keys are encodings of numbers 1..31
	// each key changes value on every txNum which is multiple of the key
	data := make(map[string][]uint64)

	prev := map[string]string{}

	for txNum := uint64(1); txNum <= txCount; txNum++ {
		writer.SetTxNum(txNum)
		for keyNum := uint64(1); keyNum <= keysCount; keyNum++ {
			if keyNum == txNum%d.aggregationStep {
				continue
			}
			var k [8]byte
			var v [8]byte
			binary.BigEndian.PutUint64(k[:], keyNum)
			binary.BigEndian.PutUint64(v[:], txNum)
			err = writer.PutWithPrev(k[:], nil, v[:], []byte(prev[string(k[:])]), 0)
			require.NoError(t, err)

			prev[string(k[:])] = string(v[:])

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
			err = writer.Flush(ctx, tx)
			require.NoError(t, err)

			collateAndMergeOnce(t, d, tx, step, true)
		}
	}
	err = writer.Flush(ctx, tx)
	require.NoError(t, err)
	dc.Close()

	// Check the history
	dc = d.BeginFilesRo()
	defer dc.Close()
	for txNum := uint64(1); txNum <= txCount; txNum++ {
		for keyNum := uint64(1); keyNum <= keysCount; keyNum++ {
			valNum := txNum
			var k [8]byte
			var v [8]byte
			label := fmt.Sprintf("txNum=%d, keyNum=%d\n", txNum, keyNum)
			binary.BigEndian.PutUint64(k[:], keyNum)
			binary.BigEndian.PutUint64(v[:], valNum)

			val, _, err := dc.GetAsOf(k[:], txNum+1, tx)
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

		storedV, _, found, err := dc.GetLatest(k[:], nil, tx)
		require.Truef(t, found, label)
		require.NoErrorf(t, err, label)
		require.EqualValues(t, v[:], storedV, label)
	}

	from, to := d.stepsRangeInDB(tx)
	require.Equal(t, 3, int(from))
	require.Equal(t, 4, int(to))

}

func TestDomain_OpenFilesWithDeletions(t *testing.T) {
	t.Parallel()
	logger := log.New()
	keyCount, txCount := uint64(4), uint64(125)
	db, dom, data := filledDomainFixedSize(t, keyCount, txCount, 16, logger)
	defer db.Close()
	clear(data)

	logEvery := time.NewTicker(30 * time.Second)
	defer logEvery.Stop()
	ctx := context.Background()

	err := db.Update(ctx, func(tx kv.RwTx) error {
		for step := uint64(0); step < txCount/dom.aggregationStep-1; step++ {
			s, ns := step*dom.aggregationStep, (step+1)*dom.aggregationStep
			c, err := dom.collate(ctx, step, s, ns, tx)
			require.NoError(t, err)
			sf, err := dom.buildFiles(ctx, step, c, background.NewProgressSet())
			require.NoError(t, err)
			dom.integrateDirtyFiles(sf, s, ns)
			dom.reCalcVisibleFiles(dom.dirtyFilesEndTxNumMinimax())

			dc := dom.BeginFilesRo()
			_, err = dc.Prune(ctx, tx, step, s, ns, math.MaxUint64, logEvery)
			dc.Close()
			require.NoError(t, err)
		}
		return nil
	})

	require.NoError(t, err)

	run1Doms, run1Hist := make([]string, 0), make([]string, 0)
	for i := 0; i < len(dom._visible.files); i++ {
		run1Doms = append(run1Doms, dom._visible.files[i].src.decompressor.FileName())
		// should be equal length
		run1Hist = append(run1Hist, dom.History._visibleFiles[i].src.decompressor.FileName())
	}

	removedHist := make(map[string]struct{})
	for i := len(dom.History._visibleFiles) - 1; i > 3; i-- {
		removedHist[dom.History._visibleFiles[i].src.decompressor.FileName()] = struct{}{}
		t.Logf("rm hist: %s\n", dom.History._visibleFiles[i].src.decompressor.FileName())

		dom.History._visibleFiles[i].src.closeFilesAndRemove()
	}
	dom.Close()

	err = dom.openFolder()
	dom.reCalcVisibleFiles(dom.dirtyFilesEndTxNumMinimax())

	require.NoError(t, err)

	// domain files for same range should not be available so lengths should match
	require.Len(t, dom._visible.files, len(run1Doms)-len(removedHist))
	require.Len(t, dom.History._visibleFiles, len(dom._visible.files))
	require.Len(t, dom.History._visibleFiles, len(run1Hist)-len(removedHist))

	for i := 0; i < len(dom._visible.files); i++ {
		require.EqualValuesf(t, run1Doms[i], dom._visible.files[i].src.decompressor.FileName(), "kv i=%d", i)
		require.EqualValuesf(t, run1Hist[i], dom.History._visibleFiles[i].src.decompressor.FileName(), " v i=%d", i)
	}

	danglingDomains := make(map[string]bool, len(removedHist))
	for i := len(run1Doms) - len(removedHist); i < len(run1Doms); i++ {
		t.Logf("dangling: %s\n", run1Doms[i])
		danglingDomains[run1Doms[i]] = false
	}

	//dom.dirtyFiles.Walk(func(items []*filesItem) bool {
	//	for _, item := range items {
	//		if _, found := danglingDomains[item.decompressor.FileName()]; found {
	//			danglingDomains[item.decompressor.FileName()] = true
	//		}
	//	}
	//	return true
	//})
	//
	//for f, persists := range danglingDomains {
	//	require.True(t, persists, f)
	//}

	// check files persist on the disk
	persistingDomains := make(map[string]bool, 0)
	err = fs.WalkDir(os.DirFS(dom.dirs.SnapDomain), ".", func(path string, d fs.DirEntry, err error) error {
		persistingDomains[filepath.Base(path)] = false
		return nil
	})
	require.NoError(t, err)

	// check all "invalid" kv files persists on disk
	for fname := range danglingDomains {
		_, found := persistingDomains[fname]
		require.True(t, found, fname)
	}

	dom.Close()
}

func TestScanStaticFilesD(t *testing.T) {
	t.Parallel()

	ii := &Domain{History: &History{InvertedIndex: emptyTestInvertedIndex(1)},
		dirtyFiles: btree2.NewBTreeG[*filesItem](filesItemLess),
	}
	files := []string{
		"v1-test.0-1.kv",
		"v1-test.1-2.kv",
		"v1-test.0-4.kv",
		"v1-test.2-3.kv",
		"v1-test.3-4.kv",
		"v1-test.4-5.kv",
	}
	ii.scanDirtyFiles(files)
	var found []string
	ii.dirtyFiles.Walk(func(items []*filesItem) bool {
		for _, item := range items {
			found = append(found, fmt.Sprintf("%d-%d", item.startTxNum, item.endTxNum))
		}
		return true
	})
	require.Equal(t, 6, len(found))
}

func TestDomain_CollationBuildInMem(t *testing.T) {
	t.Parallel()

	logEvery := time.NewTicker(30 * time.Second)
	defer logEvery.Stop()
	db, d := testDbAndDomain(t, log.New())
	ctx := context.Background()
	defer d.Close()

	tx, err := db.BeginRw(ctx)
	require.NoError(t, err)
	defer tx.Rollback()
	dc := d.BeginFilesRo()
	defer dc.Close()
	maxTx := uint64(10000)
	d.aggregationStep = maxTx

	writer := dc.NewWriter()
	defer writer.close()

	var preval1, preval2, preval3 []byte

	l := []byte("asd9s9af0afa9sfh9afha")

	for i := 0; i < int(maxTx); i++ {
		v1 := []byte(fmt.Sprintf("value1.%d", i))
		v2 := []byte(fmt.Sprintf("value2.%d", i))
		s := []byte(fmt.Sprintf("longstorage2.%d", i))

		writer.SetTxNum(uint64(i))
		err = writer.PutWithPrev([]byte("key1"), nil, v1, preval1, 0)
		require.NoError(t, err)

		err = writer.PutWithPrev([]byte("key2"), nil, v2, preval2, 0)
		require.NoError(t, err)

		err = writer.PutWithPrev([]byte("key3"), l, s, preval3, 0)
		require.NoError(t, err)

		preval1, preval2, preval3 = v1, v2, s
	}

	err = writer.Flush(ctx, tx)
	require.NoError(t, err)

	c, err := d.collate(ctx, 0, 0, maxTx, tx)

	require.NoError(t, err)
	require.True(t, strings.HasSuffix(c.valuesPath, "v1-accounts.0-1.kv"))
	require.Equal(t, 3, c.valuesCount)
	require.True(t, strings.HasSuffix(c.historyPath, "v1-accounts.0-1.v"))
	require.EqualValues(t, 3*maxTx, c.historyCount)
	require.Equal(t, 3, c.efHistoryComp.Count()/2)

	sf, err := d.buildFiles(ctx, 0, c, background.NewProgressSet())
	require.NoError(t, err)
	defer sf.CleanupOnError()
	c.Close()

	g := seg.NewReader(sf.valuesDecomp.MakeGetter(), d.compression)
	g.Reset(0)
	var words []string
	for g.HasNext() {
		w, _ := g.Next(nil)
		words = append(words, string(w))
	}
	require.EqualValues(t, []string{"key1", string(preval1), "key2", string(preval2), "key3" + string(l), string(preval3)}, words)
	// Check index
	require.Equal(t, 3, int(sf.valuesBt.KeyCount()))
	for i := 0; i < len(words); i += 2 {
		c, _ := sf.valuesBt.Seek(g, []byte(words[i]))
		require.Equal(t, words[i], string(c.Key()))
		require.Equal(t, words[i+1], string(c.Value()))
	}

	//require.Equal(t, 3, int(sf.valuesIdx.KeyCount()))
	//
	//r := recsplit.NewIndexReader(sf.valuesIdx)
	//defer r.Close()
	//for i := 0; i < len(words); i += 2 {
	//	offset := r.Lookup([]byte(words[i]))
	//	g.Reset(offset)
	//	w, _ := g.Next(nil)
	//	require.Equal(t, words[i], string(w))
	//	w, _ = g.Next(nil)
	//	require.Equal(t, words[i+1], string(w))
	//}
}

func TestDomainContext_getFromFiles(t *testing.T) {
	t.Parallel()

	db, d := testDbAndDomain(t, log.New())
	defer db.Close()
	defer d.Close()

	tx, err := db.BeginRw(context.Background())
	require.NoError(t, err)
	defer tx.Rollback()

	d.aggregationStep = 20

	keys, vals := generateInputData(t, 8, 16, 100)
	keys = keys[:20]

	var i int
	values := make(map[string][][]byte)

	dc := d.BeginFilesRo()
	defer dc.Close()
	writer := dc.NewWriter()
	defer writer.close()

	var prev []byte
	for i = 0; i < len(vals); i++ {
		writer.SetTxNum(uint64(i))

		for j := 0; j < len(keys); j++ {
			buf := types.EncodeAccountBytesV3(uint64(i), uint256.NewInt(uint64(i*100_000)), nil, 0)

			err = writer.PutWithPrev(keys[j], nil, buf, prev, 0)
			require.NoError(t, err)
			prev = buf

			if i > 0 && i+1%int(d.aggregationStep) == 0 {
				values[hex.EncodeToString(keys[j])] = append(values[hex.EncodeToString(keys[j])], buf)
			}
		}
	}
	err = writer.Flush(context.Background(), tx)
	require.NoError(t, err)
	defer dc.Close()

	ctx := context.Background()
	ps := background.NewProgressSet()
	for step := uint64(0); step < uint64(len(vals))/d.aggregationStep; step++ {
		dc := d.BeginFilesRo()

		txFrom := step * d.aggregationStep
		txTo := (step + 1) * d.aggregationStep

		fmt.Printf("Step %d [%d,%d)\n", step, txFrom, txTo)

		collation, err := d.collate(ctx, step, txFrom, txTo, tx)
		require.NoError(t, err)

		sf, err := d.buildFiles(ctx, step, collation, ps)
		require.NoError(t, err)

		d.integrateDirtyFiles(sf, txFrom, txTo)
		d.reCalcVisibleFiles(d.dirtyFilesEndTxNumMinimax())
		collation.Close()

		logEvery := time.NewTicker(time.Second * 30)

		_, err = dc.Prune(ctx, tx, step, txFrom, txTo, math.MaxUint64, logEvery)
		require.NoError(t, err)

		ranges := dc.findMergeRange(txFrom, txTo)
		vl, il, hl := dc.staticFilesInRange(ranges)

		dv, di, dh, err := dc.mergeFiles(ctx, vl, il, hl, ranges, nil, ps)
		require.NoError(t, err)

		d.integrateMergedDirtyFiles(vl, il, hl, dv, di, dh)
		d.reCalcVisibleFiles(d.dirtyFilesEndTxNumMinimax())

		logEvery.Stop()

		dc.Close()
	}

	dc = d.BeginFilesRo()
	defer dc.Close()

	for key, bufs := range values {
		var i int

		beforeTx := d.aggregationStep
		for i = 0; i < len(bufs); i++ {
			ks, _ := hex.DecodeString(key)
			val, _, err := dc.GetAsOf(ks, beforeTx, tx)
			require.NoError(t, err)
			require.EqualValuesf(t, bufs[i], val, "key %s, txn %d", key, beforeTx)
			beforeTx += d.aggregationStep
		}
	}
}

type upd struct {
	txNum uint64
	value []byte
}

func filledDomainFixedSize(t *testing.T, keysCount, txCount, aggStep uint64, logger log.Logger) (kv.RwDB, *Domain, map[uint64][]bool) {
	t.Helper()
	db, d := testDbAndDomainOfStep(t, aggStep, logger)
	ctx := context.Background()
	tx, err := db.BeginRw(ctx)
	require.NoError(t, err)
	defer tx.Rollback()
	dc := d.BeginFilesRo()
	defer dc.Close()
	writer := dc.NewWriter()
	defer writer.close()

	// keys are encodings of numbers 1..31
	// each key changes value on every txNum which is multiple of the key
	dat := make(map[uint64][]bool) // K:V is key -> list of bools. If list[i] == true, i'th txNum should persists

	var k [8]byte
	var v [8]byte
	maxFrozenFiles := (txCount / d.aggregationStep) / StepsInColdFile
	prev := map[string]string{}

	// key 0: only in frozen file 0
	// key 1: only in frozen file 1 and file 2
	// key 2: in frozen file 2 and in warm files
	// other keys: only in warm files
	for txNum := uint64(1); txNum <= txCount; txNum++ {
		writer.SetTxNum(txNum)
		step := txNum / d.aggregationStep
		frozenFileNum := step / 32
		for keyNum := uint64(0); keyNum < keysCount; keyNum++ {
			if frozenFileNum < maxFrozenFiles { // frozen data
				allowInsert := (keyNum == 0 && frozenFileNum == 0) ||
					(keyNum == 1 && (frozenFileNum == 1 || frozenFileNum == 2)) ||
					(keyNum == 2 && frozenFileNum == 2)
				if !allowInsert {
					continue
				}
				//fmt.Printf("put frozen: %d, step=%d, %d\n", keyNum, step, frozenFileNum)
			} else { //warm data
				if keyNum == 0 || keyNum == 1 {
					continue
				}
				if keyNum == txNum%d.aggregationStep {
					continue
				}
				//fmt.Printf("put: %d, step=%d\n", keyNum, step)
			}

			binary.BigEndian.PutUint64(k[:], keyNum)
			binary.BigEndian.PutUint64(v[:], txNum)
			//v[0] = 3 // value marker
			err = writer.PutWithPrev(k[:], nil, v[:], []byte(prev[string(k[:])]), 0)
			require.NoError(t, err)
			if _, ok := dat[keyNum]; !ok {
				dat[keyNum] = make([]bool, txCount+1)
			}
			dat[keyNum][txNum] = true

			prev[string(k[:])] = string(v[:])
		}
		if txNum%d.aggregationStep == 0 {
			err = writer.Flush(ctx, tx)
			require.NoError(t, err)
		}
	}
	err = tx.Commit()
	require.NoError(t, err)
	return db, d, dat
}

func generateTestDataForDomainCommitment(tb testing.TB, keySize1, keySize2, totalTx, keyTxsLimit, keyLimit uint64) map[string]map[string][]upd {
	tb.Helper()
	defer func(t time.Time) { fmt.Printf("domain_test.go:1262: %s\n", time.Since(t)) }(time.Now())

	doms := make(map[string]map[string][]upd)
	seed := 31
	//seed := time.Now().Unix()
	defer tb.Logf("generated data with seed %d, keys %d", seed, keyLimit)
	r := newRnd(0)

	accs := make(map[string][]upd)
	stor := make(map[string][]upd)
	if keyLimit == 1 {
		key1 := generateRandomKey(r, keySize1)
		accs[key1] = generateAccountUpdates(r, totalTx, keyTxsLimit)
		doms["accounts"] = accs
		return doms
	}

	for i := uint64(0); i < keyLimit/2; i++ {
		key1 := generateRandomKey(r, keySize1)
		accs[key1] = generateAccountUpdates(r, totalTx, keyTxsLimit)
		key2 := key1 + generateRandomKey(r, keySize2-keySize1)
		stor[key2] = generateArbitraryValueUpdates(r, totalTx, keyTxsLimit, 32)
	}
	doms["accounts"] = accs
	doms["storage"] = stor

	return doms
}

// generate arbitrary values for arbitrary keys within given totalTx
func generateTestData(tb testing.TB, keySize1, keySize2, totalTx, keyTxsLimit, keyLimit uint64) map[string][]upd {
	tb.Helper()

	data := make(map[string][]upd)
	r := newRnd(32)
	if keyLimit == 1 {
		key1 := generateRandomKey(r, keySize1)
		data[key1] = generateUpdates(r, totalTx, keyTxsLimit)
		return data
	}

	for i := uint64(0); i < keyLimit/2; i++ {
		key1 := generateRandomKey(r, keySize1)
		data[key1] = generateUpdates(r, totalTx, keyTxsLimit)
		key2 := key1 + generateRandomKey(r, keySize2-keySize1)
		data[key2] = generateUpdates(r, totalTx, keyTxsLimit)
	}
	return data
}

func generateRandomKey(r *rnd, size uint64) string {
	return string(generateRandomKeyBytes(r, size))
}

func generateRandomKeyBytes(r *rnd, size uint64) []byte {
	key := make([]byte, size)
	r.Read(key)
	return key
}

func generateAccountUpdates(r *rnd, totalTx, keyTxsLimit uint64) []upd {
	updates := make([]upd, 0)
	usedTxNums := make(map[uint64]bool)

	for i := uint64(0); i < keyTxsLimit; i++ {
		txNum := generateRandomTxNum(r, totalTx, usedTxNums)
		jitter := int(r.Uint64N(10e7))
		value := types.EncodeAccountBytesV3(i, uint256.NewInt(i*10e4+uint64(jitter)), nil, 0)

		updates = append(updates, upd{txNum: txNum, value: value})
		usedTxNums[txNum] = true
	}
	sort.Slice(updates, func(i, j int) bool { return updates[i].txNum < updates[j].txNum })

	return updates
}

func generateArbitraryValueUpdates(r *rnd, totalTx, keyTxsLimit, maxSize uint64) []upd {
	updates := make([]upd, 0)
	usedTxNums := make(map[uint64]bool)
	//maxStorageSize := 24 * (1 << 10) // limit on contract code

	for i := uint64(0); i < keyTxsLimit; i++ {
		txNum := generateRandomTxNum(r, totalTx, usedTxNums)

		value := make([]byte, r.Uint64N(maxSize))
		r.Read(value)

		updates = append(updates, upd{txNum: txNum, value: value})
		usedTxNums[txNum] = true
	}
	sort.Slice(updates, func(i, j int) bool { return updates[i].txNum < updates[j].txNum })

	return updates
}

func generateUpdates(r *rnd, totalTx, keyTxsLimit uint64) []upd {
	updates := make([]upd, 0)
	usedTxNums := make(map[uint64]bool)

	for i := uint64(0); i < keyTxsLimit; i++ {
		txNum := generateRandomTxNum(r, totalTx, usedTxNums)
		value := make([]byte, 10)
		r.Read(value)

		updates = append(updates, upd{txNum: txNum, value: value})
		usedTxNums[txNum] = true
	}
	sort.Slice(updates, func(i, j int) bool { return updates[i].txNum < updates[j].txNum })

	return updates
}

func generateRandomTxNum(r *rnd, maxTxNum uint64, usedTxNums map[uint64]bool) uint64 {
	txNum := r.Uint64N(maxTxNum)
	for usedTxNums[txNum] {
		txNum = r.Uint64N(maxTxNum)
	}

	return txNum
}

func TestDomain_GetAfterAggregation(t *testing.T) {
	db, d := testDbAndDomainOfStep(t, 25, log.New())
	require := require.New(t)

	tx, err := db.BeginRw(context.Background())
	require.NoError(err)
	defer tx.Rollback()

	d.historyLargeValues = false
	d.History.compression = seg.CompressNone //seg.CompressKeys | seg.CompressVals
	d.compression = seg.CompressNone         //seg.CompressKeys | seg.CompressVals
	d.filenameBase = kv.FileCommitmentDomain

	dc := d.BeginFilesRo()
	defer d.Close()
	writer := dc.NewWriter()
	defer writer.close()

	keySize1 := uint64(length.Addr)
	keySize2 := uint64(length.Addr + length.Hash)
	totalTx := uint64(3000)
	keyTxsLimit := uint64(50)
	keyLimit := uint64(200)

	// put some kvs
	data := generateTestData(t, keySize1, keySize2, totalTx, keyTxsLimit, keyLimit)
	for key, updates := range data {
		p := []byte{}
		for i := 0; i < len(updates); i++ {
			writer.SetTxNum(updates[i].txNum)
			writer.PutWithPrev([]byte(key), nil, updates[i].value, p, 0)
			p = common.Copy(updates[i].value)
		}
	}
	writer.SetTxNum(totalTx)

	err = writer.Flush(context.Background(), tx)
	require.NoError(err)

	// aggregate
	collateAndMerge(t, db, tx, d, totalTx)
	require.NoError(tx.Commit())

	tx, err = db.BeginRw(context.Background())
	require.NoError(err)
	defer tx.Rollback()
	dc.Close()

	dc = d.BeginFilesRo()
	defer dc.Close()

	kc := 0
	for key, updates := range data {
		kc++
		for i := 1; i < len(updates); i++ {
			v, ok, err := dc.GetAsOf([]byte(key), updates[i].txNum, tx)
			require.NoError(err)
			require.True(ok)
			require.EqualValuesf(updates[i-1].value, v, "(%d/%d) key %x, txn %d", kc, len(data), []byte(key), updates[i-1].txNum)
		}
		if len(updates) == 0 {
			continue
		}
		v, _, ok, err := dc.GetLatest([]byte(key), nil, tx)
		require.NoError(err)
		require.EqualValuesf(updates[len(updates)-1].value, v, "key %x latest", []byte(key))
		require.True(ok)
	}
}

func TestDomainRange(t *testing.T) {
	db, d := testDbAndDomainOfStep(t, 25, log.New())
	require := require.New(t)

	tx, err := db.BeginRw(context.Background())
	require.NoError(err)
	defer tx.Rollback()

	d.historyLargeValues = false
	d.History.compression = seg.CompressNone // seg.CompressKeys | seg.CompressVals
	d.compression = seg.CompressNone         // seg.CompressKeys | seg.CompressVals
	d.filenameBase = kv.FileAccountDomain

	dc := d.BeginFilesRo()
	defer d.Close()
	writer := dc.NewWriter()
	defer writer.close()

	keySize1 := uint64(4)
	keySize2 := uint64(4)
	totalTx := uint64(300)
	keyTxsLimit := uint64(2)
	keyLimit := uint64(10)

	// put some kvs
	data := generateTestData(t, keySize1, keySize2, totalTx, keyTxsLimit, keyLimit)
	for key, updates := range data {
		p := []byte{}
		for i := 0; i < len(updates); i++ {
			writer.SetTxNum(updates[i].txNum)
			err = writer.PutWithPrev([]byte(key), nil, updates[i].value, p, 0)
			require.NoError(err)
			p = common.Copy(updates[i].value)
		}
	}
	writer.SetTxNum(totalTx)

	err = writer.Flush(context.Background(), tx)
	require.NoError(err)

	// aggregate
	collateAndMerge(t, db, tx, d, totalTx)
	require.NoError(tx.Commit())

	tx, err = db.BeginRw(context.Background())
	require.NoError(err)
	defer tx.Rollback()
	dc.Close()

	dc = d.BeginFilesRo()
	defer dc.Close()

	//it, err := dc.DomainRangeLatest(tx, nil, nil, -1)
	it, err := dc.DomainRange(context.Background(), tx, nil, nil, 190, order.Asc, -1)
	require.NoError(err)
	keys, vals, err := stream.ToArrayKV(it)
	require.NoError(err)
	fmt.Printf("keys: %x\n", keys)
	fmt.Printf("vals: %x\n", vals)
}

func TestDomain_CanPruneAfterAggregation(t *testing.T) {
	t.Parallel()

	aggStep := uint64(25)
	db, d := testDbAndDomainOfStep(t, aggStep, log.New())
	defer db.Close()
	defer d.Close()

	tx, err := db.BeginRw(context.Background())
	require.NoError(t, err)
	defer tx.Rollback()

	d.historyLargeValues = false
	d.History.compression = seg.CompressKeys | seg.CompressVals
	d.compression = seg.CompressKeys | seg.CompressVals
	d.filenameBase = kv.FileCommitmentDomain

	dc := d.BeginFilesRo()
	defer dc.Close()
	writer := dc.NewWriter()
	defer writer.close()

	keySize1 := uint64(length.Addr)
	keySize2 := uint64(length.Addr + length.Hash)
	totalTx := uint64(5000)
	keyTxsLimit := uint64(50)
	keyLimit := uint64(200)
	SaveExecV3PrunableProgress(tx, kv.MinimumPrunableStepDomainKey, 0)
	// put some kvs
	data := generateTestData(t, keySize1, keySize2, totalTx, keyTxsLimit, keyLimit)
	for key, updates := range data {
		p := []byte{}
		for i := 0; i < len(updates); i++ {
			writer.SetTxNum(updates[i].txNum)
			writer.PutWithPrev([]byte(key), nil, updates[i].value, p, 0)
			p = common.Copy(updates[i].value)
		}
	}
	writer.SetTxNum(totalTx)

	err = writer.Flush(context.Background(), tx)
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	tx, err = db.BeginRw(context.Background())
	require.NoError(t, err)
	defer tx.Rollback()
	dc.Close()

	stepToPrune := uint64(2)
	collateAndMergeOnce(t, d, tx, stepToPrune, true)

	dc = d.BeginFilesRo()
	can, untilStep := dc.canPruneDomainTables(tx, aggStep)
	defer dc.Close()
	require.Falsef(t, can, "those step is already pruned")
	require.EqualValues(t, stepToPrune, untilStep)

	stepToPrune = 3
	collateAndMergeOnce(t, d, tx, stepToPrune, false)

	// refresh file list
	dc = d.BeginFilesRo()
	t.Logf("pruning step %d", stepToPrune)
	can, untilStep = dc.canPruneDomainTables(tx, 1+aggStep*stepToPrune)
	require.True(t, can, "third step is not yet pruned")
	require.LessOrEqual(t, stepToPrune, untilStep)

	can, untilStep = dc.canPruneDomainTables(tx, 1+aggStep*stepToPrune+(aggStep/2))
	require.True(t, can, "third step is not yet pruned, we are checking for a half-step after it and still have something to prune")
	require.LessOrEqual(t, stepToPrune, untilStep)
	dc.Close()

	stepToPrune = 30
	collateAndMergeOnce(t, d, tx, stepToPrune, true)

	dc = d.BeginFilesRo()
	can, untilStep = dc.canPruneDomainTables(tx, aggStep*stepToPrune)
	require.False(t, can, "latter step is not yet pruned")
	require.EqualValues(t, stepToPrune, untilStep)
	dc.Close()

	stepToPrune = 35
	collateAndMergeOnce(t, d, tx, stepToPrune, false)

	dc = d.BeginFilesRo()
	t.Logf("pruning step %d", stepToPrune)
	can, untilStep = dc.canPruneDomainTables(tx, 1+aggStep*stepToPrune)
	require.True(t, can, "third step is not yet pruned")
	require.LessOrEqual(t, stepToPrune, untilStep)

	can, untilStep = dc.canPruneDomainTables(tx, 1+aggStep*stepToPrune+(aggStep/2))
	require.True(t, can, "third step is not yet pruned, we are checking for a half-step after it and still have something to prune")
	require.LessOrEqual(t, stepToPrune, untilStep)
	dc.Close()
}

func TestDomain_PruneAfterAggregation(t *testing.T) {
	t.Parallel()

	db, d := testDbAndDomainOfStep(t, 25, log.New())
	defer db.Close()
	defer d.Close()

	tx, err := db.BeginRw(context.Background())
	require.NoError(t, err)
	defer tx.Rollback()

	d.historyLargeValues = false
	d.History.compression = seg.CompressNone //seg.CompressKeys | seg.CompressVals
	d.compression = seg.CompressNone         //seg.CompressKeys | seg.CompressVals

	dc := d.BeginFilesRo()
	defer dc.Close()
	writer := dc.NewWriter()
	defer writer.close()

	keySize1 := uint64(length.Addr)
	keySize2 := uint64(length.Addr + length.Hash)
	totalTx := uint64(5000)
	keyTxsLimit := uint64(50)
	keyLimit := uint64(200)

	// Key's lengths are variable so lookup should be in commitment mode.
	d.filenameBase = kv.FileCommitmentDomain

	// put some kvs
	data := generateTestData(t, keySize1, keySize2, totalTx, keyTxsLimit, keyLimit)
	for key, updates := range data {
		p := []byte{}
		for i := 0; i < len(updates); i++ {
			writer.SetTxNum(updates[i].txNum)
			writer.PutWithPrev([]byte(key), nil, updates[i].value, p, 0)
			p = common.Copy(updates[i].value)
		}
	}
	writer.SetTxNum(totalTx)

	err = writer.Flush(context.Background(), tx)
	require.NoError(t, err)

	// aggregate
	collateAndMerge(t, db, tx, d, totalTx) // expected to left 2 latest steps in db

	require.NoError(t, tx.Commit())

	tx, err = db.BeginRw(context.Background())
	require.NoError(t, err)
	defer tx.Rollback()
	dc.Close()

	dc = d.BeginFilesRo()
	defer dc.Close()

	kc := 0
	for key, updates := range data {
		kc++
		for i := 1; i < len(updates); i++ {
			v, _, err := dc.GetAsOf([]byte(key), updates[i].txNum, tx)
			require.NoError(t, err)
			require.EqualValuesf(t, updates[i-1].value, v, "(%d/%d) key %x, txn %d", kc, len(data), []byte(key), updates[i-1].txNum)
		}
		if len(updates) == 0 {
			continue
		}
		v, _, ok, err := dc.GetLatest([]byte(key), nil, tx)
		require.NoError(t, err)
		require.EqualValuesf(t, updates[len(updates)-1].value, v, "key %x latest", []byte(key))
		require.True(t, ok)
	}
}

func TestPruneProgress(t *testing.T) {
	t.Parallel()

	db, d := testDbAndDomainOfStep(t, 25, log.New())
	defer db.Close()
	defer d.Close()

	latestKey := []byte("682c02b93b63aeb260eccc33705d584ffb5f0d4c")

	t.Run("reset", func(t *testing.T) {
		tx, err := db.BeginRw(context.Background())
		require.NoError(t, err)
		defer tx.Rollback()
		err = SaveExecV3PruneProgress(tx, kv.TblAccountVals, latestKey)
		require.NoError(t, err)
		key, err := GetExecV3PruneProgress(tx, kv.TblAccountVals)
		require.NoError(t, err)
		require.EqualValuesf(t, latestKey, key, "key %x", key)

		err = SaveExecV3PruneProgress(tx, kv.TblAccountVals, nil)
		require.NoError(t, err)

		key, err = GetExecV3PruneProgress(tx, kv.TblAccountVals)
		require.NoError(t, err)
		require.Nil(t, key)
	})

	t.Run("someKey and reset", func(t *testing.T) {
		tx, err := db.BeginRw(context.Background())
		require.NoError(t, err)
		defer tx.Rollback()
		err = SaveExecV3PruneProgress(tx, kv.TblAccountVals, latestKey)
		require.NoError(t, err)

		key, err := GetExecV3PruneProgress(tx, kv.TblAccountVals)
		require.NoError(t, err)
		require.EqualValues(t, latestKey, key)

		err = SaveExecV3PruneProgress(tx, kv.TblAccountVals, nil)
		require.NoError(t, err)

		key, err = GetExecV3PruneProgress(tx, kv.TblAccountVals)
		require.NoError(t, err)
		require.Nil(t, key)
	})

	t.Run("emptyKey and reset", func(t *testing.T) {
		tx, err := db.BeginRw(context.Background())
		require.NoError(t, err)
		defer tx.Rollback()
		expected := []byte{}
		err = SaveExecV3PruneProgress(tx, kv.TblAccountVals, expected)
		require.NoError(t, err)

		key, err := GetExecV3PruneProgress(tx, kv.TblAccountVals)
		require.NoError(t, err)
		require.EqualValues(t, expected, key)

		err = SaveExecV3PruneProgress(tx, kv.TblAccountVals, nil)
		require.NoError(t, err)

		key, err = GetExecV3PruneProgress(tx, kv.TblAccountVals)
		require.NoError(t, err)
		require.Nil(t, key)
	})
}

func TestDomain_PruneProgress(t *testing.T) {
	t.Skip("fails because in domain.Prune progress does not updated")

	aggStep := uint64(1000)
	db, d := testDbAndDomainOfStep(t, aggStep, log.New())
	defer db.Close()
	defer d.Close()

	rwTx, err := db.BeginRw(context.Background())
	require.NoError(t, err)
	defer rwTx.Rollback()

	d.historyLargeValues = false
	d.History.compression = seg.CompressKeys | seg.CompressVals
	d.compression = seg.CompressKeys | seg.CompressVals

	dc := d.BeginFilesRo()
	defer dc.Close()
	writer := dc.NewWriter()
	defer writer.close()

	keySize1 := uint64(length.Addr)
	keySize2 := uint64(length.Addr + length.Hash)
	totalTx := uint64(5000)
	keyTxsLimit := uint64(150)
	keyLimit := uint64(2000)

	// put some kvs
	data := generateTestData(t, keySize1, keySize2, totalTx, keyTxsLimit, keyLimit)
	for key, updates := range data {
		p := []byte{}
		for i := 0; i < len(updates); i++ {
			writer.SetTxNum(updates[i].txNum)
			err = writer.PutWithPrev([]byte(key), nil, updates[i].value, p, 0)
			require.NoError(t, err)
			p = common.Copy(updates[i].value)
		}
	}
	writer.SetTxNum(totalTx)

	err = writer.Flush(context.Background(), rwTx)
	require.NoError(t, err)

	logEvery := time.NewTicker(30 * time.Second)
	defer logEvery.Stop()
	// aggregate
	for step := uint64(0); step < totalTx/aggStep; step++ {
		ctx := context.Background()
		txFrom, txTo := (step)*d.aggregationStep, (step+1)*d.aggregationStep

		c, err := d.collate(ctx, step, txFrom, txTo, rwTx)
		require.NoError(t, err)

		sf, err := d.buildFiles(ctx, step, c, background.NewProgressSet())
		require.NoError(t, err)
		d.integrateDirtyFiles(sf, txFrom, txTo)
		d.reCalcVisibleFiles(d.dirtyFilesEndTxNumMinimax())
	}
	require.NoError(t, rwTx.Commit())

	rwTx, err = db.BeginRw(context.Background())
	require.NoError(t, err)
	defer rwTx.Rollback()
	dc.Close()

	dc = d.BeginFilesRo()
	defer dc.Close()

	ct, cancel := context.WithTimeout(context.Background(), time.Millisecond*1)
	_, err = dc.Prune(ct, rwTx, 0, 0, aggStep, math.MaxUint64, time.NewTicker(time.Second))
	require.ErrorIs(t, err, context.DeadlineExceeded)
	cancel()

	key, err := GetExecV3PruneProgress(rwTx, dc.d.valsTable)
	require.NoError(t, err)
	require.NotNil(t, key)

	keysCursor, err := rwTx.RwCursorDupSort(dc.d.valsTable)
	require.NoError(t, err)

	k, istep, err := keysCursor.Seek(key)
	require.NoError(t, err)
	require.GreaterOrEqual(t, k, key)
	require.NotEqualValues(t, 0, ^binary.BigEndian.Uint64(istep))
	keysCursor.Close()

	var i int
	for step := uint64(0); ; step++ {
		// step changing should not affect pruning. Prune should finish step 0 first.
		i++
		ct, cancel := context.WithTimeout(context.Background(), time.Millisecond*2)
		_, err = dc.Prune(ct, rwTx, step, step*aggStep, (aggStep*step)+1, math.MaxUint64, time.NewTicker(time.Second))
		if err != nil {
			require.ErrorIs(t, err, context.DeadlineExceeded)
		} else {
			require.NoError(t, err)
		}
		cancel()

		key, err := GetExecV3PruneProgress(rwTx, dc.d.valsTable)
		require.NoError(t, err)
		if step == 0 && key == nil {

			fmt.Printf("pruned in %d iterations\n", i)

			keysCursor, err := rwTx.RwCursorDupSort(dc.d.valsTable)
			require.NoError(t, err)

			// check there are no keys with 0 step left
			for k, v, err := keysCursor.First(); k != nil && err == nil; k, v, err = keysCursor.Next() {
				require.NotEqualValues(t, 0, ^binary.BigEndian.Uint64(v))
			}

			keysCursor.Close()
			break
		}

	}
	fmt.Printf("exitiig after %d iterations\n", i)
}

func TestDomain_Unwind(t *testing.T) {
	t.Parallel()

	db, d := testDbAndDomain(t, log.New())
	defer d.Close()
	defer db.Close()
	ctx := context.Background()

	d.aggregationStep = 16
	//maxTx := uint64(float64(d.aggregationStep) * 1.5)
	maxTx := d.aggregationStep - 2
	currTx := maxTx - 1
	diffSetMap := map[uint64][]DomainEntryDiff{}

	writeKeys := func(t *testing.T, d *Domain, db kv.RwDB, maxTx uint64) {
		t.Helper()
		dc := d.BeginFilesRo()
		defer dc.Close()
		tx, err := db.BeginRw(ctx)
		require.NoError(t, err)
		defer tx.Rollback()
		writer := dc.NewWriter()
		defer writer.close()
		var preval1, preval2, preval3, preval4 []byte
		for i := uint64(0); i < maxTx; i++ {
			writer.diff = &StateDiffDomain{}
			writer.SetTxNum(i)
			if i%3 == 0 && i > 0 { // once in 3 txn put key3 -> value3.i and skip other keys update
				if i%12 == 0 { // once in 12 txn delete key3 before update
					err = writer.DeleteWithPrev([]byte("key3"), nil, preval3, 0)
					require.NoError(t, err)
					preval3 = nil
					diffSetMap[i] = writer.diff.GetDiffSet()
					continue
				}
				v3 := []byte(fmt.Sprintf("value3.%d", i))
				err = writer.PutWithPrev([]byte("key3"), nil, v3, preval3, 0)
				require.NoError(t, err)
				preval3 = v3
				diffSetMap[i] = writer.diff.GetDiffSet()
				continue
			}

			v1 := []byte(fmt.Sprintf("value1.%d", i))
			v2 := []byte(fmt.Sprintf("value2.%d", i))
			nv3 := []byte(fmt.Sprintf("valuen3.%d", i))

			err = writer.PutWithPrev([]byte("key1"), nil, v1, preval1, 0)
			require.NoError(t, err)
			err = writer.PutWithPrev([]byte("key2"), nil, v2, preval2, 0)
			require.NoError(t, err)
			err = writer.PutWithPrev([]byte("k4"), nil, nv3, preval4, 0)
			require.NoError(t, err)
			diffSetMap[i] = writer.diff.GetDiffSet()

			preval1, preval2, preval4 = v1, v2, nv3
		}
		err = writer.Flush(ctx, tx)
		require.NoError(t, err)
		err = tx.Commit()
		require.NoError(t, err)
	}

	unwindAndCompare := func(t *testing.T, d *Domain, db kv.RwDB, unwindTo uint64) {
		t.Helper()
		tx, err := db.BeginRw(ctx)
		require.NoError(t, err)
		defer tx.Rollback()

		dc := d.BeginFilesRo()
		defer dc.Close()
		writer := dc.NewWriter()
		defer writer.close()

		totalDiff := []DomainEntryDiff{}
		if currTx > unwindTo {
			totalDiff = diffSetMap[currTx]
			fmt.Println(currTx)
			for currentTxNum := currTx - 1; currentTxNum >= unwindTo; currentTxNum-- {
				d := diffSetMap[currentTxNum]
				totalDiff = MergeDiffSets(totalDiff, d)
			}
		}

		err = dc.Unwind(ctx, tx, unwindTo/d.aggregationStep, unwindTo, totalDiff)
		currTx = unwindTo
		require.NoError(t, err)
		dc.Close()
		tx.Commit()

		t.Log("=====write expected data===== \n\n")
		tmpDb, expected := testDbAndDomain(t, log.New())
		defer expected.Close()
		defer tmpDb.Close()
		writeKeys(t, expected, tmpDb, unwindTo)

		suf := fmt.Sprintf(";unwindTo=%d", unwindTo)
		t.Run("DomainRangeLatest"+suf, func(t *testing.T) {
			t.Helper()

			etx, err := tmpDb.BeginRo(ctx)
			defer etx.Rollback()
			require.NoError(t, err)

			utx, err := db.BeginRo(ctx)
			defer utx.Rollback()
			require.NoError(t, err)

			ectx := expected.BeginFilesRo()
			defer ectx.Close()
			uc := d.BeginFilesRo()
			defer uc.Close()
			et, err := ectx.DomainRangeLatest(etx, nil, nil, -1)
			require.NoError(t, err)

			ut, err := uc.DomainRangeLatest(utx, nil, nil, -1)
			require.NoError(t, err)
			compareIterators(t, et, ut)
		})

		t.Run("DomainRange"+suf, func(t *testing.T) {
			t.Helper()

			etx, err := tmpDb.BeginRo(ctx)
			defer etx.Rollback()
			require.NoError(t, err)

			utx, err := db.BeginRo(ctx)
			defer utx.Rollback()
			require.NoError(t, err)

			ectx := expected.BeginFilesRo()
			defer ectx.Close()
			uc := d.BeginFilesRo()
			defer uc.Close()
			et, err := ectx.DomainRange(context.Background(), etx, nil, nil, unwindTo, order.Asc, -1)
			require.NoError(t, err)

			ut, err := uc.DomainRange(context.Background(), etx, nil, nil, unwindTo, order.Asc, -1)
			require.NoError(t, err)

			compareIterators(t, et, ut)
		})
		t.Run("WalkAsOf"+suf, func(t *testing.T) {
			t.Helper()

			etx, err := tmpDb.BeginRo(ctx)
			defer etx.Rollback()
			require.NoError(t, err)

			utx, err := db.BeginRo(ctx)
			defer utx.Rollback()
			require.NoError(t, err)

			ectx := expected.BeginFilesRo()
			defer ectx.Close()
			uc := d.BeginFilesRo()
			defer uc.Close()

			et, err := ectx.ht.WalkAsOf(context.Background(), unwindTo-1, nil, nil, order.Asc, -1, etx)
			require.NoError(t, err)

			ut, err := uc.ht.WalkAsOf(context.Background(), unwindTo-1, nil, nil, order.Asc, -1, utx)
			require.NoError(t, err)

			compareIterators(t, et, ut)
		})
		t.Run("HistoryRange"+suf, func(t *testing.T) {
			t.Helper()

			etx, err := tmpDb.BeginRo(ctx)
			defer etx.Rollback()
			require.NoError(t, err)

			utx, err := db.BeginRo(ctx)
			defer utx.Rollback()
			require.NoError(t, err)

			ectx := expected.BeginFilesRo()
			defer ectx.Close()
			uc := d.BeginFilesRo()
			defer uc.Close()

			et, err := ectx.ht.HistoryRange(int(unwindTo)-1, -1, order.Asc, -1, etx)
			require.NoError(t, err)

			ut, err := uc.ht.HistoryRange(int(unwindTo)-1, -1, order.Asc, -1, utx)
			require.NoError(t, err)

			compareIteratorsS(t, et, ut)
		})
	}

	writeKeys(t, d, db, maxTx)
	unwindAndCompare(t, d, db, 14)
	unwindAndCompare(t, d, db, 11)
	unwindAndCompare(t, d, db, 10)
	unwindAndCompare(t, d, db, 8)
	unwindAndCompare(t, d, db, 6)
	unwindAndCompare(t, d, db, 5)
	unwindAndCompare(t, d, db, 2)

	return
}

func compareIterators(t *testing.T, et, ut stream.KV) {
	t.Helper()

	/* uncomment when mismatches amount of keys in expectedIter and unwindedIter*/
	//i := 0
	//for {
	//	ek, ev, err1 := et.Next()
	//	fmt.Printf("ei=%d %s %s %v\n", i, ek, ev, err1)
	//	i++
	//	if !et.HasNext() {
	//		break
	//	}
	//}
	//
	//i = 0
	//for {
	//	uk, uv, err2 := ut.Next()
	//	fmt.Printf("ui=%d %s %s %v\n", i, string(uk), string(uv), err2)
	//	i++
	//	if !ut.HasNext() {
	//		break
	//	}
	//}
	for {
		ek, ev, err1 := et.Next()
		uk, uv, err2 := ut.Next()
		require.EqualValues(t, err1, err2)
		require.EqualValues(t, string(ek), string(uk))
		require.EqualValues(t, string(ev), string(uv))
		if !et.HasNext() {
			require.False(t, ut.HasNext(), "unwindedIter has more keys than expectedIter got\n")
			break
		}
	}
}
func compareIteratorsS(t *testing.T, et, ut stream.KVS) {
	t.Helper()
	for {
		ek, ev, estep, err1 := et.Next()
		uk, uv, ustep, err2 := ut.Next()
		require.EqualValues(t, err1, err2)
		require.EqualValues(t, ek, uk)
		require.EqualValues(t, ev, uv)
		require.EqualValues(t, estep, ustep)
		if !et.HasNext() {
			require.False(t, ut.HasNext(), "unwindedIter has more keys than expectedIter got\n")
			break
		}
	}
}

func TestDomain_PruneSimple(t *testing.T) {
	t.Parallel()

	pruningKey := common.FromHex("701b39aee8d1ee500442d2874a6e6d0cc9dad8d9")
	writeOneKey := func(t *testing.T, d *Domain, db kv.RwDB, maxTx, stepSize uint64) {
		t.Helper()

		ctx := context.Background()

		d.aggregationStep = stepSize

		dc := d.BeginFilesRo()
		defer dc.Close()
		tx, err := db.BeginRw(ctx)
		require.NoError(t, err)
		defer tx.Rollback()
		writer := dc.NewWriter()
		defer writer.close()

		for i := 0; uint64(i) < maxTx; i++ {
			writer.SetTxNum(uint64(i))
			err = writer.PutWithPrev(pruningKey, nil, []byte(fmt.Sprintf("value.%d", i)), nil, uint64(i-1)/d.aggregationStep)
			require.NoError(t, err)
		}

		err = writer.Flush(ctx, tx)
		require.NoError(t, err)

		err = tx.Commit()
		require.NoError(t, err)
	}

	pruneOneKeyHistory := func(t *testing.T, dc *DomainRoTx, db kv.RwDB, pruneFrom, pruneTo uint64) {
		t.Helper()
		// prune history
		ctx := context.Background()
		tx, err := db.BeginRw(ctx)
		require.NoError(t, err)
		_, err = dc.ht.Prune(ctx, tx, pruneFrom, pruneTo, math.MaxUint64, true, time.NewTicker(time.Second))
		require.NoError(t, err)
		err = tx.Commit()
		require.NoError(t, err)
	}

	pruneOneKeyDomain := func(t *testing.T, dc *DomainRoTx, db kv.RwDB, step, pruneFrom, pruneTo uint64) {
		t.Helper()
		// prune
		ctx := context.Background()
		tx, err := db.BeginRw(ctx)
		require.NoError(t, err)
		_, err = dc.Prune(ctx, tx, step, pruneFrom, pruneTo, math.MaxUint64, time.NewTicker(time.Second))
		require.NoError(t, err)
		err = tx.Commit()
		require.NoError(t, err)
	}

	checkKeyPruned := func(t *testing.T, dc *DomainRoTx, db kv.RwDB, stepSize, pruneFrom, pruneTo uint64) {
		t.Helper()

		ctx := context.Background()
		tx, err := db.BeginRw(ctx)
		require.NoError(t, err)
		defer tx.Rollback()

		it, err := dc.ht.IdxRange(pruningKey, 0, int(stepSize), order.Asc, math.MaxInt, tx)
		require.NoError(t, err)

		for it.HasNext() {
			txn, err := it.Next()
			require.NoError(t, err)
			require.Truef(t, txn < pruneFrom || txn >= pruneTo, "txn %d should be pruned", txn)
		}

		hit, err := dc.ht.HistoryRange(0, int(stepSize), order.Asc, math.MaxInt, tx)
		require.NoError(t, err)

		for hit.HasNext() {
			k, v, _, err := hit.Next()
			require.NoError(t, err)

			require.EqualValues(t, pruningKey, k)
			if len(v) > 0 {
				txn, err := strconv.Atoi(string(bytes.Split(v, []byte("."))[1])) // value.<txn>
				require.NoError(t, err)
				require.Truef(t, uint64(txn) < pruneFrom || uint64(txn) >= pruneTo, "txn %d should be pruned", txn)
			}
		}
	}

	t.Run("simple history inside 1step", func(t *testing.T) {
		db, d := testDbAndDomain(t, log.New())
		defer db.Close()
		defer d.Close()

		stepSize, pruneFrom, pruneTo := uint64(10), uint64(13), uint64(17)
		writeOneKey(t, d, db, 3*stepSize, stepSize)

		dc := d.BeginFilesRo()
		defer dc.Close()
		pruneOneKeyHistory(t, dc, db, pruneFrom, pruneTo)

		checkKeyPruned(t, dc, db, stepSize, pruneFrom, pruneTo)
	})

	t.Run("simple history between 2 steps", func(t *testing.T) {
		db, d := testDbAndDomain(t, log.New())
		defer db.Close()
		defer d.Close()

		stepSize, pruneFrom, pruneTo := uint64(10), uint64(8), uint64(17)
		writeOneKey(t, d, db, 3*stepSize, stepSize)

		dc := d.BeginFilesRo()
		defer dc.Close()
		pruneOneKeyHistory(t, dc, db, pruneFrom, pruneTo)

		checkKeyPruned(t, dc, db, stepSize, pruneFrom, pruneTo)
	})

	t.Run("simple prune whole step", func(t *testing.T) {
		db, d := testDbAndDomain(t, log.New())
		defer db.Close()
		defer d.Close()

		stepSize, pruneFrom, pruneTo := uint64(10), uint64(0), uint64(10)
		writeOneKey(t, d, db, 3*stepSize, stepSize)

		ctx := context.Background()
		rotx, err := db.BeginRo(ctx)
		require.NoError(t, err)

		dc := d.BeginFilesRo()
		v, vs, ok, err := dc.GetLatest(pruningKey, nil, rotx)
		require.NoError(t, err)
		require.True(t, ok)
		t.Logf("v=%s vs=%d", v, vs)
		dc.Close()

		c, err := d.collate(ctx, 0, pruneFrom, pruneTo, rotx)
		require.NoError(t, err)
		sf, err := d.buildFiles(ctx, 0, c, background.NewProgressSet())
		require.NoError(t, err)
		d.integrateDirtyFiles(sf, pruneFrom, pruneTo)
		d.reCalcVisibleFiles(d.dirtyFilesEndTxNumMinimax())
		rotx.Rollback()

		dc = d.BeginFilesRo()
		pruneOneKeyDomain(t, dc, db, 0, pruneFrom, pruneTo)
		dc.Close()
		//checkKeyPruned(t, dc, db, stepSize, pruneFrom, pruneTo)

		rotx, err = db.BeginRo(ctx)
		defer rotx.Rollback()
		require.NoError(t, err)

		v, vs, ok, err = dc.GetLatest(pruningKey, nil, rotx)
		require.NoError(t, err)
		require.True(t, ok)
		t.Logf("v=%s vs=%d", v, vs)
		require.EqualValuesf(t, 2, vs, "expected value of step 2")
	})

	t.Run("simple history discard", func(t *testing.T) {
		db, d := testDbAndDomain(t, log.New())
		defer db.Close()
		defer d.Close()

		stepSize, pruneFrom, pruneTo := uint64(10), uint64(0), uint64(20)
		writeOneKey(t, d, db, 2*stepSize, stepSize)

		dc := d.BeginFilesRo()
		defer dc.Close()
		pruneOneKeyHistory(t, dc, db, pruneFrom, pruneTo)

		checkKeyPruned(t, dc, db, stepSize, pruneFrom, pruneTo)
	})
}

func TestDomainContext_findShortenedKey(t *testing.T) {
	db, d := testDbAndDomain(t, log.New())
	tx, err := db.BeginRw(context.Background())
	require.NoError(t, err)
	defer tx.Rollback()

	d.historyLargeValues = true
	dc := d.BeginFilesRo()
	defer dc.Close()
	writer := dc.NewWriter()
	defer writer.close()

	keySize1 := uint64(length.Addr)
	keySize2 := uint64(length.Addr + length.Hash)
	totalTx := uint64(5000)
	keyTxsLimit := uint64(50)
	keyLimit := uint64(200)

	// put some kvs
	data := generateTestData(t, keySize1, keySize2, totalTx, keyTxsLimit, keyLimit)
	for key, updates := range data {
		p := []byte{}
		for i := 0; i < len(updates); i++ {
			writer.SetTxNum(updates[i].txNum)
			writer.PutWithPrev([]byte(key), nil, updates[i].value, p, 0)
			p = common.Copy(updates[i].value)
		}
	}
	writer.SetTxNum(totalTx)

	err = writer.Flush(context.Background(), tx)
	require.NoError(t, err)

	// aggregate
	collateAndMerge(t, db, tx, d, totalTx) // expected to left 2 latest steps in db

	require.NoError(t, tx.Commit())

	tx, err = db.BeginRw(context.Background())
	require.NoError(t, err)
	defer tx.Rollback()
	dc.Close()

	dc = d.BeginFilesRo()

	findFile := func(start, end uint64) *filesItem {
		var foundFile *filesItem
		dc.d.dirtyFiles.Walk(func(items []*filesItem) bool {
			for _, item := range items {
				if item.startTxNum == start && item.endTxNum == end {
					foundFile = item
					return false
				}
			}
			return true
		})
		return foundFile
	}

	var ki int
	for key, updates := range data {

		v, found, st, en, err := dc.getFromFiles([]byte(key), 0)
		require.True(t, found)
		require.NoError(t, err)
		for i := len(updates) - 1; i >= 0; i-- {
			if st <= updates[i].txNum && updates[i].txNum < en {
				require.EqualValues(t, updates[i].value, v)
				break
			}
		}

		lastFile := findFile(st, en)
		require.NotNilf(t, lastFile, "%d-%d", st/dc.d.aggregationStep, en/dc.d.aggregationStep)

		lf := seg.NewReader(lastFile.decompressor.MakeGetter(), d.compression)

		shortenedKey, found := dc.findShortenedKey([]byte(key), lf, lastFile)
		require.Truef(t, found, "key %d/%d %x file %d %d %s", ki, len(data), []byte(key), lastFile.startTxNum, lastFile.endTxNum, lastFile.decompressor.FileName())
		require.NotNil(t, shortenedKey)
		ki++
	}
}

func TestCanBuild(t *testing.T) {
	db, d := testDbAndDomain(t, log.New())
	tx, err := db.BeginRw(context.Background())
	require.NoError(t, err)
	defer tx.Rollback()

	d.historyLargeValues = true
	dc := d.BeginFilesRo()
	defer dc.Close()

	dc.files = append(dc.files, visibleFile{startTxNum: 0, endTxNum: d.aggregationStep})

	writer := dc.NewWriter()
	defer writer.close()

	k, v := []byte{1}, []byte{1}
	// db has data which already in files
	writer.SetTxNum(0)
	_ = writer.PutWithPrev(k, nil, v, nil, 0)
	_ = writer.Flush(context.Background(), tx)
	canBuild := dc.canBuild(tx)
	require.NoError(t, err)
	require.False(t, canBuild)

	// db has data which already in files and next step. still not enough - we need full step in db.
	writer.SetTxNum(d.aggregationStep)
	_ = writer.PutWithPrev(k, nil, v, nil, 0)
	_ = writer.Flush(context.Background(), tx)
	canBuild = dc.canBuild(tx)
	require.NoError(t, err)
	require.False(t, canBuild)
	_ = writer.PutWithPrev(k, nil, v, nil, 0)

	// db has: 1. data which already in files 2. full next step 3. a bit of next-next step. -> can build
	writer.SetTxNum(d.aggregationStep * 2)
	_ = writer.PutWithPrev(k, nil, v, nil, 0)
	_ = writer.Flush(context.Background(), tx)
	canBuild = dc.canBuild(tx)
	require.NoError(t, err)
	require.True(t, canBuild)
	_ = writer.PutWithPrev(k, nil, hexutility.EncodeTs(d.aggregationStep*2+1), nil, 0)
}
