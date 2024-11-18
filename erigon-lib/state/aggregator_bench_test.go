// Copyright 2024 The Erigon Authors
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
	"fmt"
	"os"
	"path"
	"path/filepath"
	"testing"
	"time"

	"github.com/erigontech/erigon-lib/common/datadir"
	"github.com/erigontech/erigon-lib/log/v3"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/length"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/kv/mdbx"
	"github.com/erigontech/erigon-lib/recsplit"
	"github.com/erigontech/erigon-lib/seg"
)

func testDbAndAggregatorBench(b *testing.B, aggStep uint64) (kv.RwDB, *Aggregator) {
	b.Helper()
	logger := log.New()
	dirs := datadir.New(b.TempDir())
	db := mdbx.NewMDBX(logger).InMem(dirs.Chaindata).WithTableCfg(func(defaultBuckets kv.TableCfg) kv.TableCfg {
		return kv.ChaindataTablesCfg
	}).MustOpen()
	b.Cleanup(db.Close)
	agg, err := NewAggregator(context.Background(), dirs, aggStep, db, logger)
	require.NoError(b, err)
	b.Cleanup(agg.Close)
	return db, agg
}

type txWithCtx struct {
	kv.Tx
	ac *AggregatorRoTx
}

func WrapTxWithCtx(tx kv.Tx, ctx *AggregatorRoTx) *txWithCtx { return &txWithCtx{Tx: tx, ac: ctx} }
func (tx *txWithCtx) AggTx() any                             { return tx.ac }

func BenchmarkAggregator_Processing(b *testing.B) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	longKeys := queueKeys(ctx, 64, length.Addr+length.Hash)
	vals := queueKeys(ctx, 53, length.Hash)

	aggStep := uint64(100_00)
	db, agg := testDbAndAggregatorBench(b, aggStep)

	tx, err := db.BeginRw(ctx)
	require.NoError(b, err)
	defer func() {
		if tx != nil {
			tx.Rollback()
		}
	}()

	require.NoError(b, err)
	ac := agg.BeginFilesRo()
	defer ac.Close()

	domains, err := NewSharedDomains(WrapTxWithCtx(tx, ac), log.New())
	require.NoError(b, err)
	defer domains.Close()

	b.ReportAllocs()
	b.ResetTimer()

	var prev []byte
	for i := 0; i < b.N; i++ {
		key := <-longKeys
		val := <-vals
		txNum := uint64(i)
		domains.SetTxNum(txNum)
		err := domains.DomainPut(kv.StorageDomain, key[:length.Addr], key[length.Addr:], val, prev, 0)
		prev = val
		require.NoError(b, err)

		if i%100000 == 0 {
			_, err := domains.ComputeCommitment(ctx, true, domains.BlockNum(), "")
			require.NoError(b, err)
		}
	}
}

func queueKeys(ctx context.Context, seed, ofSize uint64) <-chan []byte {
	rnd := newRnd(seed)
	keys := make(chan []byte, 1)
	go func() {
		for {
			if ctx.Err() != nil {
				break
			}
			bb := make([]byte, ofSize)
			rnd.Read(bb)

			keys <- bb
		}
		close(keys)
	}()
	return keys
}

func Benchmark_BtreeIndex_Allocation(b *testing.B) {
	rnd := newRnd(uint64(time.Now().UnixNano()))
	for i := 0; i < b.N; i++ {
		now := time.Now()
		count := rnd.IntN(1000000000)
		bt := newBtAlloc(uint64(count), uint64(1<<12), true, nil, nil)
		bt.traverseDfs()
		fmt.Printf("alloc %v\n", time.Since(now))
	}
}

func Benchmark_BtreeIndex_Search(b *testing.B) {
	logger := log.New()
	rnd := newRnd(uint64(time.Now().UnixNano()))
	tmp := b.TempDir()
	defer os.RemoveAll(tmp)
	dataPath := "../../data/storage.256-288.kv"

	indexPath := path.Join(tmp, filepath.Base(dataPath)+".bti")
	comp := seg.CompressKeys | seg.CompressVals
	buildBtreeIndex(b, dataPath, indexPath, comp, 1, logger, true)

	M := 1024
	kv, bt, err := OpenBtreeIndexAndDataFile(indexPath, dataPath, uint64(M), comp, false)
	require.NoError(b, err)
	defer bt.Close()
	defer kv.Close()

	keys, err := pivotKeysFromKV(dataPath)
	require.NoError(b, err)
	getter := seg.NewReader(kv.MakeGetter(), comp)

	for i := 0; i < b.N; i++ {
		p := rnd.IntN(len(keys))
		cur, err := bt.Seek(getter, keys[p])
		require.NoErrorf(b, err, "i=%d", i)
		require.EqualValues(b, keys[p], cur.Key())
		require.NotEmptyf(b, cur.Value(), "i=%d", i)
		cur.Close()
	}
}

func benchInitBtreeIndex(b *testing.B, M uint64, compression seg.FileCompression) (*seg.Decompressor, *BtIndex, [][]byte, string) {
	b.Helper()

	logger := log.New()
	tmp := b.TempDir()
	b.Cleanup(func() { os.RemoveAll(tmp) })

	dataPath := generateKV(b, tmp, 52, 10, 1000000, logger, 0)
	indexPath := path.Join(tmp, filepath.Base(dataPath)+".bt")

	buildBtreeIndex(b, dataPath, indexPath, compression, 1, logger, true)

	kv, bt, err := OpenBtreeIndexAndDataFile(indexPath, dataPath, M, compression, false)
	require.NoError(b, err)
	b.Cleanup(func() { bt.Close() })
	b.Cleanup(func() { kv.Close() })

	keys, err := pivotKeysFromKV(dataPath)
	require.NoError(b, err)
	return kv, bt, keys, dataPath
}

func Benchmark_BTree_Seek(b *testing.B) {
	M := uint64(1024)
	compress := seg.CompressNone
	kv, bt, keys, _ := benchInitBtreeIndex(b, M, compress)
	rnd := newRnd(uint64(time.Now().UnixNano()))
	getter := seg.NewReader(kv.MakeGetter(), compress)

	b.Run("seek_only", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			p := rnd.IntN(len(keys))

			cur, err := bt.Seek(getter, keys[p])
			require.NoError(b, err)

			require.EqualValues(b, keys[p], cur.key)
			cur.Close()
		}
	})

	b.Run("seek_then_next", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			p := rnd.IntN(len(keys))

			cur, err := bt.Seek(getter, keys[p])
			require.NoError(b, err)

			require.EqualValues(b, keys[p], cur.key)

			prevKey := common.Copy(keys[p])
			ntimer := time.Duration(0)
			nextKeys := 5000
			for j := 0; j < nextKeys; j++ {
				ntime := time.Now()

				if !cur.Next() {
					break
				}
				ntimer += time.Since(ntime)

				nk := cur.Key()
				if bytes.Compare(prevKey, nk) > 0 {
					b.Fatalf("prev %s cur %s, next key should be greater", prevKey, nk)
				}
				prevKey = nk
			}
			if i%1000 == 0 {
				fmt.Printf("next_access_last[of %d keys] %v\n", nextKeys, ntimer/time.Duration(nextKeys))
			}
			cur.Close()
		}
	})
}

// requires existing KV index file at ../../data/storage.kv
func Benchmark_Recsplit_Find_ExternalFile(b *testing.B) {
	dataPath := "../../data/storage.kv"
	f, err := os.Stat(dataPath)
	if err != nil || f.IsDir() {
		b.Skip("requires existing KV index file at ../../data/storage.kv")
	}

	rnd := newRnd(uint64(time.Now().UnixNano()))
	tmp := b.TempDir()

	defer os.RemoveAll(tmp)

	indexPath := dataPath + "i"
	idx, err := recsplit.OpenIndex(indexPath)
	require.NoError(b, err)
	idxr := recsplit.NewIndexReader(idx)

	decomp, err := seg.NewDecompressor(dataPath)
	require.NoError(b, err)
	defer decomp.Close()

	getter := decomp.MakeGetter()

	keys, err := pivotKeysFromKV(dataPath)
	require.NoError(b, err)

	for i := 0; i < b.N; i++ {
		p := rnd.IntN(len(keys))

		offset, _ := idxr.Lookup(keys[p])
		getter.Reset(offset)

		require.True(b, getter.HasNext())

		key, pa := getter.Next(nil)
		require.NotEmpty(b, key)

		value, pb := getter.Next(nil)
		if pb-pa != 1 {
			require.NotEmpty(b, value)
		}

		require.NoErrorf(b, err, "i=%d", i)
		require.EqualValues(b, keys[p], key)
	}
}
