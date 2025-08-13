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
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/dir"
	"github.com/erigontech/erigon-lib/common/length"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/mdbx"
	"github.com/erigontech/erigon/db/recsplit"
	"github.com/erigontech/erigon/db/seg"
)

func testDbAndAggregatorBench(b *testing.B, aggStep uint64) (kv.RwDB, *Aggregator) {
	b.Helper()
	logger := log.New()
	dirs := datadir.New(b.TempDir())
	db := mdbx.New(kv.ChainDB, logger).InMem(dirs.Chaindata).MustOpen()
	b.Cleanup(db.Close)
	salt, err := GetStateIndicesSalt(dirs, true, logger)
	require.NoError(b, err)
	agg, err := NewAggregator2(context.Background(), dirs, aggStep, salt, db, logger)
	require.NoError(b, err)
	b.Cleanup(agg.Close)
	return db, agg
}

func BenchmarkAggregator_Processing(b *testing.B) {
	ctx := b.Context()

	longKeys := queueKeys(ctx, 64, length.Addr+length.Hash)
	vals := queueKeys(ctx, 53, length.Hash)

	aggStep := uint64(100_00)
	_db, agg := testDbAndAggregatorBench(b, aggStep)
	db := wrapDbWithCtx(_db, agg)

	tx, err := db.BeginTemporalRw(ctx)
	require.NoError(b, err)
	defer tx.Rollback()

	domains, err := NewSharedDomains(tx, log.New())
	require.NoError(b, err)
	defer domains.Close()

	b.ReportAllocs()
	b.ResetTimer()
	var blockNum uint64
	var prev []byte
	for i := 0; i < b.N; i++ {
		key := <-longKeys
		val := <-vals
		txNum := uint64(i)
		domains.SetTxNum(txNum)
		err := domains.DomainPut(kv.StorageDomain, tx, key, val, txNum, prev, 0)
		prev = val
		require.NoError(b, err)

		if i%100000 == 0 {
			_, err := domains.ComputeCommitment(ctx, true, blockNum, txNum, "")
			require.NoError(b, err)
		}
	}
}

func queueKeys(ctx context.Context, seed, ofSize uint64) <-chan []byte {
	rnd := newRnd(seed)
	keys := make(chan []byte, 1)
	go func() {
		for {
			if ctx.Err() != nil { //nolint:staticcheck
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

func Benchmark_BtreeIndex_Search(b *testing.B) {
	logger := log.New()
	rnd := newRnd(uint64(time.Now().UnixNano()))
	tmp := b.TempDir()
	defer dir.RemoveAll(tmp)
	dataPath := "../../data/storage.256-288.kv"

	indexPath := filepath.Join(tmp, filepath.Base(dataPath)+".bti")
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
		require.Equal(b, keys[p], cur.Key())
		require.NotEmptyf(b, cur.Value(), "i=%d", i)
		cur.Close()
	}
}

func benchInitBtreeIndex(b *testing.B, M uint64, compression seg.FileCompression) (*seg.Decompressor, *BtIndex, [][]byte, string) {
	b.Helper()

	logger := log.New()
	tmp := b.TempDir()
	b.Cleanup(func() { dir.RemoveAll(tmp) })

	dataPath := generateKV(b, tmp, 52, 10, 1000000, logger, 0)
	indexPath := filepath.Join(tmp, filepath.Base(dataPath)+".bt")

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

			require.Equal(b, keys[p], cur.key)
			cur.Close()
		}
	})

	b.Run("seek_then_next", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			p := rnd.IntN(len(keys))

			cur, err := bt.Seek(getter, keys[p])
			require.NoError(b, err)

			require.Equal(b, keys[p], cur.key)

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

	defer dir.RemoveAll(tmp)

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
		require.Equal(b, keys[p], key)
	}
}

func BenchmarkAggregator_BeginFilesRo_Latency(b *testing.B) {
	//BenchmarkAggregator_BeginFilesRo/begin_files_ro-16  1737404  737.3 ns/op  3216 B/op  21 allocs/op
	aggStep := uint64(100_00)
	_, agg := testDbAndAggregatorBench(b, aggStep)

	b.Run("begin_files_ro", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			agg.BeginFilesRo()
		}
	})
}

var parallel = flag.Int("bench.parallel", 1, "parallelism value") // runs 1 *maxprocs
var loopv = flag.Int("bench.loopv", 100000, "loop value")

func BenchmarkAggregator_BeginFilesRo_Throughput(b *testing.B) {
	// RESULT: deteriorates after 2^21 goroutines

	/**
	for cpu in $(seq 0 20); do
		cpus=$((1 << $cpu))  # Same as 2^cpu
		echo -n "($cpus, "
		echo -n $(go test -benchmem -run=^$ -bench ^BenchmarkAggregator_BeginFilesRo_Throughput$ github.com/erigontech/erigon/db/state  \
		-bench.parallel=$cpus -bench.loopv=1000 | grep 'BenchmarkAggregator_BeginFilesRo_Throughput' | cut -f3 | xargs|cut -d' ' -f1)
		echo -n "), "
	done
	**/
	// trying to find BeginFilesRo throughput
	if !flag.Parsed() {
		flag.Parse()
	}
	//b.Logf("Running with parallel=%d work=%d, #goroutines:%d", *parallel, *loopv, *parallel*runtime.GOMAXPROCS(0))

	aggStep := uint64(100_00)
	_, agg := testDbAndAggregatorBench(b, aggStep)

	b.SetParallelism(*parallel) // p * maxprocs
	b.RunParallel(func(b *testing.PB) {
		foo := 0
		for b.Next() {
			tx := agg.BeginFilesRo()
			for i := 0; i < *loopv; i++ {
				foo *= 2
				foo /= 2
			}
			tx.Close()
		}
	})
}

func BenchmarkDb_BeginFiles_Throughput(b *testing.B) {
	// RESULT: deteriorates after 2^21 goroutines.

	/**
	for cpu in $(seq 0 20); do
	    cpus=$((1 << $cpu))  # Same as 2^cpu
	    echo -n "($cpus, "
	    echo -n $(go test -benchmem -run=^$ -bench ^BenchmarkDb_BeginFiles_Throughput$ github.com/erigontech/erigon/db/state  \
		-bench.parallel=$cpus -bench.loopv=1000 | grep 'BenchmarkDb_BeginFiles_Throughput' | cut -f3 | xargs|cut -d' ' -f1)
	    echo -n "), "
	done
	**/

	// trying to find BeginFilesRo and Rollback throughput
	if !flag.Parsed() {
		flag.Parse()
	}
	//b.Logf("Running with parallel=%d work=%d, #goroutines:%d", *parallel, *loopv, *parallel*runtime.GOMAXPROCS(0))

	aggStep := uint64(100_00)
	db, _ := testDbAndAggregatorBench(b, aggStep)
	ctx := context.Background()

	b.SetParallelism(*parallel) // p * maxprocs
	b.RunParallel(func(pb *testing.PB) {
		//foo := 0
		for pb.Next() {
			tx, err := db.BeginRo(ctx)
			if err != nil {
				b.Fatalf("%v", err)
			}
			millis := *loopv * 1000000
			time.Sleep(time.Duration(int64(millis)))

			// for i := 0; i < *loopv; i++ {
			// 	foo *= 2
			// 	foo /= 2
			// }
			tx.Rollback()
		}
	})
}

func BenchmarkDb_BeginFiles_Throughput_IO(b *testing.B) {
	// RESULT: deteriorates after 2^17 goroutines i.e. 130k goroutines.
	// time.Sleep to emulate page faults

	/**
	for cpu in $(seq 0 20); do
	    cpus=$((1 << $cpu))  # Same as 2^cpu
	    echo -n "($cpus, "
	    echo -n $(go test -benchmem -run=^$ -bench ^BenchmarkDb_BeginFiles_Throughput_IO$ github.com/erigontech/erigon/db/state  \
		-bench.parallel=$cpus | grep 'BenchmarkDb_BeginFiles_Throughput_IO' | cut -f3 | xargs|cut -d' ' -f1)
	    echo -n "), "
	done
	**/

	// trying to find BeginFilesRo and Rollback throughput
	if !flag.Parsed() {
		flag.Parse()
	}
	//b.Logf("Running with parallel=%d work=%d, #goroutines:%d", *parallel, *loopv, *parallel*runtime.GOMAXPROCS(0))

	aggStep := uint64(100_00)
	db, _ := testDbAndAggregatorBench(b, aggStep)
	ctx := context.Background()

	b.SetParallelism(*parallel) // p * maxprocs
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			tx, err := db.BeginRo(ctx)
			if err != nil {
				b.Fatalf("%v", err)
			}
			millis := 5 * time.Millisecond
			time.Sleep(time.Duration(int64(millis)))
			tx.Rollback()
		}
	})
}
