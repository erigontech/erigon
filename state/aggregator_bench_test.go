package state

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"path"
	"path/filepath"
	"testing"
	"time"

	"github.com/ledgerwatch/log/v3"
	"github.com/stretchr/testify/require"

	"github.com/ledgerwatch/erigon-lib/commitment"
	"github.com/ledgerwatch/erigon-lib/common/length"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/mdbx"
)

func testDbAndAggregatorBench(b *testing.B, aggStep uint64) (string, kv.RwDB, *Aggregator) {
	b.Helper()
	path := b.TempDir()
	b.Cleanup(func() { os.RemoveAll(path) })
	logger := log.New()
	db := mdbx.NewMDBX(logger).InMem(path).WithTableCfg(func(defaultBuckets kv.TableCfg) kv.TableCfg {
		return kv.ChaindataTablesCfg
	}).MustOpen()
	b.Cleanup(db.Close)
	agg, err := NewAggregator(path, path, aggStep, CommitmentModeDirect, commitment.VariantHexPatriciaTrie)
	require.NoError(b, err)
	b.Cleanup(agg.Close)
	return path, db, agg
}

func BenchmarkAggregator_Processing(b *testing.B) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	longKeys := queueKeys(ctx, 64, length.Addr+length.Hash)
	vals := queueKeys(ctx, 53, length.Hash)

	aggStep := uint64(100_00)
	_, db, agg := testDbAndAggregatorBench(b, aggStep)

	tx, err := db.BeginRw(ctx)
	require.NoError(b, err)
	defer func() {
		if tx != nil {
			tx.Rollback()
		}
	}()

	agg.SetTx(tx)
	defer agg.StartWrites().FinishWrites()
	require.NoError(b, err)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		key := <-longKeys
		val := <-vals
		txNum := uint64(i)
		agg.SetTxNum(txNum)
		err := agg.WriteAccountStorage(key[:length.Addr], key[length.Addr:], val)
		require.NoError(b, err)
		err = agg.FinishTx()
		require.NoError(b, err)
	}
}

func queueKeys(ctx context.Context, seed, ofSize uint64) <-chan []byte {
	rnd := rand.New(rand.NewSource(int64(seed)))
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
	rnd := rand.New(rand.NewSource(time.Now().UnixNano()))
	for i := 0; i < b.N; i++ {
		now := time.Now()
		count := rnd.Intn(1000000000)
		bt := newBtAlloc(uint64(count), uint64(1<<12), true)
		bt.traverseDfs()
		fmt.Printf("alloc %v\n", time.Since(now))
	}
}
func Benchmark_BtreeIndex_Search(b *testing.B) {
	rnd := rand.New(rand.NewSource(time.Now().UnixNano()))
	// max := 100000000
	// count := rnd.Intn(max)
	// bt := newBtAlloc(uint64(count), uint64(1<<11))
	// bt.traverseDfs()
	// fmt.Printf("alloc %v\n", time.Since(now))

	tmp := b.TempDir()

	// dataPath := generateCompressedKV(b, tmp, 52, 10, keyCount)
	defer os.RemoveAll(tmp)
	dir, _ := os.Getwd()
	fmt.Printf("path %s\n", dir)
	dataPath := "../../data/storage.256-288.kv"

	indexPath := path.Join(tmp, filepath.Base(dataPath)+".bti")
	err := BuildBtreeIndex(dataPath, indexPath)
	require.NoError(b, err)

	M := 1024
	bt, err := OpenBtreeIndex(indexPath, dataPath, uint64(M))

	require.NoError(b, err)

	idx := NewBtIndexReader(bt)

	keys, err := pivotKeysFromKV(dataPath)
	require.NoError(b, err)

	for i := 0; i < b.N; i++ {
		p := rnd.Intn(len(keys))
		cur, err := idx.Seek(keys[p])
		require.NoErrorf(b, err, "i=%d", i)
		require.EqualValues(b, keys[p], cur.key)
		require.NotEmptyf(b, cur.Value(), "i=%d", i)
	}

	bt.Close()
}
