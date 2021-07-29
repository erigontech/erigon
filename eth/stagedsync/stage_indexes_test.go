package stagedsync

import (
	"context"
	"encoding/binary"
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"testing"
	"time"

	"github.com/ledgerwatch/erigon-lib/kv"
	kv2 "github.com/ledgerwatch/erigon-lib/kv/memdb"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/changeset"
	"github.com/ledgerwatch/erigon/common/dbutils"
	"github.com/ledgerwatch/erigon/common/math"
	"github.com/ledgerwatch/erigon/crypto"
	"github.com/ledgerwatch/erigon/ethdb/bitmapdb"
	"github.com/ledgerwatch/erigon/ethdb/prune"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIndexGenerator_GenerateIndex_SimpleCase(t *testing.T) {
	db := kv2.NewTestDB(t)
	cfg := StageHistoryCfg(db, prune.DefaultMode, t.TempDir())
	test := func(blocksNum int, csBucket string) func(t *testing.T) {
		return func(t *testing.T) {
			tx, err := db.BeginRw(context.Background())
			require.NoError(t, err)
			defer tx.Rollback()

			csInfo, ok := changeset.Mapper[csBucket]
			if !ok {
				t.Fatal("incorrect cs bucket")
			}
			addrs, expecedIndexes := generateTestData(t, tx, csBucket, blocksNum)
			cfgCopy := cfg
			cfgCopy.bufLimit = 10
			cfgCopy.flushEvery = time.Microsecond
			err = promoteHistory("logPrefix", tx, csBucket, 0, uint64(blocksNum/2), cfgCopy, nil)
			require.NoError(t, err)
			err = promoteHistory("logPrefix", tx, csBucket, uint64(blocksNum/2), uint64(blocksNum), cfgCopy, nil)
			require.NoError(t, err)

			checkIndex(t, tx, csInfo.IndexBucket, addrs[0], expecedIndexes[string(addrs[0])])
			checkIndex(t, tx, csInfo.IndexBucket, addrs[1], expecedIndexes[string(addrs[1])])
			checkIndex(t, tx, csInfo.IndexBucket, addrs[2], expecedIndexes[string(addrs[2])])

		}
	}

	t.Run("account plain state", test(2100, kv.AccountChangeSet))
	t.Run("storage plain state", test(2100, kv.StorageChangeSet))

}

func TestIndexGenerator_Truncate(t *testing.T) {
	buckets := []string{kv.AccountChangeSet, kv.StorageChangeSet}
	tmpDir, ctx := t.TempDir(), context.Background()
	kv := kv2.NewTestDB(t)
	cfg := StageHistoryCfg(kv, prune.DefaultMode, t.TempDir())
	for i := range buckets {
		csbucket := buckets[i]

		tx, err := kv.BeginRw(context.Background())
		require.NoError(t, err)
		defer tx.Rollback()

		hashes, expected := generateTestData(t, tx, csbucket, 2100)
		mp := changeset.Mapper[csbucket]
		indexBucket := mp.IndexBucket
		cfgCopy := cfg
		cfgCopy.bufLimit = 10
		cfgCopy.flushEvery = time.Microsecond
		err = promoteHistory("logPrefix", tx, csbucket, 0, uint64(2100), cfgCopy, nil)
		require.NoError(t, err)

		reduceSlice := func(arr []uint64, timestamtTo uint64) []uint64 {
			pos := sort.Search(len(arr), func(i int) bool {
				return arr[i] > timestamtTo
			})
			return arr[:pos]
		}

		//t.Run("truncate to 2050 "+csbucket, func(t *testing.T) {
		expected[string(hashes[0])] = reduceSlice(expected[string(hashes[0])], 2050)
		expected[string(hashes[1])] = reduceSlice(expected[string(hashes[1])], 2050)
		expected[string(hashes[2])] = reduceSlice(expected[string(hashes[2])], 2050)

		err = unwindHistory("logPrefix", tx, csbucket, 2050, cfg, nil)
		require.NoError(t, err)

		checkIndex(t, tx, indexBucket, hashes[0], expected[string(hashes[0])])
		checkIndex(t, tx, indexBucket, hashes[1], expected[string(hashes[1])])
		checkIndex(t, tx, indexBucket, hashes[2], expected[string(hashes[2])])
		//})

		//t.Run("truncate to 2000 "+csbucket, func(t *testing.T) {
		expected[string(hashes[0])] = reduceSlice(expected[string(hashes[0])], 2000)
		expected[string(hashes[1])] = reduceSlice(expected[string(hashes[1])], 2000)
		expected[string(hashes[2])] = reduceSlice(expected[string(hashes[2])], 2000)

		err = unwindHistory("logPrefix", tx, csbucket, 2000, cfg, nil)
		require.NoError(t, err)

		checkIndex(t, tx, indexBucket, hashes[0], expected[string(hashes[0])])
		checkIndex(t, tx, indexBucket, hashes[1], expected[string(hashes[1])])
		checkIndex(t, tx, indexBucket, hashes[2], expected[string(hashes[2])])
		//})

		//t.Run("truncate to 1999 "+csbucket, func(t *testing.T) {
		err = unwindHistory("logPrefix", tx, csbucket, 1999, cfg, nil)
		require.NoError(t, err)
		expected[string(hashes[0])] = reduceSlice(expected[string(hashes[0])], 1999)
		expected[string(hashes[1])] = reduceSlice(expected[string(hashes[1])], 1999)
		expected[string(hashes[2])] = reduceSlice(expected[string(hashes[2])], 1999)

		checkIndex(t, tx, indexBucket, hashes[0], expected[string(hashes[0])])
		checkIndex(t, tx, indexBucket, hashes[1], expected[string(hashes[1])])
		checkIndex(t, tx, indexBucket, hashes[2], expected[string(hashes[2])])
		bm, err := bitmapdb.Get64(tx, indexBucket, hashes[0], 1999, math.MaxUint32)
		require.NoError(t, err)
		if bm.GetCardinality() > 0 && bm.Maximum() > 1999 {
			t.Fatal(bm.Maximum())
		}
		bm, err = bitmapdb.Get64(tx, indexBucket, hashes[1], 1999, math.MaxUint32)
		require.NoError(t, err)
		if bm.GetCardinality() > 0 && bm.Maximum() > 1999 {
			t.Fatal()
		}
		//})

		//t.Run("truncate to 999 "+csbucket, func(t *testing.T) {
		expected[string(hashes[0])] = reduceSlice(expected[string(hashes[0])], 999)
		expected[string(hashes[1])] = reduceSlice(expected[string(hashes[1])], 999)
		expected[string(hashes[2])] = reduceSlice(expected[string(hashes[2])], 999)

		err = unwindHistory("logPrefix", tx, csbucket, 999, cfg, nil)
		if err != nil {
			t.Fatal(err)
		}
		bm, err = bitmapdb.Get64(tx, indexBucket, hashes[0], 999, math.MaxUint32)
		require.NoError(t, err)
		if bm.GetCardinality() > 0 && bm.Maximum() > 999 {
			t.Fatal()
		}
		bm, err = bitmapdb.Get64(tx, indexBucket, hashes[1], 999, math.MaxUint32)
		require.NoError(t, err)
		if bm.GetCardinality() > 0 && bm.Maximum() > 999 {
			t.Fatal()
		}

		checkIndex(t, tx, indexBucket, hashes[0], expected[string(hashes[0])])
		checkIndex(t, tx, indexBucket, hashes[1], expected[string(hashes[1])])
		checkIndex(t, tx, indexBucket, hashes[2], expected[string(hashes[2])])

		//})
		err = pruneHistoryIndex(tx, csbucket, "", tmpDir, 128, ctx)
		assert.NoError(t, err)
		expectNoHistoryBefore(t, tx, csbucket, 128)

		// double prune is safe
		err = pruneHistoryIndex(tx, csbucket, "", tmpDir, 128, ctx)
		assert.NoError(t, err)
		expectNoHistoryBefore(t, tx, csbucket, 128)
		tx.Rollback()
	}
}

func expectNoHistoryBefore(t *testing.T, tx kv.Tx, csbucket string, prunedTo uint64) {
	prefixLen := common.AddressLength
	if csbucket == kv.StorageChangeSet {
		prefixLen = common.HashLength
	}
	afterPrune := 0
	err := tx.ForEach(changeset.Mapper[csbucket].IndexBucket, nil, func(k, _ []byte) error {
		n := binary.BigEndian.Uint64(k[prefixLen:])
		require.True(t, n >= prunedTo)
		afterPrune++
		return nil
	})
	require.True(t, afterPrune > 0)
	assert.NoError(t, err)
}

func generateTestData(t *testing.T, tx kv.RwTx, csBucket string, numOfBlocks int) ([][]byte, map[string][]uint64) { //nolint
	csInfo, ok := changeset.Mapper[csBucket]
	if !ok {
		t.Fatal("incorrect cs bucket")
	}
	var isPlain bool
	if kv.StorageChangeSet == csBucket || kv.AccountChangeSet == csBucket {
		isPlain = true
	}
	addrs, err := generateAddrs(3, isPlain)
	require.NoError(t, err)
	if kv.StorageChangeSet == csBucket {
		keys, innerErr := generateAddrs(3, false)
		require.NoError(t, innerErr)

		defaultIncarnation := make([]byte, 8)
		binary.BigEndian.PutUint64(defaultIncarnation, uint64(1))
		for i := range addrs {
			addrs[i] = append(addrs[i], defaultIncarnation...)
			addrs[i] = append(addrs[i], keys[i]...)
		}
	}

	res := make([]uint64, 0)
	res2 := make([]uint64, 0)
	res3 := make([]uint64, 0)

	for i := 0; i < numOfBlocks; i++ {
		cs := csInfo.New()
		err = cs.Add(addrs[0], []byte(strconv.Itoa(i)))
		require.NoError(t, err)

		res = append(res, uint64(i))

		if i%2 == 0 {
			err = cs.Add(addrs[1], []byte(strconv.Itoa(i)))
			require.NoError(t, err)
			res2 = append(res2, uint64(i))
		}
		if i%3 == 0 {
			err = cs.Add(addrs[2], []byte(strconv.Itoa(i)))
			require.NoError(t, err)
			res3 = append(res3, uint64(i))
		}
		err = csInfo.Encode(uint64(i), cs, func(k, v []byte) error {
			return tx.Put(csBucket, k, v)
		})
		require.NoError(t, err)
	}

	return addrs, map[string][]uint64{
		string(addrs[0]): res,
		string(addrs[1]): res2,
		string(addrs[2]): res3,
	}
}

func checkIndex(t *testing.T, db kv.Tx, bucket string, k []byte, expected []uint64) {
	t.Helper()
	k = dbutils.CompositeKeyWithoutIncarnation(k)
	m, err := bitmapdb.Get64(db, bucket, k, 0, math.MaxUint32)
	if err != nil {
		t.Fatal(err, common.Bytes2Hex(k))
	}
	val := m.ToArray()
	if !reflect.DeepEqual(val, expected) {
		fmt.Printf("get     : %v\n", val)
		fmt.Printf("expected: %v\n", toU32(expected))
		t.Fatal()
	}
}

func toU32(in []uint64) []uint32 {
	out := make([]uint32, len(in))
	for i := range in {
		out[i] = uint32(in[i])
	}
	return out
}

func generateAddrs(numOfAddrs int, isPlain bool) ([][]byte, error) {
	addrs := make([][]byte, numOfAddrs)
	for i := 0; i < numOfAddrs; i++ {
		key1, err := crypto.GenerateKey()
		if err != nil {
			return nil, err
		}
		addr := crypto.PubkeyToAddress(key1.PublicKey)
		if isPlain {
			addrs[i] = addr.Bytes()
			continue
		}
		hash, err := common.HashData(addr.Bytes())
		if err != nil {
			return nil, err
		}
		addrs[i] = hash.Bytes()
	}
	return addrs, nil
}
