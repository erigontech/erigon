package btindex

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/seg"
)

func BenchmarkBpsTreeSeek(t *testing.B) {
	tmp := t.TempDir()
	logger := log.New()
	keyCount, M := 12_000_000, 256
	if testing.Short() {
		keyCount = 10_000
	}
	t.Logf("N: %d, M: %d skip since shard <= %d", keyCount, M, DefaultBtreeStartSkip)
	compressFlags := seg.CompressKeys | seg.CompressVals

	dataPath := generateKV(t, tmp, 52, 180, keyCount, logger, 0)

	indexPath := filepath.Join(tmp, filepath.Base(dataPath)+".bti")
	buildBtreeIndex(t, dataPath, indexPath, compressFlags, 1, logger, true)

	kv, bt, err := OpenBtreeIndexAndDataFile(indexPath, dataPath, uint64(M), compressFlags, false)
	require.NoError(t, err)
	require.EqualValues(t, bt.KeyCount(), keyCount)
	defer bt.Close()
	defer kv.Close()

	var key []byte

	getter := seg.NewReader(kv.MakeGetter(), compressFlags)
	getter.Reset(0)

	t.ReportAllocs()
	//r := rand.New(rand.NewSource(0))
	for t.Loop() {
		if !getter.HasNext() {
			getter.Reset(0)
		}
		key, _ = getter.Next(key[:0])
		getter.Skip()
		//_, err := bt.Seek(getter, keys[r.Intn(len(keys))])
		c, err := bt.Seek(getter, key)
		require.NoError(t, err)
		c.Close()
	}
	t.ReportAllocs()
}

func BenchmarkBpsTreeGet(t *testing.B) {
	tmp := t.TempDir()
	logger := log.New()
	keyCount, M := 12_000_000, 256
	if testing.Short() {
		keyCount = 10_000
	}
	t.Logf("N: %d, M: %d", keyCount, M)
	compressFlags := seg.CompressKeys | seg.CompressVals

	dataPath := generateKV(t, tmp, 52, 180, keyCount, logger, 0)

	indexPath := filepath.Join(tmp, filepath.Base(dataPath)+".bti")
	buildBtreeIndex(t, dataPath, indexPath, compressFlags, 1, logger, true)

	kv, bt, err := OpenBtreeIndexAndDataFile(indexPath, dataPath, uint64(M), compressFlags, false)
	require.NoError(t, err)
	require.EqualValues(t, bt.KeyCount(), keyCount)
	defer bt.Close()
	defer kv.Close()

	var key []byte

	getter := seg.NewReader(kv.MakeGetter(), compressFlags)
	getter.Reset(0)

	t.ReportAllocs()
	for t.Loop() {
		if !getter.HasNext() {
			getter.Reset(0)
		}
		key, _ = getter.Next(key[:0])
		getter.Skip()
		_, v, _, ok, err := bt.Get(key, getter)
		require.NoError(t, err)
		require.True(t, ok)
		_ = v
	}
	t.ReportAllocs()
}

func BenchmarkBpsTreeSeekLargeFile(t *testing.B) {
	tmp := t.TempDir()
	logger := log.New()
	keyCount, M := 1_000_000, 256
	if testing.Short() {
		keyCount = 10_000
	}
	t.Logf("N: %d, M: %d (prefix index threshold: %d)", keyCount, M, minKeysForPrefixIndex)
	compressFlags := seg.CompressKeys | seg.CompressVals

	// 20-byte keys: typical Ethereum hash length
	dataPath := generateKV(t, tmp, 20, 32, keyCount, logger, 0)

	indexPath := filepath.Join(tmp, filepath.Base(dataPath)+".bti")
	buildBtreeIndex(t, dataPath, indexPath, compressFlags, 1, logger, true)

	kv, bt, err := OpenBtreeIndexAndDataFile(indexPath, dataPath, uint64(M), compressFlags, false)
	require.NoError(t, err)
	require.EqualValues(t, bt.KeyCount(), keyCount)
	defer bt.Close()
	defer kv.Close()

	getter := seg.NewReader(kv.MakeGetter(), compressFlags)
	var key []byte

	t.Run("WithPrefixIndex", func(t *testing.B) {
		getter.Reset(0)
		t.ReportAllocs()
		for t.Loop() {
			if !getter.HasNext() {
				getter.Reset(0)
			}
			key, _ = getter.Next(key[:0])
			getter.Skip()
			c, err := bt.Seek(getter, key)
			require.NoError(t, err)
			c.Close()
		}
	})

	t.Run("WithoutPrefixIndex", func(t *testing.B) {
		// Disable prefix index to measure the delta
		savedPrefix := bt.bplus.prefix
		bt.bplus.prefix = nil
		defer func() { bt.bplus.prefix = savedPrefix }()

		getter.Reset(0)
		t.ReportAllocs()
		for t.Loop() {
			if !getter.HasNext() {
				getter.Reset(0)
			}
			key, _ = getter.Next(key[:0])
			getter.Skip()
			c, err := bt.Seek(getter, key)
			require.NoError(t, err)
			c.Close()
		}
	})
}
