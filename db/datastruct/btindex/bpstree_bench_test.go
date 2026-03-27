package btindex

import (
	"bytes"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/dbg"
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

func BenchmarkPrefixIndexSeek(t *testing.B) {
	tmp := t.TempDir()
	logger := log.New()
	keyCount := 1_000_000
	if testing.Short() {
		keyCount = 10_000
	}
	compressFlags := seg.CompressKeys | seg.CompressVals

	dataPath := generateKV(t, tmp, 52, 180, keyCount, logger, 0)

	indexPath := filepath.Join(tmp, filepath.Base(dataPath)+".bti")
	buildBtreeIndex(t, dataPath, indexPath, compressFlags, 1, logger, true)

	saved := dbg.UsePrefixIndex
	dbg.UsePrefixIndex = true
	defer func() { dbg.UsePrefixIndex = saved }()

	kv, bt, err := OpenBtreeIndexAndDataFile(indexPath, dataPath, DefaultBtreeM, compressFlags, false)
	require.NoError(t, err)
	require.EqualValues(t, bt.KeyCount(), keyCount)
	defer bt.Close()
	defer kv.Close()
	require.NotNil(t, bt.search, "PrefixIndex should be initialized")

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
		c, err := bt.search.Seek(getter, key)
		require.NoError(t, err)
		c.Close()
	}
	t.ReportAllocs()
}

func BenchmarkPrefixIndexGet(t *testing.B) {
	keyCount := 1_000_000
	if testing.Short() {
		keyCount = 10_000
	}
	compress := seg.CompressKeys

	logger := log.New()
	kvPath := generateKV(t, t.TempDir(), 20, 10, keyCount, logger, compress)
	keys, err := pivotKeysFromKV(kvPath)
	require.NoError(t, err)

	indexPath := strings.TrimSuffix(kvPath, ".kv") + ".bt"
	buildBtreeIndex(t, kvPath, indexPath, compress, 1, logger, true)

	saved := dbg.UsePrefixIndex
	dbg.UsePrefixIndex = true
	defer func() { dbg.UsePrefixIndex = saved }()

	decomp, bt, err := OpenBtreeIndexAndDataFile(indexPath, kvPath, DefaultBtreeM, compress, false)
	require.NoError(t, err)
	defer bt.Close()
	defer decomp.Close()
	require.NotNil(t, bt.search, "PrefixIndex should be initialized")

	getter := seg.NewReader(decomp.MakeGetter(), compress)
	rnd := newRnd(uint64(t.N))

	t.ReportAllocs()
	t.ResetTimer()
	for t.Loop() {
		p := rnd.IntN(len(keys))
		v, ok, _, err := bt.search.Get(getter, keys[p])
		if err != nil {
			t.Fatal(err)
		}
		if !ok || v == nil {
			t.Fatal("key not found")
		}
	}
}

func BenchmarkSeekComparison(t *testing.B) {
	keyCount := 1_000_000
	if testing.Short() {
		keyCount = 10_000
	}
	compress := seg.CompressKeys

	logger := log.New()
	kvPath := generateKV(t, t.TempDir(), 20, 10, keyCount, logger, compress)
	keys, err := pivotKeysFromKV(kvPath)
	require.NoError(t, err)

	indexPath := strings.TrimSuffix(kvPath, ".kv") + ".bt"
	buildBtreeIndex(t, kvPath, indexPath, compress, 1, logger, true)

	saved := dbg.UsePrefixIndex
	dbg.UsePrefixIndex = true
	defer func() { dbg.UsePrefixIndex = saved }()

	decomp, bt, err := OpenBtreeIndexAndDataFile(indexPath, kvPath, DefaultBtreeM, compress, false)
	require.NoError(t, err)
	defer bt.Close()
	defer decomp.Close()

	getter := seg.NewReader(decomp.MakeGetter(), compress)

	t.Run("BpsTree", func(t *testing.B) {
		require.NotNil(t, bt.bplus)
		rnd := newRnd(42)
		t.ReportAllocs()
		t.ResetTimer()
		for t.Loop() {
			p := rnd.IntN(len(keys))
			c, err := bt.bplus.Seek(getter, keys[p])
			if err != nil {
				t.Fatal(err)
			}
			if c == nil || !bytes.Equal(c.Key(), keys[p]) {
				t.Fatalf("key mismatch at %d", p)
			}
			c.Close()
		}
	})

	t.Run("PrefixIndex", func(t *testing.B) {
		require.NotNil(t, bt.search)
		rnd := newRnd(42)
		t.ReportAllocs()
		t.ResetTimer()
		for t.Loop() {
			p := rnd.IntN(len(keys))
			c, err := bt.search.Seek(getter, keys[p])
			if err != nil {
				t.Fatal(err)
			}
			if c == nil || !bytes.Equal(c.Key(), keys[p]) {
				t.Fatalf("key mismatch at %d", p)
			}
			c.Close()
		}
	})
}

func BenchmarkGetComparison(t *testing.B) {
	keyCount := 1_000_000
	if testing.Short() {
		keyCount = 10_000
	}
	compress := seg.CompressKeys

	logger := log.New()
	kvPath := generateKV(t, t.TempDir(), 20, 10, keyCount, logger, compress)
	keys, err := pivotKeysFromKV(kvPath)
	require.NoError(t, err)

	indexPath := strings.TrimSuffix(kvPath, ".kv") + ".bt"
	buildBtreeIndex(t, kvPath, indexPath, compress, 1, logger, true)

	saved := dbg.UsePrefixIndex
	dbg.UsePrefixIndex = true
	defer func() { dbg.UsePrefixIndex = saved }()

	decomp, bt, err := OpenBtreeIndexAndDataFile(indexPath, kvPath, DefaultBtreeM, compress, false)
	require.NoError(t, err)
	defer bt.Close()
	defer decomp.Close()

	getter := seg.NewReader(decomp.MakeGetter(), compress)

	t.Run("BpsTree", func(t *testing.B) {
		require.NotNil(t, bt.bplus)
		rnd := newRnd(42)
		t.ReportAllocs()
		t.ResetTimer()
		for t.Loop() {
			p := rnd.IntN(len(keys))
			v, ok, _, err := bt.bplus.Get(getter, keys[p])
			if err != nil {
				t.Fatal(err)
			}
			if !ok || v == nil {
				t.Fatal("key not found")
			}
		}
	})

	t.Run("PrefixIndex", func(t *testing.B) {
		require.NotNil(t, bt.search)
		rnd := newRnd(42)
		t.ReportAllocs()
		t.ResetTimer()
		for t.Loop() {
			p := rnd.IntN(len(keys))
			v, ok, _, err := bt.search.Get(getter, keys[p])
			if err != nil {
				t.Fatal(err)
			}
			if !ok || v == nil {
				t.Fatal("key not found")
			}
		}
	})
}
