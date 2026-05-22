package btindex

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/recsplit/eliasfano32"
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
	keyCount, M := 100_000, 256
	compressFlags := seg.CompressKeys | seg.CompressVals

	dataPath := generateKV(t, tmp, 52, 180, keyCount, logger, 0)
	indexPath := filepath.Join(tmp, filepath.Base(dataPath)+".bti")
	buildBtreeIndex(t, dataPath, indexPath, compressFlags, 1, logger, true)

	kv, bt, err := OpenBtreeIndexAndDataFile(indexPath, dataPath, uint64(M), compressFlags, false)
	require.NoError(t, err)
	require.EqualValues(t, bt.KeyCount(), keyCount)
	defer bt.Close()
	defer kv.Close()

	// collect all keys
	getter := seg.NewReader(kv.MakeGetter(), compressFlags)
	getter.Reset(0)
	keys := make([][]byte, 0, keyCount)
	for getter.HasNext() {
		k, _ := getter.Next(nil)
		getter.Skip()
		keys = append(keys, k)
	}

	rng := rand.New(rand.NewSource(42))
	t.ResetTimer()
	t.ReportAllocs()
	for t.Loop() {
		k := keys[rng.Intn(len(keys))]
		_, v, _, ok, err := bt.Get(k, getter)
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			t.Fatalf("key not found: %x", k)
		}
		if len(v) == 0 {
			t.Fatalf("empty value for key: %x", k)
		}
	}
}

func BenchmarkBpsTreeGetReal(t *testing.B) {
	dataDir := os.Getenv("ERIGON_DATADIR")
	if dataDir == "" {
		t.Skip("set ERIGON_DATADIR to snapshots/domain dir with .kv and .bt files")
	}
	// Find first .kv + .bt pair
	entries, err := os.ReadDir(dataDir)
	require.NoError(t, err)

	// Build maps of .kv and .bt files keyed by domain+range (e.g. "accounts.0-128")
	kvFiles := map[string]string{}
	btFiles := map[string]string{}
	for _, e := range entries {
		name := e.Name()
		// strip version prefix like "v2.0-" or "v1.1-"
		idx := 0
		for idx < len(name) && name[idx] != '-' {
			idx++
		}
		if idx >= len(name) {
			continue
		}
		stem := name[idx+1:]
		ext := filepath.Ext(stem)
		key := stem[:len(stem)-len(ext)]
		switch ext {
		case ".kv":
			kvFiles[key] = filepath.Join(dataDir, name)
		case ".bt":
			btFiles[key] = filepath.Join(dataDir, name)
		}
	}
	// Pick the largest .kv file that has a matching .bt
	var kvPath, btPath string
	var bestSize int64
	for key, kv := range kvFiles {
		if bt, ok := btFiles[key]; ok {
			fi, err := os.Stat(kv)
			if err != nil {
				continue
			}
			if fi.Size() > bestSize {
				bestSize = fi.Size()
				kvPath = kv
				btPath = bt
			}
		}
	}
	if kvPath == "" {
		t.Skip("no .kv + .bt pair found in " + dataDir)
	}
	t.Logf("kv: %s", filepath.Base(kvPath))

	d, err := seg.NewDecompressor(kvPath)
	require.NoError(t, err)
	defer d.Close()

	compressFlags := seg.DetectCompressType(d.MakeGetter())
	t.Logf("compression: %d", compressFlags)

	getter := seg.NewReader(d.MakeGetter(), compressFlags)
	bt, err := OpenBtreeIndexWithDecompressor(btPath, 256, getter)
	require.NoError(t, err)
	defer bt.Close()

	// Collect sample keys
	getter.Reset(0)
	keys := make([][]byte, 0, 10000)
	for getter.HasNext() && len(keys) < 10000 {
		k, _ := getter.Next(nil)
		getter.Skip()
		keys = append(keys, k)
	}
	t.Logf("keys sampled: %d, total: %d", len(keys), bt.KeyCount())

	rng := rand.New(rand.NewSource(42))
	t.ResetTimer()
	t.ReportAllocs()
	for t.Loop() {
		k := keys[rng.Intn(len(keys))]
		_, v, _, ok, err := bt.Get(k, getter)
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			t.Fatalf("key not found: %x", k)
		}
		_ = v
	}
}

// BenchmarkBpsTree_bs benchmarks the in-memory binary search over cached pivot nodes.
func BenchmarkBpsTree_bs(b *testing.B) {
	const keySize = 20
	rng := rand.New(rand.NewSource(42))

	for _, cfg := range []struct {
		N int
		M int
	}{
		{1_000_000, 256},
		{1_000_000, 64},
		{10_000_000, 256},
	} {
		nodeCount := cfg.N / cfg.M
		name := fmt.Sprintf("N%d_M%d_nodes%d", cfg.N, cfg.M, nodeCount)

		b.Run(name, func(b *testing.B) {
			allKeys := make([][]byte, nodeCount)
			for i := range allKeys {
				k := make([]byte, keySize)
				rng.Read(k)
				binary.BigEndian.PutUint64(k[keySize-8:], uint64(i))
				allKeys[i] = k
			}
			slices.SortFunc(allKeys, bytes.Compare)

			nodes := make([]Node, nodeCount)
			for i, k := range allKeys {
				nodes[i] = Node{key: k, di: uint64(i) * uint64(cfg.M), off: uint64(i) * 100}
			}

			totalCount := uint64(cfg.N)
			ef := eliasfano32.NewEliasFano(totalCount, totalCount*100)
			for i := uint64(0); i < totalCount; i++ {
				ef.AddOffset(i * 100)
			}
			ef.Build()

			bt := &BpsTree{M: uint64(cfg.M), offt: ef, mx: nodes}

			lookupKeys := make([][]byte, 10000)
			for i := range lookupKeys {
				if i%2 == 0 {
					lookupKeys[i] = allKeys[rng.Intn(len(allKeys))]
				} else {
					k := make([]byte, keySize)
					rng.Read(k)
					lookupKeys[i] = k
				}
			}

			b.ResetTimer()
			b.ReportAllocs()
			for b.Loop() {
				p := b.N % len(lookupKeys)
				bt.bs(lookupKeys[p])
			}
		})
	}
}
