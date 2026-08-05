package btindex

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/db/seg"
)

// realDomainPair resolves a .kv/.bt pair from ERIGON_DATADIR (a snapshots/domain
// dir). ERIGON_BT_FILE picks one by substring; otherwise the largest pair under
// ERIGON_BT_MAX_GB (default 8) is used, since PrefixIndex build scans the whole .kv.
func realDomainPair(tb testing.TB) (kvPath, btPath string) {
	dataDir := os.Getenv("ERIGON_DATADIR")
	if dataDir == "" {
		tb.Skip("set ERIGON_DATADIR to a snapshots/domain dir with .kv and .bt files")
	}
	entries, err := os.ReadDir(dataDir)
	require.NoError(tb, err)

	kvFiles, btFiles := map[string]string{}, map[string]string{}
	for _, e := range entries {
		name := e.Name()
		stem := name[strings.Index(name, "-")+1:]
		ext := filepath.Ext(stem)
		key := strings.TrimSuffix(stem, ext)
		switch ext {
		case ".kv":
			kvFiles[key] = filepath.Join(dataDir, name)
		case ".bt":
			btFiles[key] = filepath.Join(dataDir, name)
		}
	}

	want := os.Getenv("ERIGON_BT_FILE")
	maxBytes := int64(dbg.EnvInt("BT_MAX_GB", 8)) << 30
	var bestSize int64
	for key, kv := range kvFiles {
		bt, ok := btFiles[key]
		if !ok {
			continue
		}
		fi, err := os.Stat(kv)
		if err != nil {
			continue
		}
		if want != "" {
			if strings.Contains(key, want) {
				return kv, bt
			}
			continue
		}
		if fi.Size() <= maxBytes && fi.Size() > bestSize {
			bestSize, kvPath, btPath = fi.Size(), kv, bt
		}
	}
	if kvPath == "" {
		tb.Skipf("no .kv+.bt pair found in %s (want=%q max=%dGB)", dataDir, want, maxBytes>>30)
	}
	return kvPath, btPath
}

// sampleKeysUniform draws n keys spread across the whole DI range. Sampling only
// the head of the file (as the older real-file bench does) concentrates every probe
// in a handful of prefix buckets and does not represent random domain access.
func sampleKeysUniform(tb testing.TB, bt *BtIndex, g *seg.Reader, n int) [][]byte {
	count := bt.Offsets().Count()
	if uint64(n) > count {
		n = int(count)
	}
	keys := make([][]byte, 0, n)
	for i := range n {
		di := uint64(i) * count / uint64(n)
		k, _, _, err := bt.dataLookup(di, g)
		require.NoError(tb, err)
		keys = append(keys, bytes.Clone(k))
	}
	return keys
}

// BenchmarkRealDomainComparison A/Bs BpsTree against PrefixIndex on a real mainnet
// domain file, with both engines built from the same open .bt.
func BenchmarkRealDomainComparison(t *testing.B) {
	kvPath, btPath := realDomainPair(t)

	d, err := seg.NewDecompressor(kvPath)
	require.NoError(t, err)
	defer d.Close()
	compressFlags := seg.DetectCompressType(d.MakeGetter())
	getter := seg.NewReader(d.MakeGetter(), compressFlags)

	saved := dbg.UsePrefixIndex
	dbg.UsePrefixIndex = false
	start := time.Now()
	btOnly, err := OpenBtreeIndexWithDecompressor(btPath, getter)
	require.NoError(t, err)
	bpsBuild := time.Since(start)
	btOnly.Close()

	dbg.UsePrefixIndex = true
	start = time.Now()
	bt, err := OpenBtreeIndexWithDecompressor(btPath, getter)
	require.NoError(t, err)
	bothBuild := time.Since(start)
	dbg.UsePrefixIndex = saved
	defer bt.Close()
	require.NotNil(t, bt.search)

	keys := sampleKeysUniform(t, bt, getter, 20_000)
	t.Logf("file=%s keys=%d sampled=%d compress=%d M=%d",
		filepath.Base(kvPath), bt.KeyCount(), len(keys), compressFlags, bt.M())
	t.Logf("open: bpsTree=%s bpsTree+prefixIndex=%s (prefixIndex build ≈ %s)",
		bpsBuild.Round(time.Millisecond), bothBuild.Round(time.Millisecond),
		(bothBuild - bpsBuild).Round(time.Millisecond))

	t.Run("Get/BpsTree", func(t *testing.B) {
		rnd := newRnd(42)
		t.ReportAllocs()
		for t.Loop() {
			v, ok, _, err := bt.bplus.Get(getter, keys[rnd.IntN(len(keys))])
			if err != nil || !ok || v == nil {
				t.Fatalf("get failed: ok=%v err=%v", ok, err)
			}
		}
	})
	t.Run("Get/PrefixIndex", func(t *testing.B) {
		rnd := newRnd(42)
		t.ReportAllocs()
		for t.Loop() {
			v, ok, _, err := bt.search.Get(getter, keys[rnd.IntN(len(keys))])
			if err != nil || !ok || v == nil {
				t.Fatalf("get failed: ok=%v err=%v", ok, err)
			}
		}
	})
	t.Run("Seek/BpsTree", func(t *testing.B) {
		rnd := newRnd(42)
		t.ReportAllocs()
		for t.Loop() {
			c, err := bt.bplus.Seek(getter, keys[rnd.IntN(len(keys))])
			if err != nil || c == nil {
				t.Fatalf("seek failed: err=%v", err)
			}
			c.Close()
		}
	})
	t.Run("Seek/PrefixIndex", func(t *testing.B) {
		rnd := newRnd(42)
		t.ReportAllocs()
		for t.Loop() {
			c, err := bt.search.Seek(getter, keys[rnd.IntN(len(keys))])
			if err != nil || c == nil {
				t.Fatalf("seek failed: err=%v", err)
			}
			c.Close()
		}
	})
}
