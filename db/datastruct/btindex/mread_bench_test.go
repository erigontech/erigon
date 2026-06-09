package btindex

import (
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/background"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/seg"
	"github.com/erigontech/erigon/db/state/statecfg"
)

// Build a .bt at the M given by BT_M env, then benchmark random-existing-key Get.
// MREAD_KV=/path/file.kv [BT_M=256] [MREAD_SAMPLES=200000]
// go test ./db/datastruct/btindex -run '^$' -bench BenchmarkStorageGetRealM -benchtime=3s
func BenchmarkStorageGetRealM(b *testing.B) {
	kvPath := os.Getenv("MREAD_KV")
	if kvPath == "" {
		b.Skip("set MREAD_KV")
	}
	tmp := os.Getenv("MREAD_TMP")
	if tmp == "" {
		tmp = b.TempDir()
	}
	nSamples := 200000
	if e := os.Getenv("MREAD_SAMPLES"); e != "" {
		n, err := strconv.Atoi(e)
		require.NoError(b, err)
		nSamples = n
	}

	d, err := seg.NewDecompressor(kvPath)
	require.NoError(b, err)
	defer d.Close()
	cf := seg.DetectCompressType(d.MakeGetter())

	btPath := filepath.Join(tmp, "storage_M"+strconv.FormatUint(DefaultBtreeM, 10)+".bt")
	if _, statErr := os.Stat(btPath); statErr != nil {
		gb := seg.NewReader(d.MakeGetter(), cf)
		t0 := time.Now()
		require.NoError(b, BuildBtreeIndexWithDecompressor(btPath, gb, background.NewProgressSet(), tmp, 0, log.New(), true, statecfg.AccessorBTree))
		b.Logf("built M=%d in %s", DefaultBtreeM, time.Since(t0))
	}

	getter := seg.NewReader(d.MakeGetter(), cf)
	bt, err := OpenBtreeIndexWithDecompressor(btPath, DefaultBtreeM, getter)
	require.NoError(b, err)
	defer bt.Close()

	// sample random existing keys
	probe := seg.NewReader(d.MakeGetter(), cf)
	probe.Reset(0)
	var all [][]byte
	for probe.HasNext() {
		k, _ := probe.Next(nil)
		probe.Skip()
		all = append(all, k)
	}
	rng := rand.New(rand.NewSource(42))
	keys := make([][]byte, nSamples)
	for i := range keys {
		keys[i] = all[rng.Intn(len(all))]
	}
	b.Logf("kv=%s N=%d compress=%d M=%d nodes=%d", filepath.Base(kvPath), len(all), cf, DefaultBtreeM, bt.bplus.NodeCount())

	b.ResetTimer()
	b.ReportAllocs()
	i := 0
	for b.Loop() {
		_, _, _, ok, err := bt.Get(keys[i%len(keys)], getter, nil)
		if err != nil || !ok {
			b.Fatalf("miss k=%x ok=%v err=%v", keys[i%len(keys)], ok, err)
		}
		i++
	}
}
