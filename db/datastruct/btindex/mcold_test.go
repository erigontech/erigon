package btindex

import (
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/db/seg"
)

// Cold-cache M comparison. Uses prebuilt .bt files in MREAD_TMP named storage_M<M>.bt.
// MREAD_KV=/path/file.kv MREAD_TMP=/dir [MREAD_MS=64,256,1024] [MREAD_COLDSAMPLES=5000]
// go test ./db/datastruct/btindex -run TestStorageGetCold -v
func TestStorageGetCold(t *testing.T) {
	kvPath := os.Getenv("MREAD_KV")
	tmp := os.Getenv("MREAD_TMP")
	if kvPath == "" || tmp == "" {
		t.Skip("set MREAD_KV and MREAD_TMP")
	}
	ms := []uint64{64, 256, 1024}
	if e := os.Getenv("MREAD_MS"); e != "" {
		ms = ms[:0]
		for _, p := range strings.Split(e, ",") {
			n, _ := strconv.ParseUint(p, 10, 64)
			ms = append(ms, n)
		}
	}
	nCold := 5000
	if e := os.Getenv("MREAD_COLDSAMPLES"); e != "" {
		nCold, _ = strconv.Atoi(e)
	}

	d, err := seg.NewDecompressor(kvPath)
	require.NoError(t, err)
	cf := seg.DetectCompressType(d.MakeGetter())
	// sample distinct random keys (warms file, fine — we evict before timing)
	probe := seg.NewReader(d.MakeGetter(), cf)
	probe.Reset(0)
	var all [][]byte
	for probe.HasNext() {
		k, _ := probe.Next(nil)
		probe.Skip()
		all = append(all, k)
	}
	rng := rand.New(rand.NewSource(7))
	keys := make([][]byte, nCold)
	for i := range keys {
		keys[i] = all[rng.Intn(len(all))]
	}
	d.Close()

	for _, M := range ms {
		btPath := filepath.Join(tmp, "storage_M"+strconv.FormatUint(M, 10)+".bt")
		if _, e := os.Stat(btPath); e != nil {
			t.Logf("M=%d: no prebuilt %s, skip", M, btPath)
			continue
		}
		// evict kv + bt from page cache
		_ = exec.Command("vmtouch", "-e", kvPath, btPath).Run()

		dd, err := seg.NewDecompressor(kvPath)
		require.NoError(t, err)
		g := seg.NewReader(dd.MakeGetter(), cf)
		bt, err := OpenBtreeIndexWithDecompressor(btPath, M, g)
		require.NoError(t, err)

		t0 := time.Now()
		for _, k := range keys {
			_, _, _, ok, err := bt.Get(k, g, nil)
			require.NoError(t, err)
			require.True(t, ok)
		}
		dur := time.Since(t0)
		t.Logf("M=%4d nodes=%7d cold: %d lookups in %s => %d us/op", M, bt.bplus.NodeCount(), nCold, dur, dur.Microseconds()/int64(nCold))
		bt.Close()
		dd.Close()
	}
}
