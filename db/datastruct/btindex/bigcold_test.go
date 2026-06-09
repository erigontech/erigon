package btindex

import (
	"math/rand"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/db/seg"
)

// Cold per-M probe for a large .kv shared by several prebuilt .bt files (built at
// different M over the SAME .kv). Samples random existing keys via the EF offsets
// (no full scan), evicts kv+bt from page cache before each M, then times cold Get.
//
// BIGKV=/path/storage.0-8192.kv \
// BIGBTS=/a/M256.bt,/b/M64.bt,/c/M16.bt BIGMS=256,64,16 [BIGSAMPLES=3000] \
// go test ./db/datastruct/btindex -run TestBigColdProbe -v -timeout 60m
func TestBigColdProbe(t *testing.T) {
	kvPath := os.Getenv("BIGKV")
	btsEnv := os.Getenv("BIGBTS")
	msEnv := os.Getenv("BIGMS")
	if kvPath == "" || btsEnv == "" || msEnv == "" {
		t.Skip("set BIGKV, BIGBTS, BIGMS")
	}
	bts := strings.Split(btsEnv, ",")
	msStr := strings.Split(msEnv, ",")
	require.Equal(t, len(bts), len(msStr), "BIGBTS and BIGMS length mismatch")
	ms := make([]uint64, len(msStr))
	for i, s := range msStr {
		ms[i], _ = strconv.ParseUint(s, 10, 64)
	}
	nCold := 3000
	if e := os.Getenv("BIGSAMPLES"); e != "" {
		nCold, _ = strconv.Atoi(e)
	}

	d, err := seg.NewDecompressor(kvPath)
	require.NoError(t, err)
	cf := seg.DetectCompressType(d.MakeGetter())

	// Sample random existing keys via the first .bt's offsets (same .kv for all).
	g0 := seg.NewReader(d.MakeGetter(), cf)
	bt0, err := OpenBtreeIndexWithDecompressor(bts[0], ms[0], g0)
	require.NoError(t, err)
	offt := bt0.bplus.Offsets()
	n := offt.Count()
	t.Logf("kv=%s keys=%d sampling=%d", kvPath, n, nCold)
	rng := rand.New(rand.NewSource(7))
	keys := make([][]byte, nCold)
	for i := range keys {
		ord := uint64(rng.Int63n(int64(n)))
		g0.Reset(offt.Get(ord))
		k, _ := g0.Next(nil)
		kc := make([]byte, len(k))
		copy(kc, k)
		keys[i] = kc
	}
	bt0.Close()
	d.Close()

	warm := os.Getenv("BIGWARM") != ""
	btwarm := os.Getenv("BIGBTWARM") != "" // .bt resident (madvise WILLNEED), .kv cold
	for i, btPath := range bts {
		M := ms[i]
		if btwarm {
			_ = exec.Command("vmtouch", "-e", kvPath).Run()
			_ = exec.Command("vmtouch", "-t", btPath).Run()
		} else if !warm {
			_ = exec.Command("vmtouch", "-e", kvPath, btPath).Run()
		}

		dd, err := seg.NewDecompressor(kvPath)
		require.NoError(t, err)
		g := seg.NewReader(dd.MakeGetter(), cf)
		bt, err := OpenBtreeIndexWithDecompressor(btPath, M, g)
		require.NoError(t, err)

		if warm { // untimed warmup pass so the timed pass hits page cache
			for _, k := range keys {
				_, _, _, _, err := bt.Get(k, g, nil)
				require.NoError(t, err)
			}
		}

		var pt PhaseTimings
		t0 := time.Now()
		miss := 0
		for _, k := range keys {
			_, _, _, ok, err := bt.Get(k, g, &pt)
			require.NoError(t, err)
			if !ok {
				miss++
			}
		}
		dur := time.Since(t0)
		t.Logf("M=%4d nodes=%9d cold: %d lookups in %s => %d us/op | btnav=%d us/op val=%d us/op miss=%d",
			M, bt.bplus.NodeCount(), nCold, dur, dur.Microseconds()/int64(nCold),
			pt.Nav.Microseconds()/int64(nCold), pt.Val.Microseconds()/int64(nCold), miss)
		bt.Close()
		dd.Close()
	}
}
