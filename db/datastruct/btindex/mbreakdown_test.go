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

// Cold-cache phase breakdown of a storage Get, per M. Uses prebuilt storage_M<M>.bt in MREAD_TMP.
// Splits: t_bs (in-RAM pivot search) | t_btnav (in-file key compares = "bt access") | t_val (value fetch = "kv access")
// MREAD_KV=... MREAD_TMP=... [MREAD_MS=64,256,1024] [MREAD_COLDSAMPLES=5000]
// go test ./db/datastruct/btindex -run TestStorageGetBreakdown -v
func TestStorageGetBreakdown(t *testing.T) {
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
			continue
		}
		_ = exec.Command("vmtouch", "-e", kvPath, btPath).Run()

		dd, err := seg.NewDecompressor(kvPath)
		require.NoError(t, err)
		g := seg.NewReader(dd.MakeGetter(), cf)
		bt, err := OpenBtreeIndexWithDecompressor(btPath, M, g)
		require.NoError(t, err)
		bp := bt.bplus
		offt := bp.Offsets()

		var dBs, dNav, dVal time.Duration
		var probes int64
		for _, key := range keys {
			t0 := time.Now()
			_, l, r := bp.bs(key)
			dBs += time.Since(t0)

			// in-file binary search (bt navigation): each compareKey touches the .kv at a pivot offset
			tnav := time.Now()
			var found uint64
			var ok bool
			for l < r {
				m := (l + r) >> 1
				probes++
				cmp := bp.compareKey(g, key, m)
				if cmp == 0 {
					found, ok = m, true
					break
				} else if cmp < 0 {
					r = m
				} else {
					l = m + 1
				}
			}
			dNav += time.Since(tnav)

			// value fetch at the found offset (kv access)
			if ok {
				tval := time.Now()
				g.Reset(offt.Get(found))
				g.Skip()      // skip key
				g.Next(nil)   // read value
				dVal += time.Since(tval)
			}
		}
		n := int64(nCold)
		t.Logf("M=%4d nodes=%7d probes/op=%.1f | bs=%3dus btnav=%3dus val=%3dus total=%3dus",
			M, bp.NodeCount(), float64(probes)/float64(n),
			dBs.Microseconds()/n, dNav.Microseconds()/n, dVal.Microseconds()/n,
			(dBs+dNav+dVal).Microseconds()/n)
		bt.Close()
		dd.Close()
	}
}
