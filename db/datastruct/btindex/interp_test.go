package btindex

import (
	"bytes"
	"math/rand"
	"os"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/db/seg"
)

// Simulate in-window search on REAL keys: count comparisons (= cold probes) for
// binary vs guarded interpolation. Window size = M (the post-bs() window at that M).
//
// INTERP_KV=/path.kv INTERP_BT=/path.bt [INTERP_M=256] [INTERP_SAMPLES=5000]
// go test ./db/datastruct/btindex -run TestInterpVsBinary -v -timeout 30m
func TestInterpVsBinary(t *testing.T) {
	kvPath := os.Getenv("INTERP_KV")
	btPath := os.Getenv("INTERP_BT")
	if kvPath == "" || btPath == "" {
		t.Skip("set INTERP_KV and INTERP_BT")
	}
	M := uint64(256)
	if e := os.Getenv("INTERP_M"); e != "" {
		M, _ = strconv.ParseUint(e, 10, 64)
	}
	nS := 5000
	if e := os.Getenv("INTERP_SAMPLES"); e != "" {
		nS, _ = strconv.Atoi(e)
	}
	budget := 0 // 0 = pure/unbounded; >0 = interp for `budget` probes then binary
	if e := os.Getenv("INTERP_BUDGET"); e != "" {
		budget, _ = strconv.Atoi(e)
	}

	d, err := seg.NewDecompressor(kvPath)
	require.NoError(t, err)
	defer d.Close()
	cf := seg.DetectCompressType(d.MakeGetter())
	g := seg.NewReader(d.MakeGetter(), cf)
	bt, err := OpenBtreeIndexWithDecompressor(btPath, 256, g)
	require.NoError(t, err)
	defer bt.Close()
	offt := bt.bplus.Offsets()
	N := offt.Count()
	nWin := N / M

	rng := rand.New(rand.NewSource(7))
	var binTot, interpTot, interpMax, guardTrips, prefixSum int64
	var binHist, interpHist [40]int64

	win := make([][]byte, M)
	for s := 0; s < nS; s++ {
		w := uint64(rng.Int63n(int64(nWin))) * M
		for i := uint64(0); i < M; i++ {
			g.Reset(offt.Get(w + i))
			k, _ := g.Next(nil)
			win[i] = append(win[i][:0], k...)
		}
		target := win[rng.Intn(int(M))]
		bp := binaryProbes(win, target)
		ip, tripped, p := interpProbes(win, target, budget)
		binTot += int64(bp)
		interpTot += int64(ip)
		prefixSum += int64(p)
		if ip > int(interpMax) {
			interpMax = int64(ip)
		}
		if tripped {
			guardTrips++
		}
		binHist[min(bp, 39)]++
		interpHist[min(ip, 39)]++
	}

	t.Logf("file=%s M=%d windows=%d samples=%d", kvPath[len(kvPath)-28:], M, nWin, nS)
	t.Logf("  binary       avg=%.2f probes", float64(binTot)/float64(nS))
	t.Logf("  guardedInterp avg=%.2f probes  max=%d  guardTrips=%.1f%%  avgCommonPrefix=%.1fB",
		float64(interpTot)/float64(nS), interpMax,
		100*float64(guardTrips)/float64(nS), float64(prefixSum)/float64(nS))
	t.Logf("  interp probe histogram (probes:count): %v", histStr(interpHist[:]))
}

func binaryProbes(win [][]byte, target []byte) int {
	lo, hi, probes := 0, len(win)-1, 0
	for lo <= hi {
		mid := (lo + hi) / 2
		probes++
		c := bytes.Compare(target, win[mid])
		if c == 0 {
			return probes
		} else if c < 0 {
			hi = mid - 1
		} else {
			lo = mid + 1
		}
	}
	return probes
}

// pure interpolation (zero-span -> binary midpoint for correctness, clamps,
// forced progress). No early binary fallback — we want the true probe count.
func interpProbes(win [][]byte, target []byte, budget int) (probes int, tripped bool, prefix int) {
	lo, hi := 0, len(win)-1
	prefix = commonPrefixLen(win[lo], win[hi])
	for lo <= hi {
		if budget > 0 && probes >= budget { // GUARD: cap interp, finish with binary
			tripped = true
			bp := binaryProbesRange(win, target, lo, hi)
			return probes + bp, tripped, prefix
		}
		p := commonPrefixLen(win[lo], win[hi])
		a, b, x := u64At(win[lo], p), u64At(win[hi], p), u64At(target, p)
		var mid int
		if b == a {
			mid = (lo + hi) / 2
		} else {
			f := float64(x-a) / float64(b-a)
			if f < 0 {
				f = 0
			} else if f > 1 {
				f = 1
			}
			mid = lo + int(f*float64(hi-lo)+0.5)
			if mid < lo {
				mid = lo
			} else if mid > hi {
				mid = hi
			}
		}
		probes++
		c := bytes.Compare(target, win[mid])
		if c == 0 {
			return probes, tripped, prefix
		} else if c < 0 {
			hi = mid - 1
		} else {
			lo = mid + 1
		}
	}
	return probes, tripped, prefix
}

func binaryProbesRange(win [][]byte, target []byte, lo, hi int) int {
	probes := 0
	for lo <= hi {
		mid := (lo + hi) / 2
		probes++
		c := bytes.Compare(target, win[mid])
		if c == 0 {
			return probes
		} else if c < 0 {
			hi = mid - 1
		} else {
			lo = mid + 1
		}
	}
	return probes
}

func histStr(h []int64) string {
	s := ""
	for i, c := range h {
		if c > 0 {
			s += strconv.Itoa(i) + ":" + strconv.FormatInt(c, 10) + " "
		}
	}
	return s
}
