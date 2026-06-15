// Standalone prototype: learned (LeMonHash-style) MMPHF for .kvi.
//
// Idea: offsets in a .kv are already a sorted sequence (AddKey panics otherwise).
// recsplit scatters them into hash-slot order -> a random permutation that costs
// ~40 bits/key and can't be Elias-Fano compressed. Instead:
//   1. fit a bounded-error piecewise-linear model  key -> approx rank   (PGM)
//   2. recsplit stores only the small residual  local = rank - (predict-eps)
//      in ceil(log2(2*eps+1)) bits instead of the 40-bit offset
//   3. offsets stored ONCE as Elias-Fano, indexed by true rank
// Lookup: rank = predict(key) - eps + recsplit.Lookup(key); offset = EF.Get(rank).
package main

import (
	"context"
	"flag"
	"fmt"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"time"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/recsplit"
	"github.com/erigontech/erigon/db/recsplit/eliasfano32"
	"github.com/erigontech/erigon/db/seg"
)

const K = 8 // limbs => 64-byte key window

type u128 [K]uint64

const limbBase = 1.8446744073709552e19 // 2^64

func keyToX(k []byte) u128 {
	var x u128
	for i := 0; i < K*8; i++ {
		limb := i / 8
		x[limb] <<= 8
		if i < len(k) {
			x[limb] |= uint64(k[i])
		}
	}
	return x
}

func less(a, b u128) bool {
	for i := 0; i < K; i++ {
		if a[i] != b[i] {
			return a[i] < b[i]
		}
	}
	return false
}
func leq(a, b u128) bool { return !less(b, a) }

// fdelta = a - b as float64 (globally monotonic). High limbs cancel within a
// segment; deep-common-prefix groups still lose precision below ~2^-52 relative,
// which a recursive (prefix-stripping) model would recover.
func fdelta(a, b u128) float64 {
	var d float64
	for i := 0; i < K; i++ {
		d = d*limbBase + (float64(a[i]) - float64(b[i]))
	}
	return d
}

// segment: rank ~= y0 + slope*(x - x0), valid for x in [x0, next.x0)
type segment struct {
	x0     u128
	y0     float64
	slope  float64
	endIdx int // first rank NOT in this segment (for debugging)
}

func segIndex(segs []segment, x u128) int {
	lo, hi, idx := 0, len(segs)-1, 0
	for lo <= hi {
		mid := (lo + hi) / 2
		if leq(segs[mid].x0, x) {
			idx = mid
			lo = mid + 1
		} else {
			hi = mid - 1
		}
	}
	return idx
}

// GreedyPLR-style bounded-error segmentation, anchored at each segment's first point.
// Guarantees |true_rank - predict| <= eps for the points used to build it.
func buildSegments(xs []u128, eps float64) []segment {
	var segs []segment
	n := len(xs)
	i := 0
	for i < n {
		x0 := xs[i]
		y0 := float64(i)
		slopeLo := math.Inf(-1)
		slopeHi := math.Inf(1)
		j := i + 1
		for ; j < n; j++ {
			dx := fdelta(xs[j], x0)
			yj := float64(j)
			if dx == 0 {
				// tie group: must stay within eps of anchor
				if math.Abs(yj-y0) > eps {
					break
				}
				continue
			}
			lo := (yj - eps - y0) / dx
			hi := (yj + eps - y0) / dx
			if lo > slopeLo {
				slopeLo = lo
			}
			if hi < slopeHi {
				slopeHi = hi
			}
			if slopeLo > slopeHi {
				break
			}
		}
		slope := 0.0
		if !math.IsInf(slopeLo, 0) && !math.IsInf(slopeHi, 0) {
			slope = (slopeLo + slopeHi) / 2
		} else if !math.IsInf(slopeLo, 0) {
			slope = slopeLo
		} else if !math.IsInf(slopeHi, 0) {
			slope = slopeHi
		}
		segs = append(segs, segment{x0: x0, y0: y0, slope: slope, endIdx: j})
		i = j
	}
	return segs
}

// predict returns the model's estimate of rank for key-int x.
func predict(segs []segment, x u128, n int) int {
	// largest x0 <= x
	lo, hi := 0, len(segs)-1
	idx := 0
	for lo <= hi {
		mid := (lo + hi) / 2
		if leq(segs[mid].x0, x) {
			idx = mid
			lo = mid + 1
		} else {
			hi = mid - 1
		}
	}
	s := segs[idx]
	p := int(math.Round(s.y0 + s.slope*fdelta(x, s.x0)))
	if p < 0 {
		p = 0
	}
	if p >= n {
		p = n - 1
	}
	return p
}

func main() {
	kvPath := flag.String("kv", "", ".kv data file")
	eps := flag.Int("eps", 255, "model error bound (residual range = 2*eps+1)")
	limit := flag.Int("limit", 0, "max keys (0=all)")
	skipBuild := flag.Bool("skipbuild", false, "skip recsplit build + verify (fast model/EF sizing only)")
	flag.Parse()
	if *kvPath == "" {
		fmt.Println("need -kv")
		os.Exit(1)
	}
	logger := log.New()
	ctx := context.Background()

	fmt.Printf("=== %s  eps=%d ===\n", filepath.Base(*kvPath), *eps)

	d, err := seg.NewDecompressor(*kvPath)
	must(err)
	defer d.Close()
	g := d.MakeGetter()
	g.Reset(0)

	var keys [][]byte
	var offs []uint64
	var xs []u128
	buf := make([]byte, 0, 256)
	t0 := time.Now()
	for g.HasNext() {
		key, valPos := g.Next(buf[:0])
		k := make([]byte, len(key))
		copy(k, key)
		keys = append(keys, k)
		offs = append(offs, valPos)
		xs = append(xs, keyToX(k))
		if g.HasNext() {
			g.Skip() // skip value
		}
		if *limit > 0 && len(keys) >= *limit {
			break
		}
	}
	n := len(keys)
	maxOffset := offs[n-1]

	// tie diagnostics: longest run of equal x (keys indistinguishable by the prefix)
	maxRun, run, runStart, maxRunStart := 1, 1, 0, 0
	distinct := 1
	for i := 1; i < n; i++ {
		if xs[i] == xs[i-1] {
			run++
		} else {
			distinct++
			if run > maxRun {
				maxRun, maxRunStart = run, runStart
			}
			run = 1
			runStart = i
		}
	}
	if run > maxRun {
		maxRun, maxRunStart = run, runStart
	}
	fmt.Printf("tie-diag: distinct-x=%d (%.1f%%)  maxTieRun=%d at rank %d (keylen=%d %x)\n",
		distinct, 100*float64(distinct)/float64(n), maxRun, maxRunStart,
		len(keys[maxRunStart]), keys[maxRunStart][:min(len(keys[maxRunStart]), 16)])
	// divergence depth within the biggest tie group + key length stats
	{
		maxKeyLen := 0
		for i := 0; i < n; i++ {
			if len(keys[i]) > maxKeyLen {
				maxKeyLen = len(keys[i])
			}
		}
		a := keys[maxRunStart]
		divFirst, divLast := lcp(a, keys[maxRunStart+1]), lcp(a, keys[maxRunStart+maxRun-1])
		fmt.Printf("          maxKeyLen=%d  bigGroup: lcp(first,next)=%d lcp(first,last)=%d\n", maxKeyLen, divFirst, divLast)
		fmt.Printf("          k[%d]=%x\n          k[%d]=%x\n", maxRunStart, a[:min(len(a), 40)],
			maxRunStart+maxRun-1, keys[maxRunStart+maxRun-1][:min(len(keys[maxRunStart+maxRun-1]), 40)])
	}
	fmt.Printf("keys=%d  extract=%s  maxOffset=%d  .kv=%.2f GB\n",
		n, time.Since(t0).Round(time.Millisecond), maxOffset, float64(fileSize(*kvPath))/1e9)

	// --- 1. PGM model ---
	t0 = time.Now()
	segs := buildSegments(xs, float64(*eps))
	// model bytes: x0(8) + slope(8) per segment
	modelBytes := len(segs) * (K*8 + 8) // x0 anchor + slope (raw; anchors are prefix-compressible)
	fmt.Printf("model: segments=%d (%.3f bits/key)  build=%s\n",
		len(segs), float64(modelBytes*8)/float64(n), time.Since(t0).Round(time.Millisecond))

	// compute residuals as minimal-width  local = e - minE  (e = trueRank - predict)
	errs := make([]int, n)
	minE, maxE := math.MaxInt, math.MinInt
	for i := 0; i < n; i++ {
		e := i - predict(segs, xs[i], n)
		errs[i] = e
		if e < minE {
			minE = e
		}
		if e > maxE {
			maxE = e
		}
	}
	residuals := make([]uint64, n)
	worst := 0
	for i := 0; i < n; i++ {
		residuals[i] = uint64(errs[i] - minE)
		if abs(errs[i]) > abs(errs[worst]) {
			worst = i
		}
	}
	{
		i := worst
		p := predict(segs, xs[i], n)
		si := segIndex(segs, xs[i])
		s := segs[si]
		startIdx := 0
		if si > 0 {
			startIdx = segs[si-1].endIdx
		}
		fmt.Printf("worst-err: rank=%d predict=%d err=%d keylen=%d key=%x\n",
			i, p, errs[i], len(keys[i]), keys[i][:min(len(keys[i]), 40)])
		fmt.Printf("          seg#%d covers ranks [%d,%d) y0=%.0f slope=%g fdelta=%g  (own seg? %v)\n",
			si, startIdx, s.endIdx, s.y0, s.slope, fdelta(xs[i], s.x0), i >= startIdx && i < s.endIdx)
		// how many keys have |err|>1024 ?
		big := 0
		for j := 0; j < n; j++ {
			if abs(errs[j]) > 1024 {
				big++
			}
		}
		fmt.Printf("          keys with |err|>1024: %d (%.4f%%)\n", big, 100*float64(big)/float64(n))
	}
	residWidth := maxE - minE + 1
	residBits := int(math.Ceil(math.Log2(float64(residWidth))))
	if residWidth <= 1 {
		residBits = 0
	}
	fmt.Printf("model err range=[%d,%d] width=%d -> residual %d bits/key (byte-rounded by recsplit)\n",
		minE, maxE, residWidth, residBits)

	// --- 3. Elias-Fano of offsets ---
	t0 = time.Now()
	ef := eliasfano32.NewEliasFano(uint64(n), maxOffset)
	for i := 0; i < n; i++ {
		ef.AddOffset(offs[i])
	}
	ef.Build()
	efBytes := len(ef.AppendBytes(nil))
	fmt.Printf("EF(offsets): %.2f MB (%.2f bits/key)  build=%s\n",
		float64(efBytes)/1e6, float64(efBytes*8)/float64(n), time.Since(t0).Round(time.Millisecond))

	// --- 2. recsplit storing residual (not offset) ---
	var residIdxBytes int64
	mphfOverheadBits := 1.81 // measured recsplit structural overhead (golomb-rice + double-EF)
	if !*skipBuild {
		tmp, _ := os.MkdirTemp("", "mmphf")
		defer os.RemoveAll(tmp)
		idxPath := filepath.Join(tmp, "resid.kvi")
		var salt uint32 = 0
		rs, err := recsplit.NewRecSplit(recsplit.RecSplitArgs{
			KeyCount:           n,
			Enums:              false,
			LessFalsePositives: false,
			BucketSize:         recsplit.DefaultBucketSize,
			LeafSize:           recsplit.DefaultLeafSize,
			TmpDir:             tmp,
			IndexFile:          idxPath,
			Salt:               &salt,
			NoFsync:            true,
		}, logger)
		must(err)
		t0 = time.Now()
		for i := 0; i < n; i++ {
			must(rs.AddKey(keys[i], residuals[i]))
		}
		must(rs.Build(ctx))
		rs.Close()
		residIdxBytes = fileSize(idxPath)
		fmt.Printf("recsplit(residual): %.2f MB (%.2f bits/key)  build=%s\n",
			float64(residIdxBytes)/1e6, float64(residIdxBytes*8)/float64(n), time.Since(t0).Round(time.Millisecond))

		idx, err := recsplit.OpenIndex(idxPath)
		must(err)
		defer idx.Close()
		reader := recsplit.NewIndexReader(idx)
		bad := 0
		t0 = time.Now()
		for i := 0; i < n; i++ {
			local, ok := reader.Lookup(keys[i])
			if !ok {
				bad++
				continue
			}
			p := predict(segs, xs[i], n)
			rank := p + minE + int(local)
			if rank < 0 || rank >= n {
				bad++
				continue
			}
			if ef.Get(uint64(rank)) != offs[i] {
				bad++
			}
		}
		fmt.Printf("verify: %d/%d correct  (%d bad)  verify=%s\n", n-bad, n, bad, time.Since(t0).Round(time.Millisecond))

		const samples = 2_000_000
		perm := make([]int, samples)
		rng := rand.New(rand.NewSource(1))
		for i := range perm {
			perm[i] = rng.Intn(n)
		}
		var sink uint64
		t0 = time.Now()
		for _, i := range perm {
			local, _ := reader.Lookup(keys[i])
			p := predict(segs, xs[i], n)
			rank := p + minE + int(local)
			sink += ef.Get(uint64(rank))
		}
		nsOp := float64(time.Since(t0).Nanoseconds()) / float64(samples)
		fmt.Printf("lookup(warm): %.1f ns/op  (sink=%d)\n", nsOp, sink)
		mphfOverheadBits = float64(residIdxBytes*8)/float64(n) - 8*math.Ceil(float64(maxE-minE+1)/256)
		if mphfOverheadBits < 0 {
			mphfOverheadBits = 0
		}
	}

	// --- totals ---
	newTotal := residIdxBytes + int64(efBytes) + int64(modelBytes)
	efBitsPK := float64(efBytes*8) / float64(n)
	modelBitsPK := float64(modelBytes*8) / float64(n)
	idealBitsPK := efBitsPK + modelBitsPK + mphfOverheadBits + float64(residBits)
	baseKvi := *kvPath
	baseKviPath := baseKvi[:len(baseKvi)-len(".kv")] + ".kvi"
	baseSize := fileSize(baseKviPath)
	fmt.Printf("\n--- TOTALS (n=%d) ---\n", n)
	if baseSize > 0 {
		fmt.Printf("baseline .kvi:        %.2f MB  (%.2f bits/key)\n", float64(baseSize)/1e6, float64(baseSize*8)/float64(n))
	}
	fmt.Printf("new (recsplit-bound): %.2f MB  (%.2f bits/key)\n", float64(newTotal)/1e6, float64(newTotal*8)/float64(n))
	fmt.Printf("new (bit-packed ideal): %.2f bits/key  [EF=%.2f + model=%.3f + mphf=%.2f + resid=%d]\n",
		idealBitsPK, efBitsPK, modelBitsPK, mphfOverheadBits, residBits)
	if baseSize > 0 {
		fmt.Printf("reduction: recsplit-bound %.2fx | bit-packed-ideal %.2fx\n",
			float64(baseSize)/float64(newTotal), float64(baseSize*8)/float64(n)/idealBitsPK)
	}
}

func lcp(a, b []byte) int {
	n := len(a)
	if len(b) < n {
		n = len(b)
	}
	for i := 0; i < n; i++ {
		if a[i] != b[i] {
			return i
		}
	}
	return n
}
func abs(x int) int {
	if x < 0 {
		return -x
	}
	return x
}
func fileSize(p string) int64 {
	fi, err := os.Stat(p)
	if err != nil {
		return 0
	}
	return fi.Size()
}
func must(err error) {
	if err != nil {
		panic(err)
	}
}
