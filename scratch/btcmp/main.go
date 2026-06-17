// Bench .bt navigation methods on the #21778 base: binary / btinterp / PrefixIndex
// (selected via BT_INTERP / ERIGON_USE_PREFIX_INDEX env). One process = one method.
// Reports: index size (.bt, same for all), heap of the open index structure,
// cold/warm full-Get latency + major-faults/op. Samples keys (no full arena).
package main

import (
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"time"

	"github.com/erigontech/erigon/common/background"
	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datastruct/btindex"
	"github.com/erigontech/erigon/db/seg"
	"github.com/erigontech/erigon/db/state/statecfg"
)

func majflt() int64 {
	var ru syscall.Rusage
	_ = syscall.Getrusage(syscall.RUSAGE_SELF, &ru)
	return ru.Majflt
}
func fsize(p string) int64 { fi, e := os.Stat(p); if e != nil { return 0 }; return fi.Size() }
func evict(f ...string)    { _ = exec.Command("vmtouch", append([]string{"-e"}, f...)...).Run() }
func load(f ...string)     { _ = exec.Command("vmtouch", append([]string{"-t"}, f...)...).Run() }
func must(e error)         { if e != nil { panic(e) } }

func method() string {
	if dbg.UsePrefixIndex {
		return "prefix"
	}
	if btindex.BtInterp {
		return "interp"
	}
	return "binary"
}

func main() {
	kvPath := flag.String("kv", "", ".kv")
	out := flag.String("out", os.TempDir(), "out dir")
	nkeys := flag.Int("nkeys", 15000, "sample keys")
	flag.Parse()
	logger := log.New()
	M := btindex.DefaultBtreeM
	base := filepath.Base(*kvPath)
	btPath := filepath.Join(*out, fmt.Sprintf("%s.M%d.bt", base, M))
	kveiPath := strings.TrimSuffix(btPath, ".bt") + ".kvei"

	// sample keys (stride; no full arena)
	d, err := seg.NewDecompressor(*kvPath)
	must(err)
	total := d.Count() / 2
	stride := total / *nkeys
	if stride < 1 {
		stride = 1
	}
	g := seg.NewReader(d.MakeGetter(), seg.CompressKeys)
	g.Reset(0)
	var keys [][]byte
	i := 0
	for g.HasNext() {
		k, _ := g.Next(nil)
		if i%stride == 0 {
			keys = append(keys, append([]byte(nil), k...))
		}
		if g.HasNext() {
			g.Skip()
		}
		i++
	}
	d.Close()

	// build .bt once (reuse if present)
	if fsize(btPath) == 0 {
		bd, err := seg.NewDecompressor(*kvPath)
		must(err)
		r := seg.NewReader(bd.MakeGetter(), seg.CompressKeys)
		must(btindex.BuildBtreeIndexWithDecompressor(btPath, kveiPath, r, background.NewProgressSet(), *out, 1, logger, true, statecfg.AccessorBTree))
		bd.Close()
	}

	fmt.Printf("=== %s  M=%d  method=%s  keys=%d sampled=%d  .kv=%.1fGB  .bt=%.0fMB ===\n",
		base, M, method(), total, len(keys), float64(fsize(*kvPath))/1e9, float64(fsize(btPath))/1e6)

	run := func() (float64, float64, int64, int64) {
		bd, err := seg.NewDecompressor(*kvPath)
		must(err)
		bg := seg.NewReader(bd.MakeGetter(), seg.CompressKeys)
		runtime.GC()
		var m0 runtime.MemStats
		runtime.ReadMemStats(&m0)
		ot := time.Now()
		bt, err := btindex.OpenBtreeIndexWithDecompressor(btPath, M, bg)
		must(err)
		openMs := time.Since(ot).Milliseconds()
		runtime.GC()
		var m1 runtime.MemStats
		runtime.ReadMemStats(&m1)
		heap := int64(m1.HeapAlloc) - int64(m0.HeapAlloc)
		bd.Close()     // release .kv mmap so vmtouch can evict (prefix Open warmed it)
		evict(*kvPath) // timed loop must hit a cold .kv
		bd2, err := seg.NewDecompressor(*kvPath)
		must(err)
		bg2 := seg.NewReader(bd2.MakeGetter(), seg.CompressKeys)
		var sink int
		mf0 := majflt()
		t0 := time.Now()
		for _, k := range keys {
			_, v, _, _, err := bt.Get(k, bg2)
			must(err)
			sink += len(v)
		}
		us := float64(time.Since(t0).Microseconds()) / float64(len(keys))
		f := float64(majflt()-mf0) / float64(len(keys))
		bt.Close()
		bd2.Close()
		_ = sink
		return us, f, heap, openMs
	}

	evict(btPath, kveiPath, *kvPath)
	cu, cf, heap, oms := run()
	fmt.Printf("COLD  %-7s %6.0f µs/op  %.2f maj-faults/op   heap=%.0f MB  open=%d ms\n", method(), cu, cf, float64(heap)/1e6, oms)
	load(btPath, kveiPath)
	wu, wf, _, _ := run()
	fmt.Printf("WARM  %-7s %6.0f µs/op  %.2f maj-faults/op\n", method(), wu, wf)
}
