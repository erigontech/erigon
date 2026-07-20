package main

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"

	"github.com/erigontech/erigon/db/seg"
)

// valdist: bounded-memory histogram of commitment domain value sizes across .kv
// files. Sizes are counted in a fixed array (exact to 1 byte up to sizeCap); true
// max/mean/count are exact. usage: valdist <glob-or-file> [...]
const sizeCap = 1 << 20 // 1 MiB size ceiling for the histogram array (bytes beyond go to overflow, still counted in max)

func main() {
	var files []string
	for _, a := range os.Args[1:] {
		m, _ := filepath.Glob(a)
		if len(m) == 0 {
			files = append(files, a)
		}
		files = append(files, m...)
	}
	if len(files) == 0 {
		fmt.Fprintln(os.Stderr, "no files")
		os.Exit(1)
	}

	hist := make([]int64, sizeCap+1) // hist[s] = count of values with size s (s>sizeCap -> hist[sizeCap])
	var n, total, maxv int64
	stateKey := []byte("state")
	for _, f := range files {
		dec, err := seg.NewDecompressor(f)
		if err != nil {
			fmt.Fprintf(os.Stderr, "open %s: %v\n", f, err)
			continue
		}
		r := seg.NewReader(dec.MakeGetter(), seg.CompressKeys)
		var key, val []byte
		for r.HasNext() {
			key, _ = r.Next(key[:0])
			if !r.HasNext() {
				break
			}
			val, _ = r.Next(val[:0])
			if bytes.Equal(key, stateKey) {
				continue
			}
			s := len(val)
			n++
			total += int64(s)
			if int64(s) > maxv {
				maxv = int64(s)
			}
			if s > sizeCap {
				s = sizeCap
			}
			hist[s]++
		}
		dec.Close()
		fmt.Fprintf(os.Stderr, "  read %s (values so far=%d)\n", filepath.Base(f), n)
	}
	if n == 0 {
		fmt.Println("no values")
		return
	}
	// percentile from cumulative histogram
	pct := func(p float64) int {
		target := int64(float64(n) * p)
		var c int64
		for s := 0; s <= sizeCap; s++ {
			c += hist[s]
			if c >= target {
				return s
			}
		}
		return sizeCap
	}
	over := func(t int) int64 {
		var c int64
		for s := t + 1; s <= sizeCap; s++ {
			c += hist[s]
		}
		return c
	}
	fmt.Printf("files=%d values=%d totalBytes=%d mean=%.1f\n", len(files), n, total, float64(total)/float64(n))
	fmt.Printf("p50=%d p90=%d p99=%d p999=%d p9999=%d p99999=%d max=%d\n",
		pct(.5), pct(.9), pct(.99), pct(.999), pct(.9999), pct(.99999), maxv)

	buckets := []int{32, 64, 128, 256, 512, 1024, 2048, 4096, 8192, 16384, 65536, sizeCap}
	labels := []string{"<=32", "33-64", "65-128", "129-256", "257-512", "513-1K", "1K-2K", "2K-4K", "4K-8K", "8K-16K", "16K-64K", ">64K"}
	prev := -1
	fmt.Println("size bucket : count (pct)")
	for i, b := range buckets {
		var c int64
		for s := prev + 1; s <= b && s <= sizeCap; s++ {
			c += hist[s]
		}
		prev = b
		fmt.Printf("  %-8s: %12d (%6.3f%%)\n", labels[i], c, 100*float64(c)/float64(n))
	}
	fmt.Printf("values >2048B: %d (%.4f%%)   >4096B: %d (%.5f%%)   >8192B: %d   >65536B: %d\n",
		over(2048), 100*float64(over(2048))/float64(n),
		over(4096), 100*float64(over(4096))/float64(n), over(8192), over(65536))
}
