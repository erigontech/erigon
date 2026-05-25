package main

import (
	"fmt"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// renderMarkdown formats the benchmark results as a single markdown
// report: one table per file plus a roll-up across all files.
func renderMarkdown(results []fileResult, codecOrder []string) string {
	var b strings.Builder

	fmt.Fprintln(&b, "# seg-bench results")
	fmt.Fprintln(&b)
	fmt.Fprintf(&b, "Generated: %s\n\n", time.Now().UTC().Format(time.RFC3339))
	fmt.Fprintln(&b, "## Per-file results")
	fmt.Fprintln(&b)

	for _, fr := range results {
		writeFileTable(&b, fr)
		fmt.Fprintln(&b)
	}

	fmt.Fprintln(&b, "## Roll-up (mean ratios across all input files)")
	fmt.Fprintln(&b)
	writeRollup(&b, results, codecOrder)

	return b.String()
}

func writeFileTable(b *strings.Builder, fr fileResult) {
	fmt.Fprintf(b, "### `%s`\n\n", filepath.Base(fr.Path))
	fmt.Fprintf(b, "- records: %d  \n", fr.Records)
	fmt.Fprintf(b, "- raw (decompressed) size: %s  \n", humanBytes(fr.RawSize))
	fmt.Fprintf(b, "- `.seg` size on disk: %s  \n", humanBytes(fr.SegSize))
	fmt.Fprintln(b)
	fmt.Fprintln(b, "| Codec | Compressed | Ratio vs raw | Ratio vs `.seg` | Encode (ms/MB) | Decode (ms/MB) | Random-access (µs/rec, median) | Random-access (µs/rec, p99) |")
	fmt.Fprintln(b, "|---|---:|---:|---:|---:|---:|---:|---:|")

	// Baseline row: the current .seg/compress.
	fmt.Fprintf(b, "| `seg/compress` (current) | %s | %.2f× | 1.00× | n/a | n/a | n/a | n/a |\n",
		humanBytes(fr.SegSize),
		ratio(fr.SegSize, fr.RawSize))

	for _, cr := range fr.Results {
		if !cr.OK {
			fmt.Fprintf(b, "| `%s` | FAIL | — | — | — | — | — | — |\n", cr.Codec)
			fmt.Fprintf(b, "_(`%s` error: %s)_\n", cr.Codec, cr.Err)
			continue
		}
		encMsPerMB := msPerMB(cr.EncodeNs, fr.RawSize)
		decMsPerMB := msPerMB(cr.DecodeNs, fr.RawSize)
		medUs, p99Us := accessQuantiles(cr.AccessTimes)
		fmt.Fprintf(b, "| `%s` | %s | %.2f× | %.2f× | %.1f | %.1f | %.2f | %.2f |\n",
			cr.Codec,
			humanBytes(cr.CompressedSize),
			ratio(cr.CompressedSize, fr.RawSize),
			ratio(cr.CompressedSize, fr.SegSize),
			encMsPerMB,
			decMsPerMB,
			medUs,
			p99Us,
		)
	}
}

func writeRollup(b *strings.Builder, results []fileResult, codecOrder []string) {
	type acc struct {
		codec        string
		ratioVsSeg   []float64
		ratioVsRaw   []float64
		accessMedUs  []float64
		count, fails int
	}
	bag := map[string]*acc{}
	for _, name := range codecOrder {
		bag[name] = &acc{codec: name}
	}
	for _, fr := range results {
		for _, cr := range fr.Results {
			a := bag[cr.Codec]
			if a == nil {
				continue
			}
			a.count++
			if !cr.OK {
				a.fails++
				continue
			}
			a.ratioVsRaw = append(a.ratioVsRaw, ratio(cr.CompressedSize, fr.RawSize))
			a.ratioVsSeg = append(a.ratioVsSeg, ratio(cr.CompressedSize, fr.SegSize))
			if med, _ := accessQuantiles(cr.AccessTimes); med > 0 {
				a.accessMedUs = append(a.accessMedUs, med)
			}
		}
	}
	fmt.Fprintln(b, "| Codec | Mean ratio vs raw | Mean ratio vs `.seg` | Mean random-access µs/rec (median) | Files / failures |")
	fmt.Fprintln(b, "|---|---:|---:|---:|---|")
	for _, name := range codecOrder {
		a := bag[name]
		if a == nil {
			continue
		}
		fmt.Fprintf(b, "| `%s` | %.2f× | %.2f× | %.2f | %d / %d |\n",
			a.codec,
			mean(a.ratioVsRaw),
			mean(a.ratioVsSeg),
			mean(a.accessMedUs),
			a.count, a.fails,
		)
	}
}

func ratio(num, den int64) float64 {
	if den == 0 {
		return 0
	}
	return float64(num) / float64(den)
}

func mean(xs []float64) float64 {
	if len(xs) == 0 {
		return 0
	}
	var s float64
	for _, x := range xs {
		s += x
	}
	return s / float64(len(xs))
}

func msPerMB(ns, bytes int64) float64 {
	if bytes <= 0 {
		return 0
	}
	mb := float64(bytes) / (1024 * 1024)
	return (float64(ns) / 1e6) / mb
}

func accessQuantiles(times []time.Duration) (medianUs, p99Us float64) {
	if len(times) == 0 {
		return 0, 0
	}
	dur := make([]time.Duration, len(times))
	copy(dur, times)
	sort.Slice(dur, func(i, j int) bool { return dur[i] < dur[j] })
	medianUs = float64(dur[len(dur)/2].Nanoseconds()) / 1000
	p99Idx := (len(dur) * 99) / 100
	if p99Idx >= len(dur) {
		p99Idx = len(dur) - 1
	}
	p99Us = float64(dur[p99Idx].Nanoseconds()) / 1000
	return medianUs, p99Us
}

func humanBytes(n int64) string {
	const (
		kb = 1024
		mb = 1024 * kb
		gb = 1024 * mb
	)
	switch {
	case n >= gb:
		return fmt.Sprintf("%.2f GB", float64(n)/float64(gb))
	case n >= mb:
		return fmt.Sprintf("%.2f MB", float64(n)/float64(mb))
	case n >= kb:
		return fmt.Sprintf("%.2f KB", float64(n)/float64(kb))
	default:
		return fmt.Sprintf("%d B", n)
	}
}
