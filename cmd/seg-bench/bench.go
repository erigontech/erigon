package main

import (
	"bytes"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/erigontech/erigon/db/seg"
)

// codecResult is the per-codec measurement for one input file.
type codecResult struct {
	Codec          string
	OK             bool
	Err            string
	CompressedSize int64
	EncodeNs       int64         // best of repeats
	DecodeNs       int64         // best of repeats (sequential)
	AccessTimes    []time.Duration
}

// fileResult is the per-file roll-up across codecs.
type fileResult struct {
	Path    string
	Records int
	RawSize int64 // sum of decompressed record bytes
	SegSize int64 // current .seg file size on disk
	Results []codecResult
}

func runBench(args []string) {
	fs := flag.NewFlagSet("run", flag.ExitOnError)
	inputDir := fs.String("input", "", "directory containing .seg files (required)")
	outFile := fs.String("out", "report.md", "output markdown report path")
	codecList := fs.String("codecs", "snappy,zstd-3,zstd-9,zstd-19", "comma-separated codec list")
	repeats := fs.Int("repeats", 5, "encode/decode repeats per (file, codec); best time reported")
	raSamples := fs.Int("random-access-samples", 2000, "random-access decode samples per (file, codec)")
	seed := fs.Int64("seed", 1, "RNG seed for random-access index selection")
	if err := fs.Parse(args); err != nil {
		os.Exit(2)
	}
	if *inputDir == "" {
		fmt.Fprintln(os.Stderr, "run: --input is required")
		os.Exit(2)
	}

	files, err := listSegFiles(*inputDir)
	if err != nil {
		die("list seg files: %v", err)
	}
	if len(files) == 0 {
		die("no .seg files found under %s", *inputDir)
	}

	codecNames := strings.Split(*codecList, ",")
	for i, n := range codecNames {
		codecNames[i] = strings.TrimSpace(n)
	}

	rng := rand.New(rand.NewSource(*seed))

	var allResults []fileResult
	for _, f := range files {
		fmt.Printf("=== %s ===\n", filepath.Base(f))
		res, err := benchmarkFile(f, codecNames, *raSamples, *repeats, rng)
		if err != nil {
			fmt.Printf("  ERROR: %v\n", err)
			continue
		}
		for _, cr := range res.Results {
			summary := "FAIL: " + cr.Err
			if cr.OK {
				summary = fmt.Sprintf("size=%dB enc=%dms dec=%dms", cr.CompressedSize,
					cr.EncodeNs/1_000_000, cr.DecodeNs/1_000_000)
			}
			fmt.Printf("  %-10s %s\n", cr.Codec, summary)
		}
		allResults = append(allResults, res)
	}

	report := renderMarkdown(allResults, codecNames)
	if err := os.WriteFile(*outFile, []byte(report), 0644); err != nil {
		die("write report: %v", err)
	}
	fmt.Printf("\nReport written to %s\n", *outFile)
}

func listSegFiles(dir string) ([]string, error) {
	var out []string
	err := filepath.WalkDir(dir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() && strings.HasSuffix(path, ".seg") {
			out = append(out, path)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Strings(out)
	return out, nil
}

func benchmarkFile(path string, codecNames []string, raSamples, repeats int, rng *rand.Rand) (fileResult, error) {
	fr := fileResult{Path: path}

	if st, err := os.Stat(path); err == nil {
		fr.SegSize = st.Size()
	}

	// Decompress the .seg file fully into memory. For multi-GB files this
	// could be heavy; the prototype assumes the sample size is bounded
	// to fit comfortably (a single representative .seg is hundreds of MB).
	d, err := seg.NewDecompressor(path)
	if err != nil {
		return fr, fmt.Errorf("open seg: %w", err)
	}
	defer d.Close()
	fr.Records = d.Count()

	records := make([][]byte, 0, fr.Records)
	g := d.MakeGetter()
	for g.HasNext() {
		rec, _ := g.Next(nil)
		// Copy: Getter buffers may be reused.
		cp := make([]byte, len(rec))
		copy(cp, rec)
		records = append(records, cp)
		fr.RawSize += int64(len(cp))
	}

	// Random-access indices, fixed across codecs for fair comparison.
	var raIdx []int
	if raSamples > 0 && fr.Records > 0 {
		raIdx = make([]int, raSamples)
		for i := range raIdx {
			raIdx[i] = rng.Intn(fr.Records)
		}
	}

	for _, name := range codecNames {
		codec, err := makeCodec(name)
		if err != nil {
			fr.Results = append(fr.Results, codecResult{Codec: name, Err: err.Error()})
			continue
		}
		cr := runCodec(codec, records, raIdx, repeats)
		codec.Close()
		fr.Results = append(fr.Results, cr)
	}
	return fr, nil
}

func runCodec(codec Codec, records [][]byte, raIdx []int, repeats int) codecResult {
	res := codecResult{Codec: codec.Name()}

	// Single canonical encode used both for size and for round-trip
	// verify. Subsequent repeats only time encode/decode loops.
	fw := newFramedWriter()
	var encBuf []byte
	for _, rec := range records {
		fb, err := codec.EncodeOne(encBuf[:0], rec)
		if err != nil {
			res.Err = fmt.Sprintf("encode err: %v", err)
			return res
		}
		// Copy: codec may reuse encBuf.
		fc := make([]byte, len(fb))
		copy(fc, fb)
		fw.AddFrame(fc)
		encBuf = fb
	}
	framed := fw.Bytes()
	res.CompressedSize = int64(len(framed))

	reader, err := newFramedReader(framed)
	if err != nil {
		res.Err = fmt.Sprintf("framed reader: %v", err)
		return res
	}

	// Round-trip verify on the canonical encoding. Decode every record
	// and assert byte-for-byte equality against the original.
	for i, rec := range records {
		frame, err := reader.Frame(i)
		if err != nil {
			res.Err = fmt.Sprintf("verify frame %d: %v", i, err)
			return res
		}
		got, err := codec.DecodeOne(nil, frame)
		if err != nil {
			res.Err = fmt.Sprintf("verify decode %d: %v", i, err)
			return res
		}
		if !bytes.Equal(got, rec) {
			res.Err = fmt.Sprintf("verify round-trip mismatch at record %d (got %d bytes, want %d)",
				i, len(got), len(rec))
			return res
		}
	}

	// Encode-time repeats: best of N runs.
	bestEncode := time.Duration(1<<62 - 1)
	for rep := 0; rep < repeats; rep++ {
		tw := newFramedWriter()
		var buf []byte
		start := time.Now()
		for _, rec := range records {
			fb, err := codec.EncodeOne(buf[:0], rec)
			if err != nil {
				res.Err = fmt.Sprintf("encode-rep err: %v", err)
				return res
			}
			fc := make([]byte, len(fb))
			copy(fc, fb)
			tw.AddFrame(fc)
			buf = fb
		}
		_ = tw.Bytes()
		dur := time.Since(start)
		if dur < bestEncode {
			bestEncode = dur
		}
	}
	res.EncodeNs = bestEncode.Nanoseconds()

	// Sequential decode repeats: best of N runs.
	bestDecode := time.Duration(1<<62 - 1)
	for rep := 0; rep < repeats; rep++ {
		var buf []byte
		start := time.Now()
		for i := 0; i < reader.Count(); i++ {
			frame, _ := reader.Frame(i)
			b, err := codec.DecodeOne(buf[:0], frame)
			if err != nil {
				res.Err = fmt.Sprintf("decode-rep err at %d: %v", i, err)
				return res
			}
			buf = b
		}
		dur := time.Since(start)
		if dur < bestDecode {
			bestDecode = dur
		}
	}
	res.DecodeNs = bestDecode.Nanoseconds()

	// Random-access timings.
	if len(raIdx) > 0 {
		res.AccessTimes = make([]time.Duration, 0, len(raIdx))
		var buf []byte
		for _, idx := range raIdx {
			start := time.Now()
			frame, err := reader.Frame(idx)
			if err == nil {
				buf, err = codec.DecodeOne(buf[:0], frame)
			}
			res.AccessTimes = append(res.AccessTimes, time.Since(start))
			if err != nil {
				res.Err = fmt.Sprintf("random-access decode err at idx %d: %v", idx, err)
				return res
			}
		}
	}

	res.OK = true
	return res
}

func die(format string, a ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", a...)
	os.Exit(1)
}
