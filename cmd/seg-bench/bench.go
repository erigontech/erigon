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

// FramingMode selects per-record (random-access-friendly) or
// whole-stream (bulk-sync-optimal) codec framing.
type FramingMode int

const (
	FramingPerRecord FramingMode = iota
	FramingWholeStream
)

func (f FramingMode) String() string {
	switch f {
	case FramingPerRecord:
		return "per-record"
	case FramingWholeStream:
		return "whole-stream"
	default:
		return "unknown"
	}
}

func parseFramingList(s string) ([]FramingMode, error) {
	var out []FramingMode
	for _, name := range strings.Split(s, ",") {
		name = strings.TrimSpace(name)
		switch name {
		case "per-record":
			out = append(out, FramingPerRecord)
		case "whole-stream":
			out = append(out, FramingWholeStream)
		default:
			return nil, fmt.Errorf("unknown framing %q (known: per-record, whole-stream)", name)
		}
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("at least one framing mode required")
	}
	return out, nil
}

// codecResult is the per-(codec, framing) measurement for one input file.
type codecResult struct {
	Codec          string
	Framing        FramingMode
	OK             bool
	Err            string
	CompressedSize int64
	EncodeNs       int64 // best of repeats
	DecodeNs       int64 // best of repeats (sequential / full-stream)
	AccessTimes    []time.Duration
}

// fileResult is the per-file roll-up across (codec, framing) combinations.
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
	codecList := fs.String("codecs", "snappy,zstd-3,zstd-9,zstd-19", "comma-separated codec list (e.g. \"snappy,zstd-3,zstd-3+dict\")")
	framingList := fs.String("framing", "per-record", "comma-separated framing modes: per-record, whole-stream, or both")
	dictPath := fs.String("dict", "", "path to a zstd-trained dictionary file (required by any zstd-*+dict codec)")
	repeats := fs.Int("repeats", 5, "encode/decode repeats per (file, codec, framing); best time reported")
	raSamples := fs.Int("random-access-samples", 2000, "random-access decode samples per (file, codec, framing)")
	seed := fs.Int64("seed", 1, "RNG seed for random-access index selection")
	if err := fs.Parse(args); err != nil {
		os.Exit(2)
	}
	if *inputDir == "" {
		fmt.Fprintln(os.Stderr, "run: --input is required")
		os.Exit(2)
	}

	framings, err := parseFramingList(*framingList)
	if err != nil {
		die("--framing: %v", err)
	}

	var dict []byte
	if *dictPath != "" {
		b, err := os.ReadFile(*dictPath)
		if err != nil {
			die("--dict: %v", err)
		}
		dict = b
		fmt.Printf("Loaded zstd dictionary: %s (%d bytes)\n", *dictPath, len(dict))
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
		res, err := benchmarkFile(f, codecNames, framings, dict, *raSamples, *repeats, rng)
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
			fmt.Printf("  %-15s %-12s %s\n", cr.Codec, cr.Framing, summary)
		}
		allResults = append(allResults, res)
	}

	report := renderMarkdown(allResults, codecNames, framings)
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

func benchmarkFile(path string, codecNames []string, framings []FramingMode, dict []byte, raSamples, repeats int, rng *rand.Rand) (fileResult, error) {
	fr := fileResult{Path: path}

	if st, err := os.Stat(path); err == nil {
		fr.SegSize = st.Size()
	}

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
		cp := make([]byte, len(rec))
		copy(cp, rec)
		records = append(records, cp)
		fr.RawSize += int64(len(cp))
	}

	// Random-access indices, fixed across codecs+framings for fair comparison.
	var raIdx []int
	if raSamples > 0 && fr.Records > 0 {
		raIdx = make([]int, raSamples)
		for i := range raIdx {
			raIdx[i] = rng.Intn(fr.Records)
		}
	}

	for _, name := range codecNames {
		for _, framing := range framings {
			codec, err := makeCodec(name, dict)
			if err != nil {
				fr.Results = append(fr.Results, codecResult{
					Codec: name, Framing: framing, Err: err.Error(),
				})
				continue
			}
			cr := runCodec(codec, records, framing, raIdx, repeats)
			codec.Close()
			fr.Results = append(fr.Results, cr)
		}
	}
	return fr, nil
}

// runCodec dispatches to the right framing-specific benchmark loop.
func runCodec(codec Codec, records [][]byte, framing FramingMode, raIdx []int, repeats int) codecResult {
	switch framing {
	case FramingPerRecord:
		return runCodecPerRecord(codec, records, raIdx, repeats)
	case FramingWholeStream:
		return runCodecWholeStream(codec, records, raIdx, repeats)
	default:
		return codecResult{Codec: codec.Name(), Framing: framing, Err: "unknown framing"}
	}
}

// runCodecPerRecord encodes each record into its own codec frame,
// writes them into the per-record framing format (offset table +
// concatenated frames), and times sequential + random-access decode.
func runCodecPerRecord(codec Codec, records [][]byte, raIdx []int, repeats int) codecResult {
	res := codecResult{Codec: codec.Name(), Framing: FramingPerRecord}

	// Canonical encode used for size + round-trip verify.
	fw := newFramedWriter()
	var encBuf []byte
	for _, rec := range records {
		fb, err := codec.EncodeOne(encBuf[:0], rec)
		if err != nil {
			res.Err = fmt.Sprintf("encode err: %v", err)
			return res
		}
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

	// Round-trip verify: decode every record and assert byte-for-byte.
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
			res.Err = fmt.Sprintf("verify mismatch at record %d (got %d bytes, want %d)",
				i, len(got), len(rec))
			return res
		}
	}

	// Encode-time repeats (best of N).
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

	// Sequential decode repeats (best of N).
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

// runCodecWholeStream encodes the entire record stream as one codec
// frame (length-prefix-delimited internally), measuring the bulk-sync-
// optimal compressed size. Random-access decode here means "decode the
// whole stream once, then walk to the random record" — the walk cost
// is the amortised RA number (assumes the decoded buffer stays in
// memory across queries). The full-stream decode time is in the
// Decode column.
func runCodecWholeStream(codec Codec, records [][]byte, raIdx []int, repeats int) codecResult {
	res := codecResult{Codec: codec.Name(), Framing: FramingWholeStream}

	// Concatenate records with internal length-prefix framing.
	ws := newWholeStreamWriter()
	for _, rec := range records {
		ws.AddRecord(rec)
	}
	raw := ws.Bytes()

	// Canonical encode: one codec frame for the whole raw buffer.
	encoded, err := codec.EncodeOne(nil, raw)
	if err != nil {
		res.Err = fmt.Sprintf("encode err: %v", err)
		return res
	}
	res.CompressedSize = int64(len(encoded))

	// Round-trip verify.
	decoded, err := codec.DecodeOne(nil, encoded)
	if err != nil {
		res.Err = fmt.Sprintf("verify decode: %v", err)
		return res
	}
	if !bytes.Equal(decoded, raw) {
		res.Err = fmt.Sprintf("verify mismatch (got %d bytes, want %d)", len(decoded), len(raw))
		return res
	}

	// Encode-time repeats.
	bestEncode := time.Duration(1<<62 - 1)
	for rep := 0; rep < repeats; rep++ {
		start := time.Now()
		out, err := codec.EncodeOne(nil, raw)
		if err != nil {
			res.Err = fmt.Sprintf("encode-rep err: %v", err)
			return res
		}
		dur := time.Since(start)
		if dur < bestEncode {
			bestEncode = dur
		}
		_ = out
	}
	res.EncodeNs = bestEncode.Nanoseconds()

	// Decode-time repeats.
	bestDecode := time.Duration(1<<62 - 1)
	for rep := 0; rep < repeats; rep++ {
		start := time.Now()
		out, err := codec.DecodeOne(nil, encoded)
		if err != nil {
			res.Err = fmt.Sprintf("decode-rep err: %v", err)
			return res
		}
		dur := time.Since(start)
		if dur < bestDecode {
			bestDecode = dur
		}
		_ = out
	}
	res.DecodeNs = bestDecode.Nanoseconds()

	// Random-access timings (walk-cost only — assumes decoded buffer
	// kept in memory across queries; the full-stream decode cost is
	// reported separately in the Decode column).
	if len(raIdx) > 0 {
		wsr := newWholeStreamReader(decoded)
		res.AccessTimes = make([]time.Duration, 0, len(raIdx))
		for _, idx := range raIdx {
			start := time.Now()
			_, err := wsr.At(idx)
			res.AccessTimes = append(res.AccessTimes, time.Since(start))
			if err != nil {
				res.Err = fmt.Sprintf("random-access walk err at idx %d: %v", idx, err)
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
