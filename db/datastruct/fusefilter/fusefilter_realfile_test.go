package fusefilter

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/spaolacci/murmur3"

	"github.com/erigontech/erigon/db/seg"
)

// TestIndexFromFile is a "tooltest": it indexes the same key stream through
// both Writer (legacy) and WriterSharded paths, then prints a side-by-side
// comparison of peak heap / wall time / file size and verifies every input
// key is present in both readers.
//
// Hashes are streamed, never materialised into a Go slice — the same shape
// recsplit uses in production (one key at a time → AddHash → off-heap mmap
// temp file). This is the only way the tool can run on the 85 GB .kv that
// originally caused issue #20560: a 3 B-key uint64 slice would itself need
// 24 GB of heap and OOM the harness before Build even starts.
//
// Modes (auto-detected by file extension, override with FUSEFILTER_INPUT_MODE):
//
//   - kv  : seg-compressed file with alternating key/value words (.kv, .v, .seg).
//     Keys are murmur3-128-hashed (top 64 bits) before AddHash, mirroring
//     how recsplit.AddKey feeds the existence filter in production.
//   - raw : flat stream of uint64 hashes (8 bytes each, little-endian).
//     Useful when you've already extracted hashes from somewhere and
//     want to skip the key-iteration cost.
//
// Skips when FUSEFILTER_INPUT is empty, so it never runs in CI.
//
// Set FUSEFILTER_SKIP_PARITY=1 to skip the third (parity) stream pass on huge
// files where the extra disk read is expensive.
//
// Example:
//
//	FUSEFILTER_INPUT=/snapshots/v2.0-storage.7488-7496.kv \
//	    go test -run=TestIndexFromFile -v -count=1 -timeout=8h \
//	    ./db/datastruct/fusefilter/
func TestIndexFromFile(t *testing.T) {
	path := os.Getenv("FUSEFILTER_INPUT")
	if path == "" {
		t.Skip("set FUSEFILTER_INPUT=/path/to/file (.kv, .v, .seg, or raw uint64 stream) to run")
	}
	mode := os.Getenv("FUSEFILTER_INPUT_MODE")
	if mode == "" {
		switch strings.ToLower(filepath.Ext(path)) {
		case ".kv", ".v", ".seg":
			mode = "kv"
		default:
			mode = "raw"
		}
	}

	stream, err := streamerFor(path, mode)
	if err != nil {
		t.Fatalf("streamerFor: %v", err)
	}

	outDir := t.TempDir()
	plainPath := filepath.Join(outDir, "plain.efi")
	shardPath := filepath.Join(outDir, "sharded.efi")

	plain := runBuild(t, "plain", stream, plainPath, false)
	shard := runBuild(t, "sharded", stream, shardPath, true)

	if plain.keyCount != shard.keyCount {
		t.Fatalf("stream key count drift: plain=%d sharded=%d", plain.keyCount, shard.keyCount)
	}

	t.Logf("")
	t.Logf("=== summary (n=%d) ===", plain.keyCount)
	t.Logf("                 plain          sharded        delta")
	t.Logf("peak_heap   %12.1f MB %12.1f MB  %+6.1f%%", mb(plain.peakHeap), mb(shard.peakHeap), pct(plain.peakHeap, shard.peakHeap))
	t.Logf("file_size   %12.1f MB %12.1f MB  %+6.1f%%", mb(plain.fileSize), mb(shard.fileSize), pct(plain.fileSize, shard.fileSize))
	t.Logf("build_time  %12s    %12s     %+6.1f%%", plain.buildWall, shard.buildWall, pctDur(plain.buildWall, shard.buildWall))
	t.Logf("ingest_time %12s    %12s", plain.ingestWall, shard.ingestWall)
	t.Logf("ratios      peak %.2fx (plain/sharded)  size %.2fx (sharded/plain)  build_time %.2fx",
		float64(plain.peakHeap)/float64(shard.peakHeap),
		float64(shard.fileSize)/float64(plain.fileSize),
		float64(shard.buildWall)/float64(plain.buildWall))

	if os.Getenv("FUSEFILTER_SKIP_PARITY") != "" {
		t.Logf("parity check skipped (FUSEFILTER_SKIP_PARITY set)")
	} else {
		verifyParity(t, stream, plainPath, shardPath)
	}
	measureFP(t, shardPath)
}

type buildResult struct {
	keyCount   int
	peakHeap   uint64
	fileSize   uint64
	ingestWall time.Duration
	buildWall  time.Duration
}

// runBuild streams the input through AddHash, then drops the streamer, runs a
// GC to clear any source-side state, and only THEN starts the peak sampler
// and the wall-clock timer for Build(). This isolates the Go-heap pressure
// inside Build (the actual OOM site) from key-iteration overhead.
func runBuild(t *testing.T, label string, stream streamer, outPath string, sharded bool) buildResult {
	t.Helper()

	type writerLike interface {
		AddHash(uint64) error
		DisableFsync()
	}
	var (
		w     writerLike
		build func() error
		close func()
	)
	if sharded {
		ww, err := NewWriterSharded(outPath)
		if err != nil {
			t.Fatalf("%s: NewWriterSharded: %v", label, err)
		}
		w = ww
		build = ww.Build
		close = ww.Close
	} else {
		ww, err := NewWriter(outPath)
		if err != nil {
			t.Fatalf("%s: NewWriter: %v", label, err)
		}
		w = ww
		build = ww.Build
		close = ww.Close
	}
	w.DisableFsync()

	ingestStart := time.Now()
	count, err := stream.each(func(h uint64) error { return w.AddHash(h) })
	ingestWall := time.Since(ingestStart)
	if err != nil {
		t.Fatalf("%s: ingest: %v", label, err)
	}

	// Drop any per-stream buffers and let the GC run BEFORE we start measuring
	// Build. Anything still on the heap at this point is part of the writer's
	// own state — exactly what the OOM-prone path holds onto.
	runtime.GC()
	sampler := startPeakSampler(500 * time.Microsecond)
	buildStart := time.Now()
	if err := build(); err != nil {
		sampler.Stop()
		t.Fatalf("%s: Build: %v", label, err)
	}
	buildWall := time.Since(buildStart)
	peak := sampler.Stop()
	close()

	st, err := os.Stat(outPath)
	if err != nil {
		t.Fatalf("%s: stat: %v", label, err)
	}
	t.Logf("%-7s  ingest=%s  build=%s  peak=%.1f MB  size=%.1f MB  keys=%d",
		label, ingestWall, buildWall, mb(peak), mb(uint64(st.Size())), count)
	return buildResult{
		keyCount:   count,
		peakHeap:   peak,
		fileSize:   uint64(st.Size()),
		ingestWall: ingestWall,
		buildWall:  buildWall,
	}
}

func verifyParity(t *testing.T, stream streamer, plainPath, shardPath string) {
	t.Helper()
	plain, err := NewReader(plainPath)
	if err != nil {
		t.Fatalf("open plain: %v", err)
	}
	defer plain.Close()
	shard, err := NewReaderSharded(shardPath)
	if err != nil {
		t.Fatalf("open sharded: %v", err)
	}
	defer shard.Close()

	missingPlain, missingShard := 0, 0
	count, err := stream.each(func(h uint64) error {
		if !plain.ContainsHash(h) {
			missingPlain++
		}
		if !shard.ContainsHash(h) {
			missingShard++
		}
		return nil
	})
	if err != nil {
		t.Fatalf("parity stream: %v", err)
	}
	if missingPlain != 0 || missingShard != 0 {
		t.Fatalf("missing keys: plain=%d sharded=%d (must be 0 — fuse filter has 0%% false-negative rate)", missingPlain, missingShard)
	}
	t.Logf("parity ok: all %d input hashes present in both readers", count)
}

// measureFP samples the false-positive rate of the sharded reader by probing
// random uint64s. We deliberately do NOT build a presence map of input keys
// (that would defeat the streaming design at scale): on any realistic input
// size N, the chance a random uint64 collides with a known key is N/2^64 —
// negligible (3 B keys → ~1.6e-10), so collisions with the input set vanish
// into the FP-rate noise.
func measureFP(t *testing.T, shardPath string) {
	t.Helper()
	r, err := NewReaderSharded(shardPath)
	if err != nil {
		t.Fatalf("open sharded for fp probe: %v", err)
	}
	defer r.Close()

	rng := rand.New(rand.NewPCG(7, 11))
	const probes = 200_000
	fp := 0
	for i := 0; i < probes; i++ {
		if r.ContainsHash(rng.Uint64()) {
			fp++
		}
	}
	t.Logf("sharded false-positive rate: %d/%d = %.4f%% (xorfilter[uint8] target ~0.39%%)", fp, probes, float64(fp)/float64(probes)*100)
}

// streamer is a re-runnable hash producer. Calling each() walks the underlying
// file from the start and invokes fn for every hash; it can be called multiple
// times (each call re-opens the file) so plain build, sharded build and parity
// check each get a fresh pass.
type streamer interface {
	each(fn func(uint64) error) (int, error)
}

func streamerFor(path, mode string) (streamer, error) {
	switch mode {
	case "kv":
		return &kvStreamer{path: path}, nil
	case "raw":
		return &rawStreamer{path: path}, nil
	default:
		return nil, fmt.Errorf("unknown FUSEFILTER_INPUT_MODE=%q (want kv or raw)", mode)
	}
}

type kvStreamer struct{ path string }

func (s *kvStreamer) each(fn func(uint64) error) (int, error) {
	d, err := seg.NewDecompressor(s.path)
	if err != nil {
		return 0, fmt.Errorf("seg.NewDecompressor: %w", err)
	}
	defer d.Close()
	g := d.MakeGetter()

	const salt uint32 = 0
	var key []byte
	count := 0
	keyTurn := true
	for g.HasNext() {
		key, _ = g.Next(key[:0])
		if keyTurn {
			hi, _ := murmur3.Sum128WithSeed(key, salt)
			if err := fn(hi); err != nil {
				return count, err
			}
			count++
		}
		keyTurn = !keyTurn
	}
	return count, nil
}

type rawStreamer struct{ path string }

func (s *rawStreamer) each(fn func(uint64) error) (int, error) {
	f, err := os.Open(s.path)
	if err != nil {
		return 0, err
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return 0, err
	}
	if st.Size()%8 != 0 {
		return 0, fmt.Errorf("raw file size %d not a multiple of 8", st.Size())
	}
	br := bufio.NewReaderSize(f, 1<<20)
	var buf [8]byte
	count := 0
	for {
		_, err := io.ReadFull(br, buf[:])
		if errors.Is(err, io.EOF) {
			return count, nil
		}
		if err != nil {
			return count, err
		}
		if err := fn(binary.LittleEndian.Uint64(buf[:])); err != nil {
			return count, err
		}
		count++
	}
}

func mb(b uint64) float64 { return float64(b) / (1 << 20) }

func pct(plain, sharded uint64) float64 {
	if plain == 0 {
		return 0
	}
	return (float64(sharded) - float64(plain)) / float64(plain) * 100
}

func pctDur(plain, sharded time.Duration) float64 {
	if plain == 0 {
		return 0
	}
	return (float64(sharded) - float64(plain)) / float64(plain) * 100
}

// TestIndexFromFile_SyntheticSmoke generates a small synthetic raw file and
// runs the same code path to make sure the harness itself works end-to-end
// without requiring a real .kv on the box. Skips unless explicitly enabled.
func TestIndexFromFile_SyntheticSmoke(t *testing.T) {
	if os.Getenv("FUSEFILTER_SMOKE") == "" {
		t.Skip("set FUSEFILTER_SMOKE=1 to run the synthetic harness self-test")
	}
	tmp := t.TempDir()
	rawPath := filepath.Join(tmp, "in.raw")

	rng := rand.New(rand.NewPCG(42, 7))
	const n = 50_000
	f, err := os.Create(rawPath)
	if err != nil {
		t.Fatal(err)
	}
	var buf [8]byte
	for i := 0; i < n; i++ {
		binary.LittleEndian.PutUint64(buf[:], rng.Uint64())
		if _, err := f.Write(buf[:]); err != nil {
			t.Fatal(err)
		}
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	t.Setenv("FUSEFILTER_INPUT", rawPath)
	t.Setenv("FUSEFILTER_INPUT_MODE", "raw")
	TestIndexFromFile(t)
}
