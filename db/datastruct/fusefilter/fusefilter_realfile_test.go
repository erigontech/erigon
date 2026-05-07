package fusefilter

import (
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
// Example:
//
//	FUSEFILTER_INPUT=/snapshots/v2.0-storage.7488-7496.kv \
//	    go test -run=TestIndexFromFile -v -count=1 -timeout=4h \
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

	hashes, err := loadHashes(t, path, mode)
	if err != nil {
		t.Fatalf("loadHashes: %v", err)
	}
	t.Logf("loaded %d keys from %s (mode=%s)", len(hashes), path, mode)
	if len(hashes) == 0 {
		t.Skip("file produced 0 keys")
	}

	outDir := t.TempDir()
	plainPath := filepath.Join(outDir, "plain.efi")
	shardPath := filepath.Join(outDir, "sharded.efi")

	plain := runBuild(t, "plain", hashes, plainPath, false)
	shard := runBuild(t, "sharded", hashes, shardPath, true)

	t.Logf("")
	t.Logf("=== summary (n=%d) ===", len(hashes))
	t.Logf("                 plain          sharded        delta")
	t.Logf("peak_heap   %12.1f MB %12.1f MB  %+6.1f%%", mb(plain.peakHeap), mb(shard.peakHeap), pct(plain.peakHeap, shard.peakHeap))
	t.Logf("file_size   %12.1f MB %12.1f MB  %+6.1f%%", mb(plain.fileSize), mb(shard.fileSize), pct(plain.fileSize, shard.fileSize))
	t.Logf("wall_time   %12s    %12s     %+6.1f%%", plain.wall, shard.wall, pctDur(plain.wall, shard.wall))
	t.Logf("ratios      peak %.2fx  size %.2fx  time %.2fx",
		float64(plain.peakHeap)/float64(shard.peakHeap),
		float64(shard.fileSize)/float64(plain.fileSize),
		float64(shard.wall)/float64(plain.wall))

	verifyParity(t, hashes, plainPath, shardPath)
	measureFP(t, hashes, shardPath)
}

type buildResult struct {
	peakHeap uint64
	fileSize uint64
	wall     time.Duration
}

func runBuild(t *testing.T, label string, hashes []uint64, outPath string, sharded bool) buildResult {
	t.Helper()
	runtime.GC()
	sampler := startPeakSampler(500 * time.Microsecond)
	start := time.Now()

	if sharded {
		w, err := NewWriterSharded(outPath)
		if err != nil {
			t.Fatalf("%s: NewWriterSharded: %v", label, err)
		}
		w.DisableFsync()
		for _, h := range hashes {
			if err := w.AddHash(h); err != nil {
				t.Fatalf("%s: AddHash: %v", label, err)
			}
		}
		if err := w.Build(); err != nil {
			t.Fatalf("%s: Build: %v", label, err)
		}
		w.Close()
	} else {
		w, err := NewWriter(outPath)
		if err != nil {
			t.Fatalf("%s: NewWriter: %v", label, err)
		}
		w.DisableFsync()
		for _, h := range hashes {
			if err := w.AddHash(h); err != nil {
				t.Fatalf("%s: AddHash: %v", label, err)
			}
		}
		if err := w.Build(); err != nil {
			t.Fatalf("%s: Build: %v", label, err)
		}
		w.Close()
	}

	wall := time.Since(start)
	peak := sampler.Stop()

	st, err := os.Stat(outPath)
	if err != nil {
		t.Fatalf("%s: stat: %v", label, err)
	}
	t.Logf("%-7s built: peak=%.1f MB time=%s size=%.1f MB", label, mb(peak), wall, mb(uint64(st.Size())))
	return buildResult{peakHeap: peak, fileSize: uint64(st.Size()), wall: wall}
}

func verifyParity(t *testing.T, hashes []uint64, plainPath, shardPath string) {
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
	for _, h := range hashes {
		if !plain.ContainsHash(h) {
			missingPlain++
		}
		if !shard.ContainsHash(h) {
			missingShard++
		}
	}
	if missingPlain != 0 || missingShard != 0 {
		t.Fatalf("missing keys: plain=%d sharded=%d (must be 0 — fuse filter has 0%% false-negative rate)", missingPlain, missingShard)
	}
	t.Logf("parity ok: all %d input hashes present in both readers", len(hashes))
}

// measureFP samples false-positive rate of the sharded reader by probing
// random uint64s that are NOT in the input set. Uses a small bloom-style
// presence map; cap at 200k probes to keep this fast on huge inputs.
func measureFP(t *testing.T, hashes []uint64, shardPath string) {
	t.Helper()
	r, err := NewReaderSharded(shardPath)
	if err != nil {
		t.Fatalf("open sharded for fp probe: %v", err)
	}
	defer r.Close()

	present := make(map[uint64]struct{}, len(hashes))
	for _, h := range hashes {
		present[h] = struct{}{}
	}

	rng := rand.New(rand.NewPCG(7, 11))
	const probes = 200_000
	fp := 0
	tries := 0
	for i := 0; i < probes; i++ {
		var k uint64
		for {
			k = rng.Uint64()
			tries++
			if _, ok := present[k]; !ok {
				break
			}
		}
		if r.ContainsHash(k) {
			fp++
		}
	}
	t.Logf("sharded false-positive rate: %d/%d = %.4f%% (xorfilter[uint8] target ~0.39%%)", fp, probes, float64(fp)/float64(probes)*100)
}

// loadHashes reads all hashes from path according to mode.
func loadHashes(t *testing.T, path, mode string) ([]uint64, error) {
	t.Helper()
	switch mode {
	case "kv":
		return loadKV(path)
	case "raw":
		return loadRaw(path)
	default:
		return nil, fmt.Errorf("unknown FUSEFILTER_INPUT_MODE=%q (want kv or raw)", mode)
	}
}

func loadKV(path string) ([]uint64, error) {
	d, err := seg.NewDecompressor(path)
	if err != nil {
		return nil, fmt.Errorf("seg.NewDecompressor: %w", err)
	}
	defer d.Close()

	g := d.MakeGetter()
	hashes := make([]uint64, 0, d.Count()/2)
	var key []byte
	const salt uint32 = 0
	keyTurn := true
	for g.HasNext() {
		key, _ = g.Next(key[:0])
		if keyTurn {
			hi, _ := murmur3.Sum128WithSeed(key, salt)
			hashes = append(hashes, hi)
		}
		keyTurn = !keyTurn
	}
	return hashes, nil
}

func loadRaw(path string) ([]uint64, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if st.Size()%8 != 0 {
		return nil, fmt.Errorf("raw file size %d not a multiple of 8", st.Size())
	}
	hashes := make([]uint64, 0, st.Size()/8)
	var buf [8]byte
	for {
		_, err := io.ReadFull(f, buf[:])
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, err
		}
		hashes = append(hashes, binary.LittleEndian.Uint64(buf[:]))
	}
	return hashes, nil
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
