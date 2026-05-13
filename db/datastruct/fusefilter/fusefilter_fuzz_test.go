package fusefilter

import (
	"bytes"
	"path/filepath"
	"testing"
)

// FuzzReaderOnBytes asserts NewReaderOnBytes never panics on any byte input
// and that, when it succeeds, calling ContainsHash on the resulting filter
// also never panics on a sweep of probe values. This closes the OOB-read
// concern Copilot raised on the per-shard blob parser: a truncated or
// adversarially-crafted blob must surface as an error, not a runtime panic
// in xorfilter.Contains.
//
// Seed corpus: a known-good 1000-key blob plus a few obviously-bad inputs.
// Run with:
//
//	go test -run=^$ -fuzz=FuzzReaderOnBytes -fuzztime=30s ./db/datastruct/fusefilter/
func FuzzReaderOnBytes(f *testing.F) {
	good := buildValidBlob(f, 1000)
	f.Add(good)
	f.Add([]byte{})
	f.Add(make([]byte, headerSize-1))
	f.Add(make([]byte, headerSize))
	// Truncate at every header byte boundary.
	for i := 0; i < headerSize; i++ {
		if i < len(good) {
			f.Add(good[:i])
		}
	}

	probes := []uint64{0, 1, ^uint64(0), 0xdeadbeefcafebabe, 0x42, 1 << 56}

	f.Fuzz(func(t *testing.T, data []byte) {
		r, _, err := NewReaderOnBytes(data, "fuzz")
		if err != nil {
			return
		}
		// Successful parse: every probe must complete without panicking.
		for _, p := range probes {
			_ = r.ContainsHash(p)
		}
	})
}

// FuzzReaderShardedOnBytes is the same property for the sharded outer format.
// It exercises the [4-byte header | 256 × (8-byte size, blob)] layout, which
// has more dimensions of possible corruption (shard table truncation, oversize
// shard size fields, recursively-corrupt inner blobs).
//
// Run with:
//
//	go test -run=^$ -fuzz=FuzzReaderShardedOnBytes -fuzztime=30s ./db/datastruct/fusefilter/
func FuzzReaderShardedOnBytes(f *testing.F) {
	good := buildValidShardedBlob(f, 1000)
	f.Add(good)
	f.Add([]byte{})
	f.Add(make([]byte, 3))                  // shorter than outer header
	f.Add(make([]byte, 4))                  // outer header only, no shard table
	f.Add(good[:5])                         // mid-shard-table truncation
	f.Add(append([]byte(nil), 99, 0, 0, 0)) // unsupported version

	probes := []uint64{0, 1, ^uint64(0), 0xdeadbeefcafebabe, 0x42, 1 << 56}

	f.Fuzz(func(t *testing.T, data []byte) {
		r, _, err := NewReaderShardedOnBytes(data, "fuzz")
		if err != nil {
			return
		}
		for _, p := range probes {
			_ = r.ContainsHash(p)
		}
	})
}

// FuzzWriterRoundTrip asserts a build → read round-trip property: every key
// fed into the writer is reported present by the reader (xorfilter has 0%
// false-negative rate, by construction). Keys are derived deterministically
// from the fuzz inputs so the seed corpus shrinks naturally.
//
// Run with:
//
//	go test -run=^$ -fuzz=FuzzWriterRoundTrip -fuzztime=30s ./db/datastruct/fusefilter/
func FuzzWriterRoundTrip(f *testing.F) {
	f.Add(uint64(1), uint16(64))
	f.Add(uint64(0), uint16(1))
	f.Add(uint64(^uint64(0)), uint16(8192))

	f.Fuzz(func(t *testing.T, seed uint64, n uint16) {
		// Cap n so each fuzz iteration stays under a few ms.
		if n == 0 {
			return
		}
		if n > 8192 {
			n = 8192
		}

		dir := t.TempDir()
		fp := filepath.Join(dir, "f")
		w, err := NewWriterSharded(fp)
		if err != nil {
			t.Fatal(err)
		}
		w.DisableFsync()

		// Cheap deterministic key stream: splitmix64.
		x := seed
		keys := make([]uint64, 0, n)
		for i := uint16(0); i < n; i++ {
			x += 0x9E3779B97F4A7C15
			z := x
			z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9
			z = (z ^ (z >> 27)) * 0x94D049BB133111EB
			z = z ^ (z >> 31)
			keys = append(keys, z)
			if err := w.AddHash(z); err != nil {
				t.Fatal(err)
			}
		}
		if err := w.Build(); err != nil {
			t.Fatal(err)
		}
		w.Close()

		r, err := NewReaderSharded(fp)
		if err != nil {
			t.Fatal(err)
		}
		defer r.Close()
		for _, k := range keys {
			if !r.ContainsHash(k) {
				t.Fatalf("seed=%d n=%d: ContainsHash(%d) = false (xorfilter must have 0%% FN rate)", seed, n, k)
			}
		}
	})
}

// buildValidBlob is a fuzz-friendly twin of validBlob() that takes *testing.F.
func buildValidBlob(f *testing.F, n int) []byte {
	f.Helper()
	w, err := NewWriterOffHeap(filepath.Join(f.TempDir(), "v"))
	if err != nil {
		f.Fatal(err)
	}
	defer w.Close()
	for i := 0; i < n; i++ {
		if err := w.AddHash(uint64(i)); err != nil {
			f.Fatal(err)
		}
	}
	var buf bytes.Buffer
	if _, err := w.BuildTo(&buf); err != nil {
		f.Fatal(err)
	}
	return buf.Bytes()
}

func buildValidShardedBlob(f *testing.F, n int) []byte {
	f.Helper()
	w, err := NewWriterSharded(filepath.Join(f.TempDir(), "v"))
	if err != nil {
		f.Fatal(err)
	}
	defer w.Close()
	for i := 0; i < n; i++ {
		if err := w.AddHash(uint64(i)); err != nil {
			f.Fatal(err)
		}
	}
	var buf bytes.Buffer
	if _, err := w.BuildTo(&buf); err != nil {
		f.Fatal(err)
	}
	return buf.Bytes()
}
