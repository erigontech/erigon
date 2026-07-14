//go:build linux

package iouring

import (
	"os"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

// TestQDHeadroom reads N random 4KB blocks from a real commitment .kv snapshot,
// cold, at increasing queue depths, to see whether the storage delivers more
// IOPS at deep QD than at QD1 (serial pread). Set KV_FILE to override the path.
func TestQDHeadroom(t *testing.T) {
	path := os.Getenv("KV_FILE")
	if path == "" {
		path = "/erigon-data/jochemnet36_merged/snapshots/domain/v2.2-commitment.1049728-1049792.kv"
	}
	f, err := os.Open(path)
	if err != nil {
		t.Skipf("no snapshot file: %v", err)
	}
	defer f.Close()
	fd := int(f.Fd())
	st, _ := f.Stat()
	size := st.Size()
	const blk = 4096
	const N = 20000

	// Deterministic pseudo-random aligned offsets (no Math.rand needed).
	offsets := make([]int64, N)
	lens := make([]int, N)
	x := uint64(0x9e3779b97f4a7c15)
	nblocks := uint64(size / blk)
	for i := range offsets {
		x ^= x << 13
		x ^= x >> 7
		x ^= x << 17
		offsets[i] = int64((x % nblocks) * blk)
		lens[i] = blk
	}

	evict := func() {
		_ = unix.Fadvise(fd, 0, size, unix.FADV_DONTNEED)
	}

	// QD1: serial pread.
	evict()
	buf := make([]byte, blk)
	t0 := time.Now()
	for i := 0; i < N; i++ {
		if _, err := unix.Pread(fd, buf, offsets[i]); err != nil {
			t.Fatalf("pread: %v", err)
		}
	}
	d1 := time.Since(t0)
	t.Logf("QD=1   (serial pread): %6.0f IOPS  (%v for %d reads)", float64(N)/d1.Seconds(), d1, N)

	for _, qd := range []uint32{8, 32, 64, 128, 256} {
		ring, err := New(qd)
		if err != nil {
			t.Fatalf("New(%d): %v", qd, err)
		}
		scratch := make([][]byte, ring.entries)
		for i := range scratch {
			scratch[i] = make([]byte, blk)
		}
		evict()
		t0 := time.Now()
		if err := ring.BatchReadWarm(fd, offsets, lens, scratch); err != nil {
			t.Fatalf("BatchReadWarm(qd=%d): %v", qd, err)
		}
		d := time.Since(t0)
		t.Logf("QD=%-4d (io_uring):     %6.0f IOPS  (%v for %d reads)", ring.entries, float64(N)/d.Seconds(), d, N)
		ring.Close()
	}
}
