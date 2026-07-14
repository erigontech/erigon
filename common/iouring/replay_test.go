//go:build linux

package iouring

import (
	"bufio"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

// TestTraceReplay replays the exact cold-read set captured by TRACE_COLD_READS
// (coldtrace.txt: "<basename> <offset> <len>" per line) against the real .kv
// files, serially (QD1 pread) vs io_uring QD128, to measure the achievable
// batched-prefetch speedup on the *real* read set.
func TestTraceReplay(t *testing.T) {
	tracePath := os.Getenv("TRACE_FILE")
	if tracePath == "" {
		tracePath = "/erigon-data/jochemnet36_merged/coldtrace.txt"
	}
	domainDir := os.Getenv("DOMAIN_DIR")
	if domainDir == "" {
		domainDir = "/erigon-data/jochemnet36_merged/snapshots/domain"
	}
	f, err := os.Open(tracePath)
	if err != nil {
		t.Skipf("no trace: %v", err)
	}
	defer f.Close()

	type rd struct {
		off int64
		ln  int
	}
	byFile := map[string][]rd{}
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	total := 0
	for sc.Scan() {
		parts := strings.Fields(sc.Text())
		if len(parts) != 3 {
			continue
		}
		off, _ := strconv.ParseInt(parts[1], 10, 64)
		ln, _ := strconv.Atoi(parts[2])
		byFile[parts[0]] = append(byFile[parts[0]], rd{off, ln})
		total++
	}
	t.Logf("trace: %d cold reads across %d files", total, len(byFile))

	const maxBuf = 64 * 1024
	const qd = 128
	var totSerial, totIou time.Duration
	var replayed int

	for base, reads := range byFile {
		path := filepath.Join(domainDir, base)
		df, err := os.Open(path)
		if err != nil {
			t.Logf("  skip %s: %v", base, err)
			continue
		}
		fd := int(df.Fd())
		st, _ := df.Stat()
		size := st.Size()
		offs := make([]int64, len(reads))
		lens := make([]int, len(reads))
		for i, r := range reads {
			offs[i] = r.off
			lens[i] = r.ln
			if lens[i] > maxBuf {
				lens[i] = maxBuf
			}
		}

		// Serial (QD1) pread.
		_ = unix.Fadvise(fd, 0, size, unix.FADV_DONTNEED)
		buf := make([]byte, maxBuf)
		t0 := time.Now()
		for i := range offs {
			_, _ = unix.Pread(fd, buf[:lens[i]], offs[i])
		}
		serial := time.Since(t0)

		// io_uring QD128.
		_ = unix.Fadvise(fd, 0, size, unix.FADV_DONTNEED)
		ring, err := New(qd)
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		scratch := make([][]byte, ring.entries)
		for i := range scratch {
			scratch[i] = make([]byte, maxBuf)
		}
		t0 = time.Now()
		if err := ring.BatchReadWarm(fd, offs, lens, scratch); err != nil {
			t.Fatalf("BatchReadWarm: %v", err)
		}
		iou := time.Since(t0)
		ring.Close()
		df.Close()

		totSerial += serial
		totIou += iou
		replayed += len(reads)
		t.Logf("  %-42s reads=%-7d serial=%-10v iou=%-10v speedup=%.1fx", base, len(reads), serial.Round(time.Millisecond), iou.Round(time.Millisecond), float64(serial)/float64(iou))
	}
	t.Logf("TOTAL reads=%d  serial=%v (%.0f IOPS)  iouring=%v (%.0f IOPS)  SPEEDUP=%.1fx",
		replayed, totSerial.Round(time.Millisecond), float64(replayed)/totSerial.Seconds(),
		totIou.Round(time.Millisecond), float64(replayed)/totIou.Seconds(),
		float64(totSerial)/float64(totIou))
}
