//go:build linux

package seg

import (
	"os"
	"testing"

	"unsafe"

	"golang.org/x/sys/unix"

	"github.com/erigontech/erigon/common/iouring"
	"github.com/erigontech/erigon/common/mmap"
)

// These benchmarks isolate the cost the residency gate adds on top of a plain
// mmap access, separately for the warm path (page already resident: the gate
// pays a mincore probe and returns) and the cold path (page not resident: the
// gate pays mincore + an io_uring read, turning the following mmap access from a
// major fault into a minor one). Compare Warm{Mincore,Touch} for the per-read
// tax paid on every read, and Cold{Gate,MmapFault} for whether the gate's cold
// path is cheaper or more expensive than just letting the mapping major-fault.

var byteSink byte

const resBenchPages = 8192 // 32MB working file; cold benches re-evict per iteration

func setupResidencyBench(tb testing.TB) (fd int, m []byte, ps int) {
	ps = os.Getpagesize()
	f, err := os.CreateTemp(tb.TempDir(), "resbench")
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() { f.Close() })
	size := resBenchPages * ps
	chunk := make([]byte, ps)
	for i := range chunk {
		chunk[i] = byte(i*31 + 7)
	}
	for p := 0; p < resBenchPages; p++ {
		if _, err := f.WriteAt(chunk, int64(p*ps)); err != nil {
			tb.Fatal(err)
		}
	}
	if err := f.Sync(); err != nil {
		tb.Fatal(err)
	}
	fd = int(f.Fd())
	m, err = unix.Mmap(fd, 0, size, unix.PROT_READ, unix.MAP_SHARED)
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() { unix.Munmap(m) })
	// MADV_RANDOM so a major fault pulls exactly one page (no readahead), matching
	// the single-page io_uring warm — otherwise the fault path gets free readahead
	// the gate doesn't, which would flatter it.
	_ = unix.Madvise(m, unix.MADV_RANDOM)
	return fd, m, ps
}

func evictPage(fd int, m []byte, off, ps int) {
	_ = unix.Madvise(m[off:off+ps], unix.MADV_DONTNEED) // drop the PTE
	_, _, _ = unix.Syscall6(unix.SYS_FADVISE64, uintptr(fd), uintptr(off), uintptr(ps), unix.FADV_DONTNEED, 0, 0)
}

// warm path: page resident, gate pays a mincore probe and returns.
func BenchmarkResidencyWarmMincore(b *testing.B) {
	_, m, ps := setupResidencyBench(b)
	byteSink = m[0] // make page 0 resident
	region := m[0:ps]
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := mmap.Resident(region); err != nil {
			b.Fatal(err)
		}
	}
}

// warm path, mincore with a reused vec (no per-call heap alloc) to separate the
// bare syscall cost from the allocation in mmap.Resident.
func BenchmarkResidencyWarmMincoreNoAlloc(b *testing.B) {
	_, m, ps := setupResidencyBench(b)
	byteSink = m[0]
	vec := make([]byte, 1)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, errno := unix.Syscall(unix.SYS_MINCORE,
			uintptr(unsafe.Pointer(&m[0])), uintptr(ps), uintptr(unsafe.Pointer(&vec[0])))
		if errno != 0 {
			b.Fatal(errno)
		}
	}
	_ = ps
}

// warm path baseline: no gate, just read the resident byte.
func BenchmarkResidencyWarmTouch(b *testing.B) {
	_, m, _ := setupResidencyBench(b)
	byteSink = m[0]
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		byteSink = m[0]
	}
}

// cold path baseline: no gate, mmap major fault does the disk read.
func BenchmarkResidencyColdMmapFault(b *testing.B) {
	fd, m, ps := setupResidencyBench(b)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		off := (i % resBenchPages) * ps
		b.StopTimer()
		evictPage(fd, m, off, ps)
		b.StartTimer()
		byteSink = m[off]
	}
}

// always-io_uring, warm page: no mincore probe, io_uring reads already-cached
// data then the mmap access is resident. This is the per-read tax the
// no-probe design pays on the common (warm) case.
func BenchmarkResidencyWarmGateNoProbe(b *testing.B) {
	fd, m, ps := setupResidencyBench(b)
	byteSink = m[0]
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		iouring.WarmOne(fd, 0, ps)
		byteSink = m[0]
	}
}

// always-io_uring, cold page: no mincore probe, io_uring does the disk read then
// the mmap access is a minor fault.
func BenchmarkResidencyColdGateNoProbe(b *testing.B) {
	fd, m, ps := setupResidencyBench(b)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		off := (i % resBenchPages) * ps
		b.StopTimer()
		evictPage(fd, m, off, ps)
		b.StartTimer()
		iouring.WarmOne(fd, int64(off), ps)
		byteSink = m[off]
	}
}

// cold path with gate: mincore (reports not resident) + io_uring warm, then the
// mmap access is a minor fault.
func BenchmarkResidencyColdGate(b *testing.B) {
	fd, m, ps := setupResidencyBench(b)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		off := (i % resBenchPages) * ps
		b.StopTimer()
		evictPage(fd, m, off, ps)
		b.StartTimer()
		region := m[off : off+ps]
		if resident, err := mmap.Resident(region); err == nil && !resident {
			iouring.WarmOne(fd, int64(off), ps)
		}
		byteSink = m[off]
	}
}
