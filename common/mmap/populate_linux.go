package mmap

import (
	"os"
	"runtime"
	"slices"
	"sync"
	"sync/atomic"
	"unsafe"

	"golang.org/x/sys/unix"

	"github.com/erigontech/erigon/common/iouring"
)

// MMAP_POPULATE=true faults mapped pages in via madvise, =iouring reads them via io_uring; either way the disk
// wait happens in a syscall that releases the goroutine's P.
var (
	populateMode    = os.Getenv("MMAP_POPULATE")
	PopulateEnabled = populateMode == "true" || populateMode == "iouring"
	populateIouring = populateMode == "iouring"
	// BatchPrefetch enables probed reads (BeginProbe) and io_uring page batches (ReadPages).
	BatchPrefetch    = os.Getenv("BAL_BATCH_PREFETCH") == "true"
	registerMappings = populateIouring || BatchPrefetch
)

type mapping struct {
	start, end uintptr
	fd         int
}

var (
	mappingsMu sync.Mutex
	mappings   atomic.Pointer[[]mapping] // sorted by start, copy-on-write
)

func registerMapping(m []byte, f *os.File) {
	if !registerMappings || len(m) == 0 {
		return
	}
	mappingsMu.Lock()
	defer mappingsMu.Unlock()
	var cur []mapping
	if p := mappings.Load(); p != nil {
		cur = *p
	}
	start := uintptr(unsafe.Pointer(&m[0]))
	next := slices.Clone(cur)
	i, _ := slices.BinarySearchFunc(next, start, func(e mapping, s uintptr) int { return int(int64(e.start) - int64(s)) })
	next = slices.Insert(next, i, mapping{start: start, end: start + uintptr(len(m)), fd: int(f.Fd())})
	mappings.Store(&next)
}

func unregisterMapping(m []byte) {
	if !registerMappings || len(m) == 0 {
		return
	}
	mappingsMu.Lock()
	defer mappingsMu.Unlock()
	p := mappings.Load()
	if p == nil {
		return
	}
	start := uintptr(unsafe.Pointer(&m[0]))
	next := slices.DeleteFunc(slices.Clone(*p), func(e mapping) bool { return e.start == start })
	mappings.Store(&next)
}

func mappingOf(page uintptr) (mapping, bool) {
	ms := mappings.Load()
	if ms == nil {
		return mapping{}, false
	}
	i, found := slices.BinarySearchFunc(*ms, page, func(e mapping, a uintptr) int {
		if a < e.start {
			return 1
		}
		if a >= e.end {
			return -1
		}
		return 0
	})
	if !found {
		return mapping{}, false
	}
	return (*ms)[i], true
}

// Populate makes the page holding p resident; a no-op unless PopulateEnabled. On a probing goroutine it instead
// stops the read with a ColdPage panic when the page is not resident.
func Populate(p unsafe.Pointer) {
	page := uintptr(p) &^ (osPageSize - 1)
	if probesActive.Load() > 0 {
		if s := probeOfThisThread(); s != nil {
			s.check(page)
			return
		}
	}
	if !PopulateEnabled {
		return
	}
	if populateIouring {
		if m, ok := mappingOf(page); ok {
			iouring.BlockingRead(m.fd, int64(page-m.start), int(osPageSize))
			return
		}
	}
	_, _, _ = unix.Syscall(unix.SYS_MADVISE, page, osPageSize, unix.MADV_POPULATE_READ)
}

// ColdPage is the panic value a probed read raises at its first page that is not resident.
type ColdPage uintptr

type probeSlot struct {
	tid      atomic.Int32
	resident map[uintptr]struct{} // pages seen resident by this probe, owned by its thread
}

var (
	probes       [8]probeSlot
	probesActive atomic.Int32
)

// Probing reports whether any goroutine probes; the hooks check it before anything else.
func Probing() bool { return probesActive.Load() > 0 }

// BeginProbe makes reads on the calling goroutine stop with a ColdPage panic at their first non-resident page, until
// the returned end is called. The goroutine stays on its OS thread meanwhile. ok is false when all probe slots are busy.
func BeginProbe() (end func(), ok bool) {
	if !BatchPrefetch {
		return nil, false
	}
	runtime.LockOSThread()
	tid := int32(unix.Gettid())
	for i := range probes {
		if probes[i].tid.CompareAndSwap(0, tid) {
			probes[i].resident = map[uintptr]struct{}{}
			probesActive.Add(1)
			return func() {
				probesActive.Add(-1)
				probes[i].resident = nil
				probes[i].tid.Store(0)
				runtime.UnlockOSThread()
			}, true
		}
	}
	runtime.UnlockOSThread()
	return nil, false
}

func probeOfThisThread() *probeSlot {
	tid := int32(unix.Gettid())
	for i := range probes {
		if probes[i].tid.Load() == tid {
			return &probes[i]
		}
	}
	return nil
}

func (s *probeSlot) check(page uintptr) {
	if _, ok := s.resident[page]; ok {
		return
	}
	var vec [1]byte
	_, _, errno := unix.Syscall(unix.SYS_MINCORE, page, osPageSize, uintptr(unsafe.Pointer(&vec[0])))
	if errno == 0 && vec[0]&1 == 0 {
		panic(ColdPage(page))
	}
	s.resident[page] = struct{}{}
}

const batchRingEntries = 256

type batchRing struct {
	r    *iouring.Ring
	bufs [][]byte
}

var (
	batchRingsOnce sync.Once
	batchRings     chan *batchRing
)

func initBatchRings() {
	rings := make(chan *batchRing, 8)
	defer func() {
		if len(rings) > 0 {
			batchRings = rings
		}
	}()
	for range cap(rings) {
		r, err := iouring.New(batchRingEntries)
		if err != nil {
			return
		}
		br := &batchRing{r: r, bufs: make([][]byte, batchRingEntries)}
		for i := range br.bufs {
			br.bufs[i] = make([]byte, osPageSize)
		}
		rings <- br
	}
}

// ReadPages reads the given mapped pages into the page cache with io_uring, up to 8 rings of 256 reads in flight; a
// page outside the registered file mappings is skipped.
func ReadPages(pages []uintptr) {
	if len(pages) == 0 || !BatchPrefetch {
		return
	}
	batchRingsOnce.Do(initBatchRings)
	if batchRings == nil {
		return
	}
	byFd := map[int][]int64{}
	seen := make(map[uintptr]struct{}, len(pages))
	for _, p := range pages {
		if _, ok := seen[p]; ok {
			continue
		}
		seen[p] = struct{}{}
		if m, ok := mappingOf(p); ok {
			byFd[m.fd] = append(byFd[m.fd], int64(p-m.start))
		}
	}
	var wg sync.WaitGroup
	for fd, offs := range byFd {
		for len(offs) > 0 {
			n := min(len(offs), batchRingEntries)
			chunk := offs[:n]
			offs = offs[n:]
			wg.Go(func() {
				br := <-batchRings
				defer func() { batchRings <- br }()
				lens := make([]int, len(chunk))
				for i := range lens {
					lens[i] = int(osPageSize)
				}
				_ = br.r.BlockingBatchRead(fd, chunk, lens, br.bufs[:len(chunk)])
			})
		}
	}
	wg.Wait()
}
