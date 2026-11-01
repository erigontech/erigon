package mmap

import (
	"os"
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
	// BatchPrefetch enables probed reads (Probe) and io_uring page batches (ReadPages).
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

// Populate makes the page holding p resident; a no-op unless PopulateEnabled.
func Populate(p unsafe.Pointer) {
	if !PopulateEnabled {
		return
	}
	page := uintptr(p) &^ (osPageSize - 1)
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

// Probe makes the read it is handed to stop with a ColdPage panic at its first non-resident page; a nil Probe
// probes nothing. A Probe belongs to one goroutine.
type Probe struct {
	resident map[uintptr]struct{}
	coldData bool
}

func NewProbe() *Probe { return &Probe{resident: map[uintptr]struct{}{}} }

// TouchData is Touch for the first page of a value, so the caller can read the whole value once it is cold.
func (pr *Probe) TouchData(p unsafe.Pointer) {
	if pr == nil {
		return
	}
	pr.coldData = true
	pr.Touch(p)
	pr.coldData = false
}

// ColdData reports whether the last ColdPage came from TouchData, and clears it.
func (pr *Probe) ColdData() bool {
	d := pr.coldData
	pr.coldData = false
	return d
}

func (pr *Probe) Touch(p unsafe.Pointer) {
	if pr == nil {
		return
	}
	page := uintptr(p) &^ (osPageSize - 1)
	if _, ok := pr.resident[page]; ok {
		return
	}
	var vec [1]byte
	_, _, errno := unix.Syscall(unix.SYS_MINCORE, page, osPageSize, uintptr(unsafe.Pointer(&vec[0])))
	if errno == 0 && vec[0]&1 == 0 {
		panic(ColdPage(page))
	}
	pr.resident[page] = struct{}{}
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
