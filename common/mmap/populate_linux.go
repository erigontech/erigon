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
	if !populateIouring || len(m) == 0 {
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
	if !populateIouring || len(m) == 0 {
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

// Populate makes the page holding p resident; a no-op unless PopulateEnabled.
func Populate(p unsafe.Pointer) {
	if !PopulateEnabled {
		return
	}
	page := uintptr(p) &^ (osPageSize - 1)
	if populateIouring {
		if ms := mappings.Load(); ms != nil {
			i, found := slices.BinarySearchFunc(*ms, page, func(e mapping, a uintptr) int {
				if a < e.start {
					return 1
				}
				if a >= e.end {
					return -1
				}
				return 0
			})
			if found {
				m := (*ms)[i]
				iouring.BlockingRead(m.fd, int64(page-m.start), int(osPageSize))
				return
			}
		}
	}
	_, _, _ = unix.Syscall(unix.SYS_MADVISE, page, osPageSize, unix.MADV_POPULATE_READ)
}
