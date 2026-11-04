// Copyright 2026 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package mmap

import (
	"errors"
	"os"
	"slices"
	"sync"
	"sync/atomic"
	"unsafe"

	"golang.org/x/sys/unix"

	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/iouring"
)

// BatchPrefetch enables probed reads (Probe) and io_uring page batches (ReadPages).
var BatchPrefetch = dbg.BALBatchPrefetch

type mapping struct {
	start, end uintptr
	fd         int
}

var (
	mappingsMu sync.Mutex
	mappings   atomic.Pointer[[]mapping] // sorted by start, copy-on-write
)

func registerMapping(m []byte, f *os.File) {
	if !BatchPrefetch || len(m) == 0 {
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
	if !BatchPrefetch || len(m) == 0 {
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

// ColdPage is the panic value a probed read raises at its first page that is not resident.
type ColdPage uintptr

// Probe makes the read it is handed to stop with a ColdPage panic at its first non-resident page; a nil Probe
// probes nothing. A Probe belongs to one goroutine.
type Probe struct {
	resident map[uintptr]struct{}
}

// NewProbe returns nil when ReadPages cannot read pages, so callers fall back to ordinary reads.
func NewProbe() *Probe {
	if !BatchPrefetch {
		return nil
	}
	batchRingsOnce.Do(initBatchRings)
	if batchRings == nil {
		return nil
	}
	return &Probe{resident: map[uintptr]struct{}{}}
}

func (pr *Probe) Touch(p unsafe.Pointer) {
	if pr != nil {
		pr.touch(p)
	}
}

func (pr *Probe) touch(p unsafe.Pointer) {
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
		br, ok := newBatchRing()
		if !ok {
			return
		}
		rings <- br
	}
}

func newBatchRing() (*batchRing, bool) {
	r, err := iouring.New(batchRingEntries)
	if err != nil {
		return nil, false
	}
	br := &batchRing{r: r, bufs: make([][]byte, batchRingEntries)}
	for i := range br.bufs {
		br.bufs[i] = make([]byte, osPageSize)
	}
	return br, true
}

// errNoBatchRing reports a pool slot whose ring failed earlier, so the pages stay cold.
var errNoBatchRing = errors.New("mmap: no io_uring ring for this batch")

// brokenBatchRings keeps failed rings and their buffers alive: the kernel may still write into them.
var (
	brokenMu         sync.Mutex
	brokenBatchRings []*batchRing
)

// replaceBrokenBatchRing parks a failed ring; its slot stops reading, so parked rings stay bounded by the pool size.
func replaceBrokenBatchRing(br *batchRing) *batchRing {
	brokenMu.Lock()
	brokenBatchRings = append(brokenBatchRings, br)
	brokenMu.Unlock()
	return &batchRing{}
}

// ReadPages reads the given mapped pages into the page cache with io_uring, up to 8 rings of 256 reads in flight; a
// page outside the registered file mappings is skipped. It returns the first failed batch's error.
func ReadPages(pages []uintptr) error {
	if len(pages) == 0 || !BatchPrefetch {
		return nil
	}
	batchRingsOnce.Do(initBatchRings)
	if batchRings == nil {
		return nil
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
	var errOnce sync.Once
	var firstErr error
	for fd, offs := range byFd {
		for len(offs) > 0 {
			n := min(len(offs), batchRingEntries)
			chunk := offs[:n]
			offs = offs[n:]
			wg.Go(func() {
				br := <-batchRings
				defer func() { batchRings <- br }()
				if br.r == nil {
					errOnce.Do(func() { firstErr = errNoBatchRing })
					return
				}
				lens := make([]int, len(chunk))
				for i := range lens {
					lens[i] = int(osPageSize)
				}
				if err := br.r.BlockingBatchRead(fd, chunk, lens, br.bufs[:len(chunk)]); err != nil {
					errOnce.Do(func() { firstErr = err })
					if !errors.Is(err, iouring.ErrReadFailed) {
						br = replaceBrokenBatchRing(br)
					}
				}
			})
		}
	}
	wg.Wait()
	return firstErr
}
