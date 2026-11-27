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
	"context"
	"errors"
	"os"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/tidwall/btree"
	"golang.org/x/sys/unix"

	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/iouring"
	"github.com/erigontech/erigon/common/log/v3"
)

// BatchPrefetch enables probed reads (Probe) and io_uring page batches (ReadPages).
var BatchPrefetch = dbg.BALBatchPrefetch

type mapping struct {
	start, end uintptr
	fd         int
}

var (
	mappingsMu sync.Mutex
	mappingsW  btree.Map[uintptr, mapping]                 // under mappingsMu
	mappings   atomic.Pointer[btree.Map[uintptr, mapping]] // read-only copy of mappingsW, keyed by start
)

func registerMapping(m []byte, f *os.File) {
	if !BatchPrefetch || len(m) == 0 {
		return
	}
	start := uintptr(unsafe.Pointer(&m[0]))
	mappingsMu.Lock()
	defer mappingsMu.Unlock()
	mappingsW.Set(start, mapping{start: start, end: start + uintptr(len(m)), fd: int(f.Fd())})
	mappings.Store(mappingsW.Copy())
}

func unregisterMapping(m []byte) {
	if len(m) == 0 {
		return
	}
	mappingsMu.Lock()
	defer mappingsMu.Unlock()
	if _, ok := mappingsW.Delete(uintptr(unsafe.Pointer(&m[0]))); ok {
		mappings.Store(mappingsW.Copy())
	}
}

func mappingOf(page uintptr) (m mapping, ok bool) {
	if ms := mappings.Load(); ms != nil {
		ms.Descend(page, func(_ uintptr, e mapping) bool {
			m, ok = e, page < e.end
			return false
		})
	}
	return m, ok
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

// MarkResident records pages that were just read, so later touches skip their mincore.
func (pr *Probe) MarkResident(pages []uintptr) {
	for _, page := range pages {
		pr.resident[page] = struct{}{}
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

// batchReadSize is the length of one page read: reading any part of a page fills the whole page-cache page, so 4 KiB
// scratch serves kernels with larger pages too.
const batchReadSize = 4096

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
		br.bufs[i] = make([]byte, batchReadSize)
	}
	return br, true
}

// errNoBatchRing reports a pool slot whose ring failed earlier, so the pages stay cold.
var errNoBatchRing = errors.New("mmap: no io_uring ring for this batch")

var abandonedBatch atomic.Int32 // rings this pool abandoned

// replaceBrokenBatchRing abandons a failed ring and gives its slot a new one while no more rings than the pool size
// are abandoned; after that, or when a new ring cannot be made, the slot stops reading.
func replaceBrokenBatchRing(br *batchRing, err error) *batchRing {
	br.r.Abandon(br.bufs...)
	if abandonedBatch.Add(1) <= int32(cap(batchRings)) {
		if nbr, ok := newBatchRing(); ok {
			return nbr
		}
	}
	brokenBatchLog.Do(func() { log.Warn("[mmap] batch read ring failed, slot stops reading", "err", err) })
	return &batchRing{}
}

var brokenBatchLog sync.Once

// ReadPages reads the given mapped pages into the page cache with io_uring, up to 8 rings of 256 reads in flight; a
// page outside the registered file mappings is skipped. It returns the first failed batch's error. Once ctx is done no
// further batch starts.
func ReadPages(ctx context.Context, pages []uintptr) error {
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
				var br *batchRing
				select {
				case br = <-batchRings:
				case <-ctx.Done():
					errOnce.Do(func() { firstErr = ctx.Err() })
					return
				}
				defer func() { batchRings <- br }()
				if err := ctx.Err(); err != nil {
					errOnce.Do(func() { firstErr = err })
					return
				}
				if br.r == nil {
					errOnce.Do(func() { firstErr = errNoBatchRing })
					return
				}
				lens := make([]int, len(chunk))
				for i := range lens {
					lens[i] = batchReadSize
				}
				if err := br.r.BlockingBatchRead(fd, chunk, lens, br.bufs[:len(chunk)]); err != nil {
					errOnce.Do(func() { firstErr = err })
					if !errors.Is(err, iouring.ErrReadFailed) && !errors.Is(err, iouring.ErrRingReset) {
						br = replaceBrokenBatchRing(br, err)
					}
				}
			})
		}
	}
	wg.Wait()
	return firstErr
}
