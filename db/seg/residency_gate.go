// Copyright 2025 The Erigon Authors
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

package seg

import (
	"os"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"golang.org/x/sys/unix"

	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/iouring"
)

var pageSize = os.Getpagesize()

// residencyWindow is how many bytes from the reset offset the gate ensures are
// resident. State reads are scattered, so warming beyond the page holding the
// read is pure read-amplification; the default of one page covers the read
// itself and nothing more. Tunable via env (RESIDENCY_WINDOW_PAGES) without a rebuild.
var residencyWindow = dbg.EnvInt("RESIDENCY_WINDOW_PAGES", 1) * pageSize

// residencyRefresh is how often the cached residency bitmap is rebuilt from a
// fresh mincore scan, correcting bits the OS changed under us (evictions clear,
// readahead sets). Between refreshes the gate never calls mincore on the hot path.
var residencyRefresh = time.Duration(dbg.EnvInt("RESIDENCY_REFRESH_SEC", 120)) * time.Second

// EnableResidencyGate makes this getter consult a cached page-residency bitmap on
// Reset and warm believed-cold pages via io_uring before the value is decompressed
// off the mapping — turning would-be blocking mmap page faults into io_uring reads
// that release the goroutine's P. Intended for random-access readers over state .kv files.
func (g *Getter) EnableResidencyGate() { g.residencyGate = true }

// residencyRegion returns the page-aligned mmap slice covering the word extent
// at offset, along with its file offset. Returns nil when out of range.
func (g *Getter) residencyRegion(offset uint64) (region []byte, fileOffset int64) {
	full := g.d.mmapHandle1
	if len(full) == 0 || len(g.data) == 0 {
		return nil, 0
	}
	base := int(uintptr(unsafe.Pointer(&g.data[0])) - uintptr(unsafe.Pointer(&full[0])))
	absStart := base + int(offset)
	if absStart < 0 || absStart >= len(full) {
		return nil, 0
	}
	aligned := absStart &^ (pageSize - 1)
	end := min(aligned+residencyWindow, len(full))
	return full[aligned:end], int64(aligned)
}

func (g *Getter) ensureResident(offset uint64) {
	if residencyWindow <= 0 { // RESIDENCY_WINDOW_PAGES=0 disables the gate
		return
	}
	region, fileOffset := g.residencyRegion(offset)
	if region == nil {
		return
	}
	rb := g.residencyBitmap()
	first := int(fileOffset) / pageSize
	last := first + (len(region)-1)/pageSize
	if rb.residentRange(first, last) {
		return // believed resident — go straight to the mapping (a stale bit just faults)
	}
	g.warm(fileOffset, len(region))
	rb.markRange(first, last)
}

func (g *Getter) residencyBitmap() *residencyBitmap {
	if rb := g.d.residency.Load(); rb != nil {
		return rb
	}
	g.d.residencyOnce.Do(func() {
		g.d.residency.Store(newResidencyBitmap(g.d.mmapHandle1, int(g.d.f.Fd())))
	})
	return g.d.residency.Load()
}

// warm pulls the byte range into the page cache via io_uring so the following
// mmap access is a minor fault rather than a blocking disk fault; io_uring_enter
// releases the goroutine's P for the duration of the read. There is no fallback —
// if io_uring is unavailable the process exits on the first warm (see iouring.WarmOne).
func (g *Getter) warm(fileOffset int64, n int) {
	iouring.WarmOne(int(g.d.f.Fd()), fileOffset, n)
}

// residencyBitmap caches, one bit per page, whether a page of a mapped .kv file
// is believed present in the page cache. The bit is only a hint: a set bit that
// the OS has since evicted costs one blocking mmap fault; a clear bit for a page
// the OS has since faulted in costs one (fast, cache-hit) io_uring read. It
// removes the per-read mincore syscall from the hot path — a check is a single
// atomic load — and is rebuilt periodically from a real mincore scan.
type residencyBitmap struct {
	mmap   []byte
	fd     int
	nPages int
	words  []uint64
	done   chan struct{}

	mu      sync.Mutex // serializes refresh scans against stop() so no mincore runs after munmap
	stopped bool
}

func newResidencyBitmap(m []byte, fd int) *residencyBitmap {
	nPages := (len(m) + pageSize - 1) / pageSize
	rb := &residencyBitmap{
		mmap:   m,
		fd:     fd,
		nPages: nPages,
		words:  make([]uint64, (nPages+63)/64),
		done:   make(chan struct{}),
	}
	// Seed happens on the refresh goroutine's first tick, not here: a synchronous
	// mincore of a multi-GB file would stall the exec worker that triggered init.
	// Until the seed lands the bitmap reads all-cold, so early reads warm via
	// io_uring (cache hits, cheap) rather than faulting.
	go rb.refreshLoop()
	return rb
}

func (rb *residencyBitmap) residentRange(first, last int) bool {
	for p := first; p <= last; p++ {
		w := atomic.LoadUint64(&rb.words[p>>6])
		if w&(1<<uint(p&63)) == 0 {
			return false
		}
	}
	return true
}

func (rb *residencyBitmap) markRange(first, last int) {
	for p := first; p <= last; p++ {
		atomic.OrUint64(&rb.words[p>>6], 1<<uint(p&63))
	}
}

func (rb *residencyBitmap) refreshLoop() {
	rb.refresh() // seed
	t := time.NewTicker(residencyRefresh)
	defer t.Stop()
	for {
		select {
		case <-rb.done:
			return
		case <-t.C:
			rb.refresh()
		}
	}
}

// refresh rebuilds the whole bitmap from a chunked mincore scan of the mapping.
// It replaces words wholesale; a concurrent markRange that gets clobbered is a
// harmless false-negative (one extra io_uring next time).
func (rb *residencyBitmap) refresh() {
	rb.mu.Lock()
	defer rb.mu.Unlock()
	if rb.stopped {
		return
	}
	const chunkPages = 1 << 18 // 1GB per mincore call (256KB scratch), word-aligned
	vec := make([]byte, chunkPages)
	for p := 0; p < rb.nPages; p += chunkPages {
		n := min(chunkPages, rb.nPages-p)
		lenBytes := min(n*pageSize, len(rb.mmap)-p*pageSize)
		_, _, errno := unix.Syscall(unix.SYS_MINCORE,
			uintptr(unsafe.Pointer(&rb.mmap[p*pageSize])),
			uintptr(lenBytes),
			uintptr(unsafe.Pointer(&vec[0])))
		if errno != 0 {
			return
		}
		for wp := 0; wp < n; wp += 64 {
			var word uint64
			bits := min(64, n-wp)
			for b := 0; b < bits; b++ {
				if vec[wp+b]&1 != 0 {
					word |= 1 << uint(b)
				}
			}
			atomic.StoreUint64(&rb.words[(p+wp)>>6], word)
		}
	}
}

func (rb *residencyBitmap) stop() {
	rb.mu.Lock()
	rb.stopped = true
	rb.mu.Unlock()
	close(rb.done)
}
