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
	"unsafe"

	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/iouring"
	"github.com/erigontech/erigon/common/mmap"
)

var pageSize = os.Getpagesize()

// residencyWindow is how many bytes from the reset offset the gate ensures are
// resident. State reads are scattered, so warming beyond the page holding the
// read is pure read-amplification; the default of one page covers the read
// itself and nothing more. Tunable via env (RESIDENCY_WINDOW_PAGES) without a rebuild.
var residencyWindow = dbg.EnvInt("RESIDENCY_WINDOW_PAGES", 1) * pageSize

// warmConcurrency bounds how many goroutines may be blocked in a warming read
// at once. Acquiring parks the goroutine (freeing its P) when full; the read
// itself goes through Go's syscall path, so a slow read hands the P off too.
var warmSem = make(chan struct{}, 512)

var warmBufPool = sync.Pool{New: func() any {
	b := make([]byte, residencyWindow+pageSize)
	return &b
}}

// EnableResidencyGate makes this getter probe page-cache residency on Reset and
// warm cold pages with a bounded blocking read before the value is decompressed
// off the mapping — turning would-be blocking mmap page faults into read()s that
// release the goroutine's P. Intended for random-access readers over state .kv files.
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
	if resident, err := mmap.Resident(region); err != nil || resident {
		return
	}
	g.warm(fileOffset, len(region))
}

// warm pulls the byte range into the page cache so the following mmap access is
// a minor fault rather than a blocking disk fault. It prefers io_uring (whose
// io_uring_enter releases the goroutine's P during the read); if io_uring is
// unavailable or its ring pool is exhausted it falls back to a semaphore-bounded
// blocking pread. Best-effort: on any error the mmap access simply faults as before.
func (g *Getter) warm(fileOffset int64, n int) {
	if iouring.WarmOne(int(g.d.f.Fd()), fileOffset, n) {
		return
	}
	warmSem <- struct{}{}
	buf := warmBufPool.Get().(*[]byte)
	_, _ = g.d.f.ReadAt((*buf)[:n], fileOffset)
	warmBufPool.Put(buf)
	<-warmSem
}
