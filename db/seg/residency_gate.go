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
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/iouring"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/common/mmap"
)

var pageSize = os.Getpagesize()

// residencyWindow is how many bytes from the reset offset the gate ensures are
// resident. A compressed word's on-disk extent is at most a page or two, so a
// couple of pages covers effectively every value across all state domains.
// Tunable via env (RESIDENCY_WINDOW_PAGES) to experiment (e.g. =1) without a rebuild.
var residencyWindow = dbg.EnvInt("RESIDENCY_WINDOW_PAGES", 2) * pageSize

// WARM_CONC=true samples how many goroutines are concurrently inside a cold
// warm read — i.e. the effective read concurrency the gate sees.
var warmConcEnabled = dbg.EnvBool("WARM_CONC", false)

var (
	warmInflight atomic.Int64
	warmMaxWin   atomic.Int64
	warmTotal    atomic.Int64
	warmSampOnce sync.Once
)

func warmConcEnter() {
	warmSampOnce.Do(func() {
		go func() {
			for {
				time.Sleep(250 * time.Millisecond)
				log.Info("[WARMCONC] cold .kv warms", "inflight", warmInflight.Load(), "max250ms", warmMaxWin.Swap(0), "total", warmTotal.Load())
			}
		}()
	})
	warmTotal.Add(1)
	n := warmInflight.Add(1)
	for {
		m := warmMaxWin.Load()
		if n <= m || warmMaxWin.CompareAndSwap(m, n) {
			break
		}
	}
}
func warmConcExit() { warmInflight.Add(-1) }

// TRACE_COLD_READS=true appends every cold-read (basename, fileOffset, len) to
// <datadir>/coldtrace.txt so the exact cold-read set of a block can be replayed
// serially vs batched io_uring offline.
var traceColdEnabled = dbg.EnvBool("TRACE_COLD_READS", false)

var (
	traceMu sync.Mutex
	traceF  *os.File
)
var traceColdEnabledOff atomic.Bool

func traceCold(kvPath string, off int64, n int) {
	traceMu.Lock()
	defer traceMu.Unlock()
	if traceF == nil {
		datadir := filepath.Dir(filepath.Dir(filepath.Dir(kvPath)))
		f, err := os.Create(filepath.Join(datadir, "coldtrace.txt"))
		if err != nil {
			traceColdEnabledOff.Store(true)
			return
		}
		traceF = f
	}
	// Direct write (no bufio): survives the container-recreate that ends the run.
	fmt.Fprintf(traceF, "%s %d %d\n", filepath.Base(kvPath), off, n)
}

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
	// N aligned pages from the page containing the read (RESIDENCY_WINDOW_PAGES=1
	// warms exactly that one page — no spill into the next, which for scattered
	// reads would be pure read-amplification).
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

// warm reads the byte range into a scratch buffer to pull it into the page
// cache, so the following mmap access is a minor fault rather than a blocking
// disk fault. Best-effort: on any error the mmap access simply faults as before.
func (g *Getter) warm(fileOffset int64, n int) {
	if warmConcEnabled {
		warmConcEnter()
		defer warmConcExit()
	}
	if traceColdEnabled && !traceColdEnabledOff.Load() {
		traceCold(g.d.filePath, fileOffset, n)
	}
	iouring.WarmOne(int(g.d.f.Fd()), fileOffset, n)
}
