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

//go:build linux

package iouring

import (
	"os"
	"runtime"
	"sync"

	"github.com/erigontech/erigon/common/dbg"
)

// Pool of small rings for per-access warming: each goroutine borrows a ring,
// does a single-read submit+wait (which releases the P via io_uring_enter),
// and returns it. Sized to the expected number of concurrent cold readers.

// WarmBufSize is the largest region one WarmOne can pull in. Sized in pages so it
// always covers the residency gate's max window (8 pages) on any page size — the
// gate must cap its window at WarmBufSize so it never marks unwarmed pages resident.
var WarmBufSize = 8 * os.Getpagesize()

// poolSize bounds concurrent warms at ~2*GOMAXPROCS: a blocking fault holds a P,
// and an io_uring read frees it for one more, so beyond that rings sit idle. The
// cap also keeps the per-process io_uring memory footprint small (each instance
// pins kernel memory; hundreds of rings can exhaust it).
func poolSize() int {
	if n := dbg.EnvInt("RESIDENCY_IOURING_RINGS", 0); n > 0 {
		return n
	}
	return min(2*runtime.GOMAXPROCS(0), 64)
}

type pooledRing struct {
	r   *Ring
	buf []byte
	// reusable single-read argument slots, so a warm allocates nothing per call
	offs [1]int64
	lens [1]int
	bufs [1][]byte
}

var (
	// ringPool is nil when io_uring is unavailable — WarmOne then no-ops and reads
	// fall back to ordinary blocking mmap faults. There is no crash: warming is an
	// optimization, and skipping it is always safe.
	ringPool     chan *pooledRing
	ringPoolOnce sync.Once
)

func initPool() {
	n := poolSize()
	pool := make(chan *pooledRing, n)
	for range n {
		r, err := New(8)
		if err != nil { // io_uring unavailable: leave ringPool nil, close any partial pool
			close(pool)
			for pr := range pool {
				pr.r.Close()
			}
			return
		}
		pool <- &pooledRing{r: r, buf: make([]byte, WarmBufSize)}
	}
	ringPool = pool
}

// WarmOne reads [off, off+length) via a pooled io_uring ring to populate the page
// cache. It blocks for a free ring when the pool is drained (parking the goroutine,
// freeing its P). A no-op if io_uring is unavailable — the caller's mmap read then
// takes an ordinary blocking fault.
func WarmOne(fd int, off int64, length int) {
	ringPoolOnce.Do(initPool)
	if ringPool == nil {
		return
	}
	if length > WarmBufSize {
		length = WarmBufSize
	}
	pr := <-ringPool
	pr.offs[0], pr.lens[0], pr.bufs[0] = off, length, pr.buf
	if err := pr.r.BatchReadWarm(fd, pr.offs[:], pr.lens[:], pr.bufs[:]); err != nil {
		pr.r.reset() // resync after a failed enter; the lost warm is a harmless extra fault
	}
	ringPool <- pr
}

// Available reports whether io_uring can be set up in this process (kernel support,
// not blocked by seccomp). Used to warn at startup and to skip io_uring tests.
func Available() bool {
	r, err := New(1)
	if err != nil {
		return false
	}
	r.Close()
	return true
}
